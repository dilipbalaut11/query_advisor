/*-------------------------------------------------------------------------
 *
 * pg_qualstats.c
 *		Track frequently used quals.
 *
 * This extension works by installing a hooks on executor.
 * The ExecutorStart hook will enable some instrumentation for the
 * queries (INSTRUMENT_ROWS and INSTRUMENT_BUFFERS).
 *
 * The ExecutorEnd hook will look for every qual in the query, and
 * stores the quals of the form:
 *		- EXPR OPERATOR CONSTANT
 *		- EXPR OPERATOR EXPR
 *
 * If pg_stat_statements is available, the statistics will be
 * aggregated by queryid, and a not-normalized statement will be
 * stored for each different queryid. This can allow third part tools
 * to do some work on a real query easily.
 *
 * The implementation is heavily inspired by pg_stat_statements
 *
 * Copyright (c) 2014,2017 Ronan Dunklau
 * Copyright (c) 2018-2022, The Powa-Team
 *-------------------------------------------------------------------------
 */
#include <limits.h>
#include <math.h>
#include "c.h"
#include "postgres.h"
#include "access/hash.h"
#include "access/htup_details.h"
#if PG_VERSION_NUM >= 90600
#include "access/parallel.h"
#endif
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
#include "catalog/pg_authid.h"
#endif
#include "catalog/namespace.h"
#if PG_VERSION_NUM >= 110000
#include "catalog/pg_authid_d.h"
#endif
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#if PG_VERSION_NUM >= 150000
#include "postmaster/autovacuum.h"
#endif
#include "postmaster/postmaster.h"
#if PG_VERSION_NUM >= 150000
#include "replication/walsender.h"
#endif
#include "storage/ipc.h"
#include "storage/lwlock.h"
#if PG_VERSION_NUM >= 100000
#include "storage/shmem.h"
#endif
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

#include "include/query_advisor.h"
#include "include/hypopg.h"
#include "include/hypopg_index.h"
#include "include/pg_qualstats.h"

PG_MODULE_MAGIC;

#define PGQS_NAME_COLUMNS 7		/* number of column added when using
								 * pg_qualstats_column SRF */
#define PGQS_USAGE_DEALLOC_PERCENT	5	/* free this % of entries at once */
#define PGQS_MAX_DEFAULT	100000	/* default pgqs_max value */
#define PGQS_MAX_LOCAL_ENTRIES	(pgqs_max_qual * 0.2)	/* do not track more of
													 * 20% of possible entries
													 * in shared mem */

#define PGQS_FLAGS (INSTRUMENT_ROWS|INSTRUMENT_BUFFERS)

#define PGQS_RATIO	0
#define PGQS_NUM	1

#if PG_VERSION_NUM < 140000
#define ParallelLeaderBackendId ParallelMasterBackendId
#endif

/*
 * Extension version number, for supporting older extension versions' objects
 */
typedef enum pgqsVersion
{
	PGQS_V1_0 = 0,
	PGQS_V2_0
} pgqsVersion;

/*---- Function declarations ----*/

extern PGDLLEXPORT void		_PG_init(void);

extern PGDLLEXPORT Datum pg_qualstats_reset(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_status(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_2_0(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_names(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_names_2_0(PG_FUNCTION_ARGS);
static Datum pg_qualstats_common(PG_FUNCTION_ARGS, pgqsVersion api_version,
								 bool include_names);
extern PGDLLEXPORT Datum pg_qualstats_example_queries(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_generate_advise(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_qualstats_reset);
PG_FUNCTION_INFO_V1(pg_qualstats);
PG_FUNCTION_INFO_V1(pg_qualstats_status);
PG_FUNCTION_INFO_V1(pg_qualstats_2_0);
PG_FUNCTION_INFO_V1(pg_qualstats_names);
PG_FUNCTION_INFO_V1(pg_qualstats_names_2_0);
PG_FUNCTION_INFO_V1(pg_qualstats_example_queries);

static void pgqs_backend_mode_startup(void);
#if PG_VERSION_NUM >= 150000
static void pgqs_shmem_request(void);
#endif
static void pgqs_shmem_startup(void);
static void pgqs_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgqs_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
#if PG_VERSION_NUM >= 90600
				 uint64 count
#else
				 long count
#endif
#if PG_VERSION_NUM >= 100000
				 , bool execute_once
#endif
);
static void pgqs_ExecutorFinish(QueryDesc *queryDesc);
static void pgqs_ExecutorEnd(QueryDesc *queryDesc);

static void pgqs_ProcessUtility(PlannedStmt *pstmt,
								const char *queryString,
#if PG_VERSION_NUM >= 140000
								bool readOnlyTree,
#endif
								ProcessUtilityContext context,
								ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest,
#if PG_VERSION_NUM < 130000
								char *completionTag
#else
								QueryCompletion *qc
#endif
								);
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32 pgqs_hash_fn(const void *key, Size keysize);

#if PG_VERSION_NUM < 90500
static uint32 pgqs_uint32_hashfn(const void *key, Size keysize);
#endif

bool pgqs_backend = false;
static int	pgqs_query_size;
static int	pgqs_max_qual = PGQS_MAX_DEFAULT;		/* max # statements to track */
static int	pgqs_max_workload = PGQS_MAX_DEFAULT;	/* max # statements to track */

static bool pgqs_track_pgcatalog = false;	/* track queries on pg_catalog */
static bool pgqs_resolve_oids = false;	/* resolve oids */
static bool pgqs_enabled;
static bool	is_utility = false;
static bool pgqs_track_constants = false;
static double pgqs_sample_rate;
static int	pgqs_min_err_ratio;
static int	pgqs_min_err_num;
static int	query_is_sampled;	/* Is the current query sampled, per backend */
static int	nesting_level = 0;	/* Current nesting depth of ExecutorRun calls */
static bool pgqs_assign_sample_rate_check_hook(double *newval, void **extra, GucSource source);
#if PG_VERSION_NUM > 90600
static void pgqs_set_query_sampled(bool sample);
#endif
static bool pgqs_is_query_sampled(void);

/*
 * Transient state of the query tree walker - for the meaning of the counters,
 * see pgqsEntry comments.
 */
typedef struct pgqsWalkerContext
{
	pgqs_queryid queryId;
	List	   *rtable;
	PlanState  *planstate;
	PlanState  *inner_planstate;
	PlanState  *outer_planstate;
	List	   *outer_tlist;
	List	   *inner_tlist;
	List	   *index_tlist;
	uint32		qualid;
	uint32		uniquequalid;	/* Hash of the parent, including the consts */
	int64		count;
	int64		nbfiltered;
	double		err_estim[2];
	int			nentries;		/* number of entries found so far */
	char		evaltype;
	const char *querytext;
} pgqsWalkerContext;

static bool pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext *query);
static pgqsEntry *pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext *context);
static pgqsEntry *pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext *context);
static pgqsEntry *pgqs_process_booltest(BooleanTest *expr, pgqsWalkerContext *context);
static void pgqs_collectNodeStats(PlanState *planstate, List *ancestors, pgqsWalkerContext *context);
static void pgqs_collectMemberNodeStats(int nplans, PlanState **planstates, List *ancestors, pgqsWalkerContext *context);
static void pgqs_collectSubPlanStats(List *plans, List *ancestors, pgqsWalkerContext *context);
static uint32 hashExpr(Expr *expr, pgqsWalkerContext *context, bool include_const);
static void exprRepr(Expr *expr, StringInfo buffer, pgqsWalkerContext *context, bool include_const);
static void pgqs_set_planstates(PlanState *planstate, pgqsWalkerContext *context);
static Expr *pgqs_resolve_var(Var *var, pgqsWalkerContext *context);


static inline void pgqs_entry_init(pgqsEntry *entry);
static inline void pgqs_entry_copy_raw(pgqsEntry *dest, pgqsEntry *src);
static void pgqs_entry_err_estim(pgqsEntry *e, double *err_estim, int64 occurences);
static void pgqs_localentry_dealloc(int nvictims);
static void pgqs_fillnames(pgqsEntryWithNames *entry);

static Size pgqs_memsize(void);
#if PG_VERSION_NUM >= 90600
static Size pgqs_sampled_array_size(void);
#endif

/* Global Hash */
HTAB *pgqs_hash = NULL;
HTAB *pgqs_workload_hash = NULL;
pgqsSharedState *pgqs = NULL;

/* Local Hash */
static HTAB *pgqs_localhash = NULL;

MemoryContext HypoMemoryContext;


void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(WARNING, "Without shared_preload_libraries, only current backend stats will be available.");
		pgqs_backend = true;
	}
	else
	{
		pgqs_backend = false;
#if PG_VERSION_NUM >= 150000
		prev_shmem_request_hook = shmem_request_hook;
		shmem_request_hook = pgqs_shmem_request;
#endif
		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = pgqs_shmem_startup;
	}

	/* Inform the postmaster that we want to enable query_id calculation */
#if PG_VERSION_NUM >= 150000
	EnableQueryId();
#endif

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgqs_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgqs_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgqs_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgqs_ExecutorEnd;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pgqs_ProcessUtility;
	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = hypo_get_relation_info_hook;
	prev_explain_get_index_name_hook = explain_get_index_name_hook;
	explain_get_index_name_hook = hypo_explain_get_index_name_hook;

	DefineCustomBoolVariable("query_advisor.enabled",
							 "Enable / Disable pg_qualstats",
							 NULL,
							 &pgqs_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("query_advisor.max_qual_entries",
							"Sets the maximum number of qual stats tracked by query_advisor.",
							NULL,
							&pgqs_max_qual,
							PGQS_MAX_DEFAULT,
							100,
							INT_MAX,
							pgqs_backend ? PGC_USERSET : PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

		DefineCustomIntVariable("query_advisor.max_workload_entries",
							"Sets the maximum number of statements workload queries tracked by query_advisor.",
							NULL,
							&pgqs_max_workload,
							PGQS_MAX_DEFAULT,
							100,
							INT_MAX,
							pgqs_backend ? PGC_USERSET : PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomRealVariable("query_advisor.sample_rate",
							 "Sampling rate. 1 means every query, 0.2 means 1 in five queries",
							 NULL,
							 &pgqs_sample_rate,
							 -1,
							 -1,
							 1,
							 PGC_USERSET,
							 0,
							 pgqs_assign_sample_rate_check_hook,
							 NULL,
							 NULL);

	DefineCustomIntVariable("query_advisor.min_err_estimate_ratio",
							"Error estimation ratio threshold to save quals",
							NULL,
							&pgqs_min_err_ratio,
							0,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("query_advisor.min_err_estimate_num",
							"Error estimation num threshold to save quals",
							NULL,
							&pgqs_min_err_num,
							0,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	EmitWarningsOnPlaceholders("query_advisor");

	parse_int(GetConfigOption("track_activity_query_size", false, false),
			  &pgqs_query_size, 0, NULL);

	if (!pgqs_backend)
	{
#if PG_VERSION_NUM < 150000
		RequestAddinShmemSpace(pgqs_memsize());
#if PG_VERSION_NUM >= 90600
		RequestNamedLWLockTranche("pg_qualstats", 3);
#else
		RequestAddinLWLocks(2);
#endif		/* pg9.6+ */
#endif		/* pg15- */
	}
	else
		pgqs_backend_mode_startup();

	HypoMemoryContext = AllocSetContextCreate(TopMemoryContext,
											  "HypoPG context",
											  ALLOCSET_DEFAULT_SIZES);
}


/*
 * Check that the sample ratio is in the correct interval
 */
static bool
pgqs_assign_sample_rate_check_hook(double *newval, void **extra, GucSource source)
{
	double		val = *newval;

	if ((val < 0 && val != -1) || (val > 1))
		return false;
	if (val == -1)
		*newval = 1. / MaxConnections;
	return true;
}

#if PG_VERSION_NUM >= 90600
static void
pgqs_set_query_sampled(bool sample)
{
	/* the decisions should only be made in leader */
	Assert(!IsParallelWorker());

	/* not supported in backend mode */
	if (pgqs_backend)
		return;

	/* in worker processes we need to get the info from shared memory */
	LWLockAcquire(pgqs->sampledlock, LW_EXCLUSIVE);
	pgqs->sampled[MyBackendId] = sample;
	LWLockRelease(pgqs->sampledlock);
}
#endif

static bool
pgqs_is_query_sampled(void)
{
#if PG_VERSION_NUM >= 90600
	bool		sampled;

	/* in leader we can just check the global variable */
	if (!IsParallelWorker())
		return query_is_sampled;

	/* not supported in backend mode */
	if (pgqs_backend)
		return false;

	/* in worker processes we need to get the info from shared memory */
	PGQS_LWL_ACQUIRE(pgqs->sampledlock, LW_SHARED);
	sampled = pgqs->sampled[ParallelLeaderBackendId];
	PGQS_LWL_RELEASE(pgqs->sampledlock);

	return sampled;
#else
	return query_is_sampled;
#endif
}

/*
 * Do catalog search to replace oids with corresponding objects name
 */
void
pgqs_fillnames(pgqsEntryWithNames *entry)
{
#if PG_VERSION_NUM >= 110000
#define GET_ATTNAME(r, a)	get_attname(r, a, false)
#else
#define GET_ATTNAME(r, a)	get_attname(r, a)
#endif

#if PG_VERSION_NUM >= 90500
	namestrcpy(&(entry->names.rolname), GetUserNameFromId(entry->entry.key.userid, true));
#else
	namestrcpy(&(entry->names.rolname), GetUserNameFromId(entry->entry.key.userid));
#endif
	namestrcpy(&(entry->names.datname), get_database_name(entry->entry.key.dbid));

	if (entry->entry.lrelid != InvalidOid)
	{
		namestrcpy(&(entry->names.lrelname),
				   get_rel_name(entry->entry.lrelid));
		namestrcpy(&(entry->names.lattname),
				   GET_ATTNAME(entry->entry.lrelid, entry->entry.lattnum));
	}

	if (entry->entry.opoid != InvalidOid)
		namestrcpy(&(entry->names.opname), get_opname(entry->entry.opoid));

	if (entry->entry.rrelid != InvalidOid)
	{
		namestrcpy(&(entry->names.rrelname),
				   get_rel_name(entry->entry.rrelid));
		namestrcpy(&(entry->names.rattname),
				   GET_ATTNAME(entry->entry.rrelid, entry->entry.rattnum));
	}
#undef GET_ATTNAME
}

/*
 * Request rows and buffers instrumentation if pgqs is enabled
 */
static void
pgqs_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	/* Setup instrumentation */
	if (pgqs_enabled  && strlen(queryDesc->sourceText) < pgqs_query_size &&
		!qa_disable_stats && !is_utility)
	{
		/*
		 * For rate sampling, randomly choose top-level statement. Either all
		 * nested statements will be explained or none will.
		 */
		if (nesting_level == 0
#if PG_VERSION_NUM >= 90600
			&& (!IsParallelWorker())
#endif
			)
		{
#if PG_VERSION_NUM >= 150000
			query_is_sampled = (pg_prng_double(&pg_global_prng_state) <
									pgqs_sample_rate);
#else
			query_is_sampled = (random() <= (MAX_RANDOM_VALUE *
											 pgqs_sample_rate));
#endif
#if PG_VERSION_NUM >= 90600
			pgqs_set_query_sampled(query_is_sampled);
#endif
		}

		if (pgqs_is_query_sampled())
			queryDesc->instrument_options |= PGQS_FLAGS;
	}
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
pgqs_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
#if PG_VERSION_NUM >= 90600
				 uint64 count
#else
				 long count
#endif
#if PG_VERSION_NUM >= 100000
				 ,bool execute_once
#endif
)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
#if PG_VERSION_NUM >= 100000
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			prev_ExecutorRun(queryDesc, direction, count);
#endif
		else
#if PG_VERSION_NUM >= 100000
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			standard_ExecutorRun(queryDesc, direction, count);
#endif
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pgqs_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Given an arbitrarily long query string, produce a hash for the purposes of
 * identifying the query, without normalizing constants.  Used when hashing
 * utility statements.
 */
static uint64
pgss_hash_string(const char *str, int len)
{
	return DatumGetUInt64(hash_any_extended((const unsigned char *) str,
											len, 0));
}

/*
 * Save a non normalized query for the queryid if no one already exists, and
 * do all the stat collecting job
 */
static void
pgqs_ExecutorEnd(QueryDesc *queryDesc)
{
	pgqsWorkloadHashKey queryKey;
	bool		found;

	if ((pgqs || pgqs_backend) && pgqs_enabled && pgqs_is_query_sampled() &&
		!qa_disable_stats
#if PG_VERSION_NUM >= 90600
		&& (!IsParallelWorker())
#endif

	/*
	 * multiple ExecutorStart/ExecutorEnd can be interleaved, so when sampling
	 * is activated there's no guarantee that pgqs_is_query_sampled() will
	 * only detect queries that were actually sampled (thus having the
	 * required instrumentation set up).  To avoid such cases, we double check
	 * that we have the required instrumentation set up.  That won't exactly
	 * detect the sampled queries, but that should be close enough and avoid
	 * adding to much complexity.
	 */
		&& (queryDesc->instrument_options & PGQS_FLAGS) == PGQS_FLAGS
		)
	{
		HASHCTL		info;
		pgqsEntry  *localentry;
		HASH_SEQ_STATUS local_hash_seq;
		pgqsWalkerContext *context = palloc(sizeof(pgqsWalkerContext));
		pgqsWorkloadEntry *queryEntry;
		int			len = strlen(queryDesc->sourceText) + 1;

		context->queryId = queryDesc->plannedstmt->queryId;

		if (context->queryId == 0)
			context->queryId = pgss_hash_string(queryDesc->sourceText, len);

		context->rtable = queryDesc->plannedstmt->rtable;
		context->count = 0;
		context->qualid = 0;
		context->uniquequalid = 0;
		context->nbfiltered = 0;
		context->evaltype = 0;
		context->nentries = 0;
		context->querytext = queryDesc->sourceText;
		queryKey.queryid = context->queryId;

		/* create local hash table if it hasn't been created yet */
		if (!pgqs_localhash)
		{
			memset(&info, 0, sizeof(info));
			info.keysize = sizeof(pgqsHashKey);

			if (pgqs_resolve_oids)
				info.entrysize = sizeof(pgqsEntryWithNames);
			else
				info.entrysize = sizeof(pgqsEntry);

			info.hash = pgqs_hash_fn;

			pgqs_localhash = hash_create("pgqs_localhash",
										 50,
										 &info,
										 HASH_ELEM | HASH_FUNCTION);
		}

		/* retrieve quals informations, main work starts from here */
		pgqs_collectNodeStats(queryDesc->planstate, NIL, context);

		/* if any quals found, store them in shared memory */
		if (context->nentries)
		{
			/*
			 * Before acquiring exlusive lwlock, check if there's enough room
			 * to store local hash.  Also, do not remove more than 20% of
			 * maximum number of entries in shared memory (wether they are
			 * used or not). This should not happen since we shouldn't store
			 * that much entries in localhash in the first place.
			 */
			int			nvictims = hash_get_num_entries(pgqs_localhash) -
				PGQS_MAX_LOCAL_ENTRIES;

			if (nvictims > 0)
				pgqs_localentry_dealloc(nvictims);

			PGQS_LWL_ACQUIRE(pgqs->lock, LW_EXCLUSIVE);

			if (hash_get_num_entries(pgqs_hash) +
				hash_get_num_entries(pgqs_localhash) >= pgqs_max_qual)
			{
				ereport(WARNING,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("qual hash table is full so current quals are skipped"),
						 errhint("reset the stats or increase value of pgqs_max_qual and restart")));
				PGQS_LWL_RELEASE(pgqs->lock);

				goto cleanup;
			}

			hash_seq_init(&local_hash_seq, pgqs_localhash);
			while ((localentry = hash_seq_search(&local_hash_seq)) != NULL)
			{
				pgqsEntry *newEntry = (pgqsEntry *) hash_search(pgqs_hash,
													 &localentry->key,
													 HASH_ENTER, &found);

				if (!found)
				{
					/* raw copy the local entry */
					pgqs_entry_copy_raw(newEntry, localentry);
				}
				else
				{
					/* only update counters value */
					newEntry->count += localentry->count;
					newEntry->nbfiltered += localentry->nbfiltered;
					newEntry->usage += localentry->usage;
					/* compute estimation error min, max, mean and variance */
					pgqs_entry_err_estim(newEntry, localentry->mean_err_estim,
										 localentry->occurences);
				}
				/* cleanup local hash */
				hash_search(pgqs_localhash, &localentry->key, HASH_REMOVE, NULL);
			}

			PGQS_LWL_RELEASE(pgqs->lock);

			/* Lookup the hash table entry with a shared lock. */
			PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);

			queryEntry = (pgqsWorkloadEntry *) hash_search_with_hash_value(pgqs_workload_hash, &queryKey,
																		   context->queryId,
																		   HASH_FIND, &found);

			/* Create the new entry if not present */
			if (!found)
			{
				bool		excl_found;

				/* Need exclusive lock to add a new hashtable entry - promote */
				PGQS_LWL_RELEASE(pgqs->querylock);
				PGQS_LWL_ACQUIRE(pgqs->querylock, LW_EXCLUSIVE);

				if (hash_get_num_entries(pgqs_workload_hash) >= pgqs_max_workload)
				{
					ereport(WARNING,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("workload hash table is full so current query is skipped"),
							errhint("reset the stats or increase value of pgqs_max_workload and restart")));
					PGQS_LWL_RELEASE(pgqs->querylock);

					goto cleanup;
				}

				queryEntry = (pgqsWorkloadEntry *) hash_search_with_hash_value(pgqs_workload_hash, &queryKey,
																			   context->queryId,
																			   HASH_ENTER, &excl_found);

				/* Make sure it wasn't added by another backend */
				if (!excl_found)
				{
					int			num_oids = 0;
					ListCell   *lc;
					List	   *search_path_oids = fetch_search_path(false);

					if (list_length(search_path_oids) > MAX_CACHED_PATH_LEN)
						goto cleanup;

					foreach(lc, search_path_oids)
						queryEntry->search_path[num_oids++] = lfirst_oid(lc);

					list_free(search_path_oids);

					queryEntry->num_oids = num_oids;

					queryEntry->qrylen = len;
					strcpy(queryEntry->querytext, context->querytext);
					queryEntry->frequency = 1;
				}
				else
					queryEntry->frequency++;
			}
			else
				queryEntry->frequency++;

			PGQS_LWL_RELEASE(pgqs->querylock);
		}
	}

cleanup:
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * ProcessUtility hook
 */
static void
pgqs_ProcessUtility(PlannedStmt *pstmt,
					const char *queryString,
#if PG_VERSION_NUM >= 140000
					bool readOnlyTree,
#endif
					ProcessUtilityContext context,
					ParamListInfo params,
					QueryEnvironment *queryEnv,
					DestReceiver *dest,
#if PG_VERSION_NUM < 130000
					char *completionTag
#else
					QueryCompletion *qc
#endif
					)
{
	PG_TRY();
	{
		is_utility = true;

		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
								readOnlyTree,
#endif
								context, params,
								queryEnv,
								dest,
#if PG_VERSION_NUM < 130000
									completionTag
#else
									qc
#endif
								);
		else
			standard_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
									readOnlyTree,
#endif
									context, params,
									queryEnv,
									dest,
#if PG_VERSION_NUM < 130000
									completionTag
#else
									qc
#endif
						  );
		is_utility = false;
	}
	PG_CATCH();
	{
		is_utility = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/* Initialize all non-key fields of the given entry. */
static inline void
pgqs_entry_init(pgqsEntry *entry)
{
	/* Note that pgqsNames if needed will be explicitly filled after this */
	memset(&(entry->lrelid), 0, sizeof(pgqsEntry) - sizeof(pgqsHashKey));
}

/* Copy non-key and non-name fields from the given entry */
static inline void
pgqs_entry_copy_raw(pgqsEntry *dest, pgqsEntry *src)
{
	/* Note that pgqsNames if needed will be explicitly filled after this */
	memcpy(&(dest->lrelid),
		   &(src->lrelid),
		   (sizeof(pgqsEntry) - sizeof(pgqsHashKey)));
}

/*
 * Accurately compute estimation error ratio and num variance using Welford's
 * method. See <http://www.johndcook.com/blog/standard_deviation/>
 * Also maintain min and max values.
 */
static void
pgqs_entry_err_estim(pgqsEntry *e, double *err_estim, int64 occurences)
{
	int		i;

	e->occurences += occurences;

	for (i = 0; i < 2; i++)
	{
		if ((e->occurences - occurences) == 0)
		{
			e->min_err_estim[i] = err_estim[i];
			e->max_err_estim[i] = err_estim[i];
			e->mean_err_estim[i] = err_estim[i];
		}
		else
		{
			double		old_err = e->mean_err_estim[i];

			e->mean_err_estim[i] +=
				(err_estim[i] - old_err) / e->occurences;
			e->sum_err_estim[i] +=
				(err_estim[i] - old_err) * (err_estim[i] - e->mean_err_estim[i]);
		}

		/* calculate min/max counters */
		if (e->min_err_estim[i] > err_estim[i])
			e->min_err_estim[i] = err_estim[i];
		if (e->max_err_estim[i] < err_estim[i])
			e->max_err_estim[i] = err_estim[i];
	}
}

/*
 * Remove the requested number of entries from pgqs_localhash.  Since the
 * entries are all coming from the same query, remove them without any specific
 * sort.
 */
static void
pgqs_localentry_dealloc(int nvictims)
{
	pgqsEntry  *localentry;
	HASH_SEQ_STATUS local_hash_seq;
	pgqsHashKey **victims;
	bool		need_seq_term = true;
	int			i,
				ptr = 0;

	if (nvictims <= 0)
		return;

	victims = palloc(sizeof(pgqsHashKey *) * nvictims);

	hash_seq_init(&local_hash_seq, pgqs_localhash);
	while (nvictims-- >= 0)
	{
		localentry = hash_seq_search(&local_hash_seq);

		/* check if caller required too many victims */
		if (!localentry)
		{
			need_seq_term = false;
			break;
		}

		victims[ptr++] = &localentry->key;
	}

	if (need_seq_term)
		hash_seq_term(&local_hash_seq);

	for (i = 0; i < ptr; i++)
		hash_search(pgqs_localhash, victims[i], HASH_REMOVE, NULL);

	pfree(victims);
}

static void
pgqs_collectNodeStats(PlanState *planstate, List *ancestors, pgqsWalkerContext *context)
{
	Plan	   *plan = planstate->plan;
	Instrumentation *instrument = planstate->instrument;
	int64		oldcount = context->count;
	double		oldfiltered = context->nbfiltered;
	double		old_err_ratio = context->err_estim[PGQS_RATIO];
	double		old_err_num = context->err_estim[PGQS_NUM];
	double		total_filtered = 0;
	ListCell   *lc;
	List	   *parent = 0;
	List	   *indexquals = 0;
	List	   *quals = 0;

	context->planstate = planstate;

	/*
	 * We have to forcibly clean up the instrumentation state because we
	 * haven't done ExecutorEnd yet.  This is pretty grotty ...
	 */
	if (instrument)
		InstrEndLoop(instrument);

	/* Retrieve the generic quals and indexquals */
	switch (nodeTag(plan))
	{
		case T_IndexOnlyScan:
			indexquals = ((IndexOnlyScan *) plan)->indexqual;
			quals = plan->qual;
			break;
		case T_IndexScan:
			indexquals = ((IndexScan *) plan)->indexqualorig;
			quals = plan->qual;
			break;
		case T_BitmapIndexScan:
			indexquals = ((BitmapIndexScan *) plan)->indexqualorig;
			quals = plan->qual;
			break;
		case T_CteScan:
		case T_SeqScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_ModifyTable:
			quals = plan->qual;
			break;
		case T_NestLoop:
			quals = ((NestLoop *) plan)->join.joinqual;
			break;
		case T_MergeJoin:
			quals = ((MergeJoin *) plan)->mergeclauses;
			break;
		case T_HashJoin:
			quals = ((HashJoin *) plan)->hashclauses;
			break;
		default:
			break;

	}

	pgqs_set_planstates(planstate, context);
	parent = list_union(indexquals, quals);
	if (list_length(parent) > 1)
	{
		context->uniquequalid = hashExpr((Expr *) parent, context, true);
		context->qualid = hashExpr((Expr *) parent, context, false);
	}

	total_filtered = instrument->nfiltered1 + instrument->nfiltered2;
	context->nbfiltered = total_filtered;
	context->count = instrument->tuplecount + instrument->ntuples + total_filtered;

	if (plan->plan_rows == instrument->ntuples)
	{
		context->err_estim[PGQS_RATIO] = 0;
		context->err_estim[PGQS_NUM] = 0;
	}
	else if (plan->plan_rows > instrument->ntuples)
	{
		/* XXX should use use a bigger value? */
		if (instrument->ntuples == 0)
			context->err_estim[PGQS_RATIO] = plan->plan_rows * 1.0L;
		else
			context->err_estim[PGQS_RATIO] = plan->plan_rows * 1.0L / instrument->ntuples;
		context->err_estim[PGQS_NUM] = plan->plan_rows - instrument->ntuples;
	}
	else
	{
		/* plan_rows cannot be zero */
		context->err_estim[PGQS_RATIO] = instrument->ntuples * 1.0L / plan->plan_rows;
		context->err_estim[PGQS_NUM] = instrument->ntuples - plan->plan_rows;
	}

	if (context->err_estim[PGQS_RATIO] >= pgqs_min_err_ratio &&
		context->err_estim[PGQS_NUM] >= pgqs_min_err_num)
	{
		/* Add the indexquals */
		context->evaltype = 'i';
		expression_tree_walker((Node *) indexquals,
							   pgqs_whereclause_tree_walker, context);

		/* Add the generic quals */
		context->evaltype = 'f';
		expression_tree_walker((Node *) quals, pgqs_whereclause_tree_walker,
							   context);
	}

	context->qualid = 0;
	context->uniquequalid = 0;
	context->count = oldcount;
	context->nbfiltered = oldfiltered;
	context->err_estim[PGQS_RATIO] = old_err_ratio;
	context->err_estim[PGQS_NUM] = old_err_num;

	foreach(lc, planstate->initPlan)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lc);

		pgqs_collectNodeStats(sps->planstate, ancestors, context);
	}

	/* lefttree */
	if (outerPlanState(planstate))
		pgqs_collectNodeStats(outerPlanState(planstate), ancestors, context);

	/* righttree */
	if (innerPlanState(planstate))
		pgqs_collectNodeStats(innerPlanState(planstate), ancestors, context);

	/* special child plans */
	switch (nodeTag(plan))
	{
#if PG_VERSION_NUM < 140000
		case T_ModifyTable:
			pgqs_collectMemberNodeStats(((ModifyTableState *) planstate)->mt_nplans,
										((ModifyTableState *) planstate)->mt_plans,
										ancestors, context);
			break;
#endif
		case T_Append:
			pgqs_collectMemberNodeStats(((AppendState *) planstate)->as_nplans,
										((AppendState *) planstate)->appendplans,
										ancestors, context);
			break;
		case T_MergeAppend:
			pgqs_collectMemberNodeStats(((MergeAppendState *) planstate)->ms_nplans,
										((MergeAppendState *) planstate)->mergeplans,
										ancestors, context);
			break;
		case T_BitmapAnd:
			pgqs_collectMemberNodeStats(((BitmapAndState *) planstate)->nplans,
										((BitmapAndState *) planstate)->bitmapplans,
										ancestors, context);
			break;
		case T_BitmapOr:
			pgqs_collectMemberNodeStats(((BitmapOrState *) planstate)->nplans,
										((BitmapOrState *) planstate)->bitmapplans,
										ancestors, context);
			break;
		case T_SubqueryScan:
			pgqs_collectNodeStats(((SubqueryScanState *) planstate)->subplan, ancestors, context);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		pgqs_collectSubPlanStats(planstate->subPlan, ancestors, context);
}

static void
pgqs_collectMemberNodeStats(int nplans, PlanState **planstates,
							List *ancestors, pgqsWalkerContext *context)
{
	int			i;

	for (i = 0; i < nplans; i++)
		pgqs_collectNodeStats(planstates[i], ancestors, context);
}

static void
pgqs_collectSubPlanStats(List *plans, List *ancestors, pgqsWalkerContext *context)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

		pgqs_collectNodeStats(sps->planstate, ancestors, context);
	}
}

static pgqsEntry *
pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext *context)
{
	OpExpr	   *op = makeNode(OpExpr);
	int			len = 0;
	pgqsEntry  *entry = NULL;
	Expr	   *array = lsecond(expr->args);

	op->opno = expr->opno;
	op->opfuncid = expr->opfuncid;
	op->inputcollid = expr->inputcollid;
	op->opresulttype = BOOLOID;
	op->args = expr->args;
	switch (array->type)
	{
		case T_ArrayExpr:
			len = list_length(((ArrayExpr *) array)->elements);
			break;
		case T_Const:
			/* Const is an array. */
			{
				Const	   *arrayconst = (Const *) array;
				ArrayType  *array_type;

				if (arrayconst->constisnull)
					return NULL;

				array_type = DatumGetArrayTypeP(arrayconst->constvalue);

				if (ARR_NDIM(array_type) > 0)
					len = ARR_DIMS(array_type)[0];
			}
			break;
		default:
			break;
	}

	if (len > 0)
	{
		context->count *= len;
		entry = pgqs_process_opexpr(op, context);
	}

	return entry;
}

static pgqsEntry *
pgqs_process_booltest(BooleanTest *expr, pgqsWalkerContext *context)
{
	pgqsHashKey key;
	pgqsEntry  *entry;
	bool		found;
	Var		   *var;
	Expr	   *newexpr = NULL;
	Oid			opoid;
	RangeTblEntry *rte;

	/* do not store more than 20% of possible entries in shared mem */
	if (context->nentries >= PGQS_MAX_LOCAL_ENTRIES)
		return NULL;

	if (IsA(expr->arg, Var))
		newexpr = pgqs_resolve_var((Var *) expr->arg, context);

	if (!(newexpr && IsA(newexpr, Var)))
		return NULL;

	var = (Var *) newexpr;
	rte = list_nth(context->rtable, var->varno - 1);
	switch (expr->booltesttype)
	{
		case IS_TRUE:
			opoid = BooleanEqualOperator;
			break;
		case IS_FALSE:
			opoid = BooleanEqualOperator;
			break;
		case IS_NOT_TRUE:
			opoid = BooleanNotEqualOperator;
			break;
		case IS_NOT_FALSE:
			opoid = BooleanNotEqualOperator;
			break;
		case IS_UNKNOWN:
			opoid = BooleanEqualOperator;
			break;
		case IS_NOT_UNKNOWN:
			opoid = BooleanNotEqualOperator;
			break;
		default:
			/* Bail out */
			return NULL;
	}
	memset(&key, 0, sizeof(pgqsHashKey));
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.uniquequalid = context->uniquequalid;
	key.uniquequalnodeid = hashExpr((Expr *) expr, context, pgqs_track_constants);
	key.queryid = context->queryId;
	key.evaltype = context->evaltype;

	/* local hash, no lock needed */
	entry = (pgqsEntry *) hash_search(pgqs_localhash, &key, HASH_ENTER, &found);
	if (!found)
	{
		context->nentries++;

		pgqs_entry_init(entry);
		entry->qualnodeid = hashExpr((Expr *) expr, context, false);
		entry->qualid = context->qualid;
		entry->opoid = opoid;

		if (rte->rtekind == RTE_RELATION)
		{
			entry->lrelid = rte->relid;
			entry->lattnum = var->varattno;
		}

		if (pgqs_resolve_oids)
			pgqs_fillnames((pgqsEntryWithNames *) entry);
	}

	entry->nbfiltered += context->nbfiltered;
	entry->count += context->count;
	entry->usage += 1;
	/* compute estimation error min, max, mean and variance */
	pgqs_entry_err_estim(entry, context->err_estim, 1);

	return entry;
}

static void
get_const_expr(Const *constval, StringInfo buf)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (constval->constisnull)
	{
		/*
		 * Always label the type of a NULL constant to prevent misdecisions
		 * about type when reparsing.
		 */
		appendStringInfoString(buf, "NULL");
		appendStringInfo(buf, "::%s",
						 format_type_with_typemod(constval->consttype,
												  constval->consttypmod));
		return;
	}

	getTypeOutputInfo(constval->consttype, &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, constval->constvalue);

	switch (constval->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			{
				/*
				 * These types are printed without quotes unless they contain
				 * values that aren't accepted by the scanner unquoted (e.g.,
				 * 'NaN').  Note that strtod() and friends might accept NaN,
				 * so we can't use that to test.
				 *
				 * In reality we only need to defend against infinity and NaN,
				 * so we need not get too crazy about pattern matching here.
				 *
				 * There is a special-case gotcha: if the constant is signed,
				 * we need to parenthesize it, else the parser might see a
				 * leading plus/minus as binding less tightly than adjacent
				 * operators --- particularly, the cast that we might attach
				 * below.
				 */
				if (strspn(extval, "0123456789+-eE.") == strlen(extval))
				{
					if (extval[0] == '+' || extval[0] == '-')
						appendStringInfo(buf, "(%s)", extval);
					else
						appendStringInfoString(buf, extval);
				}
				else
					appendStringInfo(buf, "'%s'", extval);
			}
			break;

		case BITOID:
		case VARBITOID:
			appendStringInfo(buf, "B'%s'", extval);
			break;

		case BOOLOID:
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "true");
			else
				appendStringInfoString(buf, "false");
			break;

		default:
			appendStringInfoString(buf, quote_literal_cstr(extval));
			break;
	}

	pfree(extval);

	/*
	 * For showtype == 0, append ::typename unless the constant will be
	 * implicitly typed as the right type when it is read in.
	 */
	appendStringInfo(buf, "::%s",
					 format_type_with_typemod(constval->consttype,
											  constval->consttypmod));

}

/*-----------
 * In order to avoid duplicated entries for sementically equivalent OpExpr,
 * this function returns a canonical version of the given OpExpr.
 *
 * For now, the only modification is for OpExpr with a Var and a Const, we
 * prefer the form:
 * Var operator Const
 * with the Var on the LHS.  If the expression in the opposite form and the
 * operator has a commutator, we'll commute it, otherwise fallback to the
 * original OpExpr with the Var on the RHS.
 * OpExpr of the form Var operator Var can still be redundant.
 */
static OpExpr *
pgqs_get_canonical_opexpr(OpExpr *expr, bool *commuted)
{
	if (commuted)
		*commuted = false;

	/* Only OpExpr with 2 arguments needs special processing. */
	if (list_length(expr->args) != 2)
		return expr;

	/* If the 1st argument is a Var, nothing is done */
	if (IsA(linitial(expr->args), Var))
		return expr;

	/* If the 2nd argument is a Var, commute the OpExpr if possible */
	if (IsA(lsecond(expr->args), Var) && OidIsValid(get_commutator(expr->opno)))
	{
		OpExpr	   *newexpr = copyObject(expr);

		CommuteOpExpr(newexpr);

		if (commuted)
			*commuted = true;

		return newexpr;
	}

	return expr;
}

static pgqsEntry *
pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext *context)
{
	/* do not store more than 20% of possible entries in shared mem */
	if (context->nentries >= PGQS_MAX_LOCAL_ENTRIES)
		return NULL;

	if (list_length(expr->args) == 2)
	{
		bool		save_qual;
		Node	   *node;
		Var		   *var;
		Const	   *constant;
		Oid		   *sreliddest;
		AttrNumber *sattnumdest;
		pgqsEntry	tempentry;
		int			step;

		pgqs_entry_init(&tempentry);
		tempentry.opoid = expr->opno;

		save_qual = false;
		var = NULL;				/* will store the last Var found, if any */
		constant = NULL;		/* will store the last Constant found, if any */

		/* setup the node and LHS destination fields for the 1st argument */
		node = linitial(expr->args);
		sreliddest = &(tempentry.lrelid);
		sattnumdest = &(tempentry.lattnum);

		for (step = 0; step < 2; step++)
		{
			if (IsA(node, RelabelType))
				node = (Node *) ((RelabelType *) node)->arg;

			if (IsA(node, Var))
				node = (Node *) pgqs_resolve_var((Var *) node, context);

			switch (node->type)
			{
				case T_Var:
					var = (Var *) node;
					{
						RangeTblEntry *rte;

						rte = list_nth(context->rtable, var->varno - 1);
						if (rte->rtekind == RTE_RELATION)
						{
							save_qual = true;
							*sreliddest = rte->relid;
							*sattnumdest = var->varattno;
						}
						else
							var = NULL;
					}
					break;
				case T_Const:
					constant = (Const *) node;
					break;
				default:
					break;
			}

			/* find the node to process for the 2nd pass */
			if (step == 0)
			{
				node = NULL;

				if (var == NULL)
				{
					bool		commuted;
					OpExpr	   *newexpr = pgqs_get_canonical_opexpr(expr, &commuted);

					/*
					 * If the OpExpr was commuted we have to use the 1st
					 * argument of the new OpExpr, and keep using the LHS as
					 * destination fields.
					 */
					if (commuted)
					{
						Assert(sreliddest == &(tempentry.lrelid));
						Assert(sattnumdest == &(tempentry.lattnum));

						node = linitial(newexpr->args);
					}
				}

				/*
				 * If the 1st argument was a var, or if it wasn't and the
				 * operator couldn't be commuted, use the 2nd argument and the
				 * RHS as destination fields.
				 */
				if (node == NULL)
				{
					/* simply process the next argument */
					node = lsecond(expr->args);

					/*
					 * a Var was found and stored on the LHS, so if the next
					 * node  will be stored on the RHS
					 */
					sreliddest = &(tempentry.rrelid);
					sattnumdest = &(tempentry.rattnum);
				}
			}
		}

		if (save_qual)
		{
			pgqsHashKey key;
			pgqsEntry  *entry;
			StringInfo	buf = makeStringInfo();
			bool		found;
			int			position = -1;

			/*
			 * If we don't track rels in the pg_catalog schema, lookup the
			 * schema to make sure its not pg_catalog. Otherwise, bail out.
			 */
			if (!pgqs_track_pgcatalog)
			{
				Oid			nsp;

				if (tempentry.lrelid != InvalidOid)
				{
					nsp = get_rel_namespace(tempentry.lrelid);

					Assert(OidIsValid(nsp));

					if (nsp == PG_CATALOG_NAMESPACE)
						return NULL;
				}

				if (tempentry.rrelid != InvalidOid)
				{
					nsp = get_rel_namespace(tempentry.rrelid);

					Assert(OidIsValid(nsp));

					if (nsp == PG_CATALOG_NAMESPACE)
						return NULL;
				}
			}

			if (constant != NULL && pgqs_track_constants)
			{
				get_const_expr(constant, buf);
				position = constant->location;
			}

			memset(&key, 0, sizeof(pgqsHashKey));
			key.userid = GetUserId();
			key.dbid = MyDatabaseId;
			key.uniquequalid = context->uniquequalid;
			key.uniquequalnodeid = hashExpr((Expr *) expr, context, pgqs_track_constants);
			key.queryid = context->queryId;
			key.evaltype = context->evaltype;

			/* local hash, no lock needed */
			entry = (pgqsEntry *) hash_search(pgqs_localhash, &key, HASH_ENTER,
											  &found);
			if (!found)
			{
				context->nentries++;

				/* raw copy the temporary entry */
				pgqs_entry_copy_raw(entry, &tempentry);
				entry->position = position;
				entry->qualnodeid = hashExpr((Expr *) expr, context, false);
				entry->qualid = context->qualid;

				if (pgqs_resolve_oids)
					pgqs_fillnames((pgqsEntryWithNames *) entry);
			}

			entry->nbfiltered += context->nbfiltered;
			entry->count += context->count;
			entry->usage += 1;
			/* compute estimation error min, max, mean and variance */
			pgqs_entry_err_estim(entry, context->err_estim, 1);

			return entry;
		}
	}

	return NULL;
}

static bool
pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext *context)
{
	if (node == NULL)
		return false;

	switch (node->type)
	{
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;

				if (boolexpr->boolop == NOT_EXPR)
				{
					/* Skip, and do not keep track of the qual */
					uint32		previous_hash = context->qualid;
					uint32		previous_uniquequalnodeid = context->uniquequalid;

					context->qualid = 0;
					context->uniquequalid = 0;
					expression_tree_walker((Node *) boolexpr->args, pgqs_whereclause_tree_walker, context);
					context->qualid = previous_hash;
					context->uniquequalid = previous_uniquequalnodeid;
					return false;
				}
				else if (boolexpr->boolop == OR_EXPR)
				{
					context->qualid = 0;
					context->uniquequalid = 0;
				}
				else if (boolexpr->boolop == AND_EXPR)
				{
					context->uniquequalid = hashExpr((Expr *) boolexpr, context, pgqs_track_constants);
					context->qualid = hashExpr((Expr *) boolexpr, context, false);
				}
				expression_tree_walker((Node *) boolexpr->args, pgqs_whereclause_tree_walker, context);
				return false;
			}
		case T_OpExpr:
			pgqs_process_opexpr((OpExpr *) node, context);
			return false;
		case T_ScalarArrayOpExpr:
			pgqs_process_scalararrayopexpr((ScalarArrayOpExpr *) node, context);
			return false;
		case T_BooleanTest:
			pgqs_process_booltest((BooleanTest *) node, context);
			return false;
		default:
			expression_tree_walker(node, pgqs_whereclause_tree_walker, context);
			return false;
	}
}

static void
pgqs_backend_mode_startup(void)
{
	HASHCTL		info;
	HASHCTL		queryinfo;

	memset(&info, 0, sizeof(info));
	memset(&queryinfo, 0, sizeof(queryinfo));
	info.keysize = sizeof(pgqsHashKey);
	info.hcxt = TopMemoryContext;
	queryinfo.keysize = sizeof(pgqsWorkloadHashKey);
	queryinfo.entrysize = sizeof(pgqsWorkloadEntry) + pgqs_query_size * sizeof(char);
	queryinfo.hcxt = TopMemoryContext;

	if (pgqs_resolve_oids)
		info.entrysize = sizeof(pgqsEntryWithNames);
	else
		info.entrysize = sizeof(pgqsEntry);

	info.hash = pgqs_hash_fn;

	pgqs_hash = hash_create("pg_qualstatements_hash",
							 pgqs_max_qual,
							 &info,
							 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	pgqs_workload_hash = hash_create("pg_qualqueryexamples_hash",
											 pgqs_max_workload,
											 &queryinfo,

/* On PG > 9.5, use the HASH_BLOBS optimization for uint32 keys. */
#if PG_VERSION_NUM >= 90500
											 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#else
											 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
#endif
}

#if PG_VERSION_NUM >= 150000
static void
pgqs_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	Assert(!pgqs_backend);

	RequestAddinShmemSpace(pgqs_memsize());
	RequestNamedLWLockTranche("pg_qualstats", 3);
}
#endif

static void
pgqs_shmem_startup(void)
{
	HASHCTL		info;
	HASHCTL		queryinfo;
	bool		found;

	Assert(!pgqs_backend);

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	pgqs = NULL;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	pgqs = ShmemInitStruct("pg_qualstats",
						   (sizeof(pgqsSharedState)
#if PG_VERSION_NUM >= 90600
							+ pgqs_sampled_array_size()
#endif
							),
						   &found);
	memset(&info, 0, sizeof(info));
	memset(&queryinfo, 0, sizeof(queryinfo));
	info.keysize = sizeof(pgqsHashKey);
	queryinfo.keysize = sizeof(pgqsWorkloadHashKey);
	queryinfo.entrysize = sizeof(pgqsWorkloadEntry) + pgqs_query_size * sizeof(char);

	if (pgqs_resolve_oids)
		info.entrysize = sizeof(pgqsEntryWithNames);
	else
		info.entrysize = sizeof(pgqsEntry);

	info.hash = pgqs_hash_fn;
	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM >= 90600
		LWLockPadded *locks = GetNamedLWLockTranche("pg_qualstats");

		pgqs->lock = &(locks[0]).lock;
		pgqs->querylock = &(locks[1]).lock;
		pgqs->sampledlock = &(locks[2]).lock;
		/* mark all backends as not sampled */
		memset(pgqs->sampled, 0, pgqs_sampled_array_size());
#else
		pgqs->lock = LWLockAssign();
		pgqs->querylock = LWLockAssign();
#endif
	}
#if PG_VERSION_NUM < 90500
	queryinfo.hash = pgqs_uint32_hashfn;
#endif
	pgqs_hash = ShmemInitHash("pg_qualstatements_hash",
							  pgqs_max_qual, pgqs_max_qual,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

	pgqs_workload_hash = ShmemInitHash("pg_qualqueryexamples_hash",
											 pgqs_max_workload,
											 pgqs_max_workload,
											 &queryinfo,

/* On PG > 9.5, use the HASH_BLOBS optimization for uint32 keys. */
#if PG_VERSION_NUM >= 90500
											 HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
#else
											 HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
#endif
	LWLockRelease(AddinShmemInitLock);
}

Datum
pg_qualstats_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry	   *entry;
	pgqsWorkloadEntry *qryentry;

	if ((!pgqs && !pgqs_backend) || !pgqs_hash)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));
	}

	PGQS_LWL_ACQUIRE(pgqs->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgqs_hash, &entry->key, HASH_REMOVE, NULL);
	}

	PGQS_LWL_RELEASE(pgqs->lock);

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgqs_workload_hash);
	while ((qryentry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search_with_hash_value(pgqs_workload_hash, &qryentry->key,
									qryentry->key.queryid, HASH_REMOVE, NULL);
	}

	PGQS_LWL_RELEASE(pgqs->querylock);
	PG_RETURN_VOID();
}

/* Number of output arguments (columns) for various API versions */
#define PG_QUALSTATS_COLS_V1_0	18
#define PG_QUALSTATS_COLS_V2_0	24
#define PG_QUALSTATS_COLS		24	/* maximum of above */

/*
 * Retrieve statement statistics.
 *
 * The SQL API of this function has changed multiple times, and will likely
 * do so again in future.  To support the case where a newer version of this
 * loadable module is being used with an old SQL declaration of the function,
 * we continue to support the older API versions.  For 2.0.X and later, the
 * expected API version is identified by embedding it in the C name of the
 * function.  Unfortunately we weren't bright enough to do that for older
 * versions.
 */
Datum
pg_qualstats_2_0(PG_FUNCTION_ARGS)
{
	return pg_qualstats_common(fcinfo, PGQS_V2_0, false);
}

Datum
pg_qualstats_names_2_0(PG_FUNCTION_ARGS)
{
	return pg_qualstats_common(fcinfo, PGQS_V2_0, true);
}

Datum
pg_qualstats(PG_FUNCTION_ARGS)
{
	return pg_qualstats_common(fcinfo, PGQS_V1_0, false);
}

Datum
pg_qualstats_names(PG_FUNCTION_ARGS)
{
	return pg_qualstats_common(fcinfo, PGQS_V1_0, true);
}

Datum
pg_qualstats_common(PG_FUNCTION_ARGS, pgqsVersion api_version,
					bool include_names)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			nb_columns;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry  *entry;
	Datum	   *values;
	bool	   *nulls;

	if ((!pgqs && !pgqs_backend) || !pgqs_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Check we have the expected number of output arguments. */
	switch (tupdesc->natts)
	{
		case PG_QUALSTATS_COLS_V2_0:
		case PG_QUALSTATS_COLS_V2_0 + PGQS_NAME_COLUMNS:
			if (api_version != PGQS_V2_0)
				elog(ERROR, "incorrect number of output arguments");
			break;
		default:
			elog(ERROR, "incorrect number of output arguments");
	}

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	PGQS_LWL_ACQUIRE(pgqs->lock, LW_SHARED);
	hash_seq_init(&hash_seq, pgqs_hash);

	if (api_version == PGQS_V1_0)
		nb_columns = PG_QUALSTATS_COLS_V1_0;
	else
		nb_columns = PG_QUALSTATS_COLS_V2_0;

	if (include_names)
		nb_columns += PGQS_NAME_COLUMNS;

	Assert(nb_columns == tupdesc->natts);

	values = palloc0(sizeof(Datum) * nb_columns);
	nulls = palloc0(sizeof(bool) * nb_columns);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			i = 0;

		memset(values, 0, sizeof(Datum) * nb_columns);
		memset(nulls, 0, sizeof(bool) * nb_columns);
		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);

		if (entry->lattnum != InvalidAttrNumber)
		{
			values[i++] = ObjectIdGetDatum(entry->lrelid);
			values[i++] = Int16GetDatum(entry->lattnum);
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;
		}
		values[i++] = Int32GetDatum(entry->opoid);
		if (entry->rattnum != InvalidAttrNumber)
		{
			values[i++] = ObjectIdGetDatum(entry->rrelid);
			values[i++] = Int16GetDatum(entry->rattnum);
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;
		}
		if (entry->qualid == 0)
			nulls[i++] = true;
		else
			values[i++] = Int64GetDatum(entry->qualid);

		if (entry->key.uniquequalid == 0)
			nulls[i++] = true;
		else
			values[i++] = Int64GetDatum(entry->key.uniquequalid);

		values[i++] = Int64GetDatum(entry->qualnodeid);
		values[i++] = Int64GetDatum(entry->key.uniquequalnodeid);
		values[i++] = Int64GetDatum(entry->occurences);
		values[i++] = Int64GetDatum(entry->count);
		values[i++] = Int64GetDatum(entry->nbfiltered);

		if (api_version >= PGQS_V2_0)
		{
			int		j;

			for (j = 0; j < 2; j++)
			{
				double	stddev_estim;

				if (j == PGQS_RATIO)	/* min/max ratio are double precision */
				{
					values[i++] = Float8GetDatum(entry->min_err_estim[j]);
					values[i++] = Float8GetDatum(entry->max_err_estim[j]);
				}
				else				/* min/max num are bigint */
				{
					values[i++] = Int64GetDatum(entry->min_err_estim[j]);
					values[i++] = Int64GetDatum(entry->max_err_estim[j]);
				}
				values[i++] = Float8GetDatum(entry->mean_err_estim[j]);

				if (entry->occurences > 1)
					stddev_estim = sqrt(entry->sum_err_estim[j] / entry->occurences);
				else
					stddev_estim = 0.0;

				values[i++] = Float8GetDatumFast(stddev_estim);
			}
		}

		if (entry->key.queryid == 0)
			nulls[i++] = true;
		else
			values[i++] = Int64GetDatum(entry->key.queryid);

		if (entry->key.evaltype)
			values[i++] = CharGetDatum(entry->key.evaltype);
		else
			nulls[i++] = true;

		if (include_names)
		{
			if (pgqs_resolve_oids)
			{
				pgqsNames	names = ((pgqsEntryWithNames *) entry)->names;

				values[i++] = CStringGetTextDatum(NameStr(names.rolname));
				values[i++] = CStringGetTextDatum(NameStr(names.datname));
				values[i++] = CStringGetTextDatum(NameStr(names.lrelname));
				values[i++] = CStringGetTextDatum(NameStr(names.lattname));
				values[i++] = CStringGetTextDatum(NameStr(names.opname));
				values[i++] = CStringGetTextDatum(NameStr(names.rrelname));
				values[i++] = CStringGetTextDatum(NameStr(names.rattname));
			}
			else
			{
				for (; i < nb_columns; i++)
					nulls[i] = true;
			}
		}
		Assert(i == nb_columns);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	PGQS_LWL_RELEASE(pgqs->lock);
	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);

	return (Datum) 0;
}

Datum
pg_qualstats_example_queries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	pgqsWorkloadEntry *entry;

	if ((!pgqs && !pgqs_backend) || !pgqs_workload_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);
	hash_seq_init(&hash_seq, pgqs_workload_hash);

	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[3];
		bool		nulls[3];
		int64		queryid = entry->key.queryid;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatumFast(queryid);
		values[1] = CStringGetTextDatum(entry->querytext);
		values[2] = Int32GetDatum(entry->frequency);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	PGQS_LWL_RELEASE(pgqs->querylock);

	return (Datum) 0;
}

Datum
pg_qualstats_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[2];
	bool		nulls[2];

	if ((!pgqs && !pgqs_backend) || !pgqs_workload_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	MemoryContextSwitchTo(oldcontext);

	PGQS_LWL_ACQUIRE(pgqs->lock, LW_SHARED);
	values[0] = CStringGetTextDatum("query_advisor_qual_hash");
	values[1] = Float4GetDatum((float4) hash_get_num_entries(pgqs_hash) * 100 / pgqs_max_qual);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	PGQS_LWL_RELEASE(pgqs->lock);

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);
	values[0] = CStringGetTextDatum("query_advisor_workload_hash");
	values[1] = Float4GetDatum((float4) hash_get_num_entries(pgqs_workload_hash) * 100 / pgqs_max_qual);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	PGQS_LWL_RELEASE(pgqs->querylock);

	return (Datum) 0;
}

/*
 * Calculate hash value for a key
 */
static uint32
pgqs_hash_fn(const void *key, Size keysize)
{
	const pgqsHashKey *k = (const pgqsHashKey *) key;

	return hash_uint32((uint32) k->userid) ^
		hash_uint32((uint32) k->dbid) ^
		hash_uint32((uint32) k->queryid) ^
		hash_uint32((uint32) k->uniquequalnodeid) ^
		hash_uint32((uint32) k->uniquequalid) ^
		hash_uint32((uint32) k->evaltype);
}

static void
pgqs_set_planstates(PlanState *planstate, pgqsWalkerContext *context)
{
	context->outer_tlist = NIL;
	context->inner_tlist = NIL;
	context->index_tlist = NIL;
	context->outer_planstate = NULL;
	context->inner_planstate = NULL;
	context->planstate = planstate;
	if (IsA(planstate, AppendState))
	{
		AppendState * appendstate = (AppendState *) planstate;
		if (appendstate->as_nplans > 0)
			context->outer_planstate = appendstate->appendplans[0];
	}
	else if (IsA(planstate, MergeAppendState))
	{
		MergeAppendState * mergeappendstate = (MergeAppendState *) planstate;
		if (mergeappendstate->ms_nplans > 0)
			context->outer_planstate = mergeappendstate->mergeplans[0];
	}
#if PG_VERSION_NUM < 140000
	else if (IsA(planstate, ModifyTableState))
	{
		context->outer_planstate = ((ModifyTableState *) planstate)->mt_plans[0];
	}
#endif
	else
		context->outer_planstate = outerPlanState(planstate);

	if (context->outer_planstate)
		context->outer_tlist = context->outer_planstate->plan->targetlist;
	else
		context->outer_tlist = NIL;

	if (IsA(planstate, SubqueryScanState))
		context->inner_planstate = ((SubqueryScanState *) planstate)->subplan;
	else if (IsA(planstate, CteScanState))
		context->inner_planstate = ((CteScanState *) planstate)->cteplanstate;
	else
		context->inner_planstate = innerPlanState(planstate);

	if (context->inner_planstate)
		context->inner_tlist = context->inner_planstate->plan->targetlist;
	else
		context->inner_tlist = NIL;
	/* index_tlist is set only if it's an IndexOnlyScan */
	if (IsA(planstate->plan, IndexOnlyScan))
		context->index_tlist = ((IndexOnlyScan *) planstate->plan)->indextlist;
#if PG_VERSION_NUM >= 90500
	else if (IsA(planstate->plan, ForeignScan))
		context->index_tlist = ((ForeignScan *) planstate->plan)->fdw_scan_tlist;
	else if (IsA(planstate->plan, CustomScan))
		context->index_tlist = ((CustomScan *) planstate->plan)->custom_scan_tlist;
#endif
	else
		context->index_tlist = NIL;
}

static Expr *
pgqs_resolve_var(Var *var, pgqsWalkerContext *context)
{
	List	   *tlist = NULL;
	PlanState  *planstate = context->planstate;

	pgqs_set_planstates(context->planstate, context);
	switch (var->varno)
	{
		case INNER_VAR:
			tlist = context->inner_tlist;
			break;
		case OUTER_VAR:
			tlist = context->outer_tlist;
			break;
		case INDEX_VAR:
			tlist = context->index_tlist;
			break;
		default:
			return (Expr *) var;
	}
	if (tlist != NULL)
	{
		TargetEntry *entry = get_tle_by_resno(tlist, var->varattno);

		if (entry != NULL)
		{
			Var		   *newvar = (Var *) (entry->expr);

			if (var->varno == OUTER_VAR)
				pgqs_set_planstates(context->outer_planstate, context);

			if (var->varno == INNER_VAR)
				pgqs_set_planstates(context->inner_planstate, context);

			var = (Var *) pgqs_resolve_var(newvar, context);
		}
	}

	Assert(!(IsA(var, Var) && IS_SPECIAL_VARNO(var->varno)));

	/* If the result is something OTHER than a var, replace it by a constexpr */
	if (!IsA(var, Var))
	{
		Const	   *consttext;

		consttext = (Const *) makeConst(TEXTOID, -1, -1, -1, CStringGetTextDatum(nodeToString(var)), false, false);
		var = (Var *) consttext;
	}

	pgqs_set_planstates(planstate, context);

	return (Expr *) var;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgqs_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(pgqsSharedState));
	if (pgqs_resolve_oids)
		size = add_size(size, hash_estimate_size(pgqs_max_qual, sizeof(pgqsEntryWithNames)));
	else
		size = add_size(size, hash_estimate_size(pgqs_max_qual, sizeof(pgqsEntry)));

	size = add_size(size, hash_estimate_size(pgqs_max_workload,
											 sizeof(pgqsWorkloadEntry) + pgqs_query_size * sizeof(char)));
#if PG_VERSION_NUM >= 90600
	size = add_size(size, MAXALIGN(pgqs_sampled_array_size()));
#endif
	return size;
}

#if PG_VERSION_NUM >= 90600
static Size
pgqs_sampled_array_size(void)
{
	int _maxbackends;

#if PG_VERSION_NUM >= 150000
	Assert(MaxBackends > 0);
	_maxbackends = MaxBackends;
#else
	int32 _autovac_max_workers;
	int32 _max_wal_senders;
	const char *guc_string;

	/*
	 * autovacuum_max_workers and max_wal_senders aren't declared as
	 * PGDLLIMPORT in pg15- versions, so retrieve them using GetConfigOption to
	 * allow compilation on Windows.
	 */
	guc_string = GetConfigOption("autovacuum_max_workers",
											 false, true);

	_autovac_max_workers = pg_atoi(guc_string, 4, 0);
	Assert(_autovac_max_workers >= 1 && _autovac_max_workers <= MAX_BACKENDS);

	guc_string = GetConfigOption("max_wal_senders",
											 false, true);

	_max_wal_senders = pg_atoi(guc_string, 4, 0);
	Assert(_max_wal_senders >= 0 && _max_wal_senders <= MAX_BACKENDS);

	/*
	 * Parallel workers need to be sampled if their original query is also
	 * sampled.  We store in shared mem the sample state for each query,
	 * identified by their BackendId.  If need room for all possible backends,
	 * plus autovacuum launcher and workers, plus bg workers.
	 */
	_maxbackends = MaxConnections + _autovac_max_workers + 1
							+ max_worker_processes
#if PG_VERSION_NUM >= 120000
							/*
							 * Starting with pg12, max_wal_senders isn't part
							 * of max_connections anymore
							 */
							+ _max_wal_senders
#endif		/* pg12+ */
							+ 1;
#endif		/* pg15- */

	/* We need an extra value since BackendId numerotationn starts at 1. */
	return (sizeof(bool) * (_maxbackends + 1));
}
#endif

static uint32
hashExpr(Expr *expr, pgqsWalkerContext *context, bool include_const)
{
	StringInfo	buffer = makeStringInfo();

	exprRepr(expr, buffer, context, include_const);
	return hash_any((unsigned char *) buffer->data, buffer->len);

}

static void
exprRepr(Expr *expr, StringInfo buffer, pgqsWalkerContext *context, bool include_const)
{
	ListCell   *lc;

	if (expr == NULL)
		return;

	appendStringInfo(buffer, "%d-", expr->type);
	if (IsA(expr, Var))
		expr = pgqs_resolve_var((Var *) expr, context);

	switch (expr->type)
	{
		case T_List:
			foreach(lc, (List *) expr)
				exprRepr((Expr *) lfirst(lc), buffer, context, include_const);

			break;
		case T_OpExpr:
			{
				OpExpr	   *opexpr;

				opexpr = pgqs_get_canonical_opexpr((OpExpr *) expr, NULL);

				appendStringInfo(buffer, "%d", opexpr->opno);
				exprRepr((Expr *) opexpr->args, buffer, context, include_const);
				break;
			}
		case T_Var:
			{
				Var		   *var = (Var *) expr;

				RangeTblEntry *rte = list_nth(context->rtable, var->varno - 1);

				if (rte->rtekind == RTE_RELATION)
					appendStringInfo(buffer, "%d;%d", rte->relid, var->varattno);
				else
					appendStringInfo(buffer, "NORTE%d;%d", var->varno, var->varattno);
			}
			break;
		case T_BoolExpr:
			appendStringInfo(buffer, "%d", ((BoolExpr *) expr)->boolop);
			exprRepr((Expr *) ((BoolExpr *) expr)->args, buffer, context, include_const);
			break;
		case T_BooleanTest:
			if (include_const)
				appendStringInfo(buffer, "%d", ((BooleanTest *) expr)->booltesttype);

			exprRepr((Expr *) ((BooleanTest *) expr)->arg, buffer, context, include_const);
			break;
		case T_Const:
			if (include_const)
				get_const_expr((Const *) expr, buffer);
			else
				appendStringInfoChar(buffer, '?');

			break;
		case T_CoerceViaIO:
			exprRepr((Expr *) ((CoerceViaIO *) expr)->arg, buffer, context, include_const);
			appendStringInfo(buffer, "|%d", ((CoerceViaIO *) expr)->resulttype);
			break;
		case T_FuncExpr:
			appendStringInfo(buffer, "|%d(", ((FuncExpr *) expr)->funcid);
			exprRepr((Expr *) ((FuncExpr *) expr)->args, buffer, context, include_const);
			appendStringInfoString(buffer, ")");
			break;
		case T_MinMaxExpr:
			appendStringInfo(buffer, "|minmax%d(", ((MinMaxExpr *) expr)->op);
			exprRepr((Expr *) ((MinMaxExpr *) expr)->args, buffer, context, include_const);
			appendStringInfoString(buffer, ")");
			break;

		default:
			appendStringInfoString(buffer, nodeToString(expr));
	}
}

#if PG_VERSION_NUM < 90500
static uint32
pgqs_uint32_hashfn(const void *key, Size keysize)
{
	return ((pgqsWorkloadHashKey *) key)->queryid;
}
#endif
