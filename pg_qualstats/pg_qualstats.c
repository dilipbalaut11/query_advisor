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

#include "include/hypopg.h"
#include "include/hypopg_index.h"

PG_MODULE_MAGIC;

#define PGQS_NAME_COLUMNS 7		/* number of column added when using
								 * pg_qualstats_column SRF */
#define PGQS_USAGE_DEALLOC_PERCENT	5	/* free this % of entries at once */
#define PGQS_MAX_DEFAULT	1000	/* default pgqs_max value */
#define PGQS_MAX_LOCAL_ENTRIES	(pgqs_max * 0.2)	/* do not track more of
													 * 20% of possible entries
													 * in shared mem */
#define PGQS_CONSTANT_SIZE 80	/* Truncate constant representation at 80 */

#define PGQS_FLAGS (INSTRUMENT_ROWS|INSTRUMENT_BUFFERS)

#define PGQS_RATIO	0
#define PGQS_NUM	1

#define PGQS_LWL_ACQUIRE(lock, mode) if (!pgqs_backend) { \
	LWLockAcquire(lock, mode); \
	}

#define PGQS_LWL_RELEASE(lock) if (!pgqs_backend) { \
	LWLockRelease(lock); \
	}

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
extern PGDLLEXPORT Datum pg_qualstats_2_0(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_names(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_names_2_0(PG_FUNCTION_ARGS);
static Datum pg_qualstats_common(PG_FUNCTION_ARGS, pgqsVersion api_version,
								 bool include_names);
extern PGDLLEXPORT Datum pg_qualstats_example_query(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_example_queries(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_generate_advise(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum pg_qualstats_test_index_advise(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_qualstats_reset);
PG_FUNCTION_INFO_V1(pg_qualstats);
PG_FUNCTION_INFO_V1(pg_qualstats_2_0);
PG_FUNCTION_INFO_V1(pg_qualstats_names);
PG_FUNCTION_INFO_V1(pg_qualstats_names_2_0);
PG_FUNCTION_INFO_V1(pg_qualstats_example_query);
PG_FUNCTION_INFO_V1(pg_qualstats_example_queries);
PG_FUNCTION_INFO_V1(pg_qualstats_test_index_advise);

static PlannedStmt *pgqs_planner(Query *parse,
								 const char *query_string,
								 int cursorOptions,
								 ParamListInfo boundParams);
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

static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32 pgqs_hash_fn(const void *key, Size keysize);
static uint32 pgqs_update_hash_fn(const void *key, Size keysize);

#if PG_VERSION_NUM < 90500
static uint32 pgqs_uint32_hashfn(const void *key, Size keysize);
#endif

static bool pgqs_backend = false;
static int	pgqs_query_size;
static int	pgqs_max = PGQS_MAX_DEFAULT;			/* max # statements to track */
static bool pgqs_track_pgcatalog;	/* track queries on pg_catalog */
static bool pgqs_resolve_oids;	/* resolve oids */
static bool pgqs_enabled;
static bool pgqs_track_constants;
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


/*---- Data structures declarations ----*/
typedef struct pgqsSharedState
{
#if PG_VERSION_NUM >= 90400
	LWLock	   *lock;			/* protects counters hashtable
								 * search/modification */
	LWLock	   *querylock;		/* protects query/update hashtable
								 * search/modification */
#else
	LWLockId	lock;			/* protects counters hashtable
								 * search/modification */
	LWLockId	querylock;		/* protects query hashtable
								 * search/modification */
#endif
#if PG_VERSION_NUM >= 90600
	LWLock	   *sampledlock;	/* protects sampled array search/modification */
	bool		sampled[FLEXIBLE_ARRAY_MEMBER]; /* should we sample this
												 * query? */
#endif
} pgqsSharedState;

/* Since cff440d368, queryid becomes a uint64 internally. */

#if PG_VERSION_NUM >= 110000
typedef uint64 pgqs_queryid;
#else
typedef uint32 pgqs_queryid;
#endif

typedef struct pgqsHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	pgqs_queryid queryid;		/* query identifier (if set by another plugin */
	uint32		uniquequalnodeid;	/* Hash of the const */
	uint32		uniquequalid;	/* Hash of the parent, including the consts */
	char		evaltype;		/* Evaluation type. Can be 'f' to mean a qual
								 * executed after a scan, or 'i' for an
								 * indexqual */
} pgqsHashKey;

typedef struct pgqsNames
{
	NameData	rolname;
	NameData	datname;
	NameData	lrelname;
	NameData	lattname;
	NameData	opname;
	NameData	rrelname;
	NameData	rattname;
} pgqsNames;

typedef struct pgqsEntry
{
	pgqsHashKey key;
	Oid			lrelid;			/* LHS relation OID or NULL if not var */
	AttrNumber	lattnum;		/* LHS attribute Number or NULL if not var */
	Oid			opoid;			/* Operator OID */
	Oid			rrelid;			/* RHS relation OID or NULL if not var */
	AttrNumber	rattnum;		/* RHS attribute Number or NULL if not var */
	char		constvalue[PGQS_CONSTANT_SIZE]; /* Textual representation of
												 * the right hand constant, if
												 * any */
	uint32		qualid;			/* Hash of the parent AND expression if any, 0
								 * otherwise. */
	uint32		qualnodeid;		/* Hash of the node itself */

	int64		count;			/* # of operator execution */
	int64		nbfiltered;		/* # of lines discarded by the operator */
	int			position;		/* content position in query text */
	double		usage;			/* # of qual execution, used for deallocation */
	double		min_err_estim[2];	/* min estimation error ratio and num */
	double		max_err_estim[2];	/* max estimation error ratio and num */
	double		mean_err_estim[2];	/* mean estimation error ratio and num */
	double		sum_err_estim[2];	/* sum of variances in estimation error
									 * ratio and num */
	int64		occurences;		/* # of qual execution, 1 per query */
} pgqsEntry;

typedef struct pgqsEntryWithNames
{
	pgqsEntry	entry;
	pgqsNames	names;
} pgqsEntryWithNames;

typedef struct pgqsQueryStringHashKey
{
	pgqs_queryid queryid;
} pgqsQueryStringHashKey;

typedef struct pgqsQueryStringEntry
{
	pgqsQueryStringHashKey key;
	int			frequency;
	bool		isExplain;		
	int			qrylen;

	/*
	 * Imperatively at the end of the struct This is actually of length
	 * query_size, which is track_activity_query_size
	 */
	char		querytext[1];
} pgqsQueryStringEntry;

typedef struct pgqsUpdateHashKey
{
	pgqs_queryid 	queryid;	/* query identifier (if set by another plugin */
	Oid				dbid;		/* database oid. */
	Oid				relid;		/* relation OID of the updated column */
	AttrNumber		attnum;		/* attribute Number of the updated column */	
} pgqsUpdateHashKey;

typedef struct pgqsUpdateHashEntry
{
	pgqsUpdateHashKey	key;
	int		 			frequency;		/* frequency of execution */
	int64				updated_rows;	/* total commulative updated rows */
} pgqsUpdateHashEntry;

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

/* 
 * If 'pgqs_cost_track_enable' is set then the planner will start tracking the
 * cost of queries being planned and compute total cost in
 * 'pgqs_plan_total_cost'
 */
static bool pgqs_cost_track_enable = false;
static float pgqs_plan_cost = 0.0;

typedef struct QueryInfo
{
	double	cost;
	int		frequency;
	char   *query;
} QueryInfo;

/*
 * Track cost for current combination of the indices.
 */
typedef struct IndexCombination
{
	double	cost;
	int		nindices;
	double	overhead;
	int		indices[FLEXIBLE_ARRAY_MEMBER];
} IndexCombination;

typedef struct IndexCandidate
{
	Oid		relid;
	Oid		amoid;
	char   *amname;
	int		nattrs;
	int	   *attnum;
	int		nqueryids;
	int64  *queryids;
	int64	nupdates;
	int	   *queryinfoidx;
	int		nupdatefreq;
	double	benefit;
	double	overhead;
	char   *indexstmt;
	bool	isvalid;
	bool	isselected;
} IndexCandidate;

typedef struct IndexCombContext
{
	int		nqueries;
	int		ncandidates;
	int		maxcand;
	double		  **benefitmat;
	IndexCandidate *candidates;
	QueryInfo	   *queryinfos;
	Bitmapset	   *memberattr;
	MemoryContext	queryctx;
} IndexCombContext;

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
static void pgqsInsertOverheadHash(ModifyTable *node, pgqsWalkerContext *context);


static void pgqs_entry_dealloc(void);
static inline void pgqs_entry_init(pgqsEntry *entry);
static inline void pgqs_entry_copy_raw(pgqsEntry *dest, pgqsEntry *src);
static void pgqs_entry_err_estim(pgqsEntry *e, double *err_estim, int64 occurences);
static void pgqs_queryentry_dealloc(void);
static void pgqs_localentry_dealloc(int nvictims);
static void pgqs_fillnames(pgqsEntryWithNames *entry);

static Size pgqs_memsize(void);
#if PG_VERSION_NUM >= 90600
static Size pgqs_sampled_array_size(void);
#endif

static char **pg_qualstats_index_advise_rel(char **prevarray,
											 IndexCandidate *candidates,
											 int ncandidates, int *nindexes,
											 MemoryContext per_query_ctx,
											 bool iterative);
static QueryInfo *pg_qualstats_get_queries(IndexCandidate *candidates,
											int ncandidates, int *nqueries);
static bool pg_qulstat_is_queryid_exists(int64 *queryids, int nqueryids,
										 int64 queryid, int *idx);
static char *pg_qualstats_get_query(int64 queryid, int *freq);
static IndexCandidate *pg_qualstats_get_index_combination(IndexCandidate *candidates,
								   						  int *ncandidates);
static IndexCandidate *pg_qualstats_add_candidate_if_not_exists(
										IndexCandidate *candidates,
										IndexCandidate *cand,
										int *ncandidates, int *nmaxcand);
static bool pg_qualstats_is_exists(IndexCandidate *candidates,
								   int ncandidates, IndexCandidate *newcand,
								   int *match);
static void pg_qualstats_get_updates(IndexCandidate *candidates,
									 int ncandidates);

static bool pg_qualstats_generate_index_queries(IndexCandidate *candidates,
												int ncandidates);
static void pg_qualstats_fill_query_basecost(QueryInfo *queryinfos,
											 int nqueries, int *queryidxs);
static void pg_qualstats_plan_query(const char *query);
static void pg_qualstats_compute_index_benefit(IndexCombContext *context,
												int nqueries, int *queryidxs);
static double pg_qualstats_get_index_overhead(IndexCandidate *cand, Oid idxid);
static bool pg_qualstats_is_index_useful(double basecost, double indexcost,
										 double indexoverhead);
static Oid pg_qualstats_create_hypoindex(const char *index);
static void pg_qualstats_drop_hypoindex(Oid idxid);
static int pg_qualstats_hypoindex_size(Oid idxid);
static int pg_qualstats_get_best_candidate(IndexCombContext *context);

/* Global Hash */
static HTAB *pgqs_hash = NULL;
static HTAB *pgqs_query_examples_hash = NULL;
static pgqsSharedState *pgqs = NULL;
static HTAB *pgqs_update_hash = NULL;

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
	EnableQueryId();

	prev_planner_hook = planner_hook;
	planner_hook = pgqs_planner;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgqs_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgqs_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgqs_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgqs_ExecutorEnd;
	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = hypo_get_relation_info_hook;
	prev_explain_get_index_name_hook = explain_get_index_name_hook;
	explain_get_index_name_hook = hypo_explain_get_index_name_hook;

	DefineCustomBoolVariable("pg_qualstats.enabled",
							 "Enable / Disable pg_qualstats",
							 NULL,
							 &pgqs_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_qualstats.track_constants",
							 "Enable / Disable pg_qualstats constants tracking",
							 NULL,
							 &pgqs_track_constants,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_qualstats.max",
							"Sets the maximum number of statements tracked by pg_qualstats.",
							NULL,
							&pgqs_max,
							PGQS_MAX_DEFAULT,
							100,
							INT_MAX,
							pgqs_backend ? PGC_USERSET : PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	if (!pgqs_backend)
		DefineCustomBoolVariable("pg_qualstats.resolve_oids",
								 "Store names alongside the oid. Eats MUCH more space!",
								 NULL,
								 &pgqs_resolve_oids,
								 false,
								 PGC_POSTMASTER,
								 0,
								 NULL,
								 NULL,
								 NULL);

	DefineCustomBoolVariable("pg_qualstats.track_pg_catalog",
							 "Track quals on system catalogs too.",
							 NULL,
							 &pgqs_track_pgcatalog,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_qualstats.sample_rate",
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

	DefineCustomIntVariable("pg_qualstats.min_err_estimate_ratio",
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

	DefineCustomIntVariable("pg_qualstats.min_err_estimate_num",
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

	EmitWarningsOnPlaceholders("pg_qualstats");

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
	if (pgqs_enabled)
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
 * Save a non normalized query for the queryid if no one already exists, and
 * do all the stat collecting job
 */
static void
pgqs_ExecutorEnd(QueryDesc *queryDesc)
{
	pgqsQueryStringHashKey queryKey;
	bool		found;

	if ((pgqs || pgqs_backend) && pgqs_enabled && pgqs_is_query_sampled()
#if PG_VERSION_NUM >= 90600
		&& (!IsParallelWorker() && !pgqs_cost_track_enable)
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
		int			querylen = strlen(queryDesc->sourceText);

		context->queryId = queryDesc->plannedstmt->queryId;
		context->rtable = queryDesc->plannedstmt->rtable;
		context->count = 0;
		context->qualid = 0;
		context->uniquequalid = 0;
		context->nbfiltered = 0;
		context->evaltype = 0;
		context->nentries = 0;
		context->querytext = queryDesc->sourceText;
		queryKey.queryid = context->queryId;

		/* keep an unormalized query example for each queryid if needed */
		if (pgqs_track_constants && querylen < pgqs_query_size)
		{
			pgqsQueryStringEntry *queryEntry;


			/* Lookup the hash table entry with a shared lock. */
			PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);

			queryEntry = (pgqsQueryStringEntry *) hash_search_with_hash_value(pgqs_query_examples_hash, &queryKey,
																			  context->queryId,
																			  HASH_FIND, &found);

			/* Create the new entry if not present */
			if (!found)
			{
				bool		excl_found;

				/* Need exclusive lock to add a new hashtable entry - promote */
				PGQS_LWL_RELEASE(pgqs->querylock);
				PGQS_LWL_ACQUIRE(pgqs->querylock, LW_EXCLUSIVE);

				while (hash_get_num_entries(pgqs_query_examples_hash) >= pgqs_max)
					pgqs_queryentry_dealloc();

				queryEntry = (pgqsQueryStringEntry *) hash_search_with_hash_value(pgqs_query_examples_hash, &queryKey,
																				  context->queryId,
																				  HASH_ENTER, &excl_found);

				/* Make sure it wasn't added by another backend */
				if (!excl_found)
				{
					if (queryDesc->estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY)
						queryEntry->isExplain = true;
					else
						queryEntry->isExplain = false;

					queryEntry->qrylen = querylen;
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

			while (hash_get_num_entries(pgqs_hash) +
				   hash_get_num_entries(pgqs_localhash) >= pgqs_max)
				pgqs_entry_dealloc();

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
		}

		if (nodeTag(queryDesc->plannedstmt->planTree) == T_ModifyTable)
			pgqsInsertOverheadHash((ModifyTable *) queryDesc->plannedstmt->planTree, context);
		
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double		l_usage = (*(pgqsEntry *const *) lhs)->usage;
	double		r_usage = (*(pgqsEntry *const *) rhs)->usage;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	else
		return 0;
}

/*
 * Deallocate least used entries.
 * Caller must hold an exlusive lock on pgqs->lock
 */
static void
pgqs_entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry **entries;
	pgqsEntry  *entry;
	int			nvictims;
	int			i;
	int			base_size;

	/*
	 * Sort entries by usage and deallocate PGQS_USAGE_DEALLOC_PERCENT of
	 * them. While we're scanning the table, apply the decay factor to the
	 * usage values.
	 * pgqs_resolve_oids is irrelevant here as the array stores pointers
	 * instead of entries. The struct member used for the sort are part of
	 * pgqsEntry.
	 */
	base_size = sizeof(pgqsEntry *);

	entries = palloc(hash_get_num_entries(pgqs_hash) * base_size);

	i = 0;
	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		entry->usage *= 0.99;
	}

	qsort(entries, i, base_size, entry_cmp);

	nvictims = Max(10, i * PGQS_USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
		hash_search(pgqs_hash, &entries[i]->key, HASH_REMOVE, NULL);

	pfree(entries);
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
 * Deallocate the first example query.
 * Caller must hold an exlusive lock on pgqs->querylock
 */
static void
pgqs_queryentry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsQueryStringEntry *entry;

	hash_seq_init(&hash_seq, pgqs_query_examples_hash);
	entry = hash_seq_search(&hash_seq);

	if (entry != NULL)
	{
		hash_search_with_hash_value(pgqs_query_examples_hash, &entry->key,
									entry->key.queryid, HASH_REMOVE, NULL);
		hash_seq_term(&hash_seq);
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
	char	   *constant;
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
			constant = "TRUE::bool";
			opoid = BooleanEqualOperator;
			break;
		case IS_FALSE:
			constant = "FALSE::bool";
			opoid = BooleanEqualOperator;
			break;
		case IS_NOT_TRUE:
			constant = "TRUE::bool";
			opoid = BooleanNotEqualOperator;
			break;
		case IS_NOT_FALSE:
			constant = "FALSE::bool";
			opoid = BooleanNotEqualOperator;
			break;
		case IS_UNKNOWN:
			constant = "NULL::bool";
			opoid = BooleanEqualOperator;
			break;
		case IS_NOT_UNKNOWN:
			constant = "NULL::bool";
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

		if (pgqs_track_constants)
		{
			char	   *utf8const = (char *) pg_do_encoding_conversion((unsigned char *) constant,
																	   strlen(constant),
																	   GetDatabaseEncoding(),
																	   PG_UTF8);

			Assert(strlen(utf8const) < PGQS_CONSTANT_SIZE);
			strcpy(entry->constvalue, utf8const);
		}
		else
			memset(entry->constvalue, 0, sizeof(char) * PGQS_CONSTANT_SIZE);

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
				char	   *utf8const;
				int			len;

				context->nentries++;

				/* raw copy the temporary entry */
				pgqs_entry_copy_raw(entry, &tempentry);
				entry->position = position;
				entry->qualnodeid = hashExpr((Expr *) expr, context, false);
				entry->qualid = context->qualid;

				utf8const = (char *) pg_do_encoding_conversion((unsigned char *) buf->data,
															   strlen(buf->data),
															   GetDatabaseEncoding(),
															   PG_UTF8);
				len = strlen(utf8const);

				/*
				 * The const value can use multibyte characters, so we need to
				 * be careful when truncating the value.  Note that we need to
				 * use PG_UTF8 encoding explicitly here, as the value was just
				 * converted to this encoding.
				 */
				len = pg_encoding_mbcliplen(PG_UTF8, utf8const, len,
											PGQS_CONSTANT_SIZE - 1);

				memcpy(entry->constvalue, utf8const, len);
				entry->constvalue[len] = '\0';

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
	HASHCTL		updateinfo;

	memset(&info, 0, sizeof(info));
	memset(&queryinfo, 0, sizeof(queryinfo));
	memset(&updateinfo, 0, sizeof(updateinfo));
	info.keysize = sizeof(pgqsHashKey);
	info.hcxt = TopMemoryContext;
	queryinfo.keysize = sizeof(pgqsQueryStringHashKey);
	queryinfo.entrysize = sizeof(pgqsQueryStringEntry) + pgqs_query_size * sizeof(char);
	queryinfo.hcxt = TopMemoryContext;
	updateinfo.keysize = sizeof(pgqsUpdateHashKey);
	updateinfo.entrysize = sizeof(pgqsUpdateHashEntry);
	updateinfo.hcxt = TopMemoryContext;
	updateinfo.hash = pgqs_update_hash_fn;

	if (pgqs_resolve_oids)
		info.entrysize = sizeof(pgqsEntryWithNames);
	else
		info.entrysize = sizeof(pgqsEntry);

	info.hash = pgqs_hash_fn;

	pgqs_hash = hash_create("pg_qualstatements_hash",
							 pgqs_max,
							 &info,
							 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	pgqs_query_examples_hash = hash_create("pg_qualqueryexamples_hash",
											 pgqs_max,
											 &queryinfo,

/* On PG > 9.5, use the HASH_BLOBS optimization for uint32 keys. */
#if PG_VERSION_NUM >= 90500
											 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#else
											 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
#endif

	pgqs_update_hash = hash_create("pg_update_hash",
									pgqs_max,
									&updateinfo,
									HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
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

static void pgqs_read_dumpfile(void);

static void
pgqs_shmem_startup(void)
{
	HASHCTL		info;
	HASHCTL		queryinfo;
	HASHCTL		updateinfo;
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
	memset(&updateinfo, 0, sizeof(updateinfo));
	info.keysize = sizeof(pgqsHashKey);
	queryinfo.keysize = sizeof(pgqsQueryStringHashKey);
	queryinfo.entrysize = sizeof(pgqsQueryStringEntry) + pgqs_query_size * sizeof(char);
	updateinfo.keysize = sizeof(pgqsUpdateHashKey);
	updateinfo.entrysize = sizeof(pgqsUpdateHashEntry);
	updateinfo.hash = pgqs_update_hash_fn;

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
							  pgqs_max, pgqs_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

	pgqs_query_examples_hash = ShmemInitHash("pg_qualqueryexamples_hash",
											 pgqs_max, pgqs_max,
											 &queryinfo,

/* On PG > 9.5, use the HASH_BLOBS optimization for uint32 keys. */
#if PG_VERSION_NUM >= 90500
											 HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
#else
											 HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
#endif

	pgqs_update_hash = ShmemInitHash("pg_update_hash",
									 pgqs_max, pgqs_max,
									 &updateinfo,
									 HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
	pgqs_read_dumpfile();
	LWLockRelease(AddinShmemInitLock);
}

Datum
pg_qualstats_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry  *entry;

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
	PG_RETURN_VOID();
}

/* Number of output arguments (columns) for various API versions */
#define PG_QUALSTATS_COLS_V1_0	18
#define PG_QUALSTATS_COLS_V2_0	26
#define PG_QUALSTATS_COLS		26	/* maximum of above */

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
	Oid			userid = GetUserId();
	bool		is_allowed_role = false;
	pgqsEntry  *entry;
	Datum	   *values;
	bool	   *nulls;

#if PG_VERSION_NUM >= 140000
	/* Superusers or members of pg_read_all_stats members are allowed */
	is_allowed_role = is_member_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS);
#elif PG_VERSION_NUM >= 100000
	/* Superusers or members of pg_read_all_stats members are allowed */
	is_allowed_role = is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS);
#else
	 /* Superusers are allowed */
	is_allowed_role = superuser();
#endif

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
		case PG_QUALSTATS_COLS_V1_0:
		case PG_QUALSTATS_COLS_V1_0 + PGQS_NAME_COLUMNS:
			if (api_version != PGQS_V1_0)
				elog(ERROR, "incorrect number of output arguments");
			break;
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

		if (entry->position == -1)
			nulls[i++] = true;
		else
			values[i++] = Int32GetDatum(entry->position);

		if (entry->key.queryid == 0)
			nulls[i++] = true;
		else
			values[i++] = Int64GetDatum(entry->key.queryid);

		if (entry->constvalue[0] != '\0')
		{
			if (is_allowed_role || entry->key.userid == userid)
			{
				values[i++] = CStringGetTextDatum((char *) pg_do_encoding_conversion(
							(unsigned char *) entry->constvalue,
							strlen(entry->constvalue),
							PG_UTF8,
							GetDatabaseEncoding()));
			}
			else
			{
				/*
				 * Don't show constant text, but hint as to the reason for not
				 * doing so
				 */
				values[i++] = CStringGetTextDatum("<insufficient privilege>");
			}
		}
		else
			nulls[i++] = true;

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
pg_qualstats_example_query(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 110000
	pgqs_queryid queryid = PG_GETARG_INT64(0);
#else
	pgqs_queryid queryid = PG_GETARG_UINT32(0);
#endif
	pgqsQueryStringEntry *entry;
	pgqsQueryStringHashKey queryKey;
	bool		found;

	if ((!pgqs && !pgqs_backend) || !pgqs_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));

	/* don't search the hash table if track_constants isn't enabled */
	if (!pgqs_track_constants)
		PG_RETURN_NULL();

	queryKey.queryid = queryid;

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);
	entry = hash_search_with_hash_value(pgqs_query_examples_hash, &queryKey,
										queryid, HASH_FIND, &found);
	PGQS_LWL_RELEASE(pgqs->querylock);

	if (found)
		PG_RETURN_TEXT_P(cstring_to_text(entry->querytext));
	else
		PG_RETURN_NULL();
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
	pgqsQueryStringEntry *entry;

	if ((!pgqs && !pgqs_backend) || !pgqs_query_examples_hash)
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

	/* don't need to scan the hash table if track_constants isn't enabled */
	if (!pgqs_track_constants)
		return (Datum) 0;

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);
	hash_seq_init(&hash_seq, pgqs_query_examples_hash);

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

/*
 * Calculate hash value for a key
 */
static uint32
pgqs_update_hash_fn(const void *key, Size keysize)
{
	const pgqsUpdateHashKey *k = (const pgqsUpdateHashKey *) key;

	return hash_uint32((uint32) k->dbid) ^
		hash_uint32((uint32) k->queryid) ^
		hash_uint32((uint32) k->relid) ^
		hash_uint32((uint32) k->attnum);
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
		size = add_size(size, hash_estimate_size(pgqs_max, sizeof(pgqsEntryWithNames)));
	else
		size = add_size(size, hash_estimate_size(pgqs_max, sizeof(pgqsEntry)));

	if (pgqs_track_constants)
	{
		/*
		 * In that case, we also need an additional struct for storing
		 * non-normalized queries.
		 */
		size = add_size(size, hash_estimate_size(pgqs_max,
												 sizeof(pgqsQueryStringEntry) + pgqs_query_size * sizeof(char)));
	}
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
	return ((pgqsQueryStringHashKey *) key)->queryid;
}
#endif


/* planner hook */
static PlannedStmt *
pgqs_planner(Query *parse, const char *query_string,
						int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;

	/* Invoke the planner, possibly via a previous hook user */
	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								   boundParams);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								  boundParams);

	/* If enabled, delay by taking and releasing the specified lock */
	if (pgqs_cost_track_enable)
	{
		pgqs_plan_cost = result->planTree->total_cost;
	}

	return result;
}

static void
pgqsInsertOverheadHash(ModifyTable *node, pgqsWalkerContext *context)
{
	ListCell	*l;
	RangeTblEntry *rte;
	Instrumentation *instrumentation = context->planstate->instrument;

	/* TODO: for now only consider update overhead. */
	if (node->operation != CMD_UPDATE)
		return;

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_EXCLUSIVE);
	foreach(l, node->resultRelations)
	{
		Index					resultRelation = lfirst_int(l);
		pgqsUpdateHashKey		key;
		pgqsUpdateHashEntry	   *entry;
		List		*cols;
		ListCell	*lc;

		rte = list_nth(context->rtable, resultRelation - 1);
		cols = (List *) list_nth(node->updateColnosLists, resultRelation - 1);

		memset(&key, 0, sizeof(pgqsUpdateHashKey));
		key.dbid = MyDatabaseId;
		key.relid = rte->relid;	
		foreach(lc, cols)
		{
			AttrNumber	targetattnum = lfirst_int(lc);
			bool		found;

			key.attnum = targetattnum;
			key.queryid = context->queryId;

			entry = (pgqsUpdateHashEntry *) hash_search(pgqs_update_hash,
														&key,
														HASH_ENTER, &found);
			if (!found)
			{
				entry->frequency = 1;
				entry->updated_rows = instrumentation->ntuples;
			}
			else
			{
				entry->frequency++;
				entry->updated_rows += instrumentation->ntuples;
			}
		}
	}
	PGQS_LWL_RELEASE(pgqs->querylock);
}

#if 0

static bool
pg_qualstats_qualnodeid_exists(int64 *qualnodeids, int nqualnodes,
							   int64 qualnodeid)
{
	int		i;

	for (i = 0; i < nqualnodes; i++)
	{
		if (qualnodeids[i] == qualnodeid)
			return true;
	}

	return false;
}

static bool
pg_qualstats_attnum_exists(AttrNumber *attrs, int nattr, AttrNumber attrno)
{
	int		i;

	for (i = 0; i < nattr; i++)
	{
		if (attrs[i] == attrno)
			return true;
	}

	return false;
}

static void
pg_qualstats_qualnodeid_get_attnum(int64 qualnodeid)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry	   *entry;
	AttrNumber		attno;

	PGQS_LWL_ACQUIRE(pgqs->lock, LW_SHARED);
	hash_seq_init(&hash_seq, pgqs_hash);

	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (entry->qualnodeid == qualnodeid)
		{
			if (entry->lattnum != InvalidAttrNumber)
			{
				attno = entry->lattnum;
				break;
			}
			else if (entry->rattnum != InvalidAttrNumber)
			{
				attno = entry->rattnum;
				break;
			}
		}
	}
	PGQS_LWL_RELEASE(pgqs->lock);

	return attno;
}


AttrNumber*
pg_qualstats_qualids_get_attrs(int64 *quals, int nquals, int nidexcols)
{
	int			i;
	int			nquals_done;
	int			natt_done;
	int			nindexcol;
	int64	   *quals_done;
	int64		qualnodeid;
	AttrNumber	attno;
	AttrNumber *attno_done;
	AttrNumber *indexcol;

	indexcol = palloc0(nquals * sizeof(AttrNumber));
	quals_done = palloc0(nquals * sizeof(int64));

	for (i = 0; i < nquals; i++)
	{
		qualnodeid = quals[i];

		/* skip quals already present in the index */
		if (pg_qualstats_qualnodeid_exists(quals_done, nquals_done, qualnodeid))
			continue;

        /* skip other quals for the same column */
		attno = pg_qualstats_qualnodeid_get_attnum(qualnodeid);
		if (pg_qualstats_attnum_exists(indexcol, nindexcol, attno))
			continue;
	
		quals_done[nquals_done++] = qualnodeid;

		/* if underlying table has been dropped, stop here */
		indexcol[nindexcol++] = attno;
	}
}

Datum
pg_qualstats_test(PG_FUNCTION_ARGS)
{
	ArrayType  *quals_todo = PG_GETARG_ARRAYTYPE_P_COPY(0);
	int64	   *quals;
	AttrNumber *idxcols;
	int			nquals;
	int			nidxcols;
	int			i;
	int			ret;
	ArrayType  *result;

	quals = (int64 *) ARR_DATA_PTR(quals_todo);
	nquals = ARR_DIMS(quals_todo)[0];

	idxcols = pg_qualstats_qualids_get_attrs(quals, nquals, &nidxcols);

	/* Generate all combinations for indexes */
	/* read query id array */
	for (i = 0; i < nquals; i++)
	{
		elog(NOTICE, "qualnodeid = %lld", (long long int) quals[i]);
	}

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "pg_qualstat: SPI_connect returned %d", ret);

	/****/
	SPI_finish();

	return (Datum) 0;
}

static void
print(int64 *num, int n)
{
    int i;
    elog(NOTICE, "comb: %lld %lld %lld ", (long long int)num[0], (long long int)num[1], (long long int)num[2]);
}
#endif


#if 0
/* Given two sorted array check whether array a is subarray of array b. */
static bool
pg_qualstats_is_subarray(int *a, int *b, int la, int lb)
{
	int		i = 0;
	int		j = 0;

	while (i < la && j < lb)
	{
		if (a[i] == b[j])
		{
			i++;
			j++;

			if (i == la)
				return true;
		}
		else
			j++;
	}
	return false;
}

/* 
 * Mark all candidate invalid which are fully contained by other element
 * of same amtype.
 */
static void
pg_qualstats_mark_duplicates(IndexCandidate *candidates, int ncandidates)
{
	int			i;
	int			j;

	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *cand = &candidates[i];

		for (j = i + 1; j < ncandidates; j++)
		{
			/* if am is changed then stop comparing further. */
			if (cand->amoid != candidates[j].amoid)
				break;

			/* if it is subarray then mark invalid and stop. */
			if (pg_qualstats_is_subarray(cand->attnum, candidates[j].attnum,
										 cand->nattrs, candidates[j].nattrs))
			{
				cand->isvalid = false;
				break;
			}
		}
	}
}
#endif



#if 0
/* 
 * Get the non overlapping update count for given relation and set of
 * attributes.
 */
static int64
pg_qualstats_idx_updates(Oid dbid, Oid relid, int *attrs, int nattr)
{
	HASH_SEQ_STATUS 		hash_seq;
	pgqsUpdateHashEntry	   *entry;
	int64	   *qrueryid_done;
	int64		nupdates = 0;
	int			nqueryiddone = 0;
	int			maxqueryids = 50;

	qrueryid_done = palloc(sizeof(int64) * maxqueryids);

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);
	hash_seq_init(&hash_seq, pgqs_update_hash);

	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			i;

		if (entry->key.dbid != dbid)
			continue;
		if (entry->key.relid != relid)
			continue;
		for (i = 0; i < nattr; i++)
		{
			if (entry->key.attnum == attrs[i])
				break;
		}

		if (i == nattr)
			continue;

		for (i = 0; i < nqueryiddone; i++)
		{
			if (entry->key.queryid == qrueryid_done[i])
				break;
		}
		if (i < nqueryiddone)
			continue;

		if (nqueryiddone == maxqueryids)
		{
			maxqueryids *= 2;
			qrueryid_done = repalloc(qrueryid_done, sizeof(int64) * maxqueryids);
		}
		qrueryid_done[nqueryiddone++] = entry->key.queryid;

		/* average update per query. */
		nupdates += (entry->updated_rows / entry->frequency);
	}

	PGQS_LWL_RELEASE(pgqs->querylock);

	return nupdates;
}
#endif

#include "include/hypopg.h"
#include "include/hypopg_index.h"

#if 1 //exhaustive
static void
pg_qualstats_bms_add_candattr(IndexCombContext *context, IndexCandidate *cand)
{
	int		i;

	for (i = 0; i < cand->nattrs; i++)
	{
		context->memberattr = bms_add_member(context->memberattr,
											 cand->attnum[i]);
	}
}

static void
pg_qualstats_bms_remove_candattr(IndexCombContext *context,
								 IndexCandidate *cand)
{
	int		i;

	for (i = 0; i < cand->nattrs; i++)	
	{
		context->memberattr = bms_del_member(context->memberattr,
											 cand->attnum[i]);
	}
}

static bool
pg_qualstats_is_cand_overlaps(Bitmapset *bms, IndexCandidate *cand)
{
	int		i;

	if (bms == NULL)
		return false;

	for (i = 0; i < cand->nattrs; i++)
	{
		if (bms_is_member(cand->attnum[i], bms))
			return true;
	}
	return false;
}

/*
 * compare and retutn cheaper path and free non selected path
 *
 */
static IndexCombination *
pg_qualstats_compare_path(IndexCombination *path1,
						  IndexCombination *path2)
{
	double	cost1 = path1->cost + path1->overhead;
	double	cost2 = path2->cost + path2->overhead;

	if (cost1 <= cost2)
	{
		pfree(path2);
		return path1;
	}

	pfree(path1);
	return path2;
}

int count = 0;

/*
 * Plan all give queries to compute the total cost.
 */
static double
pg_qualstats_get_cost(QueryInfo *queryinfos, int nqueries, int *queryidxs)
{
	int		i;
	double	cost = 0.0;

	pgqs_cost_track_enable = true;

	/* 
	 * replan each query and compute the total weighted cost by multiplying
	 * each query cost with its frequency.
	 */
	for (i = 0; i < nqueries; i++)
	{
		int		index = (queryidxs != NULL) ? queryidxs[i] : i;

		pg_qualstats_plan_query(queryinfos[index].query);
		cost += (pgqs_plan_cost * queryinfos[index].frequency);
	}

	pgqs_cost_track_enable = false;

	return cost;
}

/*
 * Perform a exhaustive search with different index combinations.
 */
static IndexCombination *
pg_qualstats_compare_comb(IndexCombContext *context, int cur_cand,
						  int selected_cand)
{
	Oid						idxid;
	BlockNumber				pages;
	double					overhead;
	int						max_cand = context->maxcand;
	IndexCombination   *path1;
	IndexCombination   *path2;
	IndexCandidate		   *cand = context->candidates;

	if (cur_cand == max_cand || selected_cand == max_cand)
	{
		count++;
		path1 = palloc0(sizeof(IndexCombination) +
						context->ncandidates * sizeof(int));

		path1->cost = pg_qualstats_get_cost(context->queryinfos,
											context->nqueries, NULL);
		path1->nindices = 0;

		return path1;
	}

	/* compute total cost excluding this index */
	path1 = pg_qualstats_compare_comb(context, cur_cand + 1, selected_cand);

	/*
	 * If any of the attribute of this candidate is overlapping with any
	 * existing candidate in this combindation then don't add this candidate
	 * in the combination.
	 */
	if (!cand[cur_cand].isvalid ||
		pg_qualstats_is_cand_overlaps(context->memberattr, &cand[cur_cand]))
		return path1;

	pg_qualstats_bms_add_candattr(context, &cand[cur_cand]);

	/* compare cost with and without this index */
	idxid = hypo_create_index(cand[cur_cand].indexstmt, &pages);

	/* compute total cost including this index */
	path2 = pg_qualstats_compare_comb(context, cur_cand + 1, selected_cand + 1);
	path2->indices[path2->nindices++] = cur_cand;

	overhead = pg_qualstats_get_index_overhead(&cand[cur_cand], pages);
	path2->overhead += overhead;

	hypo_index_remove(idxid);
	pg_qualstats_bms_remove_candattr(context, &cand[cur_cand]);

	return pg_qualstats_compare_path(path1, path2);
}

/*
 * try missing candidate in selected path with greedy approach
 */
static IndexCombination *
pg_qualstats_complete_comb(IndexCombContext *context,
						   IndexCombination *path)
{
	int		i;
	int 	ncand = context->ncandidates;	
	IndexCandidate		   *cand = context->candidates;
	IndexCombination   *finalpath = path;

	for (i = 0; i < path->nindices; i++)
	{
		hypo_create_index(cand[path->indices[i]].indexstmt, NULL);
	}

	for (i = 0; i < ncand; i++)
	{
		Oid			idxid;
		int			j;

		if (!cand->isvalid)
			continue;

		for (j = 0; j < finalpath->nindices; j++)
		{
			if (finalpath->indices[j] == i)
				break;
		}
		/* 
		 * if candidate not found in path then try to add in the path and
		 * compare cost
		 */
		if (j == finalpath->nindices)
		{
			IndexCombination *newpath;
			int		size;
			double	overhead;
			BlockNumber		relpages;
			
			size = sizeof(IndexCombination) + ncand * sizeof(int);

			newpath = (IndexCombination *) palloc0(size);
			memcpy(newpath, finalpath, size);
			newpath->nindices += 1;
			idxid = hypo_create_index(cand[i].indexstmt, &relpages);
			overhead = pg_qualstats_get_index_overhead(&cand[i], relpages);
			newpath->overhead += overhead;
			newpath->cost = pg_qualstats_get_cost(context->queryinfos,
												  context->nqueries, NULL);
			if (newpath->cost > finalpath->cost)
			{
				elog(NOTICE, "cost increased in greedy");
			}

			finalpath = pg_qualstats_compare_path(finalpath, newpath);
			/* drop this index if this index is not selected. */
			if (finalpath != newpath)
				hypo_index_remove(idxid);
			else
				elog(NOTICE, "path selected in greedy");
		}
	}

	/* reset all hypo indexes. */
	hypo_index_reset();

	return finalpath;
}

/*
 * check individiual index usefulness, if not reducing the cost by some margin
 * then mark invalid.
 */
static void
pg_qualstats_mark_useless_candidate(IndexCombContext *context)
{
	int		i;

	for (i = 0; i < context->ncandidates; i++)
	{
		IndexCandidate *cand = &context->candidates[i];
		double	cost1;
		double	cost2;
		Oid		idxid;

		cost1 = pg_qualstats_get_cost(context->queryinfos, context->nqueries, NULL);
		idxid = hypo_create_index(cand->indexstmt, NULL);
		cost2 = pg_qualstats_get_cost(context->queryinfos, context->nqueries, NULL);
		hypo_index_remove(idxid);

		if (cost2 < cost1 - 100)
			cand->isvalid = true;
		else
			cand->isvalid = false;
	}
}

#endif //exhaustive

/* Location of permanent stats file (valid when database is shut down) */
#define PGQS_DUMP_FILE	"pg_qualstats.stat"
/* Magic number identifying the stats file format */
static const uint32 PGQS_FILE_HEADER = 0x20220408;

/* PostgreSQL major version number, changes in which invalidate all entries */
static const uint32 PGQS_PG_MAJOR_VERSION = PG_VERSION_NUM / 100;

static void pgqs_shmem_shutdown(int code, Datum arg);

static void
pgqs_read_dumpfile(void)
{
	bool		found;
	HASHCTL		info;
	FILE	   *file = NULL;
	FILE	   *qfile = NULL;
	uint32		header;
	int32		num;
	int32		pgver;
	int32		i;
	int			buffer_size;
	char	   *buffer = NULL;

	//TODO: Acquire lock
	/*
	 * If we're in the postmaster (or a standalone backend...), set up a shmem
	 * exit hook to dump the statistics to disk.
	 */
	//if (!IsUnderPostmaster)
		on_shmem_exit(pgqs_shmem_shutdown, (Datum) 0);

	/*
	 * Attempt to load old statistics from the dump file.
	 */
	file = AllocateFile(PGQS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;
		/* No existing persisted stats file, so we're done */
		return;
	}

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		fread(&pgver, sizeof(uint32), 1, file) != 1 ||
		fread(&num, sizeof(int32), 1, file) != 1)
		goto read_error;

//	if (header != PQSS_FILE_HEADER ||
//		pgver != PGQS_PG_MAJOR_VERSION)
//		goto data_error;

	for (i = 0; i < num; i++)
	{
		pgqsEntry	temp;
		pgqsEntry  *entry;
		Size		query_offset;

		if (fread(&temp, sizeof(pgqsEntry), 1, file) != 1)
			goto read_error;

		entry = (pgqsEntry *) hash_search(pgqs_hash,
										  &temp.key,
										  HASH_ENTER, &found);
		if (!found)
			memcpy(entry, &temp, sizeof(pgqsEntry));
	}
	FreeFile(file);

	unlink(PGQS_DUMP_FILE);

	return;

read_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read file \"%s\": %m",
					PGQS_DUMP_FILE)));
	goto fail;
data_error:
	ereport(LOG,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("ignoring invalid data in file \"%s\"",
					PGQS_DUMP_FILE)));
	goto fail;
fail:
	if (file)
		FreeFile(file);

	/* If possible, throw away the bogus file; ignore any error */
	unlink(PGQS_DUMP_FILE);
}

#if 1
/*
 * shmem_shutdown hook: Dump statistics into file.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void
pgqs_shmem_shutdown(int code, Datum arg)
{
	FILE	   *file;
	char	   *qbuffer = NULL;
	Size		qbuffer_size = 0;
	HASH_SEQ_STATUS hash_seq;
	int32		num_entries;
	pgqsEntry  *entry;
	int i=1;

	while(i);
	/* Don't try to dump during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!pgqs || !pgqs_hash)
		return;

	/* Don't dump if told not to. */
//	if (!pgqs_save)
//		return;

	file = AllocateFile(PGQS_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGQS_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;
	if (fwrite(&PGQS_PG_MAJOR_VERSION, sizeof(uint32), 1, file) != 1)
		goto error;
	num_entries = hash_get_num_entries(pgqs_hash);
	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;

	/*
	 * When serializing to disk, we store query texts immediately after their
	 * entry data.  Any orphaned query texts are thereby excluded.
	 */
	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(pgqsEntry), 1, file) != 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file into place, so we atomically replace any old one.
	 */
	(void) durable_rename(PGQS_DUMP_FILE ".tmp", PGQS_DUMP_FILE, LOG);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					PGQS_DUMP_FILE ".tmp")));
	free(qbuffer);
	if (file)
		FreeFile(file);
	unlink(PGQS_DUMP_FILE ".tmp");
}

#endif
char *query =
"WITH pgqs AS ("
"\n          SELECT dbid, min(am.oid) amoid, amname, qualid, qualnodeid,"
"\n            (coalesce(lrelid, rrelid), coalesce(lattnum, rattnum),"
"\n            opno, eval_type)::qual AS qual, queryid,"
"\n            round(avg(execution_count)) AS execution_count,"
"\n            sum(occurences) AS occurences,"
"\n            round(sum(nbfiltered)::numeric / sum(occurences)) AS avg_filter,"
"\n            CASE WHEN sum(execution_count) = 0"
"\n              THEN 0"
"\n              ELSE round(sum(nbfiltered::numeric) / sum(execution_count) * 100)"
"\n            END AS avg_selectivity"
"\n          FROM pg_qualstats() q"
"\n          JOIN pg_catalog.pg_database d ON q.dbid = d.oid"
"\n          JOIN pg_catalog.pg_operator op ON op.oid = q.opno"
"\n          JOIN pg_catalog.pg_amop amop ON amop.amopopr = op.oid"
"\n          JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod"
"\n          WHERE d.datname = current_database()"
"\n          AND eval_type = 'f'"
"\n          AND coalesce(lrelid, rrelid) != 0"
"\n          GROUP BY dbid, amname, qualid, qualnodeid, lrelid, rrelid,"
"\n            lattnum, rattnum, opno, eval_type, queryid ORDER BY lattnum, rattnum"
"\n        ),"
"\n        -- apply cardinality and selectivity restrictions"
"\n        filtered AS ("
"\n          SELECT (qual).relid, min(amoid) amoid, amname, coalesce(qualid, qualnodeid) AS parent,"
"\n            count(*) AS weight,"
"\n            array_agg(DISTINCT((qual).attnum) ORDER BY ((qual).attnum)) AS attnumlist,"
"\n            array_agg(qualnodeid) AS qualidlist,"
"\n            array_agg(DISTINCT(queryid)) AS queryidlist"
"\n          FROM pgqs"
"\n          GROUP BY (qual).relid, amname, parent"
"\n        )"
"\nSELECT * FROM filtered where amname='btree' OR amname='brin' ORDER BY relid, amname DESC, cardinality(attnumlist);";

static void
print_candidates(IndexCandidate *candidates, int ncandidates)
{
	int			i;

	for (i = 0; i < ncandidates; i++)
	{
		elog(NOTICE, "Index: %s: update: %lld freq: %d valid:%d", candidates[i].indexstmt,
			 (long long int) candidates[i].nupdates, candidates[i].nupdatefreq, candidates[i].isvalid);
	}
}

static void
print_benefit_matrix(IndexCombContext *context)
{
	double		  **benefitmat = context->benefitmat;
	int 	ncandidates = context->ncandidates;
	int		i;
	StringInfoData	row;

	initStringInfo(&row);

	appendStringInfo(&row, "======Benefit Matrix Start=======\n");
	for (i = 0; i < ncandidates; i++)
	{
		int	j;

		for (j = 0; j < context->nqueries; j++)
			appendStringInfo(&row, "%f\t", benefitmat[j][i]);
		appendStringInfo(&row, "\n");
	}
	appendStringInfo(&row, "======Benefit Matrix End=======\n");
	elog(NOTICE, "%s", row.data);
	pfree(row.data);
}

Datum
pg_qualstats_test_index_advise(PG_FUNCTION_ARGS)
{
	bool		exhaustive = PG_GETARG_BOOL(0);
	int			ret;
	int			i;
	int			ncandidates = 0;
	int			nrelcand = 0;
	int			nindexes = 0;
	int			idxcand = 0;
	Oid			prevrelid = InvalidOid;
	char	  **index_array;
	Datum	   *key_datums;
	TupleDesc	tupdesc;
	ArrayType  *result;
	IndexCandidate *candidates;
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext 	per_query_ctx;
	MemoryContext 	oldcontext;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "pg_qualstat: SPI_connect returned %d", ret);

	ret = SPI_execute(query, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
	}

	tupdesc = SPI_tuptable->tupdesc;
	ncandidates = SPI_processed;
	candidates = palloc0(sizeof(IndexCandidate) * ncandidates);

	/* Read all the index candidates */
	for (i = 0; i < ncandidates; i++)
	{
		HeapTuple		tup = SPI_tuptable->vals[i];
		Datum			dat;
		bool			isnull;
		ArrayType	   *r;
		IndexCandidate *cand;

		cand = &(candidates[i]);

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		cand->relid = DatumGetObjectId(dat);

		dat = SPI_getbinval(tup, tupdesc, 2, &isnull);
		cand->amoid = DatumGetObjectId(dat);

		cand->amname = pstrdup(SPI_getvalue(tup, tupdesc, 3));

		dat = SPI_getbinval(tup, tupdesc, 6, &isnull);
		r = DatumGetArrayTypePCopy(dat);

		cand->attnum = (int *) ARR_DATA_PTR(r);
		cand->nattrs = ARR_DIMS(r)[0];

		dat = SPI_getbinval(tup, tupdesc, 8, &isnull);
		r = DatumGetArrayTypePCopy(dat);

		cand->queryids = (int64 *) ARR_DATA_PTR(r);
		cand->nqueryids = ARR_DIMS(r)[0];

		cand->isvalid = true;
		cand->isselected = false;
	}

	/* process candidates for rel by rel and generate index advises. */
	for (i = 0; i < ncandidates; i++)
	{
		if (OidIsValid(prevrelid) && prevrelid != candidates[i].relid)
		{
			index_array = pg_qualstats_index_advise_rel(index_array,
														&candidates[idxcand],
														nrelcand, &nindexes,
														per_query_ctx,
														!exhaustive);
			nrelcand = 0;
			idxcand = i;
		}
		prevrelid = candidates[i].relid;
		nrelcand++;
	}

	index_array = pg_qualstats_index_advise_rel(index_array,
												&candidates[idxcand],
												nrelcand, &nindexes,
												per_query_ctx,
												!exhaustive);
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	key_datums = (Datum *) palloc(nindexes * sizeof(Datum));
	MemoryContextSwitchTo(oldcontext);


	SPI_finish();

	/* construct and return array. */
	for (i = 0; i < nindexes; i++)
		key_datums[i] = CStringGetTextDatum(index_array[i]);

	result = construct_array_builtin(key_datums, nindexes, TEXTOID);

	PG_RETURN_ARRAYTYPE_P(result);
}

static IndexCombination *
pg_qualstats_iterative(IndexCombContext	*context)
{
	IndexCombination   *path;
	int			ncandidates = context->ncandidates;
	int			nqueries = context->nqueries;
	int		   *queryidxs = NULL;

	QueryInfo  *queryinfos = context->queryinfos;
	IndexCandidate *candidates = context->candidates;

	path = palloc0(sizeof(IndexCombination) + ncandidates * sizeof(int));

	/*
	 * First plan query query without any new index and set base cost for each
	 * query.
	 */
	pg_qualstats_fill_query_basecost(queryinfos, nqueries, NULL);
	queryidxs = (int *) palloc (nqueries * sizeof (int));

	while (true)
	{
		int		bestcand;
		int		i;

		/*
		 * Create every index one at a time and replan every query and fill
		 * intial values of benefit matrix.
		 */
		if (nqueries < context->nqueries)
			pg_qualstats_compute_index_benefit(context, nqueries, queryidxs);
		else
			pg_qualstats_compute_index_benefit(context, nqueries, NULL);

		print_benefit_matrix(context);

		/*
		 * Compute the overall benefit of all the candidate and get the  best
		 * candidate.
		 */
		bestcand = pg_qualstats_get_best_candidate(context);
		if (bestcand == -1)
			break;

		/*
		 * Add best candidates to the path and create the hypoindex for this
		 * candidate and reiterate for the next round.  For this index we
		 * create hypoindex and do not drop so that next round assume this
		 * index is already exist now and check benefit of each candidate by
		 * assuming this candidate is already finalized.
		 */
		hypo_create_index(candidates[bestcand].indexstmt, NULL);

		/* 
		 * Best candidate is seleted so update the query base cost as if this
		 * index exists before going for next iteration.
		 */
		nqueries = 0;
		for (i = 0; i < context->nqueries; i++)
		{
			if (context->benefitmat[i][bestcand] > 0)
			{
				queryinfos[i].cost -= context->benefitmat[i][bestcand];

				/* 
				 * Remember the queries which got benefited by this index and
				 * in the next round only plan these queries.
				 */
				queryidxs[nqueries++] = i;
			}
		}

		path->indices[path->nindices++] = bestcand;
		candidates[bestcand].isselected = true;
	}

	hypo_index_reset();

	return path;
}

static IndexCombination *
pg_qualstats_exhaustive(IndexCombContext *context)
{
	IndexCombination   *path;

	pg_qualstats_mark_useless_candidate(context);
	path = pg_qualstats_compare_comb(context, 0, 0);

	/* 
	 * If path doesn't include all the candidates then try to add missing
	 * candidates with greedy approach.
	 * TODO TRY iterative for remaining candidates.
	 */
	//if (path->nindices < context->ncandidates)
	//	pg_qualstats_complete_comb(context, path);

	return path;
}

static char **
pg_qualstats_index_advise_rel(char **prevarray, IndexCandidate *candidates,
							  int ncandidates, int *nindexes,
							  MemoryContext per_query_ctx, bool iterative)
{
	char	  **index_array = NULL;
	QueryInfo  *queryinfos;
	int			nqueries;
	int			i;
	int			prev_indexes = *nindexes;
	IndexCandidate *finalcand;
	IndexCombination   *path;
	IndexCombContext		context;
	MemoryContext oldcontext;

	elog(NOTICE, "Candidate Relation %d", candidates[0].relid);

	queryinfos = pg_qualstats_get_queries(candidates, ncandidates, &nqueries);

	/* genrate all one and two length index combinations. */
	finalcand = pg_qualstats_get_index_combination(candidates, &ncandidates);
	pg_qualstats_get_updates(finalcand, ncandidates);
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (!pg_qualstats_generate_index_queries(finalcand, ncandidates))
		return prevarray;

	MemoryContextSwitchTo(oldcontext);
	print_candidates(finalcand, ncandidates);

	context.candidates = finalcand;
	context.ncandidates = ncandidates;
	context.queryinfos = queryinfos;
	context.nqueries = nqueries;
	context.maxcand = ncandidates;//Min(ncandidates, 15);
	context.memberattr = NULL;
	context.queryctx = per_query_ctx;

	/* allocate memory for benefit matrix */
	context.benefitmat = (double **) palloc0(nqueries * sizeof(double *));
	for (i = 0; i < nqueries; i++)
		context.benefitmat[i] = (double *) palloc0(ncandidates * sizeof(double));

	if (iterative)
		path = pg_qualstats_iterative(&context);
	else
		path = pg_qualstats_exhaustive(&context);

	if (path->nindices == 0)
		return prevarray;

	prev_indexes = *nindexes;
	(*nindexes) += path->nindices;

	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (prev_indexes == 0)
		index_array = (char **) palloc(path->nindices * sizeof(char *));
	else
		index_array = (char ** ) repalloc(prevarray, (*nindexes) * sizeof(char *));
	MemoryContextSwitchTo(oldcontext);

	/* construct and return array. */
	for (i = 0; i < path->nindices; i++)
		index_array[prev_indexes + i] = finalcand[path->indices[i]].indexstmt;

	return index_array;
}

static QueryInfo *
pg_qualstats_get_queries(IndexCandidate *candidates, int ncandidates,
						 int *nqueries)
{
	int			i;
	int			j;
	int			nids = 0;
	int			maxids = ncandidates;
	int64	   *queryids;
	QueryInfo  *queryinfos;

	queryids = palloc(maxids * sizeof(int64));

	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *cand = &candidates[i];

		cand->queryinfoidx = palloc(cand->nqueryids * sizeof(int));

		for (j = 0; j < cand->nqueryids ; j++)
		{
			int index = -1;

			if (pg_qulstat_is_queryid_exists(queryids, nids, cand->queryids[j],
											 &index))
			{
				cand->queryinfoidx[j] = index;
				continue;
			}

			if (nids >= maxids)
			{
				maxids *= 2;
				queryids = repalloc(queryids, maxids * sizeof(int64));
			}
			queryids[nids] = cand->queryids[j];
			cand->queryinfoidx[j] = nids;
			nids++;
		}
	}

	queryinfos = (QueryInfo *) palloc(nids * sizeof(QueryInfo));

	for (i = 0; i < nids; i++)
	{
		queryinfos[i].query = pg_qualstats_get_query(queryids[i],
													 &queryinfos[i].frequency);
		elog(NOTICE, "query %d: %s-freq:%d", i, queryinfos[i].query, queryinfos[i].frequency);
	}

	pfree(queryids);

	*nqueries = nids;

	return queryinfos;
}

static bool
pg_qulstat_is_queryid_exists(int64 *queryids, int nqueryids, int64 queryid,
							 int *idx)
{
	int			i;

	for (i = 0; i < nqueryids ; i++)
	{
		if (queryids[i] == queryid)
		{
			*idx = i;
			return true;
		}
	}

	return false;
}

static char *
pg_qualstats_get_query(int64 queryid, int *freq)
{
	pgqsQueryStringEntry   *entry;
	pgqsQueryStringHashKey	queryKey;
	char   *query = NULL;
	bool	found;

	queryKey.queryid = queryid;

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);
	entry = hash_search_with_hash_value(pgqs_query_examples_hash, &queryKey,
										queryid, HASH_FIND, &found);
	if (!found)
		return NULL;

	if (entry->isExplain)
	{
		query = palloc0(strlen(entry->querytext) + 1);
		strcpy(query, entry->querytext);
	}
	else
	{
		int		explainlen = strlen("EXPLAIN ");

		query = palloc0(explainlen + entry->qrylen);
		strcpy(query, "EXPLAIN ");
		strcpy(query + explainlen, entry->querytext);
	}

	*freq = entry->frequency;

	PGQS_LWL_RELEASE(pgqs->querylock);

	return query;
}

/*
 * From given index candidate list generate single column and two columns
 * index candidate array.
 */
static IndexCandidate *
pg_qualstats_get_index_combination(IndexCandidate *candidates,
								   int *ncandidates)
{
	IndexCandidate *finalcand;
	IndexCandidate	cand;
	int				nfinalcand = 0;
	int				nmaxcand = *ncandidates;
	int				i;
	int				j;
	int				k = 0;

	finalcand = palloc(sizeof(IndexCandidate) * nmaxcand);

	/* genrate all one and tow length index combinations. */
	for (i = 0; i < *ncandidates; i++)
	{
		for (j = 0; j < candidates[i].nattrs; j++)
		{
			/* generae one column index */
			memcpy(&cand, &candidates[i], sizeof(IndexCandidate));
			cand.nattrs = 1;
			cand.attnum = (int *) palloc0(sizeof(int));
			cand.attnum[0] = candidates[i].attnum[j];

			finalcand = pg_qualstats_add_candidate_if_not_exists(finalcand,
																 &cand,
																 &nfinalcand,
																 &nmaxcand);

			/* generate two column indexes. */
			for (k = 0; k < candidates[i].nattrs; k++)
			{
				if (k == j)
					continue;

				cand.nattrs = 2;
				cand.attnum = (int *) palloc0(sizeof(int) * 2);

				cand.attnum[0] = candidates[i].attnum[j];
				cand.attnum[1] = candidates[i].attnum[k];

				finalcand = pg_qualstats_add_candidate_if_not_exists(finalcand,
												&cand, &nfinalcand, &nmaxcand);
			}
		}
	}

	*ncandidates = nfinalcand;

	return finalcand;
}

static IndexCandidate *
pg_qualstats_add_candidate_if_not_exists(IndexCandidate *candidates,
										 IndexCandidate *cand,
										 int *ncandidates, int *nmaxcand)
{
	IndexCandidate *finalcand = candidates;
	int				match = -1;

	if (!pg_qualstats_is_exists(candidates, *ncandidates, cand, &match))
	{
		if (*ncandidates == *nmaxcand)
		{
			(*nmaxcand) *= 2;
			finalcand = repalloc(finalcand,
								 sizeof(IndexCandidate) * (*nmaxcand));
		}
		memcpy(&finalcand[*ncandidates], cand, sizeof(IndexCandidate));
		(*ncandidates)++;
	}
	else
	{
		IndexCandidate *oldcand = &candidates[match];
		int		i;
		int 	j;
		int		nqueryidx = oldcand->nqueryids;
		bool	isfirst = true;

		for (i = 0; i < cand->nqueryids; i++)
		{
			for (j = 0; j < oldcand->nqueryids; j++)
			{
				if (cand->queryinfoidx[i] == oldcand->queryinfoidx[j])
					break;
			}
			/* not found */
			if (j == oldcand->nqueryids)
			{
				if (isfirst)
				{
					int		newsize = cand->nqueryids + oldcand->nqueryids;
					int	   *queryinfoidx;

					/* 
						* Allocate new memory instead of repallocing as
						* multiple candidate which are derived from same
						* parent might share same memory.
						*/
					queryinfoidx = (int *) palloc(newsize * sizeof(int));
					memcpy(queryinfoidx, oldcand->queryinfoidx,
							sizeof(int) * oldcand->nqueryids);
					oldcand->queryinfoidx = queryinfoidx;

					isfirst = false;
				}
				oldcand->queryinfoidx[nqueryidx++] = cand->queryinfoidx[i];
			}
		}
		pfree(cand->attnum);
		oldcand->nqueryids = nqueryidx;
	}

	return finalcand;
}

static bool
pg_qualstats_is_exists(IndexCandidate *candidates, int ncandidates,
					   IndexCandidate *newcand, int *match)
{
	int		i;
	int		j;

	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *oldcand = &candidates[i];

		if (oldcand->nattrs != newcand->nattrs)
			continue;
		if (oldcand->amoid != newcand->amoid)
			continue;

		for (j = 0; j < oldcand->nattrs; j++)
		{
			if (oldcand->attnum[j] != newcand->attnum[j])
				break;
		}

		if (j == oldcand->nattrs)
		{
			*match = i;
			return true;
		}
	}

	return false;
}

/*
 * Process each index candidate and compute the number of updated tuple for
 * each index candidates based on the index column update counts.
 */
static void
pg_qualstats_get_updates(IndexCandidate *candidates, int ncandidates)
{
	HASH_SEQ_STATUS 		hash_seq;
	int64	   *qrueryid_done;
	int64		nupdates = 0;
	int			nupdatefreq = 0;
	int			nqueryiddone = 0;
	int			maxqueryids = 50;
	int			i;

	qrueryid_done = palloc(sizeof(int64) * maxqueryids);

	PGQS_LWL_ACQUIRE(pgqs->querylock, LW_SHARED);

	for (i = 0; i < ncandidates; i++)
	{
		pgqsUpdateHashEntry	   *entry;
		IndexCandidate		   *cand = &candidates[i];

		hash_seq_init(&hash_seq, pgqs_update_hash);

		while ((entry = hash_seq_search(&hash_seq)) != NULL)
		{
			int			i;

			if (entry->key.dbid != MyDatabaseId)
				continue;
			if (entry->key.relid != cand->relid)
				continue;
			for (i = 0; i < cand->nattrs; i++)
			{
				if (entry->key.attnum == cand->attnum[i])
					break;
			}

			if (i == cand->nattrs)
				continue;

			for (i = 0; i < nqueryiddone; i++)
			{
				if (entry->key.queryid == qrueryid_done[i])
					break;
			}
			if (i < nqueryiddone)
				continue;

			if (nqueryiddone == maxqueryids)
			{
				maxqueryids *= 2;
				qrueryid_done = repalloc(qrueryid_done, sizeof(int64) * maxqueryids);
			}
			qrueryid_done[nqueryiddone++] = entry->key.queryid;

			/* average update per query. */
			nupdates += entry->updated_rows;
			nupdatefreq += entry->frequency;
		}

		if (nupdates > 0)
		{
			cand->nupdates = nupdates / nupdatefreq;
			cand->nupdatefreq = nupdatefreq;
		}

		nupdates = 0;
		nupdatefreq = 0;
		nqueryiddone = 0;
	}

	PGQS_LWL_RELEASE(pgqs->querylock);
}

static bool
pg_qualstats_generate_index_queries(IndexCandidate *candidates, int ncandidates)
{
	int		i;
	int		j;
	Relation	relation;
	StringInfoData buf;

	relation = RelationIdGetRelation(candidates[0].relid);
	if (relation == NULL)
		return false;

	initStringInfo(&buf);

	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *cand = &candidates[i];

		appendStringInfo(&buf, "CREATE INDEX ON %s USING %s (",
						 relation->rd_rel->relname.data,
						 cand->amname);

		for (j = 0; j < cand->nattrs; j++)
		{
			if (j > 0)
				appendStringInfo(&buf, ",");
			appendStringInfo(&buf, "%s", relation->rd_att->attrs[cand->attnum[j] - 1].attname.data);
		}

		appendStringInfo(&buf, ");");
		cand->indexstmt = palloc(buf.len + 1);
		strcpy(cand->indexstmt, buf.data);
		resetStringInfo(&buf);
	}
	RelationClose(relation);
	return true;
}

/*
 * Plan all give queries to compute the total cost.
 */
static void
pg_qualstats_fill_query_basecost(QueryInfo *queryinfos, int nqueries, int *queryidxs)
{
	int		i;

	pgqs_cost_track_enable = true;

	/* 
	 * plan each query and get its cost
	 */
	for (i = 0; i < nqueries; i++)
	{
		int		index = (queryidxs != NULL) ? queryidxs[i] : i;

		pg_qualstats_plan_query(queryinfos[index].query);
		queryinfos[i].cost = pgqs_plan_cost;
	}

	pgqs_cost_track_enable = false;
}

/* Plan a given query */
static void
pg_qualstats_plan_query(const char *query)
{
	StringInfoData	explainquery;

	if (query == NULL)
		return;

	hypo_is_enabled = true;

	initStringInfo(&explainquery);
	appendStringInfoString(&explainquery, query);
	SPI_execute(query, false, 0);

	hypo_is_enabled = false;
}

/*
 * check individiual index benefit for each query and fill in the matrix.
 */
static void
pg_qualstats_compute_index_benefit(IndexCombContext *context,
									int nqueries, int *queryidxs)
{
	IndexCandidate *candidates = context->candidates;
	QueryInfo  *queryinfos = context->queryinfos;
	double	  **benefit = context->benefitmat;
	int 		ncandidates = context->ncandidates;
	int			i;

	pgqs_cost_track_enable = true;

	for (i = 0; i < ncandidates; i++)
	{
		IndexCandidate *cand = &candidates[i];
		Oid			idxid;
		BlockNumber	relpages;
		int		j;

		/* 
		 * If candiate is already identified as it has no value or it is
		 * seletced in the final combination then in the recheck its benefits.
		 */
		if (!cand->isvalid || cand->isselected)
			continue;

		idxid = hypo_create_index(cand->indexstmt, &relpages);

		/* If candidate overhead is not yet computed then do it now */
		if (cand->overhead == 0)
			cand->overhead = pg_qualstats_get_index_overhead(cand, relpages);

		/* 
		 * replan each query and compute the total weighted cost by multiplying
		 * each query cost with its frequency.
		 */
		for (j = 0; j < nqueries; j++)
		{
			int		index = (queryidxs != NULL) ? queryidxs[j] : j;

			pg_qualstats_plan_query(queryinfos[index].query);

			if (pg_qualstats_is_index_useful(queryinfos[index].cost,
										 	 pgqs_plan_cost, cand->overhead))
			{
				benefit[index][i] = queryinfos[index].cost - pgqs_plan_cost;
			}
			else
				benefit[index][i] = 0;
		}

		hypo_index_remove(idxid);
	}

	pgqs_cost_track_enable = false;
}

static double
pg_qualstats_get_index_overhead(IndexCandidate *cand, BlockNumber relpages)
{
	double		T = relpages;
	double		index_pages;
	double		update_io_cost;
	double		update_cpu_cost;
	double		overhead;
	int			navgupdate = cand->nupdates;
	int			nfrequency = cand->nupdatefreq;

	/* 
	 * We are commputing the page acccess cost and tuple cost based on total
	 * accumulated tuple count so we don't need to use update query frequency.
	 */
	index_pages = (2 * T * navgupdate) / (2 * T + navgupdate);
	update_io_cost = (index_pages * nfrequency) * random_page_cost;
	update_cpu_cost = (navgupdate * nfrequency) * cpu_tuple_cost;

	overhead = update_io_cost + update_cpu_cost;

	/* XXX overhead of index based on number of size and number of columns. */
	overhead += T;
	overhead += (cand->nattrs * 1000);

	return overhead;
}

static bool
pg_qualstats_is_index_useful(double basecost, double indexcost,
							 double indexoverhead)
{
	if ((indexcost < basecost * 0.95) &&
		(indexcost + indexoverhead < basecost))
		return true;

	return false;
}

#if 0
static Oid
pg_qualstats_create_hypoindex(const char *index)
{
	int				ret;
	Oid				idxid;
	StringInfoData	hypoindex;

	initStringInfo(&hypoindex);
	appendStringInfoString(&hypoindex, "SELECT indexrelid FROM hypopg_create_index('");
	appendStringInfoString(&hypoindex, index);
	appendStringInfoString(&hypoindex, "')");

	ret = SPI_execute(hypoindex.data, false, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		elog(ERROR, "pg_qualstat: could not create hypo index");
	}

	idxid = atooid(SPI_getvalue(SPI_tuptable->vals[0],
				 SPI_tuptable->tupdesc,
				 1));

	Assert(OidIsValid(idxid));

	return idxid;
}

static void
pg_qualstats_drop_hypoindex(Oid idxid)
{
	int				ret;
	StringInfoData	hypoindex;

	initStringInfo(&hypoindex);
	if (OidIsValid(idxid))
		appendStringInfo(&hypoindex, "SELECT hypopg_drop_index(%d)", idxid);
	else
		appendStringInfo(&hypoindex, "SELECT hypopg_reset_index()");

	ret = SPI_execute(hypoindex.data, false, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		elog(ERROR, "pg_qualstat: could not drop hypo index");
	}

#if 0
	if (!parse_bool(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1), &res))
	{
		SPI_finish();
		elog(ERROR, "pg_qualstat: drop hypo index should return boolean");
	}

	if (!res)
	{
		SPI_finish();
		elog(ERROR, "pg_qualstat: could not drop hypo index");
	}
#endif
}

static int
pg_qualstats_hypoindex_size(Oid idxid)
{
	int				ret;
	uint64			size;
	StringInfoData	hypoindex;

	initStringInfo(&hypoindex);
	appendStringInfo(&hypoindex, "SELECT hypopg_relation_size(%d)", idxid);

	ret = SPI_execute(hypoindex.data, false, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		elog(ERROR, "pg_qualstat: could not get hypo index size");
	}

	size = strtou64(SPI_getvalue(SPI_tuptable->vals[0],
					SPI_tuptable->tupdesc,
					1), NULL, 10);

	return size;
}
#endif

static int
pg_qualstats_get_best_candidate(IndexCombContext *context)
{
	IndexCandidate *candidates = context->candidates;
	QueryInfo	   *queryinfos = context->queryinfos;
	double		  **benefitmat = context->benefitmat;
	int 	ncandidates = context->ncandidates;
	int		i;
	int		bestcandidx = -1;
	double	max_benefit = 0;
	double	benefit;

	for (i = 0; i < ncandidates; i++)
	{
		int	j;

		if (!candidates[i].isvalid || candidates[i].isselected)
			continue;

		benefit = 0;

		for (j = 0; j < context->nqueries; j++)
			benefit += (benefitmat[j][i] * queryinfos[j].frequency);

		if (benefit == 0)
		{
			candidates[i].isvalid = false;
			continue;
		}

		if (benefit > max_benefit)
		{
			max_benefit = benefit;
			bestcandidx = i;
		}
	}

	return bestcandidx;
}
