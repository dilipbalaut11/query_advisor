/*-------------------------------------------------------------------------
 *
 * generate_advise.c
 *
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
#include "include/pg_qualstats.h"
#include "include/query_advisor.h"

/*---- Function declarations ----*/
extern PGDLLEXPORT Datum pg_qualstats_test_index_advise(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_qualstats_test_index_advise);

/* 
 * If 'pgqs_cost_track_enable' is set then the planner will start tracking the
 * cost of queries being planned and compute total cost in
 * 'pgqs_plan_total_cost'
 */
bool pgqs_cost_track_enable = false;
static float pgqs_plan_cost = 0.0;
planner_hook_type prev_planner_hook = NULL;

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
static int pg_qualstats_get_best_candidate(IndexCombContext *context);

MemoryContext HypoMemoryContext;

/* planner hook */
PlannedStmt *
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


#if 0
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

	LWLockAcquire(pgqs->querylock, LW_SHARED);
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

	LWLockRelease(pgqs->querylock);

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

	LWLockAcquire(pgqs->querylock, LW_SHARED);

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

	LWLockRelease(pgqs->querylock);
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
