/*-------------------------------------------------------------------------
 *
 * query_advisor.c
 *		Recommend potentially useful indexes based on the stats collected so
 *		far
 *
 * Copyright (c) 2023, EnterpriseDB
 *
 * IDENTIFICATION
 *	  contrib/edb_advisor/query_advisor.c (FIXME: tool name and path)
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"

#include "include/hypopg.h"
#include "include/hypopg_index.h"
#include "include/pg_qualstats.h"
#include "include/query_advisor.h"

/*---- Function declarations ----*/
extern PGDLLEXPORT Datum query_advisor_index_recommendations(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(query_advisor_index_recommendations);

/* define this to enable the developer level debugging informations */
//#define DEBUG_INDEX_ADVISOR

/*
 * If 'qa_disable_stats' is set to indicate the pg_qualstats's executor
 * hook to not to collect the stattistics for those queries because those
 * are just executed by index advisor in order to evaluate the indexes.
 */
bool qa_disable_stats = false;

/* 
 * Advisor's planner hook will store the cost of the last planned query in
 * 'qa_plan_cost' variable.
 */
static float qa_plan_cost = 0.0;
planner_hook_type prev_planner_hook = NULL;

/* Array of the final selected indexes */
typedef struct FinalIndexInfo
{
	char   *indexstmt;
	float4	pctcostbenefit;
	int64	size;

} FinalIndexInfo;

/* Index candidate information */
typedef struct CandidateInfo
{
	Oid		relid;			/* relation id */
	Oid		amoid;			/* candidate's index access method oid */
	char   *amname;			/* candidate's index access method name */
	int		nattrs;			/* number of key attributes */
	int	   *attnum;			/* key attribute number array */
	int		nqueryids;		/* number of queryids related to this index */
	int64  *queryids;		/* queryids array */
	int64	nupdates;		/* total number of update done on key attributes
							   involved in this candididate */
	int		nupdatefreq;	/* frequency of the updates on candidate keys */
	double	overhead;		/* overhead of the candidate */
	bool	isvalid;		/* is candidate still valid (not rejected)*/
	bool	isselected;		/* is candidate already selected in final list */
	FinalIndexInfo indexinfo; /* index related info which we want to give as
								 final output. */
} CandidateInfo;

/* workload query information */
typedef struct QueryInfo
{
	double	cost;			/* query based cost */
	int		frequency;		/* frequency of the execution */
	bool	failed;			/* query execution failed do not try again */
	char   *query;			/* actual query text */
} QueryInfo;

/*
 * Index advisor context, store various intermediate informations while
 * comparing candidate indexes and finding out the best candidates.
 */
typedef struct IndexAdvisorContext
{
	int		nqueries;			/* total number of workload queries */
	int		ncandidates;		/* total numbed of index candidates */
	int		maxcand;
	CandidateInfo  *candidates;	/* array of candidates */
	QueryInfo	   *queryinfos;	/* array of QueryInfo */
	MemoryContext	queryctx;	/* reference to per query context */

	/*
	 * two dimentional matrix of size (ncandidate X nqueries) where each slot
	 * represent the cost reduction a particular query got due to a particular
	 * candidate index.
	 */
	double		**benefitmat;
} IndexAdvisorContext;

/*
 * query qual information from pg_qualstats and group the related quals
 * together to generate the multi column indexes.
 *
 * XXX instead of doing union of 2 simmilar queries is there any other way to
 * output two independent rows for lrelid and rrelid.
 *
 * TODO: We should consider BRIN index only on very large tables
 *
 * XXX We should consider GIN/GIST indexes, for that we need to create
 * support for them in hypopg.
 */
char *query =
"WITH pgqs AS ("
"\n          (SELECT dbid, min(am.oid) amoid, amname, qualid, qualnodeid,"
"\n            (lrelid, lattnum,"
"\n            opno, eval_type)::qual AS qual, queryid,"
"\n            round(avg(execution_count)) AS execution_count,"
"\n            sum(occurences) AS occurences,"
"\n            round(sum(nbfiltered)::numeric / sum(occurences)) AS avg_filter,"
"\n            CASE WHEN sum(execution_count) = 0"
"\n              THEN 0"
"\n              ELSE round(sum(nbfiltered::numeric) / sum(execution_count) * 100)"
"\n            END AS avg_selectivity"
"\n          FROM query_advisor_qualstats() q"
"\n          JOIN pg_catalog.pg_database d ON q.dbid = d.oid"
"\n          JOIN pg_catalog.pg_operator op ON op.oid = q.opno"
"\n          JOIN pg_catalog.pg_amop amop ON amop.amopopr = op.oid"
"\n          JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod"
"\n          WHERE d.datname = current_database()"
"\n          AND eval_type = 'f'"
"\n			 AND lrelid IS NOT NULL"
"\n          AND lrelid != 0"
"\n          GROUP BY dbid, amname, qualid, qualnodeid, lrelid,"
"\n            lattnum, rattnum, opno, eval_type, queryid ORDER BY lattnum)"
"\n			UNION ALL "
"\n          (SELECT dbid, min(am.oid) amoid, amname, qualid, qualnodeid,"
"\n            (rrelid, rattnum,"
"\n            opno, eval_type)::qual AS qual, queryid,"
"\n            round(avg(execution_count)) AS execution_count,"
"\n            sum(occurences) AS occurences,"
"\n            round(sum(nbfiltered)::numeric / sum(occurences)) AS avg_filter,"
"\n            CASE WHEN sum(execution_count) = 0"
"\n              THEN 0"
"\n              ELSE round(sum(nbfiltered::numeric) / sum(execution_count) * 100)"
"\n            END AS avg_selectivity"
"\n          FROM query_advisor_qualstats() q"
"\n          JOIN pg_catalog.pg_database d ON q.dbid = d.oid"
"\n          JOIN pg_catalog.pg_operator op ON op.oid = q.opno"
"\n          JOIN pg_catalog.pg_amop amop ON amop.amopopr = op.oid"
"\n          JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod"
"\n          WHERE d.datname = current_database()"
"\n          AND eval_type = 'f'"
"\n          AND rrelid != 0"
"\n			 AND rrelid IS NOT NULL"
"\n          GROUP BY dbid, amname, qualid, qualnodeid, rrelid,"
"\n            lattnum, rattnum, opno, eval_type, queryid ORDER BY rattnum)"
"\n        ),"
"\n        -- apply cardinality and selectivity restrictions"
"\n        filtered AS ("
"\n          SELECT (qual).relid, min(amoid) amoid, amname, coalesce(qualid, qualnodeid) AS parent,"
"\n            count(*) AS weight,"
"\n            array_agg(DISTINCT((qual).attnum) ORDER BY ((qual).attnum)) AS attnumlist,"
"\n            array_agg(qualnodeid) AS qualidlist,"
"\n            array_agg(DISTINCT(queryid)) AS queryidlist"
"\n          FROM pgqs"
"\n          WHERE queryid IS NOT NULL AND"
"\n          avg_filter >= $1 AND"
"\n			 avg_selectivity >= $2"
"\n          GROUP BY (qual).relid, amname, parent"
"\n        )"
"\nSELECT * FROM filtered where amname='btree' ORDER BY relid, amname DESC, cardinality(attnumlist);";

/* static function declarations */
static FinalIndexInfo *qa_generate_advise(MemoryContext per_query_ctx,
										  int min_filter,
										  int min_selectivity,
										  int *nindexes);

static FinalIndexInfo *qa_process_rel(FinalIndexInfo *previnxinfos,
									  CandidateInfo *candidates,
									  int ncandidates, int *nindexes,
									  MemoryContext per_query_ctx);
static int qa_iterative(IndexAdvisorContext *context);
static QueryInfo *qa_get_queries(CandidateInfo *candidates,
								 int ncandidates, int *nqueries);
static bool qa_is_queryid_exists(int64 *queryids, int nqueryids,
								 int64 queryid, int *idx);
static char *qa_get_query(int64 queryid, int *freq);
static CandidateInfo *qa_get_final_candidates(CandidateInfo *candidates,
											  int *ncandidates);
static bool qa_is_candidate_exists(CandidateInfo *candidates,
								   CandidateInfo *cand,
								   int ncandidates);
static bool qa_is_index_exists(Relation rel, CandidateInfo *cand);
static void qa_remove_existing_candidates(CandidateInfo *candidates,
										  int ncandidates);

static void qa_get_updates(CandidateInfo *candidates,
						   int ncandidates);

static bool qa_generate_index_queries(CandidateInfo *candidates,
									  int ncandidates, Relation rel);
static double qa_set_basecost(QueryInfo *queryinfos, int nqueries);
static bool qa_plan_query(QueryInfo *queryinfo, bool *have_internal_subtxn,
			  			  MemoryContext oldcontext, ResourceOwner oldowner);
static void qa_compute_index_benefit(IndexAdvisorContext *context,
									 int nqueries, int *queryidxs);
static double qa_get_index_overhead(CandidateInfo *cand, Oid idxid);
static int qa_get_best_candidate(IndexAdvisorContext *context,
								 double *benefit);

#ifdef DEBUG_INDEX_ADVISOR
static void print_candidates(CandidateInfo *candidates, int ncandidates);
static void print_benefit_matrix(IndexAdvisorContext *context);
#endif

/*
 * Index advisor entry function, this will returns the index advises in form
 * of ddl statements for create index.
 */
Datum
query_advisor_index_recommendations(PG_FUNCTION_ARGS)
{
	int			min_filter = PG_GETARG_INT32(0);
	int			min_selectivity = PG_GETARG_INT32(1);
	int			nindexes = 0;
	int			counter;
	FinalIndexInfo *indexinfo;
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc		tupdesc;
		MemoryContext	oldctx;
		MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

		funcctx = SRF_FIRSTCALL_INIT();

		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(3);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "index",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "estimated_size_in_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "estimated_pct_cost_reduction",
						   FLOAT4OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldctx);

		/* generate index advises and save it for subsequent calls */
		funcctx->user_fctx = qa_generate_advise(per_query_ctx,
												min_filter,
												min_selectivity,
												&nindexes);
		funcctx->max_calls = nindexes;
		funcctx->call_cntr = 0;
	}
	funcctx = SRF_PERCALL_SETUP();
	indexinfo = (FinalIndexInfo*) funcctx->user_fctx;
	counter = funcctx->call_cntr;

	if (counter < funcctx->max_calls)
	{
		Datum		values[3];
		bool		nulls[3];
		HeapTuple	tuple;
		Datum		result;

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(indexinfo[counter].indexstmt);
		values[1] = Int64GetDatum(indexinfo[counter].size);
		values[2] = Float4GetDatum(indexinfo[counter].pctcostbenefit);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/* 
 * This function will execute the query and generate the index candidates and
 * send them to the core index advisor machinary for the further processing.
 */
static FinalIndexInfo *
qa_generate_advise(MemoryContext per_query_ctx, int min_filter,
				   int min_selectivity, int *nindexes)
{
	int			ret;
	int			i;
	int			ncandidates = 0;
	int			nrelcand = 0;
	int			idxcand = 0;
	Oid			prevrelid = InvalidOid;
	TupleDesc	tupdesc;
	CandidateInfo  *candidates;
	FinalIndexInfo *indexinfo = NULL;
	Oid		paramTypes[2] = { INT4OID, INT4OID };
	Datum	paramValues[2];

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "pg_qualstat: SPI_connect returned %d", ret);

	paramValues[0] = Int32GetDatum(min_filter);
	paramValues[1] = Int32GetDatum(min_selectivity);

	/*
	 * Execute query to get list of all index candidate by calling
	 * pg_qualstat() function and grouping the related qual together to
	 * generated multi-colum index candidates.
	 */
	ret = SPI_execute_with_args(query, 2, paramTypes, paramValues, NULL,
								true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
	}

	tupdesc = SPI_tuptable->tupdesc;
	ncandidates = SPI_processed;
	candidates = palloc0(sizeof(CandidateInfo) * ncandidates);

	/* 
	 * Read all the entires and prepare a index candidate array for further
	 * processing.
	 */
	for (i = 0; i < ncandidates; i++)
	{
		HeapTuple		tup = SPI_tuptable->vals[i];
		Datum			dat;
		bool			isnull;
		ArrayType	   *r;
		CandidateInfo *cand;

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
			indexinfo = qa_process_rel(indexinfo,
									   &candidates[idxcand],
									   nrelcand, nindexes,
									   per_query_ctx);
			nrelcand = 0;
			idxcand = i;
		}
		prevrelid = candidates[i].relid;
		nrelcand++;
	}

	/* process candidates of the last relation */
	indexinfo = qa_process_rel(indexinfo,
							   &candidates[idxcand],
							   nrelcand, nindexes,
							   per_query_ctx);

	SPI_finish();

	return indexinfo;
}

/*
 * Process index candidate and generate advise for one relation and append them
 * to a '*previnxinfos' which is a common array for all the relations.
 *
 * This functions do various processing with the input candidate like generate
 * workload queries, generate 1 and 2 column index candidates from these
 * candidates and discard candidate for which we already have existing indexes
 * and finally send them to a iterative index selection method to find out the
 * best candidates list.
 */
static FinalIndexInfo *
qa_process_rel(FinalIndexInfo *previnxinfos, CandidateInfo *candidates,
			   int ncandidates, int *nindexes, MemoryContext per_query_ctx)
{
	CandidateInfo   *finalcand;
	QueryInfo   *queryinfos;
	IndexAdvisorContext	context;
	MemoryContext 		oldcontext;
	FinalIndexInfo	   *indexinfos = NULL;
	Relation			rel;
	int		nqueries;
	int		nnewindexes;
	int		prev_indexes = *nindexes;
	int		i;

#ifdef DEBUG_INDEX_ADVISOR
	elog(NOTICE, "candidate Relation %d", candidates[0].relid);
#endif

	if (ncandidates == 0)
		return previnxinfos;

	/*
	 * Process all candidate and get the list of all the unique queryids along
	 * with the actual queries.
	 */
	queryinfos = qa_get_queries(candidates, ncandidates, &nqueries);

	/*
	 * Process all candidates and generate all distinct one and two column
	 * index candidates.
	 *
	 * XXX in common cases generally one and two column indexes are useful so
	 * for now we can just generate those two avoid very huge number of
	 * candidates which can increasing the time complexity.  In future if
	 * required we can consider candidates with more columns.
	 */
	finalcand = qa_get_final_candidates(candidates, &ncandidates);
	qa_remove_existing_candidates(finalcand, ncandidates);

	/*
	 * Process all the candidates and get the total update count we are
	 * performing on key attributes for each candidate.
	 */
	qa_get_updates(finalcand, ncandidates);

	rel = try_relation_open(candidates[0].relid, AccessShareLock);
	if (rel == NULL)
		return previnxinfos;

	/*
	 * Generate index creation statement for each candidate.  We need to output
	 * the index statements so store them in per query context.
	 */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (!qa_generate_index_queries(finalcand, ncandidates, rel))
	{
		relation_close(rel, AccessShareLock);
		return previnxinfos;
	}
	MemoryContextSwitchTo(oldcontext);

#ifdef DEBUG_INDEX_ADVISOR
	print_candidates(finalcand, ncandidates);
#endif

	/* prepare context for index advisor processing */
	context.candidates = finalcand;
	context.ncandidates = ncandidates;
	context.queryinfos = queryinfos;
	context.nqueries = nqueries;
	context.maxcand = ncandidates;
	context.queryctx = per_query_ctx;

	/*
	 * Process index candidates with iterative approach and find out the best
	 * candidates which can produce maximum benefit to the workload queries.
	 */
	nnewindexes = qa_iterative(&context);

	/*
	 * No index selected for this relation so directly return the previous
	 * array.
	 */
	if (nnewindexes == 0)
	{
		relation_close(rel, AccessShareLock);
		return previnxinfos;
	}

	(*nindexes) += nnewindexes;

	/* 
	 * We need to preserve the array across multiple calls of the SRF function
	 * so allocate it in per query context.
	 */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (prev_indexes == 0)
		indexinfos = (FinalIndexInfo *) palloc(nnewindexes * sizeof(FinalIndexInfo));
	else
		indexinfos = (FinalIndexInfo* ) repalloc(previnxinfos, (*nindexes) * sizeof(FinalIndexInfo));
	MemoryContextSwitchTo(oldcontext);

	/* append new indexes to the existing index array */
	for (i = 0; i < ncandidates; i++)
	{
		if (finalcand[i].isselected)
		{
			memcpy(&indexinfos[prev_indexes++], &finalcand[i].indexinfo,
				   sizeof(FinalIndexInfo));
		}
	}

	relation_close(rel, AccessShareLock);

	return indexinfos;
}

/*
 * Iterative approach for finding the best set of indexes.
 */
int
qa_iterative(IndexAdvisorContext *context)
{
	int			i;
	int			ncandidates = context->ncandidates;
	int			nseletced = 0;
	int			nqueries = context->nqueries;
	int		   *queryidxs = NULL;
	double		totalcost;
	QueryInfo  *queryinfos = context->queryinfos;
	CandidateInfo *candidates = context->candidates;

	/* allocate memory for benefit matrix */
	context->benefitmat = (double **) palloc0(nqueries * sizeof(double *));
	for (i = 0; i < nqueries; i++)
		context->benefitmat[i] = (double *) palloc0(ncandidates * sizeof(double));

	/*
	 * The main iterative algorithm for selecting the best candidate indexes.
	 *
	 * Step1: At first plan all the queries without creating any new index and
	 * set that as a base cost for each query.
	 *
	 * Step2: Now replan all the queries with each index and prepare a
	 * index-query benefit matrix.  Each element in this matrix will represent
	 * the cost benefit for a given query if that particular index exists.
	 *
	 * Step3: In this step we will compute the total benefit for each index
	 * i.e. (Sum of each query benefit * query execution frequency).
	 *
	 * Step4: Shortlist the index which is giving the maximum benefit and
	 * assume that this index is now selected.  So update the base cost of each
	 * query which got benefitted with this index.
	 *
	 * Step5: Go to Step2 and repeat the process to select the next best
	 * candidate.  This time instead of replanning all the queries, only replan
	 * the queries which got benefitted by the previous best candidate. And
	 * also note that in this round the previously selected candidate are out
	 * of the selection process.
	 */
	totalcost = qa_set_basecost(queryinfos, nqueries);
	while (true)
	{
		int		bestcand;
		int		i;
		float4	pct_benefit;
		double  benefit;
		BlockNumber	relpages;

		/*
		 * Create each index one at a time and replan every query and fill
		 * index-query benefit matrix.
		 */
		qa_compute_index_benefit(context, nqueries, queryidxs);

#ifdef DEBUG_INDEX_ADVISOR
		print_benefit_matrix(context);
#endif

		/*
		 * Compute the overall benefit of all the candidate and get the  best
		 * candidate.
		 */
		bestcand = qa_get_best_candidate(context, &benefit);
		if (bestcand == -1)
			break;

		/*
		 * Add best candidates to the path and create the hypoindex for this
		 * candidate and reiterate for the next round.  For this index we
		 * create hypoindex and do not drop so that next round assume this
		 * index is already exist now and check benefit of each candidate by
		 * assuming this candidate is already finalized.
		 */
		hypo_create_index(candidates[bestcand].indexinfo.indexstmt, &relpages);
		candidates[bestcand].indexinfo.size = relpages * BLCKSZ;

		pct_benefit =  (benefit / totalcost) * 100;
		candidates[bestcand].indexinfo.pctcostbenefit = pct_benefit;

		/*
		 * Allocate the memory to remember the query indexes which got
		 * benefitted by this index so that while selecting the next best
		 * candidate we can only plan these queries because benefit matrix for
		 * other queries should not be impacted.
		 */
		if (queryidxs == NULL)
			queryidxs = (int *) palloc (nqueries * sizeof (int));

		nqueries = 0;

		/* 
		 * Next best candidate is seleted so update the query base cost for all
		 * benefitted queries before going for next iteration.
		 */
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

		/* mark candidate as selected. */
		candidates[bestcand].isselected = true;
		nseletced++;
	}

	/* reset all hypo indexes created during processing */
	hypo_index_reset();

	return nseletced;
}

/*
 * Process all candidate and get the list of all the unique queryids from the
 * queryids stored in each candidate and also fetch the actual query from the
 * workload hash and store in query info.
 */
static QueryInfo *
qa_get_queries(CandidateInfo *candidates, int ncandidates,
			   int *nqueries)
{
	int			i;
	int			j;
	int			nids = 0;
	int			maxids = ncandidates;
	int64	   *queryids;
	QueryInfo  *queryinfos;

	queryids = palloc(maxids * sizeof(int64));

	/*
	 * Process through the queryids stored in each candidate and prepare a
	 * array of all distinct queryids. 
	 */
	for (i = 0; i < ncandidates; i++)
	{
		CandidateInfo *cand = &candidates[i];

		for (j = 0; j < cand->nqueryids ; j++)
		{
			int index = -1;

			if (qa_is_queryid_exists(queryids, nids, cand->queryids[j],
									 &index))
				continue;

			if (nids >= maxids)
			{
				maxids *= 2;
				queryids = repalloc(queryids, maxids * sizeof(int64));
			}
			queryids[nids] = cand->queryids[j];
			nids++;
		}
	}

	queryinfos = (QueryInfo *) palloc0(nids * sizeof(QueryInfo));

	/* get the actual query and execution frequency for each queryid */
	for (i = 0; i < nids; i++)
	{
		queryinfos[i].query = qa_get_query(queryids[i],
										   &queryinfos[i].frequency);
#ifdef DEBUG_INDEX_ADVISOR
		elog(NOTICE, "query %d: %s-freq:%d", i, queryinfos[i].query, queryinfos[i].frequency);
#endif
	}

	pfree(queryids);

	*nqueries = nids;

	return queryinfos;
}

static bool
qa_is_queryid_exists(int64 *queryids, int nqueryids, int64 queryid,
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
qa_get_query(int64 queryid, int *freq)
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
	{
		PGQS_LWL_RELEASE(pgqs->querylock);
		return NULL;
	}

	query = palloc0(entry->qrylen);
	strcpy(query, entry->querytext);

	*freq = entry->frequency;

	PGQS_LWL_RELEASE(pgqs->querylock);

	return query;
}

/*
 * From given index candidate list generate single column and two columns
 * index candidate array.
 */
static CandidateInfo *
qa_get_final_candidates(CandidateInfo *candidates, int *ncandidates)
{
	CandidateInfo *finalcand;
	CandidateInfo	cand;
	int				nfinalcand = 0;
	int				nmaxcand = *ncandidates;
	int				i;
	int				j;
	int				k = 0;

	/*
	 * Allocate a initial size for the final candidate array, we will
	 * expand this if we need to add more elements.
	 */
	finalcand = palloc(sizeof(CandidateInfo) * nmaxcand);

	/* genrate all one and tow length index combinations. */
	for (i = 0; i < *ncandidates; i++)
	{
		for (j = 0; j < candidates[i].nattrs; j++)
		{
			/* generae one column index */
			memcpy(&cand, &candidates[i], sizeof(CandidateInfo));
			cand.nattrs = 1;
			cand.attnum = (int *) palloc0(sizeof(int));
			cand.attnum[0] = candidates[i].attnum[j];

			if (!qa_is_candidate_exists(finalcand, &cand, nfinalcand))
			{
				/*
				 * If the number of elements are already equal to the max size
				 * then double the size and repalloc.
				 */
				if (nfinalcand == nmaxcand)
				{
					nmaxcand *= 2;
					finalcand = repalloc(finalcand,
										 sizeof(CandidateInfo) * (nmaxcand));
				}
				memcpy(&finalcand[nfinalcand], &cand, sizeof(CandidateInfo));
				nfinalcand++;
			}

			/* generate two column indexes. */
			for (k = 0; k < candidates[i].nattrs; k++)
			{
				if (k == j)
					continue;

				cand.nattrs = 2;
				cand.attnum = (int *) palloc0(sizeof(int) * 2);

				cand.attnum[0] = candidates[i].attnum[j];
				cand.attnum[1] = candidates[i].attnum[k];

				/*
				 * TODO: for brin index order of column doesn't matter so if
				 * the index with all column same as new candidate exists then
				 * we can consider this as duplicate (no need to check strict
				 * column order)
				 */
				if (!qa_is_candidate_exists(finalcand, &cand, nfinalcand))
				{
					if (nfinalcand == nmaxcand)
					{
						nmaxcand *= 2;
						finalcand = repalloc(finalcand,
											 sizeof(CandidateInfo) * (nmaxcand));
					}
					memcpy(&finalcand[nfinalcand], &cand, sizeof(CandidateInfo));
					nfinalcand++;
				}
			}
		}
	}

	*ncandidates = nfinalcand;

	return finalcand;
}

/*
 * Check whether the inpute candidate already present in the candidate array.
 */
static bool
qa_is_candidate_exists(CandidateInfo *candidates,
								  CandidateInfo *cand,
								  int ncandidates)
{
	int		i;
	int		j;

	for (i = 0; i < ncandidates; i++)
	{
		CandidateInfo *oldcand = &candidates[i];

		if (oldcand->nattrs != cand->nattrs || oldcand->amoid != cand->amoid)
			continue;

		for (j = 0; j < oldcand->nattrs; j++)
		{
			if (oldcand->attnum[j] != cand->attnum[j])
				break;
		}

		if (j == oldcand->nattrs)
			return true;
	}

	return false;
}

/*
 * Check whether there is any existing index on the relation with same
 * attributes as input candidate.
 */
static bool
qa_is_index_exists(Relation rel, CandidateInfo *cand)
{
	ListCell   *lc;
	List	   *index_oids = RelationGetIndexList(rel);

	/*
	 * Iterate through each index of the relation and determine whether the
	 * input candidate matches any existing index.
	 */
	foreach(lc, index_oids)
	{
		Oid			idxid = lfirst_oid(lc);
		Relation	irel = index_open(idxid, AccessShareLock);
		int			nattr;
		int			i;

		nattr = IndexRelationGetNumberOfAttributes(irel);

		/*
		 * If the number of attributes or the access method of the index is not
		 * the same as those of the candidate then there is no point in
		 * matching the attribute numbers.
		 */
		if (cand->nattrs != nattr || cand->amoid != irel->rd_rel->relam)
		{
			index_close(irel, AccessShareLock);
			continue;
		}

		/*
		 * Now compare each attribute number and if all attribute matches then
		 * return true.
		 */
		for (i = 0; i < nattr; i++)
		{
			if (cand->attnum[i] != irel->rd_index->indkey.values[i])
				break;
		}

		index_close(irel, AccessShareLock);

		if (i == nattr)
			return true;
	}

	return false;
}

/*
 * Process each candidate and if we found a match with any of the existing
 * index of the relation then mark that candidate invalid.  And if the
 * candidate is marked invalid then it will be excluded from further processing
 * of the index advisor alorithm.
 */
static void
qa_remove_existing_candidates(CandidateInfo *candidates,
										 int ncandidates)
{
	Relation	relation;
	int			i;

	relation = RelationIdGetRelation(candidates[0].relid);
	if (relation == NULL)
		return;

	for (i = 0; i < ncandidates; i++)
	{
		CandidateInfo *cand = &candidates[i];

		if (qa_is_index_exists(relation, cand))
			cand->isvalid = false;
	}

	RelationClose(relation);
}

/*
 * Process each index candidate and compute the number of updated tuple for
 * each index candidates based on the index column update counts.
 */
static void
qa_get_updates(CandidateInfo *candidates, int ncandidates)
{
#if PG_VERSION_NUM >= 140000	
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
		CandidateInfo		   *cand = &candidates[i];

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
#endif	
}

/*
 * Generate index creation statement for each candidate and store in the index
 * candidate structure for later use.
 */
static bool
qa_generate_index_queries(CandidateInfo *candidates, int ncandidates,
						  Relation rel)
{
	int		i;
	int		j;
	StringInfoData buf;

	initStringInfo(&buf);

	for (i = 0; i < ncandidates; i++)
	{
		CandidateInfo *cand = &candidates[i];

		appendStringInfo(&buf, "CREATE INDEX ON %s USING %s (",
						 NameStr(rel->rd_rel->relname),
						 cand->amname);

		for (j = 0; j < cand->nattrs; j++)
		{
			int attnum = cand->attnum[j] - 1;

			if (j > 0)
				appendStringInfo(&buf, ",");
			appendStringInfo(&buf, "%s",
							 NameStr(rel->rd_att->attrs[attnum].attname));
		}

		appendStringInfo(&buf, ");");
		cand->indexinfo.indexstmt = palloc(buf.len + 1);
		strcpy(cand->indexinfo.indexstmt, buf.data);
		resetStringInfo(&buf);
	}

	return true;
}

/*
 * Plan all given queries without any new index and fill base cost for each
 * query.
 */
static double
qa_set_basecost(QueryInfo *queryinfos, int nqueries)
{
	int		i;
	double	total_cost = 0.0;
	MemoryContext	oldcontext = CurrentMemoryContext;
	ResourceOwner	oldowner = CurrentResourceOwner;
	bool	have_internal_subtxn = false;

	/* enable cost tracking before planning queries */
	qa_disable_stats = true;

	/* 
	 * plan each query and get its cost
	 */
	for (i = 0; i < nqueries; i++)
	{

		if (!qa_plan_query(&queryinfos[i], &have_internal_subtxn, oldcontext,
						   oldowner))
			continue;

		queryinfos[i].cost = qa_plan_cost;
		total_cost += qa_plan_cost;
	}

	if (have_internal_subtxn)
	{
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;
		have_internal_subtxn = false;
	}

	/* disable cost tracking */
	qa_disable_stats = false;

	return total_cost;
}

#if 0
/* Plan a given query */
static void
qa_plan_query(const char *query)
{
	StringInfoData	explainquery;

	if (query == NULL)
		return;

	/*
	 * Enable hypo index injection so that we can see the cost with the
	 * hypothetical indexes we have created.
	 */
	hypo_is_enabled = true;

	initStringInfo(&explainquery);
	appendStringInfoString(&explainquery, query);
	SPI_execute(query, false, 0);

	/* diable hypo index injection */
	hypo_is_enabled = false;
}
#endif

static bool
qa_plan_query(QueryInfo *queryinfo, bool *have_internal_subtxn,
			  MemoryContext oldcontext, ResourceOwner oldowner)
{
	if (!(*have_internal_subtxn))
	{
		BeginInternalSubTransaction(NULL);
		MemoryContextSwitchTo(oldcontext);
		*have_internal_subtxn = true;
	}

	PG_TRY();
	{
		Oid	   *paramTypes = palloc0(sizeof(Oid));
		int		numparam = 0;
		List   *parseTreeList = NULL;
		Node   *parseTreeNode;
		List   *queryTreeList = NULL;
		Query  *query;
		char   *querytext = queryinfo->query;
		PlannedStmt *plan;

		parseTreeList = pg_parse_query(querytext);
		if (list_length(parseTreeList) != 1)
			ereport(ERROR, (errmsg("cannot execute multiple utility events")));

		parseTreeNode = (Node *) linitial(parseTreeList);

		queryTreeList = pg_analyze_and_rewrite_varparams((RawStmt *) parseTreeNode,
														 querytext,
														 &paramTypes,
														 &numparam,
														 NULL);
		if (list_length(queryTreeList) != 1)
			ereport(ERROR, (errmsg("can only execute a single query")));

		query = (Query *) linitial(queryTreeList);
		hypo_is_enabled = true;
		plan = pg_plan_query(query, querytext, 0, NULL);
		hypo_is_enabled = false;
		qa_plan_cost = plan->planTree->total_cost;
	}
	PG_CATCH();
	{
		/* Abort the inner transaction */
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;		
		*have_internal_subtxn = false;
		queryinfo->failed = true;
	}
	PG_END_TRY();

	if (queryinfo->failed)
		return false;
	else
		return true;
}

/*
 * Fill in the index-query benefit matrix by replanning all queries in the
 * presence of a hypothetical index with respect to each individual index
 * candidate.
 */
static void
qa_compute_index_benefit(IndexAdvisorContext *context,
						 int nqueries, int *queryidxs)
{
	int 	ncandidates = context->ncandidates;
	int		i;
	CandidateInfo  *candidates = context->candidates;
	QueryInfo	   *queryinfos = context->queryinfos;
	double		  **benefit = context->benefitmat;
	MemoryContext	oldcontext = CurrentMemoryContext;
	ResourceOwner	oldowner = CurrentResourceOwner;

	/*
	 * Make sure qualstats does not collect statistics for queries executed
	 * by the advisor.
	 */
	qa_disable_stats = true;

	/*
	 * Loop through each candidate and Replan all the queries indexed by the
	 * 'queryidxs' in the presence of each individual candidate index and
	 * record the plan cost reduction for each query in the benefit matrix.
	 */
	for (i = 0; i < ncandidates; i++)
	{
		CandidateInfo   *cand = &candidates[i];
		BlockNumber		relpages;
		Oid		idxid = InvalidOid;
		int		j;
		bool	have_internal_subtxn = false;

		/*
		 * Skip the candidate if it has been marked invalid or are already on
		 * the final list.
		 */
		if (!cand->isvalid || cand->isselected)
			continue;

		if (!have_internal_subtxn)
		{
			BeginInternalSubTransaction(NULL);
			MemoryContextSwitchTo(oldcontext);
			have_internal_subtxn = true;
		}
		/* create a hypothetical index for the candidate before replanning */
		PG_TRY();
		{
			idxid = hypo_create_index(cand->indexinfo.indexstmt, &relpages);
		}
		PG_CATCH();
		{
			FlushErrorState();

			/* Abort the inner transaction */
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;
			have_internal_subtxn = false;
			cand->isvalid = false;
		}
		PG_END_TRY();

		if (!OidIsValid(idxid))
			continue;

		/* If candidate's overhead is not yet computed then do it now */
		if (cand->overhead == 0)
			cand->overhead = qa_get_index_overhead(cand, relpages);

		/* replan each query and update benefit matrix */
		for (j = 0; j < nqueries; j++)
		{
			int		qidx = (queryidxs != NULL) ? queryidxs[j] : j;

			if (queryinfos[qidx].failed)
				continue;

			if (!qa_plan_query(&queryinfos[qidx], &have_internal_subtxn, 
							   oldcontext, oldowner))
				continue;

			/*
			 * If the index reduces the cost at least by 5% and the cost with
			 * the index including the overhead imposed by the index is lesser
			 * than the cost without the index then consider this index useful
			 * and update the benefit matrix slot with the plan cost reduction.
			 * Otherwise, set the benefit to 0.
			 */
			if ((qa_plan_cost < queryinfos[qidx].cost * 0.95) &&
				(qa_plan_cost + cand->overhead < queryinfos[qidx].cost))
			{
				benefit[qidx][i] = queryinfos[qidx].cost - qa_plan_cost;
			}
			else
				benefit[qidx][i] = 0;
		}

		if (have_internal_subtxn)
		{
			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;
			have_internal_subtxn = false;
		}

		/* now we can remove the hypothetical index */
		hypo_index_remove(idxid);
	}

	qa_disable_stats = false;
}

/*
 * Compute the overhead of given candidate index
 */
static double
qa_get_index_overhead(CandidateInfo *cand, BlockNumber relpages)
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

	/* XXX overhead of index based on index size and the number of columns. */
	overhead += T;
	overhead += (cand->nattrs * 1000);

	return overhead;
}

/*
 * Compute benefit of each candidate and return the index of the candidate
 * which is generating maximum total benefit.
 */
static int
qa_get_best_candidate(IndexAdvisorContext *context, double *cost_benefit)
{
	CandidateInfo   *candidates = context->candidates;
	QueryInfo	    *queryinfos = context->queryinfos;
	double		   **benefitmat = context->benefitmat;
	int 	ncandidates = context->ncandidates;
	int		bestcandidx = -1;
	int		i;
	double	max_benefit = 0;
	double	benefit;

	/*
	 * Loop through index query benefit matrix and indetify the candidate
	 * which is generating maximum total benefit.
	 */
	for (i = 0; i < ncandidates; i++)
	{
		int	j;

		/*
		 * Skip the candidate if it has been marked invalid or are already on
		 * the final list.
		 */
		if (!candidates[i].isvalid || candidates[i].isselected)
			continue;

		benefit = 0;

		/* total_benefit = Sum(benefit(Qi) * frequency(Qi)) */
		for (j = 0; j < context->nqueries; j++)
			benefit += (benefitmat[j][i] * queryinfos[j].frequency);

		if (benefit > max_benefit)
		{
			max_benefit = benefit;
			bestcandidx = i;
		}
	}

	*cost_benefit = max_benefit;

	return bestcandidx;
}

/* planner hook */
PlannedStmt *
qa_planner(Query *parse,
#if PG_VERSION_NUM >= 130000
		  const char *query_string,
#endif
		  int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;

	/* Invoke the planner, possibly via a previous hook user */
	if (prev_planner_hook)
		result = prev_planner_hook(parse,
#if PG_VERSION_NUM >= 130000
					   query_string,
#endif
					   cursorOptions,
					   boundParams);
	else
		result = standard_planner(parse,
#if PG_VERSION_NUM >= 130000
					  query_string,
#endif
					  cursorOptions,
					  boundParams);

	/* remember the plan cost */
	qa_plan_cost = result->planTree->total_cost;

	return result;
}

#ifdef DEBUG_INDEX_ADVISOR

static void
print_candidates(CandidateInfo *candidates, int ncandidates)
{
	int			i;

	for (i = 0; i < ncandidates; i++)
	{
		elog(NOTICE, "Index: %s: update: %lld freq: %d valid:%d", candidates[i].indexinfo.indexstmt,
			 (long long int) candidates[i].nupdates, candidates[i].nupdatefreq, candidates[i].isvalid);
	}
}

static void
print_benefit_matrix(IndexAdvisorContext *context)
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
#endif
