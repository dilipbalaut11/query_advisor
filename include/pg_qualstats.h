/*-------------------------------------------------------------------------
 *
 * pg_qualstats.h: 
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * The implementation is heavily inspired by pg_stat_statements
 *
 * Copyright (c) 2014,2017 Ronan Dunklau
 * Copyright (c) 2018-2022, The Powa-Team *
 *-------------------------------------------------------------------------
*/
#ifndef _PG_QUALSTATS_H_
#define _PG_QUALSTATS_H_

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/catalog.h"
#include "commands/explain.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"

extern bool pgqs_backend;

/* Since cff440d368, queryid becomes a uint64 internally. */

#define PGQS_CONSTANT_SIZE 80	/* Truncate constant representation at 80 */

#define PGQS_LWL_ACQUIRE(lock, mode) if (!pgqs_backend) { \
	LWLockAcquire(lock, mode); \
	}

#define PGQS_LWL_RELEASE(lock) if (!pgqs_backend) { \
	LWLockRelease(lock); \
	}

#if PG_VERSION_NUM >= 110000
typedef uint64 pgqs_queryid;
#else
typedef uint32 pgqs_queryid;
#endif

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

/* Global Hash */
extern HTAB *pgqs_hash;
extern HTAB *pgqs_query_examples_hash;
extern pgqsSharedState *pgqs;
extern HTAB *pgqs_update_hash;

#endif
