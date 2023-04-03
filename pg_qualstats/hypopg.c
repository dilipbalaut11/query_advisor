/*-------------------------------------------------------------------------
 *
 * hypopg.c: Implementation of hypothetical indexes for PostgreSQL
 *
 * Some functions are imported from PostgreSQL source code, theses are present
 * in hypopg_import.* files.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2015-2023: Julien Rouhaud
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "fmgr.h"

#if PG_VERSION_NUM < 120000
#include "access/sysattr.h"
#endif
#include "access/transam.h"
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif
#if PG_VERSION_NUM >= 110000
#include "catalog/partition.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#endif
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/elog.h"

#include "include/hypopg.h"
#include "include/hypopg_import.h"
#include "include/hypopg_index.h"

/*--- Variables exported ---*/

bool		isExplain = true;
bool		hypo_is_enabled = true;
bool		hypo_use_real_oids;

/*--- Private variables ---*/

static Oid last_oid = InvalidOid;
static Oid min_fake_oid = InvalidOid;
static bool oid_wraparound = false;

/*--- Functions --- */
static void
			hypo_utility_hook(
#if PG_VERSION_NUM >= 100000
							  PlannedStmt *pstmt,
#else
							  Node *parsetree,
#endif
							  const char *queryString,
#if PG_VERSION_NUM >= 140000
							  bool readOnlyTree,
#endif
#if PG_VERSION_NUM >= 90300
							  ProcessUtilityContext context,
#endif
							  ParamListInfo params,
#if PG_VERSION_NUM >= 100000
							  QueryEnvironment *queryEnv,
#endif
#if PG_VERSION_NUM < 90300
							  bool isTopLevel,
#endif
							  DestReceiver *dest,
#if PG_VERSION_NUM < 130000
							  char *completionTag
#else
							  QueryCompletion *qc
#endif
							  );
static ProcessUtility_hook_type prev_utility_hook = NULL;

static void hypo_executorEnd_hook(QueryDesc *queryDesc);
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;


static Oid hypo_get_min_fake_oid(void);
get_relation_info_hook_type prev_get_relation_info_hook = NULL;

static bool hypo_index_match_table(hypoIndex *entry, Oid relid);

/*---------------------------------
 * Return a new OID for an hypothetical index.
 *
 * To avoid locking on pg_class (required to safely call GetNewOidWithIndex or
 * similar) and to be usable on a standby node, use the oids unused in the
 * FirstBootstrapObjectId / FirstNormalObjectId range rather than real oids.
 * For performance, always start with the biggest oid lesser than
 * FirstNormalObjectId.  This way the loop to find an unused oid will only
 * happens once a single backend has created more than ~2.5k hypothetical
 * indexes.
 *
 * For people needing to have thousands of hypothetical indexes at the same
 * time, we also allow to use the initial implementation that relies on real
 * oids, which comes with all the limitations mentioned above.
 */
Oid
hypo_getNewOid(Oid relid)
{
	Oid			newoid = InvalidOid;

	if (hypo_use_real_oids)
	{
		Relation	pg_class;
		Relation	relation;

		/* Open the relation on which we want a new OID */
		relation = table_open(relid, AccessShareLock);

		/* Close the relation and release the lock now */
		table_close(relation, AccessShareLock);

		/* Open pg_class to aks a new OID */
		pg_class = table_open(RelationRelationId, RowExclusiveLock);

		/* ask for a new Oid */
		newoid = GetNewOidWithIndex(pg_class, ClassOidIndexId,
#if PG_VERSION_NUM < 120000
									ObjectIdAttributeNumber
#else
									Anum_pg_class_oid
#endif
									);

		/* Close pg_class and release the lock now */
		table_close(pg_class, RowExclusiveLock);
	}
	else
	{
		/*
		 * First, make sure we know what is the biggest oid smaller than
		 * FirstNormalObjectId present in pg_class.  This can never change so
		 * we cache the value.
		 */
		if (!OidIsValid(min_fake_oid))
			min_fake_oid = hypo_get_min_fake_oid();

		Assert(OidIsValid(min_fake_oid));

		/* Make sure there's enough room to get one more Oid */
		if (list_length(hypoIndexes) >= (FirstNormalObjectId - min_fake_oid))
		{
			ereport(ERROR,
					(errmsg("hypopg: not more oid available"),
					errhint("Remove hypothetical indexes "
						"or enable hypopg.use_real_oids")));
		}

		while(!OidIsValid(newoid))
		{
			CHECK_FOR_INTERRUPTS();

			if (!OidIsValid(last_oid))
				newoid = last_oid = min_fake_oid;
			else
				newoid = ++last_oid;

			/* Check if we just exceeded the fake oids range */
			if (newoid >= FirstNormalObjectId)
			{
				newoid = min_fake_oid;
				last_oid = InvalidOid;
				oid_wraparound = true;
			}

			/*
			 * If we already used all available fake oids, we have to make sure
			 * that the oid isn't used anymore.
			 */
			if (oid_wraparound)
			{
				if (hypo_get_index(newoid) != NULL)
				{
					/* We can't use this oid.  Reset newoid and start again */
					newoid = InvalidOid;
				}
			}
		}
	}

	Assert(OidIsValid(newoid));
	return newoid;
}

/* Reset the state of the fake oid generator. */
void
hypo_reset_fake_oids(void)
{
	Assert(hypoIndexes == NIL);
	last_oid = InvalidOid;
	oid_wraparound = false;
}

static bool
hypo_index_match_table(hypoIndex *entry, Oid relid)
{
	/* Hypothetical index on the exact same relation, use it. */
	if (entry->relid == relid)
		return true;
#if PG_VERSION_NUM >= 110000
	/*
	 * If the table is a partition, see if the hypothetical index belongs to
	 * one of the partition parent.
	 */
	if (get_rel_relispartition(relid))
	{
		List *parents = get_partition_ancestors(relid);
		ListCell *lc;

		foreach(lc, parents)
		{
			Oid oid = lfirst_oid(lc);

			if (oid == entry->relid)
				return true;
		}
	}
#endif

	return false;
}

/*
 * Return the minmum usable oid in the FirstBootstrapObjectId -
 * FirstNormalObjectId range.
 */
static Oid
hypo_get_min_fake_oid(void)
{
	int			ret, nb;
	Oid			oid = InvalidOid;

	/*
	 * Connect to SPI manager
	 */
	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "SPI connect failure - returned %d", ret);

	ret = SPI_execute("SELECT max(oid)"
			" FROM pg_catalog.pg_class"
			" WHERE oid < " CppAsString2(FirstNormalObjectId),
			true, 1);
	nb = SPI_processed;

	if (ret != SPI_OK_SELECT || nb == 0)
	{
		SPI_finish();
		elog(ERROR, "hypopg: could not find the minimum fake oid");
	}

	oid = atooid(SPI_getvalue(SPI_tuptable->vals[0],
				 SPI_tuptable->tupdesc,
				 1)) + 1;

	/* release SPI related resources (and return to caller's context) */
	SPI_finish();

	Assert(OidIsValid(oid));
	return oid;
}

/*
 * This function will execute the "hypo_injectHypotheticalIndex" for every
 * hypothetical index found for each relation if the isExplain flag is setup.
 */
void
hypo_get_relation_info_hook(PlannerInfo *root,
							Oid relationObjectId,
							bool inhparent,
							RelOptInfo *rel)
{
	if (isExplain && hypo_is_enabled)
	{
		Relation	relation;

		/* Open the current relation */
		relation = table_open(relationObjectId, AccessShareLock);

		if (relation->rd_rel->relkind == RELKIND_RELATION
#if PG_VERSION_NUM >= 90300
			|| relation->rd_rel->relkind == RELKIND_MATVIEW
#endif
			)
		{
			ListCell   *lc;

			foreach(lc, hypoIndexes)
			{
				hypoIndex  *entry = (hypoIndex *) lfirst(lc);

				if (hypo_index_match_table(entry, RelationGetRelid(relation)))
				{
					/*
					 * hypothetical index found, add it to the relation's
					 * indextlist
					 */
					hypo_injectHypotheticalIndex(root, relationObjectId,
												 inhparent, rel, relation, entry);
				}
			}
		}

		/* Close the relation release the lock now */
		table_close(relation, AccessShareLock);
	}

	if (prev_get_relation_info_hook)
		prev_get_relation_info_hook(root, relationObjectId, inhparent, rel);
}

