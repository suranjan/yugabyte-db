/*-------------------------------------------------------------------------
 *
 * execReplication.c
 *	  miscellaneous executor routines for logical replication
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execReplication.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/* YB includes */
#include "access/tupconvert.h"
#include "catalog/pg_yb_catalog_version_d.h"
#include "executor/ybModifyTable.h"
#include "pg_yb_utils.h"

/* YB declarations */
static HeapTuple YbBuildTriggerTupleFromSlot(TupleTableSlot *searchslot,
											 bool *shouldFree,
											 ItemPointer *tid);
static List *YbExecSimpleRelationInsert(ResultRelInfo *resultRelInfo,
										EState *estate,
										TupleTableSlot *slot);
static List *YbExecSimpleRelationUpdate(ResultRelInfo *resultRelInfo,
										EState *estate,
										TupleTableSlot *searchslot,
										TupleTableSlot *slot);
static bool YbExecSimpleRelationDelete(ResultRelInfo *resultRelInfo,
									   EState *estate,
									   TupleTableSlot *searchslot);

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 *
 * Returns whether any column contains NULLs.
 *
 * This is not generic routine, it expects the idxrel to be replication
 * identity of a rel and meet all limitations associated with that.
 */
static bool
build_replindex_scan_key(ScanKey skey, Relation rel, Relation idxrel,
						 TupleTableSlot *searchslot)
{
	int			attoff;
	bool		isnull;
	Datum		indclassDatum;
	oidvector  *opclass;
	int2vector *indkey = &idxrel->rd_index->indkey;
	bool		hasnulls = false;

	Assert(RelationGetReplicaIndex(rel) == RelationGetRelid(idxrel) ||
		   RelationGetPrimaryKeyIndex(rel) == RelationGetRelid(idxrel));

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	/* Build scankey for every attribute in the index. */
	for (attoff = 0; attoff < IndexRelationGetNumberOfKeyAttributes(idxrel); attoff++)
	{
		Oid			operator;
		Oid			opfamily;
		RegProcedure regop;
		int			pkattno = attoff + 1;
		int			mainattno = indkey->values[attoff];
		Oid			optype = get_opclass_input_type(opclass->values[attoff]);

		/*
		 * Load the operator info.  We need this to get the equality operator
		 * function for the scan key.
		 */
		opfamily = get_opclass_family(opclass->values[attoff]);

		operator = get_opfamily_member(opfamily, optype,
									   optype,
									   BTEqualStrategyNumber);
		if (!OidIsValid(operator))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 BTEqualStrategyNumber, optype, optype, opfamily);

		regop = get_opcode(operator);

		/* Initialize the scankey. */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					searchslot->tts_values[mainattno - 1]);

		skey[attoff].sk_collation = idxrel->rd_indcollation[attoff];

		/* Check for null value. */
		if (searchslot->tts_isnull[mainattno - 1])
		{
			hasnulls = true;
			skey[attoff].sk_flags |= SK_ISNULL;
		}
	}

	return hasnulls;
}

/*
 * Search the relation 'rel' for tuple using the index.
 *
 * If a matching tuple is found, lock it with lockmode, fill the slot with its
 * contents, and return true.  Return false otherwise.
 */
bool
RelationFindReplTupleByIndex(Relation rel, Oid idxoid,
							 LockTupleMode lockmode,
							 TupleTableSlot *searchslot,
							 TupleTableSlot *outslot)
{
	ScanKeyData skey[INDEX_MAX_KEYS];
	IndexScanDesc scan;
	SnapshotData snap;
	TransactionId xwait;
	Relation	idxrel;
	bool		found;

	/* Open the index. */
	idxrel = index_open(idxoid, RowExclusiveLock);

	/* Start an index scan. */
	InitDirtySnapshot(snap);
	scan = index_beginscan(rel, idxrel, &snap,
						   IndexRelationGetNumberOfKeyAttributes(idxrel),
						   0);

	/* Build scan key. */
	build_replindex_scan_key(skey, rel, idxrel, searchslot);

retry:
	found = false;

	index_rescan(scan, skey, IndexRelationGetNumberOfKeyAttributes(idxrel), NULL, 0);

	/* Try to find the tuple */
	if (index_getnext_slot(scan, ForwardScanDirection, outslot))
	{
		found = true;
		ExecMaterializeSlot(outslot);

		/* YB: no locker to wait for; see RelationFindReplTupleSeq. */
		if (!IsYBRelation(rel))
		{
			xwait = TransactionIdIsValid(snap.xmin) ?
				snap.xmin : snap.xmax;

			/*
			 * If the tuple is locked, wait for locking transaction to finish and
			 * retry.
			 */
			if (TransactionIdIsValid(xwait))
			{
				XactLockTableWait(xwait, NULL, NULL, XLTW_None);
				goto retry;
			}
		}
	}

	/*
	 * Found tuple; lock it unless this is a YB relation.  See
	 * RelationFindReplTupleSeq.
	 */
	if (found && !IsYBRelation(rel))
	{
		TM_FailureData tmfd;
		TM_Result	res;

		PushActiveSnapshot(GetLatestSnapshot());

		res = table_tuple_lock(rel, &(outslot->tts_tid), GetLatestSnapshot(),
							   outslot,
							   GetCurrentCommandId(false),
							   lockmode,
							   LockWaitBlock,
							   0 /* don't follow updates */ ,
							   &tmfd);

		PopActiveSnapshot();

		switch (res)
		{
			case TM_Ok:
				break;
			case TM_Updated:
				/* XXX: Improve handling here */
				if (ItemPointerIndicatesMovedPartitions(&tmfd.ctid))
					ereport(LOG,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("tuple to be locked was already moved to another partition due to concurrent update, retrying")));
				else
					ereport(LOG,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("concurrent update, retrying")));
				goto retry;
			case TM_Deleted:
				/* XXX: Improve handling here */
				ereport(LOG,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("concurrent delete, retrying")));
				goto retry;
			case TM_Invisible:
				elog(ERROR, "attempted to lock invisible tuple");
				break;
			default:
				elog(ERROR, "unexpected table_tuple_lock status: %u", res);
				break;
		}
	}

	index_endscan(scan);

	/* Don't release lock until commit. */
	index_close(idxrel, NoLock);

	return found;
}

/*
 * Compare the tuples in the slots by checking if they have equal values.
 */
static bool
tuples_equal(TupleTableSlot *slot1, TupleTableSlot *slot2,
			 TypeCacheEntry **eq)
{
	int			attrnum;

	Assert(slot1->tts_tupleDescriptor->natts ==
		   slot2->tts_tupleDescriptor->natts);

	slot_getallattrs(slot1);
	slot_getallattrs(slot2);

	/* Check equality of the attributes. */
	for (attrnum = 0; attrnum < slot1->tts_tupleDescriptor->natts; attrnum++)
	{
		Form_pg_attribute att;
		TypeCacheEntry *typentry;

		att = TupleDescAttr(slot1->tts_tupleDescriptor, attrnum);

		/*
		 * Ignore dropped and generated columns as the publisher doesn't send
		 * those
		 */
		if (att->attisdropped || att->attgenerated)
			continue;

		/*
		 * If one value is NULL and other is not, then they are certainly not
		 * equal
		 */
		if (slot1->tts_isnull[attrnum] != slot2->tts_isnull[attrnum])
			return false;

		/*
		 * If both are NULL, they can be considered equal.
		 */
		if (slot1->tts_isnull[attrnum] || slot2->tts_isnull[attrnum])
			continue;

		typentry = eq[attrnum];
		if (typentry == NULL)
		{
			typentry = lookup_type_cache(att->atttypid,
										 TYPECACHE_EQ_OPR_FINFO);
			if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not identify an equality operator for type %s",
								format_type_be(att->atttypid))));
			eq[attrnum] = typentry;
		}

		if (!DatumGetBool(FunctionCall2Coll(&typentry->eq_opr_finfo,
											att->attcollation,
											slot1->tts_values[attrnum],
											slot2->tts_values[attrnum])))
			return false;
	}

	return true;
}

/*
 * Search the relation 'rel' for tuple using the sequential scan.
 *
 * If a matching tuple is found, lock it with lockmode, fill the slot with its
 * contents, and return true.  Return false otherwise.
 *
 * Note that this stops on the first matching tuple.
 *
 * This can obviously be quite slow on tables that have more than few rows.
 */
bool
RelationFindReplTupleSeq(Relation rel, LockTupleMode lockmode,
						 TupleTableSlot *searchslot, TupleTableSlot *outslot)
{
	TupleTableSlot *scanslot;
	TableScanDesc scan;
	SnapshotData snap;
	TypeCacheEntry **eq;
	TransactionId xwait;
	bool		found;
	TupleDesc	desc PG_USED_FOR_ASSERTS_ONLY = RelationGetDescr(rel);

	Assert(equalTupleDescs(desc, outslot->tts_tupleDescriptor));

	eq = palloc0(sizeof(*eq) * outslot->tts_tupleDescriptor->natts);

	/* Start a heap scan. */
	InitDirtySnapshot(snap);
	scan = table_beginscan(rel, &snap, 0, NULL);
	scanslot = table_slot_create(rel, NULL);

retry:
	found = false;

	/* YB: the scan was just begun and is never retried (see below). */
	if (!IsYBRelation(rel))
		table_rescan(scan, NULL);

	/* Try to find the tuple */
	while (table_scan_getnextslot(scan, ForwardScanDirection, scanslot))
	{
		if (!tuples_equal(scanslot, searchslot, eq))
			continue;

		found = true;
		ExecCopySlot(outslot, scanslot);

		/*
		 * YB: nothing fills the dirty snapshot, so there is no locker to wait
		 * for; same as check_exclusion_or_unique_constraint's YB branch.
		 */
		if (IsYBRelation(rel))
			break;

		xwait = TransactionIdIsValid(snap.xmin) ?
			snap.xmin : snap.xmax;

		/*
		 * If the tuple is locked, wait for locking transaction to finish and
		 * retry.
		 */
		if (TransactionIdIsValid(xwait))
		{
			XactLockTableWait(xwait, NULL, NULL, XLTW_None);
			goto retry;
		}

		/* Found our tuple and it's not locked */
		break;
	}

	/*
	 * Found tuple; lock it unless this is a YB relation.  YB slots carry the
	 * row already and identify it by ybctid, while table_tuple_lock expects a
	 * buffer heap tuple addressed by ctid.
	 */
	if (found && !IsYBRelation(rel))
	{
		TM_FailureData tmfd;
		TM_Result	res;

		PushActiveSnapshot(GetLatestSnapshot());

		res = table_tuple_lock(rel, &(outslot->tts_tid), GetLatestSnapshot(),
							   outslot,
							   GetCurrentCommandId(false),
							   lockmode,
							   LockWaitBlock,
							   0 /* don't follow updates */ ,
							   &tmfd);

		PopActiveSnapshot();

		switch (res)
		{
			case TM_Ok:
				break;
			case TM_Updated:
				/* XXX: Improve handling here */
				if (ItemPointerIndicatesMovedPartitions(&tmfd.ctid))
					ereport(LOG,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("tuple to be locked was already moved to another partition due to concurrent update, retrying")));
				else
					ereport(LOG,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("concurrent update, retrying")));
				goto retry;
			case TM_Deleted:
				/* XXX: Improve handling here */
				ereport(LOG,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("concurrent delete, retrying")));
				goto retry;
			case TM_Invisible:
				elog(ERROR, "attempted to lock invisible tuple");
				break;
			default:
				elog(ERROR, "unexpected table_tuple_lock status: %u", res);
				break;
		}
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(scanslot);

	return found;
}

/*
 * Insert tuple represented in the slot to the relation, update the indexes,
 * and execute any constraints and per-row triggers.
 *
 * Caller is responsible for opening the indexes.
 */
void
ExecSimpleRelationInsert(ResultRelInfo *resultRelInfo,
						 EState *estate, TupleTableSlot *slot)
{
	bool		skip_tuple = false;
	Relation	rel = resultRelInfo->ri_RelationDesc;

	/* For now we support only tables. */
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);

	CheckCmdReplicaIdentity(rel, CMD_INSERT);

	/* BEFORE ROW INSERT Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_insert_before_row)
	{
		if (!ExecBRInsertTriggers(estate, resultRelInfo, slot))
			skip_tuple = true;	/* "do nothing" */
	}

	if (!skip_tuple)
	{
		List	   *recheckIndexes = NIL;

		/* Compute stored generated columns */
		if (rel->rd_att->constr &&
			rel->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_INSERT);

		/* Check the constraints of the tuple */
		if (rel->rd_att->constr)
			ExecConstraints(resultRelInfo, slot, estate, NULL /* YB: no mtstate */ );
		if (rel->rd_rel->relispartition)
			ExecPartitionCheck(resultRelInfo, slot, estate, true);

		if (IsYBRelation(rel))	/* YB: DocDB write, not the heap AM */
			recheckIndexes = YbExecSimpleRelationInsert(resultRelInfo, estate,
														slot);
		else
		{
			/* OK, store the tuple and create index entries for it */
			simple_table_tuple_insert(resultRelInfo->ri_RelationDesc, slot);

			if (resultRelInfo->ri_NumIndices > 0)
				recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
													   slot, estate, false, false,
													   NULL, NIL);
		}						/* YB */

		/* AFTER ROW INSERT Triggers */
		ExecARInsertTriggers(estate, resultRelInfo, slot,
							 recheckIndexes, NULL);

		/*
		 * XXX we should in theory pass a TransitionCaptureState object to the
		 * above to capture transition tuples, but after statement triggers
		 * don't actually get fired by replication yet anyway
		 */

		list_free(recheckIndexes);
	}
}

/*
 * Find the searchslot tuple and update it with data in the slot,
 * update the indexes, and execute any constraints and per-row triggers.
 *
 * Caller is responsible for opening the indexes.
 */
void
ExecSimpleRelationUpdate(ResultRelInfo *resultRelInfo,
						 EState *estate, EPQState *epqstate,
						 TupleTableSlot *searchslot, TupleTableSlot *slot)
{
	bool		skip_tuple = false;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	ItemPointer tid = &(searchslot->tts_tid);

	/* YB declarations */
	HeapTuple	yb_oldtup = NULL;
	bool		yb_should_free = false;

	/*
	 * We support only non-system tables, with
	 * check_publication_add_relation() accountable.
	 */
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	Assert(!IsCatalogRelation(rel));

	CheckCmdReplicaIdentity(rel, CMD_UPDATE);

	/*
	 * YB: triggers need an old tuple, and there is no ctid to fetch it by;
	 * materialize it from the slot, as the normal YB executor path does via
	 * the wholerow junk attribute.
	 */
	if (IsYBRelation(rel) &&
		resultRelInfo->ri_TrigDesc &&
		(resultRelInfo->ri_TrigDesc->trig_update_before_row ||
		 resultRelInfo->ri_TrigDesc->trig_update_after_row))
		yb_oldtup = YbBuildTriggerTupleFromSlot(searchslot,
												&yb_should_free, &tid);

	/* BEFORE ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_before_row)
	{
		if (!ExecBRUpdateTriggers(estate, epqstate, resultRelInfo,
								  tid, yb_oldtup, slot, NULL))
			skip_tuple = true;	/* "do nothing" */
	}

	if (!skip_tuple)
	{
		List	   *recheckIndexes = NIL;
		bool		update_indexes;

		/* Compute stored generated columns */
		if (rel->rd_att->constr &&
			rel->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_UPDATE);

		/* Check the constraints of the tuple */
		if (rel->rd_att->constr)
			ExecConstraints(resultRelInfo, slot, estate, NULL /* YB: no mtstate */ );
		if (rel->rd_rel->relispartition)
			ExecPartitionCheck(resultRelInfo, slot, estate, true);

		if (IsYBRelation(rel))	/* YB: DocDB write, not the heap AM */
			recheckIndexes = YbExecSimpleRelationUpdate(resultRelInfo, estate,
														searchslot, slot);
		else
		{
			simple_table_tuple_update(rel, tid, slot, estate->es_snapshot,
									  &update_indexes);

			if (resultRelInfo->ri_NumIndices > 0 && update_indexes)
				recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
													   slot, estate, true, false,
													   NULL, NIL);
		}						/* YB */

		/* AFTER ROW UPDATE Triggers */
		ExecARUpdateTriggers(estate, resultRelInfo,
							 NULL, NULL,
							 tid, yb_oldtup, slot,
							 recheckIndexes, NULL, false);

		list_free(recheckIndexes);
	}

	if (yb_should_free)
		heap_freetuple(yb_oldtup);
}

/*
 * Find the searchslot tuple and delete it, and execute any constraints
 * and per-row triggers.
 *
 * Caller is responsible for opening the indexes.
 */
void
ExecSimpleRelationDelete(ResultRelInfo *resultRelInfo,
						 EState *estate, EPQState *epqstate,
						 TupleTableSlot *searchslot,
						 bool *yb_row_missing)
{
	bool		skip_tuple = false;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	ItemPointer tid = &searchslot->tts_tid;

	/* YB declarations */
	HeapTuple	yb_oldtup = NULL;
	bool		yb_should_free = false;

	CheckCmdReplicaIdentity(rel, CMD_DELETE);

	/*
	 * YB: triggers need an old tuple, and there is no ctid to fetch it by;
	 * materialize it from the slot.  See ExecSimpleRelationUpdate.
	 */
	if (IsYBRelation(rel) &&
		resultRelInfo->ri_TrigDesc &&
		(resultRelInfo->ri_TrigDesc->trig_delete_before_row ||
		 resultRelInfo->ri_TrigDesc->trig_delete_after_row))
		yb_oldtup = YbBuildTriggerTupleFromSlot(searchslot,
												&yb_should_free, &tid);

	/* BEFORE ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_before_row)
	{
		skip_tuple = !ExecBRDeleteTriggers(estate, epqstate, resultRelInfo,
										   tid, yb_oldtup, NULL);
	}

	if (!skip_tuple)
	{
		if (IsYBRelation(rel))	/* YB: DocDB write, not the heap AM */
		{
			bool		row_deleted =
				YbExecSimpleRelationDelete(resultRelInfo, estate, searchslot);

			if (yb_row_missing)
				*yb_row_missing = !row_deleted;
		}
		else
		{
			/* OK, delete the tuple */
			simple_table_tuple_delete(rel, tid, estate->es_snapshot);
		}						/* YB */

		/* AFTER ROW DELETE Triggers */
		ExecARDeleteTriggers(estate, resultRelInfo,
							 tid, yb_oldtup, NULL, false);
	}

	if (yb_should_free)
		heap_freetuple(yb_oldtup);
}

/*
 * Check if command can be executed with current replica identity.
 */
void
CheckCmdReplicaIdentity(Relation rel, CmdType cmd)
{
	PublicationDesc pubdesc;

	/*
	 * Skip checking the replica identity for partitioned tables, because the
	 * operations are actually performed on the leaf partitions.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return;

	/* We only need to do checks for UPDATE and DELETE. */
	if (cmd != CMD_UPDATE && cmd != CMD_DELETE)
		return;

	/*
	 * It is only safe to execute UPDATE/DELETE when all columns, referenced
	 * in the row filters from publications which the relation is in, are
	 * valid - i.e. when all referenced columns are part of REPLICA IDENTITY
	 * or the table does not publish UPDATEs or DELETEs.
	 *
	 * XXX We could optimize it by first checking whether any of the
	 * publications have a row filter for this relation. If not and relation
	 * has replica identity then we can avoid building the descriptor but as
	 * this happens only one time it doesn't seem worth the additional
	 * complexity.
	 */
	RelationBuildPublicationDesc(rel, &pubdesc);
	if (cmd == CMD_UPDATE && !pubdesc.rf_valid_for_update)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("cannot update table \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Column used in the publication WHERE expression is not part of the replica identity.")));
	else if (cmd == CMD_UPDATE && !pubdesc.cols_valid_for_update)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("cannot update table \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Column list used by the publication does not cover the replica identity.")));
	else if (cmd == CMD_DELETE && !pubdesc.rf_valid_for_delete)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("cannot delete from table \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Column used in the publication WHERE expression is not part of the replica identity.")));
	else if (cmd == CMD_DELETE && !pubdesc.cols_valid_for_delete)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("cannot delete from table \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Column list used by the publication does not cover the replica identity.")));

	/* If relation has replica identity we are always good. */
	if (OidIsValid(RelationGetReplicaIndex(rel)))
		return;

	/* REPLICA IDENTITY FULL is also good for UPDATE/DELETE. */
	if (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
		return;

	/*
	 * YB: When yb_cdcsdk_allow_dml_without_pk is true, allow UPDATE/DELETE
	 * for DEFAULT and CHANGE replica identity even without a PK.
	 * When false (default), both fall through to the error below (PG parity).
	 */
	if (IsYugaByteEnabled() &&
		(rel->rd_rel->relreplident == YB_REPLICA_IDENTITY_CHANGE ||
		 rel->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT) &&
		yb_cdcsdk_allow_dml_without_pk)
		return;

	/*
	 * YB: In per-database catalog version mode at the end of a global-impact
	 * DDL statement, we internally call yb_increment_all_db_catalog_versions
	 * which sets yb_non_ddl_txn_for_sys_tables_allowed to true in order to
	 * update pg_yb_catalog_version table. More generally, a user may want
	 * to manually set yb_non_ddl_txn_for_sys_tables_allowed to true and then
	 * perform an update on pg_yb_catalog_version table to force catalog cache
	 * refresh.
	 * NOTE: we may need to allow more system tables in YB context.
	 *
	 * TODO(#20143): Revisit if and when we introduce support for replica
	 * identity, to identify how REPLICA_IDENTITY_NOTHING is handled here.
	 */
	if (IsYugaByteEnabled() &&
		yb_non_ddl_txn_for_sys_tables_allowed)
		return;

	/*
	 * This is UPDATE/DELETE and there is no replica identity.
	 *
	 * Check if the table publishes UPDATES or DELETES.
	 */
	if (cmd == CMD_UPDATE && pubdesc.pubactions.pubupdate)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot update table \"%s\" because it does not have a replica identity and publishes updates",
						RelationGetRelationName(rel)),
				 errhint("To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.")));
	else if (cmd == CMD_DELETE && pubdesc.pubactions.pubdelete)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot delete from table \"%s\" because it does not have a replica identity and publishes deletes",
						RelationGetRelationName(rel)),
				 errhint("To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.")));
}


/*
 * Check if we support writing into specific relkind.
 *
 * The nspname and relname are only needed for error reporting.
 */
void
CheckSubscriptionRelkind(char relkind, const char *nspname,
						 const char *relname)
{
	if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot use relation \"%s.%s\" as logical replication target",
						nspname, relname),
				 errdetail_relkind_not_supported(relkind)));
}

/*
 * YB: materialize a slot into a HeapTuple for the trigger functions.  They
 * assert exactly one of (tupleid, fdw_trigtuple) is valid and a YB slot has no
 * ctid, so t_self is left invalid and carries the ybctid, as in the normal YB
 * executor path.  *tid points at that t_self; free the tuple if *shouldFree.
 */
static HeapTuple
YbBuildTriggerTupleFromSlot(TupleTableSlot *searchslot,
							bool *shouldFree,
							ItemPointer *tid)
{
	HeapTuple	oldtup;

	/* Materializing, so this either returns a tuple or errors out. */
	oldtup = ExecFetchSlotHeapTuple(searchslot, true, shouldFree);
	ItemPointerSetInvalid(&(oldtup->t_self));
	oldtup->t_tableOid = searchslot->tts_tableOid;
	HEAPTUPLE_YBCTID(oldtup) = TABLETUPLE_YBCTID(searchslot);
	*tid = &(oldtup->t_self);
	return oldtup;
}

/*
 * YB: return true if any primary key column differs between the old and new
 * tuples of a replicated UPDATE.  updatedCols cannot answer it: on this path it
 * holds every column the publisher sent, which always includes the key.
 * Compared by image, as the stored and the replicated value of a varlena differ
 * in header size.
 */
static bool
YbReplPrimaryKeyChanged(Relation rel,
						TupleTableSlot *old_slot,
						TupleTableSlot *new_slot)
{
	Bitmapset  *pkey = YBGetTablePrimaryKeyBms(rel);
	AttrNumber	minattr = YBGetFirstLowInvalidAttributeNumber(rel);
	int			col = -1;

	while ((col = bms_next_member(pkey, col)) >= 0)
	{
		AttrNumber	attnum = col + minattr;
		Form_pg_attribute att = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
		Datum		old_val,
					new_val;
		bool		old_null,
					new_null;

		old_val = slot_getattr(old_slot, attnum, &old_null);
		new_val = slot_getattr(new_slot, attnum, &new_null);

		if (old_null != new_null ||
			(!old_null &&
			 !datum_image_eq(old_val, new_val, att->attbyval, att->attlen)))
			return true;
	}
	return false;
}

/*
 * YB: remove the row's secondary index entries - a DocDB row write does not.
 * oldYbctid must be the storage ybctid: it is a key column of every non-unique
 * index, so a zero would write tombstones keyed on 0 and leave the real entries
 * behind.  The delete leaf gets it as a side effect of YBCExecuteDelete, hence
 * the assert.
 */
static void
YbReplDeleteIndexTuples(ResultRelInfo *resultRelInfo, Datum oldYbctid,
						TupleTableSlot *searchslot, EState *estate)
{
	HeapTuple	oldtuple;
	bool		shouldFree;

	Assert(oldYbctid != 0);

	oldtuple = ExecFetchSlotHeapTuple(searchslot, false, &shouldFree);
	ExecDeleteIndexTuples(resultRelInfo, oldYbctid, oldtuple,
						  false /* persist_missing_row */ , estate);
	if (shouldFree)
		pfree(oldtuple);
}

/*
 * YB: apply a replicated INSERT, returning the indexes needing recheck.
 * Separate from ExecSimpleRelationInsert so that body stays a one-line branch
 * and merges textually.  All three leaves rely on worker.c building a fresh
 * EState per change (create_edata_for_relation / finish_edata), which makes the
 * per-tuple allocations safe without pfrees.
 */
static List *
YbExecSimpleRelationInsert(ResultRelInfo *resultRelInfo, EState *estate,
						   TupleTableSlot *slot)
{
	Relation	rel = resultRelInfo->ri_RelationDesc;
	List	   *recheckIndexes = NIL;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot->tts_tableOid = RelationGetRelid(rel);

	/*
	 * YB: a conflicting row reports a duplicate key, as on PG.  A transaction
	 * resent after a restart never gets here: the apply worker skips it using
	 * the subscription progress table (see yb_xact_already_applied).
	 */
	YBCExecuteInsert(rel, slot, ONCONFLICT_NONE);

	if (YBCRelInfoHasSecondaryIndices(resultRelInfo))
		recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, estate,
											   false, false, NULL, NIL);

	MemoryContextSwitchTo(oldContext);
	return recheckIndexes;
}

/*
 * YB: apply has no ModifyTable plan; an empty one makes every plan-derived
 * step in YBCExecuteUpdate a no-op.  Read-only - unconstify at the call site
 * rather than handing out a mutable process-wide plan.
 */
static const ModifyTable yb_repl_mt_plan = {
	.plan.type = T_ModifyTable,
	.operation = CMD_UPDATE,
};

/*
 * YB: the columns YBCExecuteUpdate may write for a replicated UPDATE.  Key
 * columns are excluded - bound via ybctid, and YBCExecuteUpdate asserts
 * updatedCols names none.  whole_row is the trigger / stored-generated case;
 * spelling the set out beats the InvalidAttrNumber sentinel, which would bind
 * every column including the key.  updatedCols must already be in rel's
 * attribute numbers.
 */
static Bitmapset *
YbReplUpdatedCols(Relation rel, Bitmapset *updatedCols, bool whole_row)
{
	TupleDesc	desc = RelationGetDescr(rel);
	AttrNumber	minattr = YBGetFirstLowInvalidAttributeNumber(rel);
	Bitmapset  *cols = NULL;

	if (!whole_row)
		cols = bms_copy(updatedCols);
	else
		for (int idx = 0; idx < desc->natts; idx++)
		{
			AttrNumber	attnum = TupleDescAttr(desc, idx)->attnum;

			if (IsRealYBColumn(rel, attnum))
				cols = bms_add_member(cols, attnum - minattr);
		}

	return bms_del_members(cols, YBGetTablePrimaryKeyBms(rel));
}

/*
 * YB: apply a replicated UPDATE to a DocDB-backed relation, returning the
 * indexes needing recheck.  See YbExecSimpleRelationInsert on why this is a
 * separate function.
 */
static List *
YbExecSimpleRelationUpdate(ResultRelInfo *resultRelInfo, EState *estate,
						   TupleTableSlot *searchslot, TupleTableSlot *slot)
{
	Relation	rel = resultRelInfo->ri_RelationDesc;
	List	   *recheckIndexes = NIL;
	Bitmapset  *updatedCols;
	Datum		oldYbctid;
	bool		target_tuple_fetched;
	bool		whole_row;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	searchslot->tts_tableOid = RelationGetRelid(rel);
	slot->tts_tableOid = RelationGetRelid(rel);
	updatedCols = ExecGetUpdatedCols(resultRelInfo, estate);

	/*
	 * YB: a routed partition has no range table entry of its own, so this
	 * bitmap is the root's, in the root's attribute numbers.  Upstream
	 * translates it when ri_RangeTableIndex is zero, but YB gives routed
	 * partitions the root's index (see ExecInitPartitionInfo), so do it here -
	 * otherwise a leaf whose numbering differs from the root's, one that has had
	 * a column dropped, silently keeps those columns' old values.
	 */
	if (resultRelInfo->ri_RangeTableIndex != 0 &&
		resultRelInfo->ri_RootResultRelInfo != NULL &&
		resultRelInfo->ri_RootToPartitionMap != NULL)
		updatedCols =
			execute_attr_map_cols(resultRelInfo->ri_RootToPartitionMap->attrMap,
								  updatedCols, rel);

	/*
	 * YB: the bitmap was fixed from the message, but a BEFORE ROW UPDATE trigger
	 * or ExecComputeStoredGenerated can have changed the slot since.  Both run
	 * only on the fetched-row path, so every slot value is the row's true value.
	 */
	whole_row = ((resultRelInfo->ri_TrigDesc &&
				  resultRelInfo->ri_TrigDesc->trig_update_before_row) ||
				 (rel->rd_att->constr &&
				  rel->rd_att->constr->has_generated_stored));
	updatedCols = YbReplUpdatedCols(rel, updatedCols, whole_row);

	/*
	 * YB: a zero ybctid encodes "nothing read this row"; derive it from the key
	 * columns then.  The old index entries are keyed by this ybctid, so capture
	 * it from searchslot before the write, never from the new tuple.
	 */
	target_tuple_fetched = (TABLETUPLE_YBCTID(searchslot) != 0);
	if (!target_tuple_fetched)
		TABLETUPLE_YBCTID(searchslot) =
			YBCComputeYBTupleIdFromSlot(rel, searchslot);
	oldYbctid = TABLETUPLE_YBCTID(searchslot);

	if (YbReplPrimaryKeyChanged(rel, searchslot, slot))
	{
		/*
		 * YB: the ybctid is the row's DocDB key, so a key change cannot be
		 * applied in place - delete plus insert, as ExecUpdateAct routes it.
		 */
		YBCExecuteUpdateReplace(rel, searchslot, slot, estate);
	}
	else
		YBCExecuteUpdate(resultRelInfo,
						 searchslot,
						 slot,
						 NULL,	/* oldtuple */
						 estate,
						 unconstify(ModifyTable *, &yb_repl_mt_plan),
						 target_tuple_fetched,
						 YB_TRANSACTIONAL,
						 updatedCols,
						 false);	/* canSetTag */

	if (YBCRelInfoHasSecondaryIndices(resultRelInfo))
	{
		YbReplDeleteIndexTuples(resultRelInfo, oldYbctid, searchslot, estate);
		recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, estate,
											   true, false, NULL, NIL);
	}

	MemoryContextSwitchTo(oldContext);
	return recheckIndexes;
}

/*
 * YB: apply a replicated DELETE to a DocDB-backed relation.  See
 * YbExecSimpleRelationInsert on why this is a separate function.
 */
static bool
YbExecSimpleRelationDelete(ResultRelInfo *resultRelInfo, EState *estate,
						   TupleTableSlot *searchslot)
{
	Relation	rel = resultRelInfo->ri_RelationDesc;
	MemoryContext oldContext;
	bool		row_deleted;

	oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	searchslot->tts_tableOid = RelationGetRelid(rel);

	/*
	 * YB: the rows-affected count costs a synchronous flush per change and feeds
	 * only the caller's DEBUG1 missing-row report.  So when that report cannot be
	 * emitted, derive the ybctid and present the tuple as fetched, which keeps
	 * the delete batched and stops the count being requested.
	 */
	if (TABLETUPLE_YBCTID(searchslot) == 0 &&
		!message_level_is_interesting(DEBUG1))
		TABLETUPLE_YBCTID(searchslot) =
			YBCComputeYBTupleIdFromSlot(rel, searchslot);

	row_deleted = YBCExecuteDelete(rel, searchslot, NIL,
								   TABLETUPLE_YBCTID(searchslot) != 0,	/* target_tuple_fetched */
								   YB_TRANSACTIONAL,
								   false,	/* changingPart */
								   false,	/* persist_missing_row */
								   estate);

	if (YBCRelInfoHasSecondaryIndices(resultRelInfo))
		/* YB: YBCExecuteDelete stored the row's ybctid on searchslot. */
		YbReplDeleteIndexTuples(resultRelInfo, TABLETUPLE_YBCTID(searchslot),
								searchslot, estate);

	MemoryContextSwitchTo(oldContext);
	return row_deleted;
}
