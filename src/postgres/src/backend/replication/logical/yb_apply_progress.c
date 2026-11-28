/*-------------------------------------------------------------------------
 *
 * yb_apply_progress.c
 *	  Logical replication apply progress, recorded in the transaction it
 *	  describes.
 *
 * YB does not persist replication origin progress with the commit, so after a
 * restart the publisher resends whatever it had not been told was flushed.
 * The apply worker records what it applied in
 * yb_system.pg_yb_subscription_progress, in the same transaction as the
 * applied rows, and skips anything at or below it.  The table lives in the
 * yb_system database, which the master creates, so every access is
 * cross-database, as LISTEN/NOTIFY's writes to pg_yb_notifications are (see
 * async.c).
 *
 * Copyright (c) YugabyteDB, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/yb_apply_progress.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/yb_special_scans.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/ybModifyTable.h"
#include "miscadmin.h"
#include "pg_yb_utils.h"
#include "replication/yb_apply_progress.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"

/* Must match the CREATE TABLE the master runs in ysql_manager.cc. */
static FormData_pg_attribute yb_progress_subid_att = {
	.attname = {"subid"},
	.atttypid = OIDOID,
	.attlen = sizeof(Oid),
	.attnum = 1,
	.attcacheoff = -1,
	.atttypmod = -1,
	.attbyval = true,
	.attalign = TYPALIGN_INT,
	.attstorage = TYPSTORAGE_PLAIN,
	.attnotnull = true,
	.attislocal = true,
};

static FormData_pg_attribute yb_progress_relid_att = {
	.attname = {"relid"},
	.atttypid = OIDOID,
	.attlen = sizeof(Oid),
	.attnum = 2,
	.attcacheoff = -1,
	.atttypmod = -1,
	.attbyval = true,
	.attalign = TYPALIGN_INT,
	.attstorage = TYPSTORAGE_PLAIN,
	.attnotnull = true,
	.attislocal = true,
};

static FormData_pg_attribute yb_progress_commit_ht_att = {
	.attname = {"commit_ht"},
	.atttypid = INT8OID,
	.attlen = sizeof(int64),
	.attnum = 3,
	.attcacheoff = -1,
	.atttypmod = -1,
	.attbyval = FLOAT8PASSBYVAL,
	.attalign = TYPALIGN_DOUBLE,
	.attstorage = TYPSTORAGE_PLAIN,
	.attnotnull = true,
	.attislocal = true,
};

static FormData_pg_attribute yb_progress_end_lsn_att = {
	.attname = {"end_lsn"},
	.atttypid = PG_LSNOID,
	.attlen = sizeof(XLogRecPtr),
	.attnum = 4,
	.attcacheoff = -1,
	.atttypmod = -1,
	.attbyval = FLOAT8PASSBYVAL,
	.attalign = TYPALIGN_DOUBLE,
	.attstorage = TYPSTORAGE_PLAIN,
	.attnotnull = true,
	.attislocal = true,
};

static FormData_pg_attribute *YbProgressAtts[] = {
	&yb_progress_subid_att,
	&yb_progress_relid_att,
	&yb_progress_commit_ht_att,
	&yb_progress_end_lsn_att,
};

#define YB_PROGRESS_NATTS lengthof(YbProgressAtts)

static Relation yb_progress_rel = NULL;
static bool yb_progress_missing = false;

/*
 * The table, or NULL when it does not exist.  The master creates it only with
 * ysql_yb_enable_pg_subscription, which a subscriber enabled through
 * ysql_pg_conf_csv alone does not set, and asynchronously.  Without it nothing
 * is recorded or skipped, which is how apply behaved before; that is decided
 * once per process.  Like ybNotificationsRel(), only the fields the YB DML
 * helpers read are populated.
 */
static Relation
yb_progress_relation(bool warn_if_missing)
{
	Oid			reloid = InvalidOid;
	Oid			relfilenode = InvalidOid;
	MemoryContext oldcxt;
	Form_pg_class rel_form;

	if (yb_progress_rel || yb_progress_missing)
		return yb_progress_rel;

	if (OidIsValid(YbSystemDbOid()))
		HandleYBStatus(YBCGetYbSystemTableInfo(PG_PUBLIC_NAMESPACE,
											   PgYbSubscriptionProgressTableName,
											   &reloid, &relfilenode));
	if (!OidIsValid(reloid))
	{
		yb_progress_missing = true;
		if (warn_if_missing)
			ereport(WARNING,
					(errmsg("logical replication apply progress is not recorded"),
					 errdetail("%s.%s does not exist, so transactions resent after a restart cannot be detected.",
							   YbSystemDbName, PgYbSubscriptionProgressTableName),
					 errhint("Set ysql_yb_enable_pg_subscription on the masters and tservers.")));
		return NULL;
	}

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	rel_form = (Form_pg_class) palloc0(CLASS_TUPLE_SIZE);
	rel_form->oid = reloid;
	rel_form->relfilenode = relfilenode;
	rel_form->relnamespace = PG_PUBLIC_NAMESPACE;
	rel_form->relkind = RELKIND_RELATION;
	rel_form->relnatts = YB_PROGRESS_NATTS;

	yb_progress_rel = (Relation) palloc0(sizeof(RelationData));
	yb_progress_rel->rd_rel = rel_form;
	yb_progress_rel->rd_id = reloid;
	yb_progress_rel->rd_att = CreateTupleDesc(YB_PROGRESS_NATTS, YbProgressAtts);
	yb_progress_rel->belongs_to_yb_system_db = true;
	MemoryContextSwitchTo(oldcxt);

	return yb_progress_rel;
}

/* A slot carrying just the key; the caller fills the other columns. */
static TupleTableSlot *
yb_progress_key_slot(Relation rel, Oid subid, Oid relid)
{
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
													&TTSOpsVirtual);

	ExecClearTuple(slot);
	memset(slot->tts_isnull, true, YB_PROGRESS_NATTS * sizeof(bool));
	slot->tts_values[0] = ObjectIdGetDatum(subid);
	slot->tts_isnull[0] = false;
	slot->tts_values[1] = ObjectIdGetDatum(relid);
	slot->tts_isnull[1] = false;
	return slot;
}

/*
 * Reads the progress recorded for (subid, relid).  Returns false when there is
 * none, including when the table does not exist.
 */
bool
YbReadApplyProgress(Oid subid, Oid relid, uint64 *commit_ht, XLogRecPtr *end_lsn)
{
	Relation	rel = yb_progress_relation(true /* warn_if_missing */ );
	TupleTableSlot *slot;
	HeapTuple	tuple = NULL;
	bool		started_tx = false;
	bool		found;

	*commit_ht = 0;
	*end_lsn = InvalidXLogRecPtr;
	if (!rel)
		return false;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		started_tx = true;
	}

	slot = yb_progress_key_slot(rel, subid, relid);
	ExecStoreVirtualTuple(slot);
	found = YbFetchHeapTuple(rel, YBCComputeYBTupleIdFromSlot(rel, slot), &tuple);
	if (found)
	{
		bool		isnull;

		*commit_ht = (uint64) DatumGetInt64(heap_getattr(tuple, 3,
														  RelationGetDescr(rel),
														  &isnull));
		*end_lsn = DatumGetLSN(heap_getattr(tuple, 4, RelationGetDescr(rel),
											&isnull));
		heap_freetuple(tuple);
	}
	ExecDropSingleTupleTableSlot(slot);

	if (started_tx)
		CommitTransactionCommand();
	return found;
}

/*
 * Records (subid, relid) as applied up to commit_ht / end_lsn.  Must run in the
 * transaction that applied it, so the row commits or aborts with that work.
 */
void
YbWriteApplyProgress(Oid subid, Oid relid, uint64 commit_ht, XLogRecPtr end_lsn)
{
	Relation	rel = yb_progress_relation(true /* warn_if_missing */ );
	TupleTableSlot *slot;

	if (!rel)
		return;
	Assert(IsTransactionState());

	slot = yb_progress_key_slot(rel, subid, relid);
	slot->tts_values[2] = Int64GetDatum((int64) commit_ht);
	slot->tts_isnull[2] = false;
	slot->tts_values[3] = LSNGetDatum(end_lsn);
	slot->tts_isnull[3] = false;
	ExecStoreVirtualTuple(slot);

	YBCExecuteInsertForDb(YbSystemDbOid(), rel, slot, ONCONFLICT_YB_REPLACE,
						  NULL /* ybctid */ , YB_TRANSACTIONAL);
	ExecDropSingleTupleTableSlot(slot);
}

/* Forgets (subid, relid), in the current transaction.  No-op without the table. */
void
YbDeleteApplyProgress(Oid subid, Oid relid)
{
	Relation	rel = yb_progress_relation(false /* warn_if_missing */ );
	TupleTableSlot *slot;
	EState	   *estate;

	if (!rel)
		return;
	Assert(IsTransactionState());

	estate = CreateExecutorState();
	slot = yb_progress_key_slot(rel, subid, relid);
	ExecStoreVirtualTuple(slot);
	TABLETUPLE_YBCTID(slot) = YBCComputeYBTupleIdFromSlot(rel, slot);

	/*
	 * target_tuple_fetched lets the delete be buffered with the rest of the
	 * transaction; nothing is fetched, as in async.c.
	 */
	YBCExecuteDelete(rel, slot, NIL, true /* target_tuple_fetched */ ,
					 YB_TRANSACTIONAL, false /* changingPart */ ,
					 false /* persist_missing_row */ , estate);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
}
