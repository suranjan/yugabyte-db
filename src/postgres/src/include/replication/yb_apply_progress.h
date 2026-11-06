/*-------------------------------------------------------------------------
 *
 * yb_apply_progress.h
 *	  Logical replication apply progress in yb_system.pg_yb_subscription_progress.
 *
 * Copyright (c) YugabyteDB, Inc.
 *
 * src/include/replication/yb_apply_progress.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef YB_APPLY_PROGRESS_H
#define YB_APPLY_PROGRESS_H

#include "access/xlogdefs.h"

/* relid is InvalidOid for the apply worker, the table's oid for tablesync. */
extern bool YbReadApplyProgress(Oid subid, Oid relid, uint64 *commit_ht,
								XLogRecPtr *end_lsn);
extern void YbWriteApplyProgress(Oid subid, Oid relid, uint64 commit_ht,
								 XLogRecPtr end_lsn);
extern void YbDeleteApplyProgress(Oid subid, Oid relid);

#endif							/* YB_APPLY_PROGRESS_H */
