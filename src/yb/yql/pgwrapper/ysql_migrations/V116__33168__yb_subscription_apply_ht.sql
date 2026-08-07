BEGIN;
    SET LOCAL yb_non_ddl_txn_for_sys_tables_allowed TO true;
    -- Add an output column for yb_last_commit_ht to pg_stat_get_subscription.
    -- TODO: As a workaround for GHI #13500, we perform a delete + insert instead
    -- of an update into pg_proc. Restore to UPDATE once fixed.
    DELETE FROM pg_catalog.pg_proc
    WHERE proname = 'pg_stat_get_subscription' AND pronamespace = 'pg_catalog'::regnamespace;

    INSERT INTO pg_catalog.pg_proc (
      oid, proname, pronamespace, proowner, prolang, procost, prorows, provariadic, prosupport,
      prokind, prosecdef, proleakproof, proisstrict, proretset, provolatile, proparallel,
      pronargs, pronargdefaults, prorettype, proargtypes, proallargtypes, proargmodes,
      proargnames, proargdefaults, protrftypes, prosrc, probin, proconfig, proacl
    ) VALUES (
      6118, 'pg_stat_get_subscription', 11, 10, 12, 1, 10, 0, '-', 'f', false,
      false, false, true, 's', 'r', 1, 0, 2249, '26',
      '{26,26,26,23,3220,1184,1184,3220,1184,20}',
      '{i,o,o,o,o,o,o,o,o,o}',
      '{subid,subid,relid,pid,received_lsn,last_msg_send_time,last_msg_receipt_time,latest_end_lsn,latest_end_time,yb_last_commit_ht}',
      NULL, NULL, 'pg_stat_get_subscription', NULL, NULL, NULL)
    ON CONFLICT DO NOTHING;

COMMIT;

-- Recreating the system view pg_stat_subscription
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT true FROM pg_attribute
    WHERE attrelid = 'pg_catalog.pg_stat_subscription'::regclass
      AND attname  = 'yb_last_commit_ht'
      AND NOT attisdropped
  ) THEN
    CREATE OR REPLACE VIEW pg_catalog.pg_stat_subscription
    WITH (use_initdb_acl = true)
    AS
      SELECT
              su.oid AS subid,
              su.subname,
              st.pid,
              st.relid,
              st.received_lsn,
              st.last_msg_send_time,
              st.last_msg_receipt_time,
              st.latest_end_lsn,
              st.latest_end_time,
              st.yb_last_commit_ht
      FROM pg_subscription su
              LEFT JOIN pg_stat_get_subscription(NULL) st
                        ON (st.subid = su.oid);
  END IF;
END $$;
