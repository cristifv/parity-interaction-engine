-- parity_interaction_engine: the CDR's parent call (contracts 0.13.0 "The xsid"; IE ie-integration-1).
-- A consult, a callback or any call another call causes is its own interaction (its own xsid);
-- parent_interaction_id is the xsid of the call that caused it (`created.parent_interaction_id`,
-- the logs' parent_xsid). NULL for a call that entered the platform on its own (inbound, an API
-- call). Additive and repeatable: old writers still insert (the column is nullable).
-- Rollback: the column can stay (old code ignores it); DROP INDEX cdr_parent_idx.
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS parent_interaction_id uuid;
ALTER TABLE cdr_progress ADD COLUMN IF NOT EXISTS parent_interaction_id uuid;
-- A call's children, by tenant (row-level security applies as for every CDR read).
CREATE INDEX IF NOT EXISTS cdr_parent_idx ON cdr (tenant_id, parent_interaction_id)
    WHERE parent_interaction_id IS NOT NULL;
