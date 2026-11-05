-- parity_interaction_engine: the CDR, step 6 (IE-046). Additive and repeatable; IE's login role
-- needs only SELECT and INSERT (deploy PL-051: its tables are insert-only for IE).
-- 1. The queue in the CDR: which queue and entry, when it was entered and left, how it went, the
--    agents who accepted it and the one who connected; recording per leg and the agent's DTMF
--    actions (counts only: never a digit). New columns are nullable or default 0: old writers
--    still insert.
-- 2. cdr_progress: what IE knew of a live call (insert-only snapshots, at answer and at the
--    queue's moments). A call whose IE died mid-call has its snapshots; the IE restarted under the
--    same replica id writes its CDR from the latest one and TC's end (DEMO step 5). The CDR row is
--    still written once, at the end.
-- Rollback: DROP TABLE cdr_progress; the added cdr columns can stay (the old code ignores them).
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS queue_id           text;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS entry_id           text;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS queued_at          timestamptz;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS dequeued_at        timestamptz;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS wait_ms            bigint;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS queue_outcome      text;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS offers             integer NOT NULL DEFAULT 0;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS agent_id           text;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS reservation_id     text;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS connected_at       timestamptz;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS recorded_legs      integer NOT NULL DEFAULT 0;
-- Legs whose recording TC refused (a consented caller not recorded: a failure to report).
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS recording_failures integer NOT NULL DEFAULT 0;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS recording_pauses   integer NOT NULL DEFAULT 0;
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS mutes              integer NOT NULL DEFAULT 0;
-- 'owner': written by the call's owner; 'restart': by IE restarted under the same replica id,
-- from the call's latest snapshot and TC's end.
ALTER TABLE cdr ADD COLUMN IF NOT EXISTS closed_by          text;

-- Exactly one CDR per interaction, whatever the tenant a writer names: the CDR insert is
-- ON CONFLICT DO NOTHING on either key, so a replayed or duplicated end never writes a second row.
CREATE UNIQUE INDEX IF NOT EXISTS cdr_interaction_id_unique ON cdr (interaction_id);

-- Snapshots are minimal: never a digit; numbers only as the masking rules print them (last 3
-- digits), so a CDR written from a snapshot after a crash carries masked numbers. No DELETE for
-- IE: retention is the deployment's job (IE-063).
CREATE TABLE IF NOT EXISTS cdr_progress (
    tenant_id          text        NOT NULL,
    interaction_id     uuid        NOT NULL,
    -- The call's own counter: a snapshot written twice is one row.
    progress_seq       integer     NOT NULL,
    node_id            text        NOT NULL,
    direction          text        NOT NULL,
    called_number      text        NOT NULL,
    caller_number      text        NOT NULL,
    flow               text        NOT NULL,
    last_step          text        NOT NULL,
    consent            text        NOT NULL,
    consent_basis      text        NOT NULL,
    started_at         timestamptz NOT NULL,
    answered_at        timestamptz,
    owner_epoch        bigint      NOT NULL,
    gathers            integer     NOT NULL,
    no_inputs          integer     NOT NULL,
    queue_id           text,
    entry_id           text,
    queued_at          timestamptz,
    dequeued_at        timestamptz,
    queue_outcome      text,
    offers             integer     NOT NULL DEFAULT 0,
    agent_id           text,
    reservation_id     text,
    connected_at       timestamptz,
    recorded_legs      integer     NOT NULL DEFAULT 0,
    recording_failures integer     NOT NULL DEFAULT 0,
    recording_pauses   integer     NOT NULL DEFAULT 0,
    mutes              integer     NOT NULL DEFAULT 0,
    written_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, interaction_id, progress_seq)
);
ALTER TABLE cdr_progress ENABLE ROW LEVEL SECURITY;
ALTER TABLE cdr_progress FORCE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'cdr_progress' AND policyname = 'cdr_progress_tenant') THEN
        CREATE POLICY cdr_progress_tenant ON cdr_progress
            USING (tenant_id = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
    END IF;
END
$$;
