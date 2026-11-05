-- parity_interaction_engine: the CDR (PLAN step 4; "Privacy": numbers stay here, under row-level
-- security). One row per interaction: (tenant_id, interaction_id) is the idempotency key, so a
-- writer that sees an interaction's end twice inserts once (ON CONFLICT DO NOTHING). Repeatable:
-- applied at start in the dev, demo and test profiles. Monthly partitions and the 13-month
-- retention come with the event pipeline (step 9).
CREATE TABLE IF NOT EXISTS cdr (
    tenant_id      text        NOT NULL,
    interaction_id uuid        NOT NULL,
    node_id        text        NOT NULL,
    direction      text        NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    called_number  text        NOT NULL,
    caller_number  text        NOT NULL,
    flow           text        NOT NULL,
    last_step      text        NOT NULL,
    consent        text        NOT NULL CHECK (consent IN ('granted', 'refused', 'undecided')),
    consent_basis  text        NOT NULL,
    started_at     timestamptz NOT NULL,
    answered_at    timestamptz,
    ended_at       timestamptz NOT NULL,
    duration_ms    bigint      NOT NULL,
    end_reason     text        NOT NULL,
    ended_by       text        NOT NULL,
    sip_status     integer     NOT NULL,
    owner_epoch    bigint      NOT NULL,
    gathers        integer     NOT NULL,
    no_inputs      integer     NOT NULL,
    written_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, interaction_id)
);
ALTER TABLE cdr ENABLE ROW LEVEL SECURITY;
ALTER TABLE cdr FORCE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'cdr' AND policyname = 'cdr_tenant') THEN
        CREATE POLICY cdr_tenant ON cdr
            USING (tenant_id = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
    END IF;
END
$$;
