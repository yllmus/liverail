-- A model that retrains itself needs somewhere to prove it got better.
--
-- WHY THIS EXISTS. The transfer probability was shipped unverified for weeks
-- and turned out to be systematically pessimistic in every reliability bin
-- (see docs/delay-forecasting.md). Nothing detected that, because nothing was
-- scoring it. A system that retrains on new data automatically has the same
-- failure mode with a shorter fuse: it can degrade silently and continuously.
--
-- So every candidate is scored against the incumbent on the SAME held-out
-- data, both results are written here, and the candidate is promoted only if
-- it wins. The table is the audit trail: if quality moves, this says when and
-- by how much, rather than leaving someone to bisect commits.
--
-- WHAT IS DELIBERATELY NOT HERE. No model weights. LightGBM boosters are
-- written to disk under model_path; Postgres on this box holds 12 GB of
-- observations already and is the wrong place for binary blobs.

CREATE TABLE IF NOT EXISTS model_eval (
    id            bigserial PRIMARY KEY,
    country_code  text        NOT NULL REFERENCES countries(code),
    -- 'transfer_risk' today; the loop is not specific to it.
    task          text        NOT NULL,
    -- 'mixture' (the analytic incumbent), 'lgbm', or a future name. Never a
    -- version number alone - the name has to say what KIND of model it is,
    -- because comparing v4 to v5 is meaningless if they are different shapes.
    model_kind    text        NOT NULL,
    trained_at    timestamp   NOT NULL DEFAULT now(),

    -- The evaluation window. Recorded because a Brier score is meaningless
    -- without knowing what it was measured on: a quiet week and a strike week
    -- produce very different numbers for an unchanged model.
    train_from    date,
    train_to      date,
    test_from     date,
    test_to       date,
    n_train       integer,
    n_test        integer,

    brier         double precision NOT NULL,
    -- Scored separately because the overall figure is dominated by easy
    -- 20-60 minute buffers where everything succeeds. A model can improve
    -- overall while getting worse at the only case anyone asks about.
    brier_tight   double precision,
    base_rate     double precision,
    calibration_err double precision,

    promoted      boolean     NOT NULL DEFAULT false,
    -- Why it was or was not promoted, in words. A gate that rejects silently
    -- is a gate nobody trusts.
    decision      text,
    model_path    text,
    notes         text
);

CREATE INDEX IF NOT EXISTS idx_model_eval_lookup
    ON model_eval (country_code, task, trained_at DESC);

-- The current champion per (country, task): the most recent promoted row.
CREATE INDEX IF NOT EXISTS idx_model_eval_champion
    ON model_eval (country_code, task, promoted, trained_at DESC)
    WHERE promoted;

GRANT ALL ON model_eval TO trainuser;
GRANT ALL ON SEQUENCE model_eval_id_seq TO trainuser;
