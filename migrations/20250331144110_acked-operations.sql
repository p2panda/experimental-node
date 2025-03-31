CREATE TABLE IF NOT EXISTS acked_v1 (
    public_key              TEXT            NOT NULL,
    log_id                  TEXT            NOT NULL,
    seq_num                 TEXT            NOT NULL,
    PRIMARY_KEY(public_key, log_id)
);