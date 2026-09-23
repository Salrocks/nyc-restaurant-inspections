CREATE TABLE dim_restaurant (
    camis VARCHAR(12) PRIMARY KEY,
    dba TEXT,
    boro VARCHAR(20),
    building VARCHAR(50),
    street TEXT,
    zipcode CHAR(5),
    phone VARCHAR(12),
    cuisine_description TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE fct_inspection(
    inspection_id BIGSERIAL PRIMARY KEY,
    camis VARCHAR(12) NOT NULL REFERENCES dim_restaurant(camis),
    inspection_date DATE,
    inspection_program  TEXT,
    inspection_type TEXT,
    action TEXT,
    score NUMERIC(5,1),
    grade  CHAR(1),
    grade_date DATE,
    is_closed BOOLEAN,
    is_uninspected BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at TIMESTAMP  NOT NULL DEFAULT NOW(),

    CONSTRAINT score_non_negative CHECK (score IS NULL OR score >= 0),
    CONSTRAINT grade_valid        CHECK (grade IS NULL OR grade IN ('A','B','C','N','P','Z'))

);

CREATE UNIQUE INDEX uq_fct_inspection_grain
    ON fct_inspection (camis, inspection_date, inspection_program, inspection_type)
    NULLS NOT DISTINCT;


CREATE TABLE fct_violation (
    violation_id          BIGSERIAL PRIMARY KEY,
    camis                 VARCHAR(12)  NOT NULL REFERENCES dim_restaurant(camis),
    inspection_date       DATE,
    inspection_program    TEXT,
    inspection_type       TEXT,
    violation_code        VARCHAR(10)  NOT NULL,
    violation_description TEXT,
    critical_flag         VARCHAR(20),
    is_critical           BOOLEAN,
    loaded_at             TIMESTAMP  NOT NULL DEFAULT NOW(),

    CONSTRAINT critical_flag_valid CHECK (
        critical_flag IS NULL
        OR critical_flag IN ('Critical','Not Critical','Not Applicable')
    )
);


CREATE TABLE quarantine (
    quarantine_id   BIGSERIAL    PRIMARY KEY,
    camis           TEXT,
    dba             TEXT,
    inspection_date TEXT,
    reject_reason   TEXT         NOT NULL,
    raw_record      JSONB,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dim_restaurant_boro    ON dim_restaurant (boro);
CREATE INDEX idx_dim_restaurant_cuisine ON dim_restaurant (cuisine_description);

CREATE INDEX idx_fct_inspection_camis   ON fct_inspection (camis);
CREATE INDEX idx_fct_inspection_date    ON fct_inspection (inspection_date);
CREATE INDEX idx_fct_inspection_grade   ON fct_inspection (grade);

CREATE INDEX idx_fct_violation_camis    ON fct_violation (camis);
CREATE INDEX idx_fct_violation_code     ON fct_violation (violation_code);
CREATE INDEX idx_fct_violation_critical ON fct_violation (critical_flag);

CREATE INDEX idx_quarantine_reason      ON quarantine (reject_reason);