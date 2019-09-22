CREATE TABLE domain (
    id                  SMALLSERIAL PRIMARY KEY,
    domain_code         CHAR(3)         NOT NULL,
    domain_name         VARCHAR(100)    NOT NULL,
    originating_user    SMALLINT,
    url                 VARCHAR(300)
);

CREATE TABLE waterway (
    id                  SMALLSERIAL PRIMARY KEY,
    waterway_code       CHAR(3)         NOT NULL,
    waterway_name       VARCHAR(100)    NOT NULL
);

CREATE TABLE site (
    id                  SMALLSERIAL PRIMARY KEY,
    domain              SMALLINT        NOT NULL REFERENCES domain (id),
    waterway            SMALLINT        NOT NULL REFERENCES waterway (id),
    site_code           CHAR(3)         NOT NULL,
    site_name           VARCHAR(100)    NOT NULL,
    latitude            FLOAT           NOT NULL,
    longitude           FLOAT           NOT NULL,
    datum               VARCHAR(100)    NOT NULL,
    add_date            TIMESTAMPTZ     NOT NULL,
    first_sensor_record TIMESTAMPTZ,
    last_sensor_record  TIMESTAMPTZ,
    first_grab_record   TIMESTAMPTZ,
    last_grab_record    TIMESTAMPTZ,
    sensor_var_list     TEXT [],
    grab_var_list       TEXT [],
    ws_summary_var_list TEXT [],
    originating_user    SMALLINT,
    url                 VARCHAR(300),
    contact_email       VARCHAR(100)
);

CREATE TABLE unit (
    id                  SMALLSERIAL PRIMARY KEY,
    unit_code           CHAR(3)         NOT NULL,
    unit_name           VARCHAR(100)    NOT NULL
);

CREATE TABLE method (
    id                  SMALLSERIAL PRIMARY KEY,
    method_code         CHAR(3)         NOT NULL,
    method_name         VARCHAR(100)    NOT NULL
);

CREATE TABLE variable (
    id                  SMALLSERIAL PRIMARY KEY,
    variable_code       CHAR(3)         NOT NULL,
    variable_name       VARCHAR(100)    NOT NULL,
    variable_type       CHAR(9)         NOT NULL,
    std_unit            SMALLINT        NOT NULL REFERENCES unit (id),
    std_method          SMALLINT        NOT NULL REFERENCES method (id),
    unit_list           TEXT []
);

ALTER TABLE variable
    ADD CONSTRAINT check_types
    CHECK (variable_type IN (
        'sensor', 'grab', 'ws_summary', 'blob', 'meta'
    ) );

CREATE TABLE flag_sensor (
    id                  SERIAL          PRIMARY KEY,
    flag_start          TIMESTAMPTZ     NOT NULL,
    flag_end            TIMESTAMPTZ     NOT NULL,
    flag_type           TEXT []         NOT NULL,
    flag_detail         TEXT []         DEFAULT ARRAY ['']
);
-- flagtype            INTEGER         NOT NULL REFERENCES flagtypes (id),

-- ALTER TABLE flag_sensor
--     ADD CONSTRAINT check_types
--     CHECK (flag_type IN (
--         '', 'sensor_concern', 'unit_unknown', 'unit_concern',
--         'method_unknown', 'method_concern', 'general_concern'
--     ) );
-- if you update flagtype list, make it jibe with charsize limit

CREATE TABLE flag_grab (
    id                  SERIAL          PRIMARY KEY,
    flag_start          TIMESTAMPTZ     NOT NULL,
    flag_end            TIMESTAMPTZ     NOT NULL,
    flag_type           CHAR(15)        NOT NULL,
    flag_detail         VARCHAR(100)    DEFAULT ''
);

CREATE TABLE data_sensor (
    datetime            TIMESTAMPTZ     NOT NULL,
    site                SMALLINT        NOT NULL REFERENCES SITE (id),
    variable            SMALLINT        NOT NULL REFERENCES variable (id),
    value               FLOAT,
    flag                INTEGER         NOT NULL REFERENCES flag_sensor (id)
);
    -- id                  BIGSERIAL       PRIMARY KEY,

SELECT create_hypertable('data_sensor', 'datetime');
        -- unit                SMALLINT        NOT NULL REFERENCES unit (id),
        -- method              SMALLINT        NOT NULL REFERENCES method (id),

CREATE TABLE data_grab (
    datetime            TIMESTAMPTZ     NOT NULL,
    site                SMALLINT        NOT NULL REFERENCES SITE (id),
    variable            SMALLINT        NOT NULL REFERENCES variable (id),
    value               FLOAT,
    flag                INTEGER         NOT NULL REFERENCES flag_grab (id)
);
    -- id                  SERIAL          PRIMARY KEY,

SELECT create_hypertable('data_grab', 'datetime');
        -- unit                SMALLINT        NOT NULL REFERENCES unit (id),
        -- method              SMALLINT        NOT NULL REFERENCES method (id),
