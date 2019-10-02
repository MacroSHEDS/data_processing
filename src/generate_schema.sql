CREATE TABLE IF NOT EXISTS domain (
    id                  SMALLSERIAL PRIMARY KEY,
    domain_code         CHAR(3)         NOT NULL,
    domain_name         VARCHAR(100)    NOT NULL,
    originating_user    SMALLINT,
    url                 VARCHAR(300)
);

CREATE TABLE IF NOT EXISTS waterway (
    id                  SMALLSERIAL PRIMARY KEY,
    waterway_code       CHAR(3)         NOT NULL,
    waterway_name       VARCHAR(100)    NOT NULL
);

CREATE TABLE IF NOT EXISTS site (
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

CREATE TABLE IF NOT EXISTS unit (
    id                  SMALLSERIAL PRIMARY KEY,
    unit_code           CHAR(3)         NOT NULL,
    unit_name           VARCHAR(100)    NOT NULL
);

CREATE TABLE IF NOT EXISTS method (
    id                  SMALLSERIAL PRIMARY KEY,
    method_code         CHAR(3)         NOT NULL,
    method_name         VARCHAR(100)    NOT NULL
);

CREATE TABLE IF NOT EXISTS variable (
    id                  SMALLSERIAL PRIMARY KEY,
    variable_code       VARCHAR(15)     NOT NULL,
    variable_name       VARCHAR(100)    NOT NULL,
    unit                SMALLINT        NOT NULL REFERENCES unit (id),
    method              SMALLINT        NOT NULL REFERENCES method (id),
    variable_type       CHAR(10)        NOT NULL,
    variable_subtype    CHAR(7)         NOT NULL
);
--    unit_list           TEXT []

ALTER TABLE variable
    ADD CONSTRAINT check_types
    CHECK (variable_type IN (
        'sensor', 'grab', 'ws_summary', 'blob', 'meta'
    ) );

CREATE TABLE IF NOT EXISTS flag_sensor (
    id                  SERIAL          PRIMARY KEY,
    flag_type           TEXT []         NOT NULL,
    flag_detail         TEXT []         NOT NULL DEFAULT ARRAY ['']
);
--    flag_type           INTEGER         NOT NULL REFERENCES flagtypes (id),
--    flag_start          TIMESTAMPTZ     NOT NULL,
--    flag_end            TIMESTAMPTZ     NOT NULL,

-- ALTER TABLE flag_sensor
--     ADD CONSTRAINT check_types
--     CHECK (flag_type IN (
--         '', 'sensor_concern', 'unit_unknown', 'unit_concern',
--         'method_unknown', 'method_concern', 'general_concern'
--     ) );
-- if you update flagtype list, make it jibe with charsize limit

CREATE TABLE IF NOT EXISTS flag_grab (
    id                  SERIAL          PRIMARY KEY,
    flag_type           TEXT []         NOT NULL,
    flag_detail         TEXT []         NOT NULL DEFAULT ARRAY ['']
);
--    flag_start          TIMESTAMPTZ     NOT NULL,
--    flag_end            TIMESTAMPTZ     NOT NULL,

CREATE TABLE IF NOT EXISTS data_sensor (
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

CREATE TABLE IF NOT EXISTS data_grab (
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

insert into waterway (waterway_code, waterway_name) values ('plc', 'placeholder');
insert into domain (domain_code, domain_name) values ('plc', 'placeholder');
insert into method (method_code, method_name) values ('plc', 'placeholder');
insert into unit (method_code, method_name) values ('plc', 'placeholder');
