library(RMariaDB)
library(RPostgreSQL)
library(DBI)
library(plyr)
library(data.table)
library(dtplyr)
library(tidyverse)
library(lubridate)
library(feather)
library(googledrive)
library(tidylog)
# library(rethinker)

`%>%` = dplyr::`%>%`
`.` = plyr::`.`
`.SD` = data.table::`.SD`

setwd('/home/mike/git/macrosheds/data_acquisition/')

source('src/helpers.R')
conf = readLines('config.txt')
mysql_pw = extract_from_config('MYSQL_PW')
postgres_pw = extract_from_config('POSTGRESQL_PW')

con = DBI::dbConnect(RMariaDB::MariaDB(), dbname='hbef',
    username='root', password=mysql_pw)

#can't use lazy_dt with summarize_if yet
grab_cur = DBI::dbReadTable(con, 'chemistry') %>%
# grab_cur = dtplyr::lazy_dt(DBI::dbReadTable(con, 'chemistry')) %>%
    tidylog::mutate(uniqueID=gsub('_Dup', '', uniqueID)) %>%
    data.table::data.table(.) %>%
    .[!is.na(uniqueID) & !is.na(datetime),
        lapply(.SD, function(x) {
            if(is.numeric(x)) mean(x, na.rm=TRUE) else x[! is.na(x)][1L]
        }),
        by=list(uniqueID, datetime)] %>%
    tibble::as_tibble(.) %>%
    # filter(!is.na(uniqueID) & !is.na(datetime)) %>%
    # group_by(uniqueID, datetime) %>%
    # summarize_if(is.numeric, mean, na.rm=TRUE) %>%
    # ungroup() %>%
    tidylog::mutate(site=stringr::str_match(uniqueID, '^(.+?)_.+$')[,2],
        datetime=lubridate::with_tz(as.POSIXct(datetime, tz='US/Eastern',
            format='%m/%e/%y %H:%M'), tzone='UTC')) %>%
    tidylog::select(-refNo, -uniqueID, -duplicate) %>%
    replace(is.na(.), NA) %>% #for NaNs
    tibble::as_tibble()

grab_hist = DBI::dbReadTable(con, 'historical') %>%
    tidylog::mutate(uniqueID=gsub('_Dup', '', uniqueID)) %>%
    data.table::data.table(.) %>%
   .[!is.na(uniqueID) & !is.na(datetime),
        lapply(.SD, function(x) {
            if(is.numeric(x)) mean(x, na.rm=TRUE) else x[! is.na(x)][1L]
        }),
        by=list(uniqueID, datetime)] %>%
    tibble::as_tibble(.) %>%
    tidylog::mutate(site=stringr::str_match(uniqueID, '^(.+?)_.+$')[,2],
        datetime=lubridate::with_tz(as.POSIXct(datetime, tz='US/Eastern',
            format='%m/%e/%y %H:%M'), tzone='UTC')) %>%
    tidylog::select(-refNo, -uniqueID, -date, -timeEST, -duplicate) %>%
    replace(is.na(.), NA) %>%
    tibble::as_tibble()

grab = tidylog::full_join(grab_cur, grab_hist) %>%
    dplyr::arrange(datetime)

sensor_Q = dtplyr::lazy_dt(DBI::dbReadTable(con, 'sensor2')) %>%
    tidylog::mutate(site=paste0('W', watershedID)) %>%
    tidylog::select(-id, -watershedID) %>%
    replace(is.na(.), NA) %>%
    tibble::as_tibble()

sensor_etc = dtplyr::lazy_dt(DBI::dbReadTable(con, 'sensor3')) %>%
    dplyr::rename_all(function(x) gsub('S3__', '', x)) %>%
    tidylog::mutate(site=paste0('W', watershedID)) %>%
    tidylog::select(-id, -watershedID) %>%
    replace(is.na(.), NA) %>%
    tibble::as_tibble()

sensor = tidylog::full_join(sensor_Q, sensor_etc) %>%
    dplyr::arrange(datetime)

# tidyr::gather(grab_cur,'variable', 'value',
#     data = gather(data, 'Variable', 'Value', Light_PAR:Discharge_m3s)

DBI::dbDisconnect(con)

#save locally and zip
feather::write_feather(grab, 'data/hbef/grab.feather')
feather::write_feather(sensor, 'data/hbef/sensor.feather')

zip('data/hbef.zip', list.files('data/hbef', recursive=TRUE, full.names=TRUE))
out = googledrive::drive_upload("data/hbef.zip",
    googledrive::as_id('https://drive.google.com/drive/folders/0ABfF-JkuRvL5Uk9PVA'))

#write data to timescaledb
con = DBI::dbConnect(dbDriver('PostgreSQL'), host='localhost', dbname='test',
    user='mike', password=postgres_pw)

create_domain_table =
    "CREATE TABLE domain (
        id                  SMALLSERIAL PRIMARY KEY,
        domainCode          CHAR(3)         NOT NULL,
        domainName          VARCHAR(100)    NOT NULL,
        originatingUser     SMALLINT,
        url                 VARCHAR(300)
    );"

create_waterway_table =
    "CREATE TABLE waterway (
        id                  SMALLSERIAL PRIMARY KEY,
        waterwayCode        CHAR(3)         NOT NULL,
        waterwayName        VARCHAR(100)    NOT NULL
    );"

create_site_table =
    "CREATE TABLE site (
        id                  SMALLSERIAL PRIMARY KEY,
        domain              SMALLINT        NOT NULL REFERENCES domain (id),
        waterway            SMALLINT        NOT NULL REFERENCES waterway (id),
        siteCode            CHAR(3)         NOT NULL,
        siteName            VARCHAR(100)    NOT NULL,
        latitude            FLOAT           NOT NULL,
        longitude           FLOAT           NOT NULL,
        datum               VARCHAR(100)    NOT NULL,
        addDate             TIMESTAMPTZ     NOT NULL,
        firstSensorRecord   TIMESTAMPTZ,
        lastSensorRecord    TIMESTAMPTZ,
        firstGrabRecord     TIMESTAMPTZ,
        lastGrabRecord      TIMESTAMPTZ,
        sensorVarList       TEXT [],
        grabVarList         TEXT [],
        wsSummaryVarList    TEXT [],
        originatingUser     SMALLINT,
        url                 VARCHAR(300),
        contactEmail        VARCHAR(100)
    );"

create_unit_table =
    "CREATE TABLE unit (
        id                  SMALLSERIAL PRIMARY KEY,
        unitCode            CHAR(3)         NOT NULL,
        unitName            VARCHAR(100)    NOT NULL
    );"

create_method_table =
    "CREATE TABLE method (
        id                  SMALLSERIAL PRIMARY KEY,
        methodCode          CHAR(3)         NOT NULL,
        methodName          VARCHAR(100)    NOT NULL
    );"

create_variable_table =
    "CREATE TABLE variable (
        id                  SMALLSERIAL PRIMARY KEY,
        variableCode        CHAR(3)         NOT NULL,
        variableName        VARCHAR(100)    NOT NULL,
        variableType        CHAR(9)         NOT NULL,
        stdUnit             SMALLINT        NOT NULL REFERENCES unit (id),
        stdMethod           SMALLINT        NOT NULL REFERENCES method (id),
        unitList            TEXT []
    );

    ALTER TABLE variable
        ADD CONSTRAINT check_types
        CHECK (variableType IN (
            'sensor', 'grab', 'wsSummary', 'blob', 'meta'
        ) );"

create_flagSensor_table =
    "CREATE TABLE flagSensor (
        id                  SERIAL          PRIMARY KEY,
        dtStart             TIMESTAMPTZ     NOT NULL,
        dtEnd               TIMESTAMPTZ     NOT NULL,
        flagType            CHAR(15)        NOT NULL,
        flagDetail          VARCHAR(100)    DEFAULT ''
    );"

create_flagGrab_table =
    "CREATE TABLE flagGrab (
        id                  SERIAL          PRIMARY KEY,
        dtStart             TIMESTAMPTZ     NOT NULL,
        dtEnd               TIMESTAMPTZ     NOT NULL,
        flagType            CHAR(15)        NOT NULL,
        flagDetail          VARCHAR(100)    DEFAULT ''
    );"


#####HERE: specifying pkey and creating hypertable
create_dataSensor_table =
        # id                  BIGSERIAL       PRIMARY KEY,
    "CREATE TABLE dataSensor (
        timestamp           TIMESTAMPTZ     NOT NULL,
        variable            SMALLINT        NOT NULL REFERENCES variable (id),
        unit                SMALLINT        NOT NULL REFERENCES unit (id),
        method              SMALLINT        NOT NULL REFERENCES method (id),
        flag                INTEGER         NOT NULL REFERENCES flagSensor (id)
    );
    SELECT create_hypertable('dataSensor', 'timestamp');"

create_dataGrab_table =
    "CREATE TABLE dataGrab (
        id                  SERIAL          PRIMARY KEY,
        timestamp           TIMESTAMPTZ     NOT NULL,
        variable            SMALLINT        NOT NULL REFERENCES variable (id),
        unit                SMALLINT        NOT NULL REFERENCES unit (id),
        method              SMALLINT        NOT NULL REFERENCES method (id),
        flag                INTEGER         NOT NULL REFERENCES flagGrab (id)
    );"

DBI::dbExecute(con, create_domain_table)
DBI::dbExecute(con, create_waterway_table)
DBI::dbExecute(con, create_site_table)
DBI::dbExecute(con, create_unit_table)
DBI::dbExecute(con, create_method_table)
DBI::dbExecute(con, create_variable_table)
DBI::dbExecute(con, create_flagSensor_table)
DBI::dbExecute(con, create_flagGrab_table)
DBI::dbExecute(con, create_dataSensor_table)
DBI::dbExecute(con, create_dataGrab_table)
        # temperature     DOUBLE PRECISION    NULL"
        # device_id  INTEGER CHECK (device_id > 0),
       #"SELECT create_hypertable('gg', 'time');"
# dbClearResult(dbListResults(con)[[1]])

# #write data to rethinkdb
# recon = openConnection(host="localhost", port=28015, authKey=NULL, v="V0_4")
#
# dbs = cursorToList(r()$dbList()$run(recon))
# if(! 'macrosheds' %in% dbs){
#     r()$dbCreate('macrosheds')$run(recon)
# }
#
# tables = cursorToList(r('macrosheds')$tableList()$run(recon))
# if(! 'sourcedata' %in% dbs){
#     r('macrosheds')$tableCreate('sourcedata')$run(recon)
# }
#
# grablong = gather(select(grab, -waterYr), 'variable', 'value', Ca:canonical)
# grablong$id = 1:nrow(grablong)
# grablist = plyr::daply(grablong, .(id), list)
# grablist = unname(grablist)
#
# chunker_ingester = function(l, chunksize=100000){
#
#     #determine chunks based on number of records
#     n_recs = length(l)
#     n_full_chunks = floor(n_recs / chunksize)
#     partial_chunk_len = n_recs %% chunksize
#
#     #convert directly to dict if small enough, otherwise do it chunkwise
#     if(n_full_chunks == 0){
#         r('macrosheds', 'sourcedata')$insert(l)$run(recon)
#     } else {
#         for(i in 1:n_full_chunks){
#             chunk = l[1:chunksize]
#             l[1:chunksize] = NULL
#             r('macrosheds', 'sourcedata')$insert(chunk)$run(recon)
#         }
#     }
#
#     if(partial_chunk_len){
#         r('macrosheds', 'sourcedata')$insert(l)$run(recon)
#     }
# }
#
# chunker_ingester(grablist)
#
# r('macrosheds', 'sourcedata')$insert(grablist[1:5])$run(recon)
# r('macrosheds', 'sourcedata')$get(1)$run(recon)
# r('macrosheds', 'sourcedata')$delete()$run(recon)
# r('macrosheds', 'sourcedata')$count()$run(recon)
# close(recon)
