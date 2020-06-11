library(RMariaDB)
library(RPostgres)
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

DBI::dbDisconnect(con)

#save locally and zip
write_feather(grablong, 'data/lter/hbef/grab.feather')
write_feather(sensor, 'data/lter/hbef/sensor.feather')
zip('data/hbef.zip', list.files('data/lter/hbef', recursive=TRUE, full.names=TRUE))
out = googledrive::drive_upload("data/lter/hbef.zip",
    googledrive::as_id('https://drive.google.com/drive/folders/0ABfF-JkuRvL5Uk9PVA'))

#connect to timescaledb; generate schema if necessary
con = DBI::dbConnect(RPostgres::Postgres(), host='localhost',
    dbname='macrosheds', user='mike', password=postgres_pw)

# con = DBI::dbConnect(dbDriver('PostgreSQL'), host='localhost',
#     dbname='macrosheds', user='mike', password=postgres_pw)
# generate_schema_script = readr::read_file('src/generate_schema.sql')
# DBI::dbExecute(con, generate_schema_script)
# dbDisconnect(con)

#shape datasets for db entry
grab_insert = dplyr::mutate(grab, flag=paste('example flag', notes)) %>%
    dplyr::mutate(flag=replace(flag, flag == 'example flag NA', NA)) %>%
    tidylog::select(-sampleType, -hydroGraph,
        -fieldCode, -canonical, -waterYr, -ionError, -notes) %>%
    tidylog::gather('variable', 'value', Ca:precipCatch, -site) %>%
    filter(! variable %in% c('gageHt'))#, 'flowGageHt', 'precipCatch'))

#map all flag values to canonical flag types before db insertion
grab_insert = dplyr::mutate(grab_insert,
    flag_type=get_flag_types(flagmap, flag))

#read flag information from postgresql and convert from array form to strings
flag_grab = DBI::dbReadTable(con, 'flag_grab') %>%
    # dplyr::select(-id) %>%
    dplyr::mutate_all(list(~ stringr::str_replace_all(., '[\\{\\}]', ''))) %>%
    dplyr::mutate_all(list(~ stringr::str_replace_all(., '\\",', '";;'))) %>%
    dplyr::mutate_all(list(~ stringr::str_replace_all(., '\\"', '')))

existing_flags = paste(flag_grab$flag_type, flag_grab$flag_detail)

flag_insert = tidylog::select(grab_insert, flag_detail=flag, flag_type) %>%
    distinct() %>%
    dplyr::mutate(flag_detail=
        tidylog::replace_na(flag_detail, '')) %>%
    tidylog::filter(! paste(flag_type, flag_detail) %in% existing_flags)
# flag_insert[1,1] = 'aa,bb;;cc'

if(nrow(flag_insert)){
    flag_insert = as.data.frame(lapply(flag_insert, function(x) {
            x = resolve_commas(x, comma_standin=';;')
            x = postgres_arrayify(x)
            return(x)
        }))
}

#insert new flags into flag table
DBI::dbAppendTable(con, 'flag_grab', flag_insert)


#eventually set up variable corroboration like with flags above.
#for now, just importing from the CSV we've been working from
variable_insert = read_csv('../portal/data/variables.csv') %>%
    dplyr::select(-R_display) %>%
    dplyr::mutate(unit=1, method=1)

variable = DBI::dbReadTable(con, 'variable')

variable_insert = filter(variable_insert,
    ! variable_code %in% variable$variable_code)

#insert new vars into variable table
DBI::dbAppendTable(con, 'variable', variable_insert)

#still gotta build out communications with site, unit, method, waterway,
#domain, variable tables. all full of placeholders for now.

sites = unique(grab_insert$site)
site_insert = data.frame(site_name=sites) %>%
    dplyr::mutate(domain=1, waterway=1,
        site_code=substr(paste0(site_name, 'xx'), 1, 3),
        latitude=1, longitude=1, datum='WGS 84', add_date=Sys.time())

#insert new sites into site table
# DBI::dbAppendTable(con, 'site', site_insert)

#read flag information from postgresql and convert from array form to strings
flag_grab = DBI::dbReadTable(con, 'flag_grab') %>%
    # dplyr::select(-id) %>%
    dplyr::mutate_all(list(~ stringr::str_replace_all(., '[\\{\\}]', ''))) %>%
    dplyr::mutate_all(list(~ stringr::str_replace_all(., '\\",', '";;'))) %>%
    dplyr::mutate_all(list(~ stringr::str_replace_all(., '\\"', '')))

#update data with flag IDs
flags_combined = paste(grab_insert$flag_type, grab_insert$flag)
existing_flags_combined = paste(flag_grab$flag_type, flag_grab$flag_detail)
existing_flags_combined[which(existing_flags_combined == 'clean ')] = 'clean NA'
flag_ids = flag_grab$id[match(flags_combined, existing_flags_combined)]

grab_insert = dplyr::select(grab_insert, -flag, -flag_type) %>%
    dplyr::mutate(flag=flag_ids)

#update data with variable IDs and site IDs
variable = DBI::dbReadTable(con, 'variable')
variable_ids = variable$id[match(grab_insert$variable, variable$variable_code)]
grab_insert = dplyr::mutate(grab_insert, variable=variable_ids)

site = DBI::dbReadTable(con, 'site')
site_ids = site$id[match(grab_insert$site, site$site_name)]
grab_insert = dplyr::mutate(grab_insert, site=site_ids)

#insert new data into grab data table
DBI::dbAppendTable(con, 'data_grab', grab_insert)

dbDisconnect(con)
