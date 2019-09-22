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
feather::write_feather(grab, 'data/lter/hbef/grab.feather')
feather::write_feather(sensor, 'data/lter/hbef/sensor.feather')
zip('data/hbef.zip', list.files('data/lter/hbef', recursive=TRUE, full.names=TRUE))
out = googledrive::drive_upload("data/lter/hbef.zip",
    googledrive::as_id('https://drive.google.com/drive/folders/0ABfF-JkuRvL5Uk9PVA'))

#write data to timescaledb
con = DBI::dbConnect(dbDriver('PostgreSQL'), host='localhost', dbname='test',
    user='mike', password=postgres_pw)

generate_schema_script = read_file('src/generate_schema.sql')
DBI::dbExecute(con, generate_schema_script)


dbWriteTable(con, 'data_grab',
    value=data_grab, append=TRUE, row.names=FALSE)
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
