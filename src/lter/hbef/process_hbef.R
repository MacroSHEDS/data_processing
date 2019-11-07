library(tidyverse)
library(glue)
library(lubridate)
library(imputeTS)
library(feather)
library(tidylog)

`%>%` = magrittr::`%>%`
`.` = plyr::`.`
mutate = dplyr::mutate
rename = dplyr::rename
group_by = dplyr::group_by
ungroup = dplyr::ungroup
arrange = dplyr::arrange
filter = tidylog::filter
select = tidylog::select
gather = tidylog::gather
spread = tidylog::spread
left_join = tidylog::left_join
right_join = tidylog::right_join
summarize = tidylog::summarize
distinct = tidylog::distinct

setwd('/home/mike/git/macrosheds/data_acquisition/data/lter/hbef/')
outdir = '/home/mike/git/macrosheds/portal/data/hbef/'
source('../../../src/lter/retrieve_lter.R')
source('../../../src/helpers.R')

lter_download('/home/mike/git/macrosheds/data_acquisition/data/lter',
    'hbef')

#ex:
# gg = read_feather('~/git/macrosheds/portal/data/hjandrews/grab.feather')

#grab
grab = readr::read_csv('raw/stream_precip_chemistry/stream chemistry',
        guess_max=30000) %>%
    rename(site_name=site) %>%
    mutate(datetime=lubridate::as_datetime(paste(date, timeEST), tz='EST')) %>%
    mutate(datetime=lubridate::with_tz(datetime, tz='UTC')) %>%
    select(site_name, datetime, dplyr::everything(),
        -one_of('archived', 'pHmetrohm', 'uniqueID', 'gageHt', 'sampleType',
            'hydroGraph', 'flowGageHt', 'fieldCode', 'waterYr', 'canonical',
            'duplicate', 'date', 'timeEST', 'notes', 'precipCatch')) %>%
    distinct(.keep_all=TRUE) %>%
    arrange(site_name, datetime) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))

feather::write_feather(grab, paste0(outdir, '/grab.feather'))

#precip chemistry
pchem = readr::read_csv('raw/stream_precip_chemistry/precipitation chemistry',
    guess_max=30000) %>%
    rename(site_name=site) %>%
    mutate(datetime=lubridate::as_datetime(paste(date, timeEST), tz='EST')) %>%
    mutate(datetime=lubridate::with_tz(datetime, tz='UTC')) %>%
    select(site_name, datetime, dplyr::everything(),
        -one_of('archived', 'pHmetrohm', 'uniqueID', 'gageHt', 'sampleType',
            'hydroGraph', 'flowGageHt', 'fieldCode', 'waterYr', 'canonical',
            'duplicate', 'date', 'timeEST', 'notes', 'precipCatch')) %>%
    distinct(.keep_all=TRUE) %>%
    arrange(site_name, datetime) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))

feather::write_feather(pchem, paste0(outdir, '/pchem.feather'))

#P (must be exported in mm)
precip = readr::read_csv('raw/precipitation/dailyGagePrecip1956-2019.csv') %>%
    mutate(datetime=lubridate::as_datetime(DATE, tz='UTC')) %>%
    select(-DATE) %>%
    rename(site_name=rainGage, precip=Precip) %>%
    distinct(.keep_all=TRUE) %>%
    arrange(site_name, datetime) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))

feather::write_feather(precip, paste0(outdir, '/precip.feather'))
clear_from_mem(precip)

#Q (must be exported in L/s)
sets = dir('raw/discharge', full.names=TRUE)
sets_old = sets[grep('^(?!.*5min).*$', sets, perl=TRUE)]
sets_5min = sets[grep('5min', sets)]

discharge = tibble::tibble()

for(s in sets_old){

    d = readr::read_csv(s,
            col_types=readr::cols(
                DATETIME=readr::col_character()
            )) %>%
        select(-Gage_ft, -Discharge_cfs) %>%
        rename(site_name=WS, Q=Discharge_ls, datetime=DATETIME) %>%
        mutate(site_name=paste0('W', site_name)) %>%
        distinct(.keep_all=TRUE) %>%
        dplyr::filter_at(dplyr::vars(-site_name, -datetime),
            dplyr::any_vars(! is.na(.)))
    #lubridate can't handle invalid datetime formats like 2017-01-01 24:00
    d$datetime = as.POSIXct(d$datetime, tz='UTC')

    discharge = dplyr::bind_rows(discharge, d)
}

for(s in sets_5min){

    d = readr::read_csv(s,
            col_types=readr::cols(
                DATETIME=readr::col_character(),
                Flag=readr::col_character()
            )) %>%
        select(-Gage_ft, -Discharge_cfs, -Flag) %>%
        filter(substr(DATETIME, 15, 16) %in% c('00', '30')) %>%
        rename(site_name=WS, Q=Discharge_ls, datetime=DATETIME) %>%
        mutate(site_name=paste0('W', site_name)) %>%
        distinct(.keep_all=TRUE) %>%
        dplyr::filter_at(dplyr::vars(-site_name, -datetime),
            dplyr::any_vars(! is.na(.)))
    #lubridate can't handle invalid datetime formats like 2017-01-01 24:00
    d$datetime = as.POSIXct(d$datetime, tz='UTC')

    discharge = dplyr::bind_rows(discharge, d)
}

discharge = arrange(discharge, site_name, datetime)

feather::write_feather(discharge, paste0(outdir, '/discharge.feather'))
# discharge = feather::read_feather(paste0(outdir, '/discharge.feather'))

#flux
site_data = read.csv('../../general/site_data.csv', stringsAsFactors=FALSE)
non_flux_vars = c('ANC960', 'ANCMet', 'anionCharge', 'cationCharge',
    'flowGageHt', 'ionBalance, OMAl', 'pH', 'precip', 'spCond',
    'temp', 'theoryCond', 'TMAl', 'Alk', 'suspSed')

grab_daily = grab %>%
    tidylog::select(- dplyr::one_of(non_flux_vars)) %>%
    dplyr::mutate(date=lubridate::floor_date(datetime, 'day')) %>%
    dplyr::group_by(site_name, date) %>%
    tidylog::summarize_if(is.numeric, mean, na.rm=TRUE) %>%
    dplyr::ungroup()
clear_from_mem(grab)

grab_rng = tibble::tibble(date=seq(min(grab_daily$date),
    max(grab_daily$date), by='1 day'))
grab_daily = grab_daily %>%
    tidylog::right_join(grab_rng, by='date') %>%
    tidylog::right_join(discharge, by=c(date='datetime', 'site_name')) %>%
    dplyr::arrange(site_name, date)
clear_from_mem(discharge, grab_rng)

#interpolate by site and var, but skip sets of all NA; requires a dplyr god
for(s in unique(grab_daily$site_name)){
    sitevar = grab_daily[grab_daily$site_name == s, ]
    for(c in 3:ncol(sitevar)){
        if(is.numeric(sitevar[, c]) & sum(! is.na(sitevar[, c])) > 2){
            sitevar[, c] = imputeTS::na_interpolation(sitevar[, c],
                option='linear', maxgap=15)
        }
    }
    grab_daily[grab_daily$site_name == s, ] = sitevar
}

Q_daily = grab_daily %>%
    dplyr::group_by(site_name, date) %>%
    tidylog::summarize(Q_Ld=sum(Q * 60 * 60, na.rm=TRUE)) %>%
    dplyr::ungroup()

flux_daily = grab_daily %>%
    tidylog::left_join(Q_daily, by=c('date', 'site_name')) %>%
    dplyr::mutate_at(dplyr::vars(-site_name, -date, -Q_Ld),
        ~(. * Q_Ld) / (1000 * 1000)) %>%
    tidylog::left_join(dplyr::select(site_data, site_name, ws_area_ha),
        by='site_name') %>%
    dplyr::mutate_at(dplyr::vars(-site_name, -date, -Q_Ld),
        ~(. / ws_area_ha)) %>%
    dplyr::mutate(datetime=as.POSIXct(date)) %>%
    tidylog::select(datetime, dplyr::everything(),
        -ws_area_ha, -date, -Q_Ld, -Q) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))
clear_from_mem(grab_daily, Q_daily)

feather::write_feather(flux_daily, path=paste0(outdir, '/flux.feather'))
