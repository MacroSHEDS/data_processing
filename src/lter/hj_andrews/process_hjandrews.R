library(tidyverse)
library(glue)
library(lubridate)
library(imputeTS)
library(feather)
library(tidylog)

#NOTE: this script could be clarified with elements from subsequently written
#processing scripts, e.g. process_hbef.R

`%>%` = magrittr::`%>%`
`.` = plyr::`.`

setwd('/home/mike/git/macrosheds/data_acquisition/data/lter/hjandrews/')
outdir = '/home/mike/git/macrosheds/portal/data/hjandrews/'
source('../../../src/lter/retrieve_lter.R')

lter_download('/home/mike/git/macrosheds/data_acquisition/data/lter',
    'hjandrews')

lter_download('/home/mike/git/macrosheds/data_acquisition/data/lter',
    'south_umpqua')

#P (must be exported in mm)
precip = readr::read_csv('raw/precipitation/MS00403') %>%
    tidylog::select(DATE, SITECODE, PRECIP_TOT_DAY, PRECIP_TOT_FLAG) %>%
    tidylog::filter(PRECIP_TOT_FLAG %in% c('A', 'E', 'T', 'U')) %>%
    tidylog::select(-PRECIP_TOT_FLAG) %>%
    dplyr::rename(datetime=DATE, site_name=SITECODE, precip=PRECIP_TOT_DAY) %>%
    dplyr::mutate(datetime=as.POSIXct(datetime)) %>%
    tidylog::distinct(.keep_all=TRUE) %>%
    dplyr::arrange(site_name, datetime) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))
attr(precip$datetime, 'tzone') = 'UTC'

feather::write_feather(precip, paste0(outdir, '/precip.feather'))

#Q (must be exported in L/s)
discharge = readr::read_csv('raw/discharge/HF00402') %>%
    tidylog::select(DATE, SITECODE, MAX_Q) %>%
    dplyr::mutate(MAX_Q=MAX_Q * 28.32) %>%
    dplyr::rename(datetime=DATE, site_name=SITECODE, Q=MAX_Q) %>%
    dplyr::mutate(datetime=as.POSIXct(datetime)) %>%
    tidylog::distinct(.keep_all=TRUE) %>%
    dplyr::arrange(site_name, datetime) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))
attr(discharge$datetime, 'tzone') = 'UTC'

feather::write_feather(discharge, paste0(outdir, '/discharge.feather'))

#conc (make sure datetime is POSIXct and not Date)
grab = readr::read_csv('raw/stream_chemistry/CF00201') %>%
    dplyr::rename(Na=X46) %>%
    tidylog::select(-one_of('STCODE', 'LABNO', 'TYPE', 'ENTITY', 'WATERYEAR',
        'INTERVAL', 'MEAN_LPS', 'Q_AREA_CM', 'QCODE', 'PVOL', 'PVOLCODE'))

grabcodes = tidylog::select(grab, SITECODE, DATE_TIME,
        dplyr::ends_with('CODE')) %>%
    tidylog::gather('code', 'codeval', -SITECODE, -DATE_TIME)
grabvars = tidylog::select(grab, -dplyr::ends_with('CODE'), SITECODE) %>%
    tidylog::gather('var', 'val', -SITECODE, -DATE_TIME) %>%
    tidylog::select(-SITECODE, -DATE_TIME)

grab = dplyr::bind_cols(grabcodes, grabvars) %>%
    tidylog::filter(codeval %in% c('A', 'E')) %>%
    tidylog::select(-code, -codeval) %>%
    tidylog::spread(var, val) %>%
    dplyr::rename_all(dplyr::recode, SITECODE='site_name', DATE_TIME='datetime',
        ALK='alk', CA='Ca', CL='Cl', COND='spCond', MG='Mg', NH3N='NH3_N',
        NO3N='NO3_N', PH='pH', PO4P='PO4_P', SI='SiO2', SO4S='SO4_S',
        SSED='suspSed') %>%
    dplyr::mutate(spCond=spCond / 1000) %>% #convert to S/cm
    tidylog::distinct(.keep_all=TRUE) %>%
    dplyr::arrange(site_name, datetime) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))

feather::write_feather(grab, paste0(outdir, '/grab.feather'))

#precip chemistry
pchem = readr::read_csv('raw/precip_chemistry/CP00201', guess_max=30000) %>%
    dplyr::rename(Na=X44) %>%
    tidylog::select(-one_of('STCODE', 'LABNO', 'TYPE', 'ENTITY', 'WATERYEAR',
        'INTERVAL', 'MEAN_LPS', 'Q_AREA_CM', 'QCODE', 'PVOL', 'PVOLCODE',
        'PRECIP_CM'))

pchemcodes = tidylog::select(pchem, SITECODE, DATE_TIME,
        dplyr::ends_with('CODE'), -PCODE) %>%
    tidylog::gather('code', 'codeval', -SITECODE, -DATE_TIME)
pchemvars = tidylog::select(pchem, -dplyr::ends_with('CODE'), SITECODE) %>%
    tidylog::gather('var', 'val', -SITECODE, -DATE_TIME) %>%
    tidylog::select(-SITECODE, -DATE_TIME)

pchem = dplyr::bind_cols(pchemcodes, pchemvars) %>%
    tidylog::filter(codeval %in% c('A', 'E')) %>%
    tidylog::select(-code, -codeval) %>%
    tidylog::spread(var, val) %>%
    dplyr::rename_all(dplyr::recode, SITECODE='site_name', DATE_TIME='datetime',
        ALK='alk', CA='Ca', CL='Cl', COND='spCond', MG='Mg', NH3N='NH3_N',
        NO3N='NO3_N', PH='pH', PO4P='PO4_P', SI='SiO2', SO4S='SO4_S',
        SSED='suspSed') %>%
    dplyr::mutate(spCond=spCond / 1000) %>% #convert to S/cm
    tidylog::distinct(.keep_all=TRUE) %>%
    dplyr::arrange(site_name, datetime) %>%
    dplyr::filter_at(dplyr::vars(-site_name, -datetime),
        dplyr::any_vars(! is.na(.)))

feather::write_feather(pchem, paste0(outdir, '/pchem.feather'))

#flux
site_data = read.csv('../../general/site_data.csv', stringsAsFactors=FALSE)
non_flux_vars = c('ANC960', 'ANCMet', 'anionCharge', 'cationCharge',
    'flowGageHt', 'ionBalance, OMAl', 'pH', 'precip', 'spCond',
    'temp', 'theoryCond', 'TMAl', 'Alk', 'suspSed')

# #method 1: seems to be removing data that should remain
# grab_daily = grab %>%
#     tidylog::select(- one_of(non_flux_vars)) %>%
#     dplyr::mutate(date=as.Date(datetime)) %>%
#     dplyr::group_by(site_name, date) %>%
#     tidylog::summarize_if(is.numeric, mean, na.rm=TRUE)
#
# grab_rng = seq(min(grab$datetime), max(grab$datetime), by='1 hour')
# hours_only = tibble::tibble(datehour=grab_rng %>%
#         lubridate::round_date(., 'hours')) %>%
#     tidyr::nest()
#
# full_hours = tibble::tibble(site_name=unique(discharge$site_name),
#         full_hours=rep(hours_only$data, length(site_name))) %>%
#     tidyr::unnest(full_hours)
#
# #currently aggregating by day, but this could be useful down the road
# q_hourly = discharge %>%
#     dplyr::mutate(datetime=as.POSIXct(datetime)) %>%
#     dplyr::mutate(datehour=lubridate::round_date(datetime, 'hours')) %>%
#     tidylog::right_join(., full_hours, by=c('site_name', 'datehour')) %>%
#     dplyr::arrange(datehour) %>%
#     dplyr::group_by(site_name) %>%
#     dplyr::mutate(Q_full=imputeTS::na_interpolation(Q,
#         option='linear', maxgap=30)) %>%
#     tidylog::filter(! is.na(Q_full)) %>%
#     dplyr::group_by(datehour, site_name) %>%
#     tidylog::summarize(Q=mean(Q_full, na.rm=TRUE))
#
# q_daily = q_hourly %>%
#     dplyr::mutate(date=as.Date(datehour)) %>%
#     dplyr::group_by(site_name, date) %>%
#     tidylog::summarize(Q_Ld=sum(Q * 60 * 60, na.rm=TRUE))
#
# qc_daily_conc = tidylog::left_join(q_daily, grab_daily,
#         by=c('site_name', 'date')) %>%
#     dplyr::group_by(site_name) %>%
#     dplyr::arrange(site_name, date) %>%
#     dplyr::ungroup()
#
# interpcols = sapply(qc_daily_conc, function(x) is.numeric(x) & ! all(is.na(x)))
# qc_daily_conc = qc_daily_conc %>%
#     tidylog::mutate_if(interpcols,
#         imputeTS::na_interpolation, option='linear', maxgap=15)
#
# qc_daily_flux = qc_daily_conc %>%
#     dplyr::mutate_at(dplyr::vars(-site_name, -date, -Q_Ld), ~(. * Q_Ld) / (1000 * 1000)) %>%
#     tidylog::left_join(dplyr::select(site_data, site_name, ws_area_ha)) %>%
#     dplyr::mutate_at(dplyr::vars(-site_name, -date, -Q_Ld), ~(. / ws_area_ha)) %>%
#     dplyr::mutate(datetime=as.POSIXct(date)) %>%
#     tidylog::select(-ws_area_ha, -date, -Q_Ld)

#method 2: simplified
grab_daily = grab %>%
    tidylog::select(- dplyr::one_of(non_flux_vars)) %>%
    dplyr::mutate(date=lubridate::floor_date(datetime, 'day')) %>%
    # dplyr::mutate(date2=as.Date(datetime)) %>%
    dplyr::group_by(site_name, date) %>%
    tidylog::summarize_if(is.numeric, mean, na.rm=TRUE) %>%
    dplyr::ungroup()

grab_rng = tibble::tibble(date=seq(min(grab_daily$date),
    max(grab_daily$date), by='1 day'))
grab_daily = grab_daily %>%
    tidylog::right_join(grab_rng, by='date') %>%
    tidylog::right_join(discharge, by=c(date='datetime', 'site_name')) %>%
    dplyr::arrange(site_name, date)

# interpcols = sapply(grab_daily, function(x) is.numeric(x) & sum(! is.na(x)) > 2)
# grab_daily = grab_daily %>%
#     dplyr::group_by(site_name) %>%
#     dplyr::arrange(site_name, date) %>%
#     mutate_if(function(x) is.numeric(x) & sum(! is.na(x)) > 2,
#         imputeTS::na_interpolation, option='linear', maxgap=15) %>%
#     dplyr::ungroup()

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

feather::write_feather(flux_daily, path=paste0(outdir, '/flux.feather'))
