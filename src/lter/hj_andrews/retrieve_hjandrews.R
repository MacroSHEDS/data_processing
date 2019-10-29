library(tidyverse)
library(glue)
library(feather)
library(tidylog)

setwd('/home/mike/git/macrosheds/data_acquisition/data/lter/hjandrews/')
outdir = '/home/mike/git/macrosheds/portal/data/hjandrews/'
source('../../../src/lter/retrieve_lter.R')

lter_download('/home/mike/git/macrosheds/data_acquisition/data/lter',
    'hjandrews')

lter_download('/home/mike/git/macrosheds/data_acquisition/data/lter',
    'south_umpqua')

sets = dir('raw')
# elems = dir(glue('raw/{set}', set=sets[i]))

precip = readr::read_csv('raw/precipitation/MS00403.csv') %>%
    tidylog::select(DATE, SITECODE, PRECIP_TOT_DAY, PRECIP_TOT_FLAG) %>%
    tidylog::filter(PRECIP_TOT_FLAG %in% c('A', 'E', 'T', 'U')) %>%
    tidylog::select(-PRECIP_TOT_FLAG) %>%
    dplyr::rename(datetime=DATE, site_name=SITECODE, precipCatch=PRECIP_TOT_DAY)

write_feather(precip, paste0(outdir, '/precip.feather'))

discharge = readr::read_csv('raw/discharge/HF00402.csv') %>%
    tidylog::select(DATE, SITECODE, MAX_Q) %>%
    dplyr::mutate(MAX_Q=MAX_Q * 28.32) %>%
    dplyr::rename(datetime=DATE, site_name=SITECODE, Q=MAX_Q)

write_feather(discharge, paste0(outdir, '/discharge.feather'))

grab = readr::read_csv('raw/stream_chemistry/CF00201.csv') %>%
    dplyr::rename(Na=X46) %>%
    tidylog::select(-STCODE, -LABNO, -TYPE, -ENTITY, -WATERYEAR, -INTERVAL,
        -MEAN_LPS, -Q_AREA_CM, -QCODE, -PVOL, -PVOLCODE)

grabcodes = tidylog::select(grab, SITECODE, DATE_TIME, dplyr::ends_with('CODE')) %>%
    tidylog::gather('code', 'codeval', -SITECODE, -DATE_TIME)
grabvars = tidylog::select(grab, -dplyr::ends_with('CODE'), SITECODE) %>%
    tidylog::gather('var', 'val', -SITECODE, -DATE_TIME) %>%
    tidylog::select(-SITECODE, -DATE_TIME)

grab = bind_cols(grabcodes, grabvars) %>%
    tidylog::filter(codeval %in% c('A', 'E')) %>%
    tidylog::select(-code, -codeval) %>%
    tidylog::spread(var, val) %>%
    dplyr::rename(site_name=SITECODE, date_time=DATE_TIME, alk=ALK, Ca=CA,
        Cl=CL, spCond=COND, Mg=MG, NH3_N=NH3N, NO3_N=NO3N, pH=PH, PO4_P=PO4P,
        SiO2=SI, SO4_S=SO4S, suspSed=SSED) %>%
    dplyr::mutate(spCond=spCond / 1000) #convert to S/cm

write_feather(grab, paste0(outdir, '/grab.feather'))


###HERE: CALC FLUX; TEST DOMAIN SELECTOR
# cc = read_feather('data/lter/hbef/grab.feather')
# q = read_feather('data/lter/hbef/sensorQ.feather')

site_data = read.csv('data/general/site_data.csv', stringsAsFactors=FALSE)
non_flux_vars = c('ANC960', 'ANCMet', 'anionCharge', 'cationCharge',
    'flowGageHt', 'ionBalance, OMAl', 'pH', 'precipCatch', 'spCond',
    'temp', 'theoryCond', 'TMAl', 'Alk', 'suspSed')

grab_daily = grab %>%
    select(- one_of(non_flux_vars)) %>%
    select(-ANC960,-ANCMet,-anionCharge,-cationCharge,-flowGageHt,-ionBalance,
        -OMAl,-pH,-precipCatch,-spCond,-temp,-theoryCond,-TMAl) %>%
    mutate(date = as.Date(datetime)) %>%
    group_by(site_name,date) %>%
    summarize_if(is.numeric,mean,na.rm=T)

hours_only = tibble(datehour= seq(min(cc$datetime),max(cc$datetime),by='1 hour') %>%
        round_date(.,'hours')) %>%
    nest()


full_hours = tibble(site_name = unique(q$site_name),
    full_hours = rep(hours_only$data,length(site_name))) %>%
    unnest(full_hours)




q_hourly = q %>%
    mutate(datehour = round_date(datetime,'hours')) %>%
    right_join(.,full_hours,by=c('site_name','datehour')) %>%
    arrange(datehour) %>%
    group_by(site_name) %>%
    mutate(Q_full = na_interpolation(Q, option='linear', maxgap = 30)) %>%
    filter(!is.na(Q_full)) %>%
    group_by(datehour,site_name) %>%
    summarize(Q = mean(Q_full))

q_daily = q_hourly %>%
    mutate(date=as.Date(datehour)) %>%
    group_by(site_name,date) %>%
    summarize(Q_Ld = sum(Q*60*60))

qc_daily_conc = left_join(q_daily,c_daily,by=c('site_name','date')) %>%
    group_by(site_name) %>%
    arrange(site_name,date) %>%
    mutate_if(is.numeric,na_interpolation,option='linear',maxgap=15)

qc_daily_flux = qc_daily_conc %>%
    mutate_at(vars(-site_name,-date,-Q_Ld),~(.*Q_Ld)/(1000*1000)) %>%
    left_join(select(site_data, site_name, ws_area_ha)) %>%
    mutate_at(vars(-site_name,-date,-Q_Ld),~(./ws_area_ha)) %>%
    select(-ws_area_ha)

write_feather(qc_daily_flux,path='data/lter/hbef/flux.feather')

# f = read_feather('/home/mike/git/macrosheds/data_acquisition/data/lter/hbef/grab.feather')
# ff = read_feather('/home/mike/git/macrosheds/data_acquisition/data/lter/hbef/grab_unabridged.feather')
# fs = read_feather('/home/mike/git/macrosheds/data_acquisition/data/lter/hbef/sensorQ.feather')
# fm = read_feather('/home/mike/git/macrosheds/portal/data/hbef/grab.feather')

# precip - https://portal.lternet.edu/nis/metadataviewer?packageid=knb-lter-and.5482.3
    #mL
