library(tidyverse)
library(imputeTS)
library(lubridate)

# flux = read_feather('/home/mike/git/macrosheds/portal/data/hbef/flux.feather')
pchem = read_feather('/home/mike/git/macrosheds/portal/data/hbef/pchem.feather')
pcip = read_feather('/home/mike/git/macrosheds/portal/data/hbef/precip.feather')
conc = read_feather('/home/mike/git/macrosheds/portal/data/hbef/grab.feather')
disch = read_feather('/home/mike/git/macrosheds/portal/data/hbef/discharge.feather')
variables = read_csv('/home/mike/git/macrosheds/portal/data/variables.csv')
fluxvars = variables$variable_code[as.logical(variables$flux_convertible)]

psites = intersect(pchem$site_name, pcip$site_name)
csites = intersect(disch$site_name, conc$site_name)

pcip = pcip %>%
    filter(site_name %in% psites)

disch = disch %>%
    filter(site_name %in% csites)

pchem = pchem %>%
    filter(site_name %in% psites) %>%
    select(one_of(fluxvars, 'site_name', 'datetime')) %>%
    mutate(datetime=round_date(datetime, 'hours')) %>%
    group_by(datetime, site_name) %>%
    summarize_all(~mean(., na.rm=TRUE)) %>%
    ungroup()

pdaterange = range(pchem$datetime)
pfulldt = seq(pdaterange[1], pdaterange[2], by='hour')
pfulldt = tibble(site_name=rep(psites, each=length(pfulldt)),
    datetime=rep(pfulldt, times=length(psites)))

precip_flux = pchem %>%
    full_join(pcip, by=c('datetime', 'site_name')) %>%
    full_join(pfulldt, by=c('datetime', 'site_name')) %>%
    filter(datetime >= pdaterange[1], datetime <= pdaterange[2]) %>%
    arrange(datetime) %>%
    select_if(~( sum(is.na(.))/length(.) != 1)) %>%
    group_by(site_name) %>%
    mutate_at(vars(-datetime, -site_name), na_interpolation, maxgap=30) %>%
    ungroup() %>%
    group_by(datetime, site_name) %>%
    summarize_all(~mean(., na.rm=TRUE)) %>%
    ungroup() %>%
    mutate_at(vars(-datetime, -site_name, -precip), ~(. * precip) )

par(mfrow=c(5, 4), oma=c(0,0,0,0), mar=c(0,0,3,0))
for(v in colnames(precip_flux)[-(1:2)]){
    plot(precip_flux[,'datetime', drop=TRUE],
        precip_flux[,v, drop=TRUE], type='l', main=v)
}

write.csv(precip_flux, '~/Desktop/precip_flux_inst.csv')

conc = conc %>%
    filter(site_name %in% csites) %>%
    select(one_of(fluxvars, 'site_name', 'datetime')) %>%
    mutate(datetime=round_date(datetime, 'hours')) %>%
    group_by(datetime, site_name) %>%
    summarize_all(~mean(., na.rm=TRUE)) %>%
    ungroup()

cdaterange = range(conc$datetime)
cfulldt = seq(cdaterange[1], cdaterange[2], by='hour')
cfulldt = tibble(site_name=rep(csites, each=length(cfulldt)),
    datetime=rep(cfulldt, times=length(csites)))

flux = conc %>%
    full_join(disch, by=c('datetime', 'site_name')) %>%
    full_join(cfulldt, by=c('datetime', 'site_name')) %>%
    filter(datetime >= cdaterange[1], datetime <= cdaterange[2]) %>%
    arrange(datetime) %>%
    select_if(~( sum(is.na(.))/length(.) != 1)) %>%
    group_by(site_name) %>%
    mutate_at(vars(-datetime, -site_name), na_interpolation, maxgap=30) %>%
    ungroup() %>%
    group_by(datetime, site_name) %>%
    summarize_all(~mean(., na.rm=TRUE)) %>%
    ungroup() %>%
    mutate_at(vars(-datetime, -site_name, -Q), ~(. * Q) )

par(mfrow=c(5, 4), oma=c(0,0,0,0), mar=c(0,0,3,0))
for(v in colnames(flux)[-(1:2)]){
    plot(flux[,'datetime', drop=TRUE],
        flux[,v, drop=TRUE], type='l', main=v)
}

write.csv(flux, '~/Desktop/stream_flux_inst.csv')
