library(tidyverse)
library(imputeTS)
library(lubridate)

# flux = feather::read_feather('/home/mike/git/macrosheds/portal/data/hbef/flux.feather')
pchem = feather::read_feather('/home/mike/git/macrosheds/portal/data/hbef/pchem.feather')
pcip = feather::read_feather('/home/mike/git/macrosheds/portal/data/hbef/precip.feather')
conc = feather::read_feather('/home/mike/git/macrosheds/portal/data/hbef/grab.feather')
disch = feather::read_feather('/home/mike/git/macrosheds/portal/data/hbef/discharge.feather')

psites = intersect(pchem$site_name, pcip$site_name)
csites = intersect(disch$site_name, conc$site_name)
pdaterange = range(pchem$datetime)
cdaterange = range(conc$datetime)
pfulldt = seq(pdaterange[1], pdaterange[2], by='hour')
cfulldt = seq(cdaterange[1], cdaterange[2], by='hour')
pfulldt = tibble(site_name=rep(psites, each=length(pfulldt)),
    datetime=rep(pfulldt, times=length(psites)))
cfulldt = tibble(site_name=rep(csites, each=length(cfulldt)),
    datetime=rep(cfulldt, times=length(csites)))

pcip = pcip %>%
    filter(site_name %in% psites)

disch = disch %>%
    filter(site_name %in% csites)

precip_flux = pchem %>%
    filter(site_name %in% psites) %>%
    mutate(datetime=round_date(datetime, 'hours')) %>%
    group_by(datetime, site_name) %>%
    summarize_all(~mean(., na.rm=TRUE)) %>%
    ungroup() %>%
    full_join(pcip, by=c('datetime', 'site_name')) %>%
    full_join(pfulldt, by=c('datetime', 'site_name')) %>%
    filter(datetime >= pdaterange[1], datetime <= pdaterange[2]) %>%
    arrange(datetime) %>%
    select_if(~( sum(is.na(.))/length(.) != 1)) %>%
    mutate_at(vars(-datetime, -site_name), na_interpolation, maxgap=30) %>%
    mutate_at(vars(-datetime, -site_name, -precip), ~(. * precip) )

flux = conc %>%
    filter(site_name %in% csites) %>%
    mutate(datetime=round_date(datetime, 'hours')) %>%
    group_by(datetime, site_name) %>%
    summarize_all(~mean(., na.rm=TRUE)) %>%
    ungroup() %>%
    full_join(disch, by=c('datetime', 'site_name')) %>%
    full_join(cfulldt, by=c('datetime', 'site_name')) %>%
    filter(datetime >= cdaterange[1], datetime <= cdaterange[2]) %>%
    arrange(datetime) %>%
    select_if(~( sum(is.na(.))/length(.) != 1)) %>%
    mutate_at(vars(-datetime, -site_name), na_interpolation, maxgap=30) %>%
    mutate_at(vars(-datetime, -site_name, -Q), ~(. * Q) )

unique(pchem$site_name)
unique(conc$site_name)
unique(disch$site_name)
unique(pcip$site_name)
for(i in
