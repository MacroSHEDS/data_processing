#this is written in 2024 when site_data has its own column for ws_status.
#that should be removed in 2025 so that discrepancies can't arise

#"ws_status" is the column name in site_data
#"watershed_type" is the colname in disturbance_record

library(tidyverse)

sites_ = read_csv('~/Downloads/site_data - Sheet1(3).csv')
sites = filter(sites_, site_type != 'rain_gauge', in_workflow == 1)
dist_ = read_csv('~/Downloads/disturbance_record - Sheet1(2).csv')
dist = distinct(dist_, site_code, watershed_type)
zz = full_join(sites, dist, by = 'site_code')

#should return nothing
zz %>%
    select(domain, site_code, watershed_type, ws_status) %>%
    mutate(same = watershed_type == ws_status) %>%
    filter(! is.na(same) & !same)

#should be all-NA
sites_ %>%
    filter(site_type == 'rain_gauge') %>%
    # select(domain, site_code, ws_status)
    pull(ws_status)

#status doesn't need to be all-NA, but is for now (2024)
sites_ %>%
    filter(site_type == 'stream_sampling_point') %>%
    select(domain, site_code, ws_status) %>%  print(n=1000)
    pull(ws_status)

#should be no NAs
sites_ %>%
    filter(in_workflow == 1, site_type == 'stream_gauge') %>%
    pull(ws_status)

#should be empty. if any site names were modified this round, you'll find out here.
zz %>%
    select(domain, site_code, ws_status, watershed_type) %>%
    filter(is.na(watershed_type) & ! is.na(ws_status)) %>%
    print(n = 1000)

# d = filter(dist_, domain == 'arctic') %>% pull(site_code) %>% unique()
# s = filter(sites, domain == 'arctic') %>% pull(site_code) %>% unique()
# setdiff(d, s)
# setdiff(s, d)

#should be empty
zz %>%
    select(domain, site_code, ws_status, watershed_type) %>%
    filter(! is.na(watershed_type) & is.na(ws_status)) %>%
    print(n = 1000)

# mapview::mapview(sf::st_read('~/git/macrosheds/portal/data/general/shed_boundary/shed_boundary.shp'))

#all types filled in?
table(dist_$watershed_type, useNA = 'ifany')

#are exp descriptions always coupled to "exp"?
dist_ %>%
    filter(! is.na(disturbance_source)) %>%
    filter(watershed_type != 'exp')
#no, but that makes sense. judgment call.
