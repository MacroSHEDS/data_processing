library(tidyverse)
library(feather)
library(glue)

vsn = 1

setwd(glue('/home/mike/git/macrosheds/data_acquisition/macrosheds_figshare_v{vsn}/1_watershed_attribute_data'))

d = map_dfr(list.files('ws_attr_timeseries', full.names = TRUE), read_csv)
filter(d, domain == 'hbef') %>%
    write_feather('~/Desktop/hbef_ws_attr_timeseries.feather')

d = map_dfr(list.files('../4_CAMELS-compliant_Daymet_forcings/', full.names = TRUE), read_csv)
filter(d, site_code %in% paste0('w', 1:9)) %>%
    write_feather('~/Desktop/hbef_ws_attr_timeseries_daymet.feather')

d = map_dfr(list.files('../3_CAMELS-compliant_watershed_attributes/', full.names = TRUE), read_csv)
filter(d, site_code %in% paste0('w', 1:9)) %>%
    select(site_code, everything()) %>%
    write_feather('~/Desktop/hbef_ws_attr_camels.csv')

d = read_csv('../1_watershed_attribute_data/ws_attr_summaries.csv')
filter(d, domain == 'hbef') %>%
    write_feather('~/Desktop/hbef_ws_attr.csv')

