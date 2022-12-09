library(tidyverse)

setwd('~/git/macrosheds/data_acquisition/eml/data_links')

ws1 = read_csv('ws_attr_summaries.csv')
ws2 = read_csv('CAMELS_compliant_ws_attr_summaries.csv')

colnames(ws1)
colnames(ws2)

ws1 = ws1 %>%
    select(catchment_name = site_code,
           catchment_area_ha = ws_area_ha,
           # mean_annual_precip_mm = cc_mean_annual_precip,
           mean_annual_temp_C = cc_mean_annual_temp,
           mean_annual_aet_mm = ci_mean_annual_et)

ws2 = ws2 %>%
    mutate(pet_mean = pet_mean * 365,
           p_mean = p_mean * 365,
           q_mean = q_mean * 365) %>%
    select(latitude = gauge_lat,
           longitude = gauge_lon,
           mean_annual_precip_mm = p_mean,
           mean_annual_runoff_mm = q_mean,
           mean_annual_pet_mm = pet_mean,
           catchment_name = site_code)

out = full_join(ws1, ws2, by = 'catchment_name') %>%
    select(catchment_name, catchment_area_ha, latitude, longitude, mean_annual_precip_mm,
           mean_annual_runoff_mm, mean_annual_temp_C, mean_annual_pet_mm, mean_annual_aet_mm)

write_csv(out, '/tmp/townhall_data_macrosheds.csv')
