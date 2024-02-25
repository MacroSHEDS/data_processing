
#filter sites for Dustin Kincaid's study
#included site-vars must have:
# - at least 3 years of data
# - at least 20 paired C-Q obs
# - Q that covers 50% of the observed range

library(tidyverse)
library(feather)
library(sf)
library(lubridate)
# library(ggplot2)
# library(ggmap)
# library(usmap)
# library(leaflet)
library(tmap)

vsn = 1

setwd(glue('~/git/macrosheds/data_acquisition/macrosheds_figshare_v{vsn}'))

# conf <- jsonlite::fromJSON('../config.json',
#                            simplifyDataFrame = FALSE)

#get chem, q, site data
dd = list.dirs('2_timeseries_data')
dd = grep('stream_chemistry', dd, value = TRUE)
all_chem = tibble()
for(d in dd){
    all_chem = map_dfr(list.files(d, full.names = T), read_feather) %>%
        bind_rows(all_chem)
}

dd = list.dirs('2_timeseries_data')
dd = grep('discharge', dd, value = TRUE)
all_q = tibble()
for(d in dd){
    all_q = map_dfr(list.files(d, full.names = T), read_feather) %>%
        bind_rows(all_q)
}

sites = read_csv('0_documentation_and_metadata/04_site_documentation/04a_site_metadata.csv') %>%
    filter(! is.na(latitude), ! is.na(longitude), site_type == 'stream_gauge') %>%
    filter(! domain %in% c('mcmurdo', 'krycklan'))
    # st_as_sf(coords = c('longitude', 'latitude'), crs = 4326)

cc = all_chem %>%
    mutate(pfx = macrosheds::ms_extract_var_prefix(var)) %>%
    filter(pfx == 'IS') %>%
    group_by(site_code, var) %>%
    summarize(n = n()) %>%
    ungroup()

c2 = all_chem %>%
    mutate(pfx = macrosheds::ms_extract_var_prefix(var)) %>%
    filter(pfx == 'IN') %>%
    group_by(site_code, var) %>%
    summarize(n = n()) %>%
    ungroup()

# cc = left_join(cc, select(sites, site_code, domain))
# c2 = left_join(c2, select(sites, site_code, domain))
# write_csv(cc, '/tmp/installed_sensor_sitevars.csv')
# write_csv(c2, '/tmp/installed_nonsensor_sitevars.csv')

# gg_vars = c('GN_NH4_NH3_N', 'GN_NO3_NO2_N', 'GN_TN', 'GN_UTKN', 'GN_UTP',
#             'GN_DON', 'GN_NH3_N', 'GN_NO3_N', 'GN_TPN', 'GN_TKN', 'GN_TDP',
#             'GN_PO4_P', 'GN_TPP', 'GN_DOC', 'GN_NH4_N', 'GN_P', 'GN_TDN',
#             'GN_TPC', 'GN_POC', 'GN_PON', 'IN_NO3_NO2_N', 'IN_TN', 'IN_TOC',
#             'IN_TP', 'GN_NO2_N', 'GN_DOP', 'GN_TP', 'GN_TOC', 'GN_TIP',
#             'GN_TIN', 'GN_SRP', 'GN_DIC', 'GN_UTN', 'IN_DIC', 'IN_DOC',
#             'IN_DON', 'IN_NO3_N', 'IN_TDN', 'GN_CO2', 'GN_BPC', 'GN_BPN',
#             'GN_CH4', 'GN_CO2_C', 'GN_CH4_C', 'GN_DIN', 'GN_NO2', 'GN_TIC',
#             'IN_TIC', 'IN_NO2', 'IN_PO4_P', 'GN_TC')

ggvars2 = c('DOC', 'Al', 'Ca', 'Cl', 'DON', 'K', 'Mg', 'Na', 'NO3', 'NO3_N',
            'Si', 'SO4', 'SO4_S', 'NH3', 'NH3_N', 'PO4_P', 'orthophosphate_P')

xx = read_csv('../../portal/data/general/catalog_files/all_variables.csv') %>%
    mutate(len = (as.Date(LastRecordUTC) - as.Date(FirstRecordUTC)) / 365) %>%
    filter(len >= 30,
           VariableCode == 'NO3_N')

chm = all_chem %>%
    mutate(var_ = macrosheds::ms_drop_var_prefix(var)) %>%
    filter(var_ %in% ggvars2)

qq = chm %>%
    group_by(site_code, var_) %>%
    summarize(nyr = length(unique(lubridate::year(datetime)))) %>%
    ungroup() %>%
    filter(nyr >= 30)

#filter by n years requiremend
chm = right_join(chm, qq, by = c('site_code', 'var_'))

jn = left_join(chm, all_q, by = c('datetime', 'site_code'))

jn = jn %>%
    filter(! is.na(val.y)) %>%
    filter(! is.na(val.x))

nobs = jn %>%
    group_by(site_code, var_) %>%
    summarize(n = n(),
              nyr = unique(nyr)) %>%
    ungroup()

#filter by n paired obs requirement
nobs = filter(nobs, `n` >= 20)

jnn = right_join(jn, nobs, by = c('site_code', 'var_'))

q_ranges = all_q %>%
    group_by(site_code) %>%
    summarize(obs_min = min(val, na.rm = TRUE),
              obs_max = max(val, na.rm = TRUE)) %>%
    ungroup()

q_ranges_sub = jnn %>%
    group_by(site_code) %>%
    summarize(obs_min = min(val.y, na.rm = TRUE),
              obs_max = max(val.y, na.rm = TRUE)) %>%
    ungroup()

qr = left_join(q_ranges_sub, q_ranges, by = c('site_code'))

qr$q50 = apply(qr[,4:5], 1, function(x) quantile(x, probs = 0.5))

qr = qr %>%
    filter(obs_max.x >= q50)

#filter to sites with paired q that's at least 50% of obs q range
jnn = filter(jnn, site_code %in% qr$site_code)

#plot
plt = jnn %>%
    group_by(site_code, var_) %>%
    summarize(nyr = unique(nyr.x),
              nobs = unique(n)) %>%
    ungroup() %>%
    left_join(select(sites, site_code, latitude, longitude)) %>%
    rename(n_years = nyr)

# plt = filter(plt, ! is.na(geometry))
plt = filter(plt, ! is.na(latitude), ! is.na(longitude))
plt = st_as_sf(plt, coords = c('longitude', 'latitude'), crs = 4326)

# states <- map_data("state")
# usa = get_map("usa", source = "google", apikey = conf$gmaps_apikey)
#
# # Create map plot
# register_google(key = conf$gmaps_apikey)
# # ggmap(get_map("usa", source = "osm")) +
# ggmap::ggmap(usa) +
# # ggmap(get_map("usa", source = "stamen", maptype = 'toner')) +
# # map_osm(xlim = c(-126, -66), ylim = c(24, 50), zoom = 4)
# # plot_usmap(regions = "states") +
# # ggplot() %>%
#     # map_data("usa")
#     geom_polygon(data = states, aes(x = long, y = lat, group = group), color = "black", fill = "white") +
#     geom_point(data = plt, aes(x = longitude, y = latitude, color = n_years)) +
#     scale_color_gradient(low = "darkblue", high = "lightblue") +
#     facet_wrap(~ var_)

# leaflet() %>%
#     setView(-96, 37.8, 4) %>%
#     addTiles() %>%

tmap_mode("plot")
data(World)

# tm_shape(world, query = "NAME = 'United States of America'")
# tm_shape(World) +
#     tm_borders("World", lwd = 0.4) +
#     tm_shape(subset = World$NAME == "United States of America") +
#     tm_borders("World", lwd = 0.8, col = "black")
ex = sf::st_bbox(plt)

mp = tm_shape(World[World$name == "United States", ], bbox = ex) +
    tm_borders(lwd = 1, col = "gray20") +
    # tm_shape(World[World$NAME == "United States", ]) +
    tm_shape(plt) +
    # tm_dots(col = "red", size = 0.5,
    tm_dots(col = "n_years", size = 0.5,
              legend.is.portrait = FALSE,
              title = "", palette = "Blues") +
    tm_facets('var_') +
    tm_layout(outer.margins = c(0,0,0,0),
              legend.outside.position =  "bottom",
              asp=4)

tmap_save(mp, filename="../plots/sitevars_for_cq.png",
          bg="white", dpi = 300)
          # bg="white", dpi = 300, width = 10, height = 8)

plt %>%
    as_tibble() %>%
    select(-geometry) %>%
    group_by(var_) %>%
    summarize(n_sites = length(unique(site_code))) %>%
    ungroup()
