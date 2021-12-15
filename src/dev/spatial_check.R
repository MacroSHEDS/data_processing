#### bare_cover
fils <- list.files('data/lter/hjandrews/ws_traits/bare_cover/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vb_bare_cover_median') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)

#### bfi
fils <- list.files('data/lter/hjandrews/ws_traits/bfi/', full.names = T)
# sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(fils, read_feather)
sum_data %>%
    filter(var == 'vb_bare_cover_median') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)

#### precip
fils <- list.files('data/lter/hjandrews/ws_traits/cc_precip/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'cc_cumulative_precip') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'cc_precip_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

look <- raw_data %>%
    group_by(site_code, datetime, var) %>%
    summarise(n = n())

#### temp
fils <- list.files('data/lter/hjandrews/ws_traits/cc_temp/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'cc_temp_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'cc_temp_mean_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

look <- raw_data %>%
    group_by(site_code, datetime) %>%
    summarise(n = n())

#### end of season
fils <- list.files('data/lter/hjandrews/ws_traits/end_season/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vd_eos_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'cc_temp_mean_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### et_ref
fils <- list.files('data/lter/hjandrews/ws_traits/et_ref/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'ck_et_ref_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'ck_et_ref_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### fpar
fils <- list.files('data/lter/hjandrews/ws_traits/fpar/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vb_fpar_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vb_fpar_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### geochemical
fils <- list.files('data/lter/hjandrews/ws_traits/geochemical/', full.names = T)
sum_data <- map_dfr(fils, read_feather)

#### gpp
fils <- list.files('data/lter/hjandrews/ws_traits/gpp/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'va_gpp_sum') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'va_gpp_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### lai
fils <- list.files('data/lter/hjandrews/ws_traits/lai/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vb_lai_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vb_lai_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### length
fils <- list.files('data/lter/hjandrews/ws_traits/length_season/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vd_los_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vb_lai_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### max_season
fils <- list.files('data/lter/hjandrews/ws_traits/max_season/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vd_mos_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vb_lai_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### nadp
fils <- list.files('data/lter/hjandrews/ws_traits/nadp/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'ch_annual_SO4_flux_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

#### ndvi
fils <- list.files('data/lter/hjandrews/ws_traits/ndvi/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vb_ndvi_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vb_ndvi_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### nlcd
fils <- list.files('data/lter/hjandrews/ws_traits/nlcd/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'lg_nlcd_forest_dec') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vb_ndvi_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### npp
fils <- list.files('data/lter/hjandrews/ws_traits/npp/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'va_npp_median') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vb_ndvi_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### nrcs_soils
fils <- list.files('data/lter/hjandrews/ws_traits/nrcs_soils/', full.names = T)
sum_data <- map_dfr(fils, read_feather)

#### nsidc
fils <- list.files('data/lter/hjandrews/ws_traits/nsidc/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'cl_snow_depth_ann_max') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'cl_snow_depth_mean') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### pelletier_soil_thickness
fils <- list.files('data/lter/hjandrews/ws_traits/pelletier_soil_thickness/', full.names = T)
sum_data <- map_dfr(fils, read_feather)

#### start_season
fils <- list.files('data/lter/hjandrews/ws_traits/start_season/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vd_sos_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'cl_snow_depth_mean') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### tcw
fils <- list.files('data/lter/hjandrews/ws_traits/tcw/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vj_tcw_mean') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

raw_fils <- grep('raw', fils, value = TRUE)
raw_data <- map_dfr(raw_fils, read_feather)
raw_data %>%
    filter(var == 'vj_tcw_median') %>%
    ggplot(aes(datetime, val, col = site_code)) +
    geom_line()

#### terrain
fils <- list.files('data/lter/hjandrews/ws_traits/terrain/', full.names = T)
sum_data <- map_dfr(fils, read_feather)

#### tree_cover
fils <- list.files('data/lter/hjandrews/ws_traits/tree_cover/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vb_tree_cover_median') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

#### veg_cover
fils <- list.files('data/lter/hjandrews/ws_traits/veg_cover/', full.names = T)
sum_fils <- grep('sum', fils, value = TRUE)
sum_data <- map_dfr(sum_fils, read_feather)
sum_data %>%
    filter(var == 'vb_veg_cover_median') %>%
    ggplot(aes(year, val, col = site_code)) +
    geom_line()

