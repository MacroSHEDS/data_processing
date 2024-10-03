source('src/global/camels_formatting_scripts/actual_camels_code/hydro/hydro_signatures.R')

#### Hydrology ####

# read in hydrology files
all_q_fil <- list.files(glue('macrosheds_dataset_v{vsn}'),
                      pattern = '\\.feather*',
                      recursive = TRUE,
                      full.names = TRUE) %>%
    str_subset('/discharge')

all_q <- map_dfr(all_q_fil, read_feather)

# Prep data
site_doms <- site_data %>%
    filter(site_type == 'stream_gauge',
           in_workflow == 1) %>%
    select(site_code, domain)

site_area <- site_data %>%
    filter(site_type == 'stream_gauge',
           in_workflow == 1) %>%
    select(site_code, ws_area_ha)

# Scale Q to watershed area
warning('not computing camels hydro attributes for mcmurdo')
q_daily <- all_q %>%
    filter(! is.na(val)) %>%
    left_join(site_doms, by = 'site_code') %>%
    filter(! domain == 'mcmurdo') %>%
    # left_join(sites_to_remove, by = 'site_code') %>%
    # filter(remove == 0) %>%
    # select(-remove) %>%
    # group_by(site_code, datetime) %>%
    # summarize(val = mean(val),
    #           ms_status = max(ms_status),
    #           ms_interp = max(ms_interp)) %>%
    # ungroup() %>%
    mutate(q_scaled = (val * 86400) / 1000) %>%
    left_join(site_area, by = 'site_code') %>%
    mutate(q_scaled = q_scaled / (ws_area_ha * 10000)) %>%
    mutate(q_scaled = q_scaled * 1000) %>%
    filter(! is.na(q_scaled))

# Look at watershed with mostly full wateryears (camels functions operate on wateryear)
q_check <- q_daily %>%
    mutate(year = year(date),
           month = month(date)) %>%
    mutate(water_year = ifelse(month %in% c(10, 11, 12), year + 1, year)) %>%
    group_by(site_code, water_year) %>%
    summarize(n = n(),
              ms_status = sum(ms_status),
              ms_interp = sum(ms_interp)) %>%
    ungroup()

frz_dry_sites <- q_check %>%
    group_by(site_code) %>%
    summarize(nyears = n(),
              mean_ndays = mean(n),
              max_ndays = max(n),
              prop_mean = mean_ndays / max_ndays) %>%
    filter(max_ndays < 365,
           nyears > 1)

q_check <- left_join(q_check,
                     select(frz_dry_sites, site_code, max_ndays),
                     by = 'site_code')

good_site_years <- q_check %>%
    filter(n >= 311 |
               (site_code %in% frz_dry_sites$site_code & n >= max_ndays * 0.4)) %>%
    select(site_code, water_year) %>%
    mutate(good = 1)

all_scaled <- q_daily %>%
    mutate(year = year(date),
           month = month(date)) %>%
    mutate(water_year = ifelse(month %in% c(10, 11, 12), year + 1, year)) %>%
    filter(! is.na(q_scaled)) %>%
    group_by(site_code, water_year) %>%
    summarize(sum = sum(q_scaled, na.rm = TRUE),
              n = n(),
              ms_status = sum(ms_status),
              ms_interp = sum(ms_interp)) %>%
    ungroup() %>%
    # mutate(sum = sum*1000) %>%
    left_join(good_site_years, by = c('site_code', 'water_year')) %>%
    filter(good == 1) %>%
    left_join(site_doms, by = 'site_code')

# annual_flow <- all_scaled %>%
#     # filter(! site_code %in% c('ON02', 'TE03')) %>%
#     group_by(site_code) %>%
#     summarize(sum = mean(sum, na.rm = T)) %>%
#     ungroup() %>%
#     full_join(site_eco, by = 'site_code') %>%
#     filter(! is.na(eco_region)) %>%
#     filter(! is.na(sum)) %>%
#     as.data.frame() %>%
#     select(site_code, sum) %>%
#     left_join(site_eco, by = 'site_code')

# Get daymet precip

all_daymet <- list.files('scratch/camels_assembly/daymet_with_pet',
                         full.names = TRUE) %>%
    map_dfr(~read_csv(., show_col_types = FALSE))

# daymet_annual <- all_daymet %>%
#     group_by(site_code, date) %>%
#     summarize(prcp = mean(prcp)) %>%
#     ungroup() %>%
#     mutate(year = year(date),
#            month = month(date)) %>%
#     mutate(water_year = ifelse(month %in% c(10, 11, 12), year + 1, year)) %>%
#     group_by(site_code, water_year) %>%
#     summarize(prcp = sum(prcp, na.rm = TRUE),
#               n = n()) %>%
#     ungroup()

# runof_ratios <- left_join(all_scaled, daymet_annual,
#                           by = c('site_code', 'water_year')) %>%
#     mutate(runoff_ratio = sum / prcp) %>%
#     filter(runoff_ratio < 2) %>%
#     # filter(! site_code %in% c('ON02', 'TE03')) %>%
#     group_by(site_code) %>%
#     summarize(mean_rr = mean(runoff_ratio, na.rm = TRUE)) %>%
#     ungroup() %>%
#     full_join(site_eco, by = 'site_code') %>%
#     filter(! is.na(eco_region)) %>%
#     filter(! is.na(mean_rr))

hydro_sites <- good_site_years %>%
    pull(site_code) %>%
    unique()

warning('mike manually edited CAMELS code (hydro_signatures.R) on 2022-12-01. See the lines with "MIKE EDITED" comments. If CAMELS upstream code changes, re-apply these edits.')
all_site_hydro <- tibble()
for(i in seq_along(hydro_sites)){

    if(i == 1 || i %% 50 == 0) print(paste0(i, '/', length(hydro_sites)))

    good_years <- good_site_years %>%
        filter(site_code == !!hydro_sites[i]) %>%
        pull(water_year)

    one_site_q <- q_daily %>%
        filter(site_code == !!hydro_sites[i]) %>%
        mutate(year = year(date),
               month = month(date),
               water_year = ifelse(month %in% c(10, 11, 12), year + 1, year)) %>%
        filter(water_year %in% !!good_years)

    one_site_precip <- all_daymet %>%
        mutate(year = year(date),
               month = month(date),
               water_year = ifelse(month %in% c(10, 11, 12), year + 1, year),
               date = as.Date(date)) %>%
        filter(site_code == !!hydro_sites[i],
               water_year %in% !!good_years) %>%
        group_by(date, site_code) %>%
        summarize(precip = mean(prcp, na.rm = TRUE),
                  .groups = 'drop')
        # rename(datetime = date)

    one_site <- full_join(one_site_q, one_site_precip,
                          by = c('date', 'site_code')) %>%
        # filter(! is.na(precip),
        #        ! is.na(val)) %>%
        select(q = q_scaled, p = precip, d = date) %>%
        mutate(d = as_date(d))

    site_fin <- try({
        compute_hydro_signatures_camels(
            # q = one_site$q,
            # p = one_site$p,
            # d = one_site$d, #see modifications to this function
            q = one_site_q$q_scaled,
            d = one_site_q$date,
            qpd = one_site,
            tol = 0.1,
            hy_cal = 'oct_us_gb'
        ) %>%
            mutate(site_code = !!hydro_sites[i])
    })

    if(inherits(site_fin, 'try-error')){
        stop('oi')
    }

    all_site_hydro <- bind_rows(all_site_hydro, site_fin)
}

all_site_hydro <- all_site_hydro %>%
    relocate(site_code) %>%
    left_join(site_doms, by = 'site_code')

for(dmn in unique(all_site_hydro$domain)){
    all_site_hydro %>%
        filter(domain == !!dmn) %>%
        select(-domain) %>%
        write_feather(glue('scratch/camels_assembly/ms_attributes/{dmn}/hydro.feather'))
}
