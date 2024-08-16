source('src/dev/camels_formatting_workspace/camels_helpers.R')
dir.create('scratch/camels_assembly/daymet_with_pet', recursive = TRUE, showWarnings = FALSE)

a_cof <- terra::rast('~/ssd2/q_sim/in/CAMELS/aptt1_30s/aptt1')

for(dmnrow in 1:nrow(network_domain)){

    ntw <- network_domain$network[dmnrow]
    dmn <- network_domain$domain[dmnrow]

    dmn_daymet <- try(read_feather(glue('data/{ntw}/{dmn}/ws_traits/daymet/domain_climate.feather')),
                      silent = TRUE)
    if(inherits(dmn_daymet, 'try-error')){
        message('no daymet data for ', dmn)
        next
    }

    dmn_sites <- site_data %>%
        filter(domain == dmn,
               in_workflow == 1,
               site_type == 'stream_gauge') %>%
        select(site_code, latitude, longitude)
    # dmn_sites <- read_csv('in/NEON/dmn_site_info.csv') %>%
    #     filter(SiteID != 'TOOK') %>%
    #     select(site_code = SiteID, latitude = Latitude, longitude = Longitude)

    dmn_elevs <- map_dfr(list.files(glue('data/{ntw}/{dmn}/ws_traits/terrain'),
                                    full.names = TRUE),
                         read_feather) %>%
        filter(var == 'te_elev_mean') %>%
        select(site_code, elev = val)

    ugh <- anti_join(dmn_sites, dmn_elevs, by = 'site_code')
    if(nrow(ugh)){
        warning('ignoring site(s) ', paste(ugh$site_code, collapse = ', '), ' from domain ',
                dmn, ' because of missing ws_traits. not cool')
        dmn_sites <- dmn_sites %>%
            filter(! site_code %in% ugh$site_code)
    }

    if(! nrow(dmn_elevs) || any(is.na(dmn_elevs$elev))) stop('oi!')
    # dmn_elevs <- read_csv('in/NEON/dmn_site_info2.csv') %>%
    #     filter(field_site_id %in% dmn_sites$site_code) %>%
    #     select(site_code = field_site_id, elev = field_mean_elevation_m)

    bndf <- list.files(glue('data/{ntw}/{dmn}/derived'), pattern = 'ws_boundary')
    dmn_boundaries <- map_dfr(list.files(glue('data/{ntw}/{dmn}/derived/{bndf}'),
                                         recursive = TRUE,
                                         pattern = '\\.shp$',
                                         full.names = TRUE),
                              ~st_read(., quiet = TRUE)) %>%
        select(site_code, geometry)
    # dmn_boundaries <- st_read() %>%
    #     filter(SiteID %in% dmn_sites$site_code) %>%
    #     select(site_code = SiteID, geometry)

    df <- left_join(dmn_daymet, dmn_sites, by = 'site_code') %>%
        left_join(., dmn_elevs, by = 'site_code')

    dmn_alpha <- tibble()
    for(i in seq_len(nrow(dmn_sites))){

        site_i <- dmn_boundaries[i, ] %>%
            sf::st_transform(., sf::st_crs(a_cof)) %>%
            terra::vect(.)

        vals <- terra::crop(a_cof, site_i) %>%
            terra::mask(site_i) %>%
            terra::values() %>%
            {.[, 1]}

        dmn_alpha <- tibble(site_code = site_i$site_code,
                            alpha = mean(vals, na.rm = TRUE) / 100) %>%
            bind_rows(dmn_alpha)
    }

    df <- left_join(df, dmn_alpha, by = 'site_code')

    calc_daymet_pet(df) %>%
        select(date, site_code, dayl, prcp, srad = s_rad, swe, tmax = t_max,
               tmin = t_min, vp, pet) %>%
        # srad is converted to different units in function from camels, converting back
        mutate(srad = srad * 11.57407) %>%
        write_csv(glue('scratch/camels_assembly/daymet_with_pet/{dmn}.csv'))
}
