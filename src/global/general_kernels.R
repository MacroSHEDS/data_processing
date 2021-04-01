
#npp: STATUS=READY
#. handle_errors
process_3_ms805 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == site)

    npp <- try(get_gee_standard(network = network,
                                domain = domain,
                                gee_id = 'UMT/NTSG/v2/LANDSAT/NPP',
                                band = 'annualNPP',
                                prodname = 'npp',
                                rez = 30,
                                ws_prodname = site_boundary))

    if(is.null(npp)) {
        return(generate_ms_exception(glue('No data was retrived for {s}',
                                          s = site)))
    }

    if(class(npp) == 'try-error'){
        return(generate_ms_err(glue('error in retrieving {s}',
                                    s = site)))
    }

    npp <- npp$table %>%
        mutate(year = ifelse(nchar(date) == 4,
                             date,
                             as.numeric(lubridate::year(date)))) %>%
        select(-date)


    dir <- glue('data/{n}/{d}/ws_traits/npp/',
                 n = network,
                 d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('{d}{s}.feather',
                 d = dir,
                 s = site)

    write_feather(npp, path)

    return()
}

#gpp: STATUS=READY
#. handle_errors
process_3_ms806 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == site)

    gpp <- try(get_gee_standard(network = network,
                                domain = domain,
                                gee_id = 'UMT/NTSG/v2/LANDSAT/GPP',
                                band = 'GPP',
                                prodname = 'gpp',
                                rez = 30,
                                ws_prodname = site_boundary))

    if(is.null(gpp)) {
        return(generate_ms_exception(glue('No data was retrived for {s}',
                                          s = site)))
    }

    if(class(gpp) == 'try-error'){
        return(generate_ms_err(glue('error in retrieving {s}',
                                    s = site)))
    }

    if(gpp$type == 'batch'){
        gpp <- gpp$table %>%
            mutate(year = substr(date, 1,4)) %>%
            mutate(doy = substr(date, 5,7)) %>%
            mutate(date_ = ymd(paste(year, '01', '01', sep = '-'))) %>%
            mutate(date = as.Date(as.numeric(doy), format = '%j', origin = date_)) %>%
            select(site_name, date, year, val, var)
    } else {
        gpp <- gpp$table %>%
            mutate(date = ymd(date)) %>%
            mutate(year = year(date))  %>%
            select(site_name, date, year, val, var)
  }

    gpp_sum <- gpp %>%
        filter(var == 'gpp_median') %>%
        group_by(site_name, year, var) %>%
        summarise(val = sum(val, na.rm = TRUE),
                  count = n()) %>%
        mutate(val = (val/(count*16))*365) %>%
        mutate(var = 'gpp_sum') %>%
        select(-count)

    gpp_sd_year <- gpp %>%
        filter(var == 'gpp_median') %>%
        group_by(site_name, year, var) %>%
        summarise(val = sd(val, na.rm = TRUE)) %>%
        mutate(var = 'gpp_sd_year')

    gpp_sd <- gpp %>%
        filter(var == 'gpp_sd') %>%
        group_by(site_name, year, var) %>%
        summarise(val = mean(val, na.rm = TRUE)) %>%
        mutate(var = 'gpp_sd_space')

    gpp_final <- rbind(gpp_sum, gpp_sd_year, gpp_sd)

    dir <- glue('data/{n}/{d}/ws_traits/gpp/',
                 n = network,
                 d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('{d}{s}.feather',
                 d = dir,
                 s = site)

    write_feather(gpp_final, path)

    return()
}

#lai; fpar: STATUS=READY
#. handle_errors
process_3_ms807 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == site)

    if(prodname_ms == 'lai__ms007') {
        lai <- try(get_gee_standard(network = network,
                                    domain = domain,
                                    gee_id = 'MODIS/006/MOD15A2H',
                                    band = 'Lai_500m',
                                    prodname = 'lai',
                                    rez = 500,
                                    ws_prodname = site_boundary))

        if(is.null(lai)) {
            return(generate_ms_err(glue('No data was retrived for {s}',
                                        s = site)))
        }

        if(class(lai) == 'try-error'){
            return(generate_ms_err(glue('error in retrieving {s}',
                                        s = site)))
        }

        if(lai$type == 'batch'){
            lai <- lai$table %>%
                mutate(year = substr(date, 1,4)) %>%
                mutate(date = ymd(date)) %>%
                select(site_name, date, year, val, var)

            } else {
                lai <- lai$table %>%
                    mutate(date = ymd(date)) %>%
                    mutate(year = year(date))  %>%
                    select(site_name, date, year, val, var)
                }

    lai_means <- lai %>%
        filter(var == 'lai_median') %>%
        group_by(site_name, year) %>%
        summarise(lai_max = max(val, na.rm = TRUE),
                  lai_min = min(val, na.rm = TRUE),
                  lai_mean = mean(val, na.rm = TRUE),
                  lai_sd_year = sd(val, na.rm = TRUE)) %>%
      pivot_longer(cols = c('lai_max', 'lai_min', 'lai_mean', 'lai_sd_year'),
                   names_to = 'var',
                   values_to = 'val')

    lai_sd <- lai %>%
        filter(var == 'lai_sd') %>%
        group_by(site_name, year) %>%
        summarise(val = mean(val, na.rm = TRUE)) %>%
        mutate(var = 'lai_sd_space')

    lai_final <- rbind(lai_means, lai_sd)

    dir <- glue('data/{n}/{d}/ws_traits/lai/',
                n = network, d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('{d}{s}.feather',
                 d = dir, s = site)

    write_feather(lai_final, path)
  }

  if(prodname_ms == 'fpar__ms007') {
    fpar <- try(get_gee_standard(network = network,
                                 domain = domain,
                                 gee_id = 'MODIS/006/MOD15A2H',
                                 band = 'Fpar_500m',
                                 prodname = 'fpar',
                                 rez = 500,
                                 ws_prodname = site_boundary))

    if(is.null(fpar)) {
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site)))
    }

    if(class(fpar) == 'try-error'){
        return(generate_ms_err(glue('error in retrieving {s}',
                                    s = site)))
    }

    if(fpar$type == 'batch'){
        fpar <- fpar$table %>%
          mutate(year = substr(date, 1,4)) %>%
          mutate(date = ymd(date)) %>%
          select(site_name, date, year, val, var)
    } else {
        fpar <- fpar$table %>%
            mutate(date = ymd(date)) %>%
            mutate(year = year(date))  %>%
            select(site_name, date, year, val, var)
    }

    fpar_means <- fpar %>%
      filter(var == 'fpar_median') %>%
      group_by(site_name, year) %>%
      summarise(fpar_max = max(val, na.rm = TRUE),
                fpar_min = min(val, na.rm = TRUE),
                fpar_mean = mean(val, na.rm = TRUE),
                fpar_sd_year = sd(val, na.rm = TRUE)) %>%
      pivot_longer(cols = c('fpar_max', 'fpar_min', 'fpar_mean', 'fpar_sd_year'),
                   names_to = 'var',
                   values_to = 'val')

    fpar_sd <- fpar %>%
        filter(var == 'fpar_sd') %>%
        group_by(site_name, year) %>%
        summarise(val = mean(val, na.rm = TRUE)) %>%
        mutate(var = 'fpar_sd_space')

    fpar_final <- rbind(fpar_means, fpar_sd)

    dir <- glue('data/{n}/{d}/ws_traits/fpar/',
                n = network, d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('{d}{s}.feather',
                 d = dir,
                 s = site)

    write_feather(fpar_final, path)
    }
  return()
}

#tree_cover; veg_cover; bare_cover: STATUS=READY
#. handle_errors
process_3_ms808 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == site)

    if(prodname_ms == 'tree_cover__ms808') {
        var <- try(get_gee_standard(network = network,
                                    domain = domain,
                                    gee_id = 'MODIS/006/MOD44B',
                                    band = 'Percent_Tree_Cover',
                                    prodname = 'tree_cover',
                                    rez = 500,
                                    ws_prodname = site_boundary))
    }

    if(prodname_ms == 'veg_cover__ms808') {
        var <- try(get_gee_standard(network=network,
                                domain=domain,
                                gee_id='MODIS/006/MOD44B',
                                band='Percent_NonTree_Vegetation',
                                prodname='veg_cover',
                                rez=500,
                                ws_prodname=site_boundary))
    }

    if(prodname_ms == 'bare_cover__ms808') {
    var <- try(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_NonVegetated',
                            prodname='bare_cover',
                            rez=500,
                            ws_prodname=site_boundary))
  }

    if(is.null(var)) {
        return(generate_ms_exception(glue('No data was retrived for {s}',
                                          s = site)))
    }

    if(class(var) == 'try-error'){
      return(generate_ms_err(glue('error in retrieving {s}',
                                  s = site)))
    }

    if(var$type == 'batch'){
        var <- var$table %>%
            mutate(date = ymd(date)) %>%
            mutate(year = year(date)) %>%
            select(site_name, date, year, val, var)
    } else {
        var <- var$table %>%
            mutate(date = ymd(date)) %>%
            mutate(year = year(date))  %>%
            select(site_name, date, year, val, var)
    }

    type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]

    var_final <- var %>%
        select(-date)

    dir <- glue('data/{n}/{d}/ws_traits/{v}/',
                n = network, d = domain, v = type)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('{d}{s}.feather',
                 d = dir,
                 s = site)

    write_feather(var_final, path)

    return()
}

#prism_precip; prism_temp_mean: STATUS=READY
#. handle_errors
process_3_ms809 <- function(network, domain, prodname_ms, site,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == site)

  if(prodname_ms == 'prism_precip__ms809') {
    final <- try(get_gee_standard(network = network,
                                  domain = domain,
                                  gee_id = 'OREGONSTATE/PRISM/AN81d',
                                  band = 'ppt',
                                  prodname = 'prism_precip',
                                  rez = 4000,
                                  ws_prodname = site_boundary,
                                  batch = TRUE))
  }

  if(prodname_ms == 'prism_temp_mean__ms809') {
    final <- try(get_gee_standard(network = network,
                                  domain = domain,
                                  gee_id = 'OREGONSTATE/PRISM/AN81d',
                                  band = 'tmean',
                                  prodname = 'prism_temp_mean',
                                  rez = 4000,
                                  ws_prodname = site_boundary,
                                  batch = TRUE))
  }

  if(is.null(final)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site)))
  }

  if(class(final) == 'try-error'){
    return(generate_ms_err(glue('error in retrieving {s}',
                                s = site)))
  }

  if(final$type == 'batch'){
    final <- final$table %>%
      mutate(date = ymd(date))
  } else {
    final <- final$table
  }

  type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]

  dir <- glue('data/{n}/{d}/ws_traits/{v}/',
              n = network, d = domain, v = type)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  path <- glue('data/{n}/{d}/ws_traits/{v}/{s}.feather',
               n = network, d = domain, v = type, s = site)

  write_feather(final, path)

  return()
}

#start_season; end_season; max_season; season_length: STATUS=READY
#. handle_errors
process_3_ms810 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == site)

    if(prodname_ms == 'start_season__ms810') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                      time='start_season', ws_prodname=site_boundary, site_name=site))

    }

    if(prodname_ms == 'max_season__ms810') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                         time='max_season', ws_prodname=site_boundary, site_name=site))

    }

    if(prodname_ms == 'end_season__ms810') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                         time='end_season', ws_prodname=site_boundary, site_name=site))
    }

    if(prodname_ms == 'season_length__ms810') {

      sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                       time='length_season', ws_prodname=site_boundary, site_name=site))
    }

    return()
}

#terrain: STATUS=READY
#. handle_errors
process_3_ms811 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    dir.create(glue('data/{n}/{d}/ws_traits/terrain/',
               n = network,
               d = domain), recursive = TRUE)

    site_boundary <- boundaries %>%
        filter(site_name == site)

    area <- as.numeric(sf::st_area(site_boundary)/1000000)

    z_level <- case_when(area > 50 ~ 8,
                         area >= 25 & area <= 50 ~ 12,
                         area < 25 ~ 14)

    dem <- elevatr::get_elev_raster(site_boundary, z = z_level)

    dem <- dem %>%
      terra::crop(., site_boundary) %>%
      terra::mask(., site_boundary)

    # Elevation
    elev_values <- raster::values(dem)
    elev_mean <- mean(elev_values, na.rm = TRUE)
    elev_sd <- sd(elev_values, na.rm = TRUE)
    elev_min <- min(elev_values, na.rm = TRUE)
    elev_max <- max(elev_values, na.rm = TRUE)


    dem_path <- tempfile(fileext = '.tif')
    raster::writeRaster(dem, dem_path)

    # Slope
    slope_path <- tempfile(fileext = '.tif')

    whitebox::wbt_slope(dem_path, slope_path)

    slope <- raster::raster(slope_path)
    slope_values <- raster::values(slope)
    slope_mean <- mean(slope_values, na.rm = TRUE)
    slope_sd <- sd(slope_values, na.rm = TRUE)

    # Aspect
    aspect_path <- tempfile(fileext = '.tif')
    whitebox::wbt_aspect(dem_path, aspect_path)

    aspect <- raster::raster(aspect_path)
    aspect_values <- raster::values(aspect)
    aspect_mean <- mean(aspect_values, na.rm = TRUE)
    aspect_sd <- sd(aspect_values, na.rm = TRUE)

    site_terrain <- tibble(site_name = site,
                           year = NA,
                           var = c('slope_mean', 'slope_sd', 'elev_mean', 'elev_sd',
                                   'elev_min', 'elev_max', 'aspect_mean', 'aspect_sd'),
                           val = c(slope_mean, slope_sd, elev_mean, elev_sd, elev_min,
                                   elev_max, aspect_mean, aspect_sd))

    write_feather(site_terrain, glue('data/{n}/{d}/ws_traits/terrain/{s}.feather',
                                     n = network,
                                     d = domain,
                                     s = site))
}

#nrcs_soils: STATUS=READY
#. handle_errors
process_3_ms812 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    dir.create(glue('data/{n}/{d}/ws_traits/nrcs_soils/',
                    n = network,
                    d = domain), recursive = TRUE)

    # Soil Organic Matter
    soil_tib <- sm(get_nrcs_soils(network = network,
                                  domain = domain,
                                  nrcs_var_name = c(
                                    # percent
                                    'soil_org' = 'om_r',
                                    # percent
                                    'soil_sand' = 'sandtotal_r',
                                    # percent
                                    'soil_silt' = 'silttotal_r',
                                    # percent
                                    'soil_clay' = 'claytotal_r',
                                    # mass/volume
                                    'soil_partical_density' = 'partdensity',
                                    # micrometers per second
                                    'soil_ksat' = 'ksat_r',
                                    # centimeters of water per centimeter of soil,
                                    # quantity of water that the soil is capable
                                    # of storing for use by plants
                                    'soil_awc' = 'awc_r',
                                    # Water content, 1/10 bar, is the amount of soil
                                    # water retained at a tension of 15 bars, expressed
                                    # as a volumetric percentage of the whole
                                    # soil material.
                                    'soil_water_0.1bar' = 'wtenthbar_r',
                                    # Water content, 1/3 bar, is the amount of soil
                                    # water retained at a tension of 15 bars, expressed
                                    # as a volumetric percentage of the whole
                                    # soil material. 15 bar = wilting point
                                    'soil_water_0.33bar' = 'wthirdbar_r',
                                    # Water content, 15 bar, is the amount of soil
                                    # water retained at a tension of 15 bars, expressed
                                    # as a volumetric percentage of the whole
                                    # soil material. 15 bar = field capacity
                                    'soil_water_15bar' = 'wfifteenbar_r',
                                    # Water content, 0 bar, is the amount of soil
                                    # water retained at a tension of 15 bars, expressed
                                    # as a volumetric percentage of the whole
                                    # soil material.
                                    'soil_water_0bar' = 'wsatiated_r',
                                    # percent of carbonates, by weight, in the
                                    # fraction of the soil less than 2 millimeters
                                    # in size.
                                    'soil_carbonate' = 'caco3_r',
                                    # percent, by weight hydrated calcium sulfates
                                    # in the fraction of the soil less than 20
                                    # millimeters in size
                                    'soil_gypsum' = 'gypsum_r',
                                    # Cation-exchange capacity (CEC-7) is the
                                    # total amount of extractable cations that can
                                    # be held by the soil, expressed in terms of
                                    # milliequivalents per 100 grams of soil at
                                    # neutrality (pH 7.0)
                                    'soil_cat_exchange_7' = 'cec7_r',
                                    # Effective cation-exchange capacity refers to
                                    # the sum of extractable cations plus aluminum
                                    # expressed in terms of milliequivalents per
                                    # 100 grams of soil
                                    'soil_cat_exchange_eff' = 'ecec_r',
                                    # Electrical conductivity (EC) is the electrolytic
                                    # conductivity of an extract from saturated
                                    # soil paste, expressed as decisiemens per
                                    # meter at 25 degrees C.
                                    'soil_elec_cond' = 'ec_r',
                                    # Sodium adsorption ratio is a measure of the
                                    # amount of sodium (Na) relative to calcium (Ca)
                                    # and magnesium (Mg) in the water extract from
                                    # saturated soil paste. It is the ratio of the
                                    # Na concentration divided by the square root
                                    # of one-half of the Ca + Mg concentration.
                                    # Soils that have SAR values of 13 or more may
                                    # be characterized by an increased dispersion
                                    # of organic matter and clay particles, reduced
                                    # saturated hydraulic conductivity (Ksat) and
                                    # aeration, and a general degradation of soil
                                    # structure.
                                    'soil_SAR' = 'sar_r',
                                    # pH is the 1:1 water method. A crushed soil
                                    # sample is mixed with an equal amount of water,
                                    # and a measurement is made of the suspension.
                                    'soil_ph' = 'ph1to1h2o_r',
                                    # Bulk density, one-third bar, is the ovendry
                                    # weight of the soil material less than 2
                                    # millimeters in size per unit volume of soil
                                    # at water tension of 1/3 bar, expressed in
                                    # grams per cubic centimeter
                                    'soil_bulk_density' = 'dbthirdbar_r',
                                    #Linear extensibility refers to the change in
                                    # length of an unconfined clod as moisture
                                    # content is decreased from a moist to a dry state.
                                    # It is an expression of the volume change
                                    # between the water content of the clod at
                                    # 1/3- or 1/10-bar tension (33kPa or 10kPa tension)
                                    # and oven dryness. The volume change is reported
                                    # as percent change for the whole soil
                                    'soil_linear_extend' = 'lep_r',
                                    # Liquid limit (LL) is one of the standard
                                    # Atterberg limits used to indicate the plasticity
                                    # characteristics of a soil. It is the water
                                    # content, on a percent by weight basis, of
                                    # the soil (passing #40 sieve) at which the
                                    # soil changes from a plastic to a liquid state
                                    'soil_liquid_limit' = 'll_r',
                                    # Plasticity index (PI) is one of the standard
                                    # Atterberg limits used to indicate the plasticity
                                    # characteristics of a soil. It is defined as
                                    # the numerical difference between the liquid
                                    # limit and plastic limit of the soil. It is
                                    # the range of water content in which a soil
                                    # exhibits the characteristics of a plastic solid.
                                    'soil_plasticity_index' = 'pi_r'),
                                  site = site,
                                  ws_boundaries = boundaries))

    if(is_ms_exception(soil_tib)) {
        return(soil_tib)
    } else{
        write_feather(soil_tib, glue('data/{n}/{d}/ws_traits/nrcs_soils/{s}.feather',
                                     n = network,
                                     d = domain,
                                     s = site))
    }


}
