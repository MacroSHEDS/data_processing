
#npp: STATUS=READY
#. handle_errors
process_3_ms805 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == !!site_name)

    npp <- try(get_gee_standard(network = network,
                                domain = domain,
                                gee_id = 'UMT/NTSG/v2/LANDSAT/NPP',
                                band = 'annualNPP',
                                prodname = 'npp',
                                rez = 30,
                                site_boundary = site_boundary))

    if(is.null(npp)) {
        return(generate_ms_exception(glue('No data was retrived for {s}',
                                          s = site_name)))
    }

    if(class(npp) == 'try-error'){
        return(generate_ms_err(glue('error in retrieving {s}',
                                    s = site_name)))
    }

    if(npp$type == 'ee_extract'){
      npp$table <- npp$table %>%
        mutate(datetime = year(ymd(datetime)))
    } else{
      npp$table <- npp$table %>%
        mutate(datetime = as.numeric(substr(datetime, 0, 4)))
    }
    npp <- npp$table %>%
      select(year=datetime, site_name, var, val)

    if(all(is.na(npp$val))) {
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
    }

    dir <- glue('data/{n}/{d}/ws_traits/npp/',
                 n = network,
                 d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('{d}sum_{s}.feather',
                 d = dir,
                 s = site_name)
    
    npp <- append_unprod_prefix(npp, prodname_ms)
    
    write_feather(npp, path)

    return()
}

#gpp: STATUS=READY
#. handle_errors
process_3_ms806 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == !!site_name)

    gpp <- try(get_gee_standard(network = network,
                                domain = domain,
                                gee_id = 'UMT/NTSG/v2/LANDSAT/GPP',
                                band = 'GPP',
                                prodname = 'gpp',
                                rez = 30,
                                site_boundary = site_boundary))

    if(is.null(gpp)) {
        return(generate_ms_exception(glue('No data was retrived for {s}',
                                          s = site_name)))
    }

    if(class(gpp) == 'try-error'){
        return(generate_ms_err(glue('error in retrieving {s}',
                                    s = site_name)))
    }

    if(gpp$type == 'batch'){
        gpp <- gpp$table %>%
            mutate(year = substr(datetime, 1,4)) %>%
            mutate(doy = substr(datetime, 5,7)) %>%
            mutate(date_ = ymd(paste(year, '01', '01', sep = '-'))) %>%
            mutate(datetime = as.Date(as.numeric(doy), format = '%j', origin = date_)) %>%
            mutate(year = as.numeric(year)) %>%
            select(site_name, datetime, year, var, val)
    } else {
        gpp <- gpp$table %>%
            mutate(datetime = ymd(datetime)) %>%
            mutate(year = year(datetime))  %>%
            select(site_name, datetime, year, var, val)
    }

    if(all(gpp$val == 0) || all(is.na(gpp$val))){
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
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

    gpp_final <- rbind(gpp_sum, gpp_sd_year, gpp_sd) %>%
      select(year, site_name, var, val)

    gpp_raw <- gpp %>%
        select(datetime, site_name, var, val)

    dir <- glue('data/{n}/{d}/ws_traits/gpp/',
                 n = network,
                 d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    sum_path <- glue('{d}sum_{s}.feather',
                      d = dir,
                      s = site_name)
    raw_path <- glue('{d}raw_{s}.feather',
                     d = dir,
                     s = site_name)
    
    gpp_final <- append_unprod_prefix(gpp_final, prodname_ms)
    write_feather(gpp_final, sum_path)
    
    gpp_raw <- append_unprod_prefix(gpp_raw, prodname_ms)
    write_feather(gpp_raw, raw_path)

    return()
}

#lai; fpar: STATUS=READY
#. handle_errors
process_3_ms807 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == !!site_name)

    if(grepl('lai', prodname_ms)) {
        lai <- try(get_gee_standard(network = network,
                                    domain = domain,
                                    gee_id = 'MODIS/006/MOD15A2H',
                                    band = 'Lai_500m',
                                    prodname = 'lai',
                                    rez = 500,
                                    site_boundary = site_boundary,
                                    qa_band = 'FparLai_QC',
                                    bit_mask = '1',
                                    batch = TRUE))

        if(is.null(lai)) {
            return(generate_ms_err(glue('No data was retrived for {s}',
                                        s = site_name)))
        }

        if(class(lai) == 'try-error'){
            return(generate_ms_err(glue('error in retrieving {s}',
                                        s = site_name)))
        }

        if(lai$type == 'batch'){
            lai <- lai$table %>%
                mutate(year = as.numeric(substr(datetime, 1,4))) %>%
                mutate(datetime = ymd(datetime)) %>%
                select(site_name, datetime, year, var, val)

            } else {
                lai <- lai$table %>%
                    mutate(datetime = ymd(datetime)) %>%
                    mutate(year = year(datetime)) %>%
                    mutate(var =  substr(var, 7, nchar(var))) %>%
                    select(site_name, datetime, year, var, val)
            }

        if(all(lai$val == 0) || all(is.na(lai$val))){
          return(generate_ms_exception(glue('No data was retrived for {s}',
                                            s = site_name)))
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

    lai_final <- rbind(lai_means, lai_sd) %>%
      select(year, site_name, var, val)

    lai_raw <- lai %>%
        select(datetime, site_name, var, val)

    dir <- glue('data/{n}/{d}/ws_traits/lai/',
                n = network, d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    sum_path <- glue('{d}sum_{s}.feather',
                      d = dir, s = site_name)
    raw_path <- glue('{d}raw_{s}.feather',
                     d = dir, s = site_name)

    lai_final <- append_unprod_prefix(lai_final, prodname_ms)
    write_feather(lai_final, sum_path)
    
    lai_raw <- append_unprod_prefix(lai_raw, prodname_ms)
    write_feather(lai_raw, raw_path)
  }

  if(grepl('fpar', prodname_ms)) {
    fpar <- try(get_gee_standard(network = network,
                                 domain = domain,
                                 gee_id = 'MODIS/006/MOD15A2H',
                                 band = 'Fpar_500m',
                                 prodname = 'fpar',
                                 rez = 500,
                                 site_boundary = site_boundary,
                                 qa_band = 'FparLai_QC',
                                 bit_mask = '1',
                                 batch = TRUE))

    if(is.null(fpar)) {
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
    }

    if(class(fpar) == 'try-error'){
        return(generate_ms_err(glue('error in retrieving {s}',
                                    s = site_name)))
    }

    if(fpar$type == 'batch'){
        fpar <- fpar$table %>%
          mutate(year = as.numeric(substr(datetime, 1,4))) %>%
          mutate(datetime = ymd(datetime)) %>%
          select(site_name, datetime, year, var, val)
    } else {
        fpar <- fpar$table %>%
            mutate(datetime = ymd(datetime)) %>%
            mutate(year = year(datetime))  %>%
            mutate(var =  substr(var, 7, nchar(var))) %>%
            select(site_name, datetime, year, var, val)
    }

    if(all(fpar$val == 0) || all(is.na(fpar$val))){
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
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

    fpar_final <- rbind(fpar_means, fpar_sd) %>%
      select(year, site_name, var, val)

    fpar_raw <- fpar %>%
      select(datetime, site_name, val, var)

    dir <- glue('data/{n}/{d}/ws_traits/fpar/',
                n = network, d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    sum_path <- glue('{d}sum_{s}.feather',
                      d = dir,
                      s = site_name)
    raw_path <- glue('{d}raw_{s}.feather',
                     d = dir,
                     s = site_name)

    fpar_final <- append_unprod_prefix(fpar_final, prodname_ms)
    write_feather(fpar_final, sum_path)
    
    fpar_raw <- append_unprod_prefix(fpar_raw, prodname_ms)
    write_feather(fpar_raw, raw_path)
  }

    return()
}

#tree_cover; veg_cover; bare_cover: STATUS=READY
#. handle_errors
process_3_ms808 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == !!site_name)

    if(grepl('tree_cover', prodname_ms)) {
        var <- try(get_gee_standard(network = network,
                                    domain = domain,
                                    gee_id = 'MODIS/006/MOD44B',
                                    band = 'Percent_Tree_Cover',
                                    prodname = 'tree_cover',
                                    rez = 250,
                                    site_boundary = site_boundary))
    }

    if(grepl('veg_cover', prodname_ms)) {
        var <- try(get_gee_standard(network=network,
                                domain=domain,
                                gee_id='MODIS/006/MOD44B',
                                band='Percent_NonTree_Vegetation',
                                prodname='veg_cover',
                                rez=250,
                                site_boundary=site_boundary))
    }

    if(grepl('bare_cover', prodname_ms)) {
    var <- try(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_NonVegetated',
                            prodname='bare_cover',
                            rez=250,
                            site_boundary=site_boundary))
  }

    if(is.null(var)) {
        return(generate_ms_exception(glue('No data was retrived for {s}',
                                          s = site_name)))
    }

    if(class(var) == 'try-error'){
      return(generate_ms_err(glue('error in retrieving {s}',
                                  s = site_name)))
    }

    if(var$type == 'batch'){
        var <- var$table %>%
            mutate(datetime = ymd(datetime)) %>%
            mutate(year = year(datetime)) %>%
            select(site_name, datetime, year, val, var)
    } else {
        var <- var$table %>%
            mutate(datetime = ymd(datetime)) %>%
            mutate(year = year(datetime))  %>%
            mutate(var =  substr(var, 7, nchar(var))) %>%
            select(site_name, datetime, year, val, var)
    }

    type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]

    var_final <- var %>%
        select(site_name, year, var, val)

    dir <- glue('data/{n}/{d}/ws_traits/{v}/',
                n = network, d = domain, v = type)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('{d}sum_{s}.feather',
                 d = dir,
                 s = site_name)

    var_final <- append_unprod_prefix(var_final, prodname_ms)
    write_feather(var_final, path)

    return()
}

#prism_precip; prism_temp_mean: STATUS=READY
#. handle_errors
process_3_ms809 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  if(grepl('prism_precip', prodname_ms)) {
    final <- try(get_gee_standard(network = network,
                                  domain = domain,
                                  gee_id = 'OREGONSTATE/PRISM/AN81d',
                                  band = 'ppt',
                                  prodname = 'precip',
                                  rez = 4000,
                                  site_boundary = site_boundary,
                                  batch = TRUE))
  }

  if(grepl('prism_temp_mean', prodname_ms)) {
    final <- try(get_gee_standard(network = network,
                                  domain = domain,
                                  gee_id = 'OREGONSTATE/PRISM/AN81d',
                                  band = 'tmean',
                                  prodname = 'temp_mean',
                                  rez = 4000,
                                  site_boundary = site_boundary,
                                  batch = TRUE))
  }

  if(is.null(final)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site_name)))
  }

  if(class(final) == 'try-error'){
    return(generate_ms_err(glue('error in retrieving {s}',
                                s = site_name)))
  }


    final <- final$table %>%
        mutate(datetime = substr(datetime, 0, 8)) %>%
        mutate(datetime = ymd(datetime))

    if(all(is.na(final$val)) || all(final$val == 0)){
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
    }

    if(grepl('prism_precip', prodname_ms)){

      final_sum_c <- final %>%
        filter(var == 'precip_median') %>%
        mutate(year = year(datetime)) %>%
        group_by(site_name, year) %>%
        summarise(cumulative_precip = sum(val, na.rm = TRUE),
                  precip_sd_year = sd(val, na.rm = TRUE)) %>%
        pivot_longer(cols = c('cumulative_precip', 'precip_sd_year'),
                     names_to = 'var',
                     values_to = 'val') %>%
        filter(val > 0)

      final_sum_sd <- final %>%
        filter(var == 'precip_sd') %>%
        mutate(year = year(datetime)) %>%
        group_by(site_name, year) %>%
        summarise(val = mean(val, na.rm = TRUE)) %>%
        mutate(var = 'precip_sd_space')

      final_sum <- rbind(final_sum_c, final_sum_sd) %>%
        select(year, site_name, var, val)

    } else{

      final_temp <- final %>%
        filter(var == 'temp_mean_median') %>%
        mutate(year = year(datetime)) %>%
        group_by(site_name, year) %>%
        summarise(temp_mean = mean(val, na.rm = TRUE),
                  temp_sd_year = sd(val, na.rm = TRUE)) %>%
        pivot_longer(cols = c('temp_mean', 'temp_sd_year'),
                     names_to = 'var',
                     values_to = 'val')

      temp_sd <- final %>%
        filter(var == 'temp_mean_sd') %>%
        mutate(year = year(datetime)) %>%
        group_by(site_name, year) %>%
        summarise(val = mean(val, na.rm = TRUE)) %>%
        mutate(var = 'temp_sd_space')

      final_sum <- rbind(final_temp, temp_sd) %>%
        select(year, site_name, var, val)
    }

    final <- final %>%
      select(datetime, site_name, var, val)

  type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]
  type <- ifelse(type == 'prism_precip', 'cc_precip', 'cc_temp')

  dir <- glue('data/{n}/{d}/ws_traits/{v}/',
              n = network, d = domain, v = type)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  path_sum <- glue('data/{n}/{d}/ws_traits/{v}/sum_{s}.feather',
                    n = network, d = domain, v = type, s = site_name)
  path_raw <- glue('data/{n}/{d}/ws_traits/{v}/raw_{s}.feather',
                   n = network, d = domain, v = type, s = site_name)

  final <- append_unprod_prefix(final, prodname_ms)
  write_feather(final, path_raw)
  
  final_sum <- append_unprod_prefix(final_sum, prodname_ms)
  write_feather(final_sum, path_sum)

  return()
}

#start_season; end_season; max_season; season_length: STATUS=READY
#. handle_errors
process_3_ms810 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == !!site_name)

    if(prodname_ms == 'start_season__ms810') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                      time='start_season', site_boundary=site_boundary, site_name=site_name))

    }

    if(prodname_ms == 'max_season__ms810') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                         time='max_season', site_boundary=site_boundary, site_name=site_name))

    }

    if(prodname_ms == 'end_season__ms810') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                         time='end_season', site_boundary=site_boundary, site_name=site_name))
    }

    if(prodname_ms == 'season_length__ms810') {

      sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                       time='length_season', site_boundary=site_boundary, site_name=site_name))
    }

    return()
}

#terrain: STATUS=READY
#. handle_errors
process_3_ms811 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

    dir.create(glue('data/{n}/{d}/ws_traits/terrain/',
               n = network,
               d = domain), recursive = TRUE)

    site_boundary <- boundaries %>%
        filter(site_name == !!site_name)

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
    elev_values <- elev_values[elev_values >= 0]
    elev_mean <- mean(elev_values, na.rm = TRUE)
    elev_sd <- sd(elev_values, na.rm = TRUE)
    elev_min <- min(elev_values, na.rm = TRUE)
    elev_max <- max(elev_values, na.rm = TRUE)

    # Slope
    dem_path <- tempfile(fileext = '.tif')
    raster::writeRaster(dem, dem_path)
    slope_path <- tempfile(fileext = '.tif')

    whitebox::wbt_slope(dem_path, slope_path)

    slope <- raster::raster(slope_path) %>%
      terra::crop(., site_boundary) %>%
      terra::mask(., site_boundary)

    slope_values <- raster::values(slope)
    slope_mean <- mean(slope_values, na.rm = TRUE)
    slope_sd <- sd(slope_values, na.rm = TRUE)

    # Aspect
    aspect_path <- tempfile(fileext = '.tif')
    whitebox::wbt_aspect(dem_path, aspect_path)

    aspect <- raster::raster(aspect_path) %>%
      terra::crop(., site_boundary) %>%
      terra::mask(., site_boundary)

    aspect_values <- raster::values(aspect)
    aspect_mean <- mean(aspect_values, na.rm = TRUE)
    aspect_sd <- sd(aspect_values, na.rm = TRUE)

    site_terrain <- tibble(year = NA,
                           site_name = site_name,
                           var = c('slope_mean', 'slope_sd', 'elev_mean',
                                   'elev_sd', 'elev_min', 'elev_max',
                                   'aspect_mean', 'aspect_sd'),
                           val = c(slope_mean, slope_sd, elev_mean, elev_sd, elev_min,
                                   elev_max, aspect_mean, aspect_sd))

    site_terrain <- append_unprod_prefix(site_terrain, prodname_ms)

    write_feather(site_terrain, glue('data/{n}/{d}/ws_traits/terrain/{s}.feather',
                                     n = network,
                                     d = domain,
                                     s = site_name))
}

#nrcs_soils: STATUS=READY
#. handle_errors
process_3_ms812 <- function(network, domain, prodname_ms, site_name,
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
                                    # 'soil_partical_density' = 'partdensity',
                                    # micrometers per second
                                    # 'soil_ksat' = 'ksat_r',
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
                                    # 'pf_soil_carbonate' = 'caco3_r',
                                    # percent, by weight hydrated calcium sulfates
                                    # in the fraction of the soil less than 20
                                    # millimeters in size
                                    # 'pf_soil_gypsum' = 'gypsum_r',
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
                                    # 'pf_soil_cat_exchange_eff' = 'ecec_r',
                                    # Electrical conductivity (EC) is the electrolytic
                                    # conductivity of an extract from saturated
                                    # soil paste, expressed as decisiemens per
                                    # meter at 25 degrees C.
                                    # 'pf_soil_elec_cond' = 'ec_r',
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
                                    # 'pf_soil_SAR' = 'sar_r',
                                    # pH is the 1:1 water method. A crushed soil
                                    # sample is mixed with an equal amount of water,
                                    # and a measurement is made of the suspension.
                                     'soil_ph' = 'ph1to1h2o_r'),
                                    # Bulk density, one-third bar, is the ovendry
                                    # weight of the soil material less than 2
                                    # millimeters in size per unit volume of soil
                                    # at water tension of 1/3 bar, expressed in
                                    # grams per cubic centimeter
                                    # 'pf_soil_bulk_density' = 'dbthirdbar_r'),
                                    #Linear extensibility refers to the change in
                                    # length of an unconfined clod as moisture
                                    # content is decreased from a moist to a dry state.
                                    # It is an expression of the volume change
                                    # between the water content of the clod at
                                    # 1/3- or 1/10-bar tension (33kPa or 10kPa tension)
                                    # and oven dryness. The volume change is reported
                                    # as percent change for the whole soil
                                    # 'soil_linear_extend' = 'lep_r',
                                    # Liquid limit (LL) is one of the standard
                                    # Atterberg limits used to indicate the plasticity
                                    # characteristics of a soil. It is the water
                                    # content, on a percent by weight basis, of
                                    # the soil (passing #40 sieve) at which the
                                    # soil changes from a plastic to a liquid state
                                    # 'soil_liquid_limit' = 'll_r',
                                    # Plasticity index (PI) is one of the standard
                                    # Atterberg limits used to indicate the plasticity
                                    # characteristics of a soil. It is defined as
                                    # the numerical difference between the liquid
                                    # limit and plastic limit of the soil. It is
                                    # the range of water content in which a soil
                                    # exhibits the characteristics of a plastic solid.
                                    # 'soil_plasticity_index' = 'pi_r'
                                  site = site_name,
                                  ws_boundaries = boundaries))

    if(is_ms_exception(soil_tib)) {
        return(soil_tib)
    } else{
        soil_tib <- append_unprod_prefix(soil_tib, prodname_ms)
        write_feather(soil_tib, glue('data/{n}/{d}/ws_traits/nrcs_soils/{s}.feather',
                                     n = network,
                                     d = domain,
                                     s = site_name))
    }


}

#nlcd: STATUS=READY
#. handle_errors
process_3_ms813 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  nlcd_dir <- glue('data/{n}/{d}/ws_traits/nlcd/',
                   n = network,
                   d = domain)

  dir.create(nlcd_dir,
             recursive = TRUE,
             showWarnings = FALSE)

  # Load landcover defs
  color_key = read_csv('data/spatial/nlcd/pixel_color_key.csv')

  nlcd_summary = color_key %>%
    as_tibble() %>%
    select(1, 3) %>%
    rename(id = class_code) %>%
    mutate(id = as.character(id))

  # 1992 to common name here: https://pubs.usgs.gov/of/2008/1379/pdf/ofr2008-1379.pdf
  color_key_1992 = read_csv('data/spatial/nlcd/1992_pixel_color_key.csv')

  nlcd_summary_1992 = color_key_1992 %>%
    as_tibble() %>%
    select(class_code, macrosheds_1992_code, macrosheds_code) %>%
    rename(id = class_code) %>%
    mutate(id = as.character(id))

  # Get site boundary and check if the watershed is in Puerto Rico, Alaska, or Hawaii
  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  ak_bb <- sf::st_bbox(obj	= c(xmin = -173, ymin = 51.22, xmax = -129,
                               ymax = 71.35), crs = 4326) %>%
    sf::st_as_sfc(., crs = 4326)

  pr_bb <- sf::st_bbox(obj	= c(xmin = -67.95, ymin = 17.91, xmax = -65.22,
                               ymax = 18.51), crs = 4326) %>%
    sf::st_as_sfc(., crs = 4326)

  hi_bb <- sf::st_bbox(obj	= c(xmin = -160.24, ymin = 18.91, xmax = -154.81,
                               ymax = 22.23), crs = 4326) %>%
    sf::st_as_sfc(., crs = 4326)

  usa_bb <- sf::st_bbox(obj	= c(xmin = -124.725, ymin = 24.498, xmax = -66.9499,
                               ymax = 49.384), crs = 4326) %>%
    sf::st_as_sfc(., crs = 4326)

  is_ak <- length(sm(sf::st_intersects(ak_bb, site_boundary))[[1]]) == 1
  is_pr <- length(sm(sf::st_intersects(pr_bb, site_boundary))[[1]]) == 1
  is_hi <- length(sm(sf::st_intersects(hi_bb, site_boundary))[[1]]) == 1
  is_usa <- length(sm(sf::st_intersects(usa_bb, site_boundary))[[1]]) == 1

  if(is_ak){ nlcd_epochs = c('2001_AK', '2011_AK', '2016_AK') }
  if(is_pr){ nlcd_epochs = '2001_PR' }
  if(is_hi){ nlcd_epochs = '2001_HI' }
  if(is_usa){
    nlcd_epochs = as.character(c(1992, 2001, 2004, 2006, 2008, 2011, 2013, 2016))
  }

  if(!is_ak && !is_pr && !is_hi && !is_usa){

      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
  }

  wb_ee = sf_as_ee(site_boundary)

  nlcd_all <- tibble()
  for(e in nlcd_epochs){

    #subset_id = paste0('NLCD', as.character(e))
    img = ee$ImageCollection('USGS/NLCD_RELEASES/2016_REL')$
      select('landcover')$
      filter(ee$Filter$eq('system:index', e))$
      first()$
      clip(wb_ee)

    ee_description <-  glue('{n}_{d}_{s}_{p}',
                            d = domain,
                            n = network,
                            s = site_name,
                            p = str_split_fixed(prodname_ms, '__', n = Inf)[1,1])

    ee_task <- ee$batch$Export$image$toDrive(image = img,
                                             description = ee_description,
                                             folder = 'GEE',
                                             fileNamePrefix = 'nlcd',
                                             region = wb_ee$geometry(),
                                             maxPixels=NULL)

    start_mess <- try(ee_task$start())
    if(class(start_mess) == 'try-error'){
      return(generate_ms_err(glue('error in retrieving {s}',
                                  s = site_name)))
    }
    ee_monitoring(ee_task)

    temp_rgee <- tempfile(fileext = '.tif')
    googledrive::drive_download(file = 'GEE/nlcd.tif',
                                temp_rgee)

    nlcd_rast <- raster::raster(temp_rgee)

    nlcd_rast[raster::values(nlcd_rast) == 0] <- NA

    googledrive::drive_rm('GEE/nlcd.tif')

    tabulated_values = raster::values(nlcd_rast) %>%
      table() %>%
      as_tibble() %>%
      rename(id = '.',
             CellTally = 'n')

    if(is_ak || is_pr || is_hi){
      e <- as.numeric(str_split_fixed(e, pattern = '_', n = Inf)[1,1])
    }

    if(e == 1992){

      nlcd_e = full_join(nlcd_summary_1992,
                         tabulated_values,
                         by = 'id') %>%
        mutate(sum = sum(CellTally, na.rm = TRUE))

      nlcd_e_1992names <- nlcd_e %>%
        mutate(percent = round((CellTally/sum)*100, 1)) %>%
        mutate(percent = ifelse(is.na(percent), 0, percent)) %>%
        select(var = macrosheds_1992_code, val = percent) %>%
        mutate(year = !!e)

      nlcd_e_norm_names <- nlcd_e %>%
        group_by(macrosheds_code) %>%
        summarise(CellTally1992 = sum(CellTally, na.rm = TRUE)) %>%
        ungroup() %>%
        mutate(sum = sum(CellTally1992, na.rm = TRUE)) %>%
        mutate(percent = round((CellTally1992/sum)*100, 1)) %>%
        mutate(percent = ifelse(is.na(percent), 0, percent)) %>%
        select(var = macrosheds_code, val = percent) %>%
        mutate(year = !!e)

      nlcd_e <- rbind(nlcd_e_1992names, nlcd_e_norm_names)

    } else{

      nlcd_e = full_join(nlcd_summary,
                         tabulated_values,
                         by = 'id')

      nlcd_e <- nlcd_e %>%
        mutate(percent = round((CellTally*100)/sum(CellTally, na.rm = TRUE), 1)) %>%
        mutate(percent = ifelse(is.na(percent), 0, percent)) %>%
        select(var = macrosheds_code, val = percent) %>%
        mutate(year = !!e)
    }

    nlcd_all = rbind(nlcd_all, nlcd_e)
  }

  nlcd_final <- nlcd_all %>%
    mutate(year = as.numeric(year),
           site_name = !!site_name) %>%
    select(year, site_name, var, val)

  nlcd_final <- append_unprod_prefix(nlcd_final, prodname_ms)
  write_feather(nlcd_final, glue('{d}sum_{s}.feather',
                                 d = nlcd_dir,
                                 s = site_name))

}

#nadp: STATUS=READY
#. handle_errors
process_3_ms814 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  # https://gaftp.epa.gov/Epadatacommons/ORD/NHDPlusLandscapeAttributes/LakeCat/Documentation/DataDictionary.html
  # https://www.sciencebase.gov/catalog/item/53481333e4b06f6ce034aae7
  # https://github.com/USEPA/StreamCat/blob/master/ControlTable_StreamCat.csv

  nadp_dir <- glue('data/{n}/{d}/ws_traits/nadp/',
                   n = network,
                   d = domain)

  dir.create(nadp_dir, recursive = TRUE, showWarnings = FALSE)

  nadp_files <- list.files('data/spatial/ndap', recursive = TRUE, full.names = TRUE)

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  all_vars <- tibble()
  for(p in 1:length(nadp_files)){

    year <- str_split_fixed(nadp_files[p], '/', n = Inf)[1,4]
    var <- str_split_fixed(str_split_fixed(nadp_files[p], '/', n = Inf)[1,5], '_', Inf)[1,2]

    ws_values <- try(extract_ws_mean(site_boundary = site_boundary,
                                     raster_path = nadp_files[p]),
                     silent = TRUE)

    if(class(ws_values) == 'try-error') {
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
    }

    val_mean <- round(unname(ws_values['mean']), 3)
    val_sd <- round(unname(ws_values['sd']), 3)
    percent_na <-round(unname(ws_values['pctCellErr']), 2)

    one_year_var <- tibble(year = year,
                           val = c(val_mean, val_sd),
                           var = c(var, var),
                           pctCellErr = percent_na,
                           type = c('mean', 'sd_space'))

    all_vars <- rbind(all_vars, one_year_var)
  }

  fin_nadp <- all_vars %>%
    mutate(var = case_when(var == 'ca' ~ 'annual_Ca_flux',
                           var == 'cl' ~ 'annual_Cl_flux',
                           var == 'hplus' ~ 'annual_H_flux',
                           var == 'k' ~ 'annual_K_flux',
                           var == 'mg' ~ 'annual_Mg_flux',
                           var == 'na' ~ 'annual_Na_flux',
                           var == 'nh4' ~ 'annual_NH4_flux',
                           var == 'no3' ~ 'annual_NO3_flux',
                           var == 'so4' ~ 'annual_SO4_flux',
                           var == 'splusn' ~ 'annual_S_N_flux',
                           var == 'totalN' ~ 'annual_N_flux')) %>%
    mutate(var = paste0(var, '_', type)) %>%
    mutate(site_name = !!site_name,
           year = as.numeric(year)) %>%
    select(year, site_name, var, val)

  fin_nadp <- append_unprod_prefix(fin_nadp, prodname_ms)
  write_feather(fin_nadp, glue('{d}sum_{s}.feather',
                               d = nadp_dir,
                               s = site_name))
}

#pelletier_soil_thickness: STATUS=READY
#. handle_errors
process_3_ms815 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  # https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=1304

  thickness_dir <- glue('data/{n}/{d}/ws_traits/pelletier_soil_thickness/',
                        n = network,
                        d = domain)

  dir.create(thickness_dir, recursive = TRUE, showWarnings = FALSE)

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  thinkness_files <- 'data/spatial/pelletier_soil_thickness/average_soil_and_sedimentary-deposit_thickness.tif'


  ws_values <- try(extract_ws_mean(site_boundary = site_boundary,
                              raster_path = thinkness_files))


  if(class(ws_values) == 'try-error'){

    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site_name)))
  }

  thinkness_tib <- tibble(year = NA,
                          val = unname(ws_values['mean']),
                          var = 'soil_thickness',
                          pctCellErr = unname(ws_values['pctCellErr']),
                          ms_status = NA)  %>%
    mutate(site_name = !!site_name) %>%
    select(year, site_name, var, val, pctCellErr)

  thinkness_tib <- append_unprod_prefix(thinkness_tib, prodname_ms)
  write_feather(thinkness_tib, glue('{d}{s}.feather',
                               d = thickness_dir,
                               s = site_name))
}

#geochemical: STATUS=READY
#. handle_errors
process_3_ms816 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  geomchem_dir <- glue('data/{n}/{d}/ws_traits/geochemical',
                       n = network,
                       d = domain)

  dir.create(geomchem_dir, recursive = TRUE, showWarnings = FALSE)

  geomchem_files <- list.files('data/spatial/geochemical/', recursive = TRUE, full.names = TRUE)

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  all_vars <- tibble()
  for(p in 1:length(geomchem_files)){

    var <- str_split_fixed(geomchem_files[p], '/', n = Inf)[1,4]
    var <- str_split_fixed(var, '[.]', n = Inf)[1,1]
    var <- str_split_fixed(var, '_', n = Inf)[1,1]

    ws_values <- try(extract_ws_mean(site_boundary = site_boundary,
                                 raster_path = geomchem_files[p]),
                     silent = TRUE)

    if(class(ws_values) == 'try-error') {
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site_name)))
    }

    val_mean <- round(unname(ws_values['mean']), 2)
    val_sd <- round(unname(ws_values['sd']), 2)
    percent_na <-round(unname(ws_values['pctCellErr']), 2)

    one_var <- tibble(val = c(val_mean, val_sd),
                      var = c(paste0(var, '_mean'), paste0(var, '_sd')),
                      pctCellErr = percent_na)

    all_vars <- rbind(all_vars, one_var)
  }

  all_vars <- all_vars %>%
    mutate(site_name = !!site_name,
           year = NA,
           var = paste0('geo_', var)) %>%
    select(year, site_name, var, val)

  all_vars <- append_unprod_prefix(all_vars, prodname_ms)
  write_feather(all_vars, glue('{d}/{s}.feather',
                               d = geomchem_dir,
                               s = site_name))
}

#ndvi: STATUS=READY
#. handle_errors
process_3_ms817 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  ndvi <- try(get_gee_standard(network = network,
                               domain = domain,
                               gee_id = 'MODIS/006/MOD13Q1',
                               band = 'NDVI',
                               prodname = 'ndvi',
                               rez = 250,
                               site_boundary = site_boundary,
                               qa_band = 'SummaryQA',
                               bit_mask = '11',
                               batch = TRUE))

  if(is.null(ndvi)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site_name)))
  }

  if(class(ndvi) == 'try-error'){
    return(generate_ms_err(glue('error in retrieving {s}',
                                s = site_name)))
  }

  # if(ndvi$type == 'ee_extract'){
  #   ndvi$table <- ndvi$table %>%
  #     mutate(var = substr(var, 7, nchar(var)))
  #
  # }

  ndvi <- ndvi$table %>%
    mutate(datetime = ymd(datetime)) %>%
    select(site_name, datetime, var, val) %>%
    mutate(year = year(datetime)) %>%
    mutate(val = val/100)

  ndvi_means <- ndvi %>%
    filter(var == 'ndvi_median') %>%
    group_by(site_name, year) %>%
    summarise(vb_ndvi_max = max(val, na.rm = TRUE),
              vb_ndvi_min = min(val, na.rm = TRUE),
              vb_ndvi_mean = mean(val, na.rm = TRUE),
              vb_ndvi_sd_year = sd(val, na.rm = TRUE)) %>%
    pivot_longer(cols = c('ndvi_max', 'ndvi_min', 'ndvi_mean', 'ndvi_sd_year'),
                 names_to = 'var',
                 values_to = 'val')

  ndvi_sd <- ndvi %>%
    filter(var == 'ndvi_sd') %>%
    group_by(site_name, year) %>%
    summarise(val = mean(val, na.rm = TRUE)) %>%
    mutate(var = 'ndvi_sd_space')

  ndvi_final <- rbind(ndvi_means, ndvi_sd) %>%
    select(year, site_name, var, val)

  ndvi_raw <- ndvi %>%
    select(datetime, site_name, var, val)

  dir <- glue('data/{n}/{d}/ws_traits/ndvi/',
              n = network, d = domain)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  sum_path <- glue('{d}sum_{s}.feather',
                   d = dir, s = site_name)
  raw_path <- glue('{d}raw_{s}.feather',
                   d = dir, s = site_name)

  ndvi_final <- append_unprod_prefix(ndvi_final, prodname_ms)
  write_feather(ndvi_final, sum_path)
  
  ndvi_raw <- append_unprod_prefix(ndvi_raw, prodname_ms)
  write_feather(ndvi_raw, raw_path)

  return()
}

#bfi: STATUS=READY
#. handle_errors
process_3_ms818 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  # https://gaftp.epa.gov/epadatacommons/ORD/NHDPlusLandscapeAttributes/StreamCat/Documentation/DataDictionary.html
  # https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=1304

  bfi_dir <- glue('data/{n}/{d}/ws_traits/bfi/',
                        n = network,
                        d = domain)

  dir.create(bfi_dir, recursive = TRUE, showWarnings = FALSE)

  bfi_files <- 'data/spatial/bfi/bfi.tif'

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  ws_values <- try(extract_ws_mean(site_boundary = site_boundary,
                                   raster_path = bfi_files),
                   silent = TRUE)

  if(class(ws_values) == 'try-error') {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site_name)))
  }

  val_mean <- round(unname(ws_values['mean']), 2)
  val_sd <- round(unname(ws_values['sd']), 2)
  percent_na <-round(unname(ws_values['pctCellErr']), 2)

  bfi_tib <- tibble(year = NA,
                    val = c(val_mean, val_sd),
                    var = c('bfi_mean', 'bfi_sd'),
                    pctCellErr = percent_na,
                    ms_status = NA)  %>%
    mutate(site_name = !!site_name) %>%
    select(year, site_name, var, val, pctCellErr)

  bfi_tib <- append_unprod_prefix(bfi_tib, prodname_ms)
  write_feather(bfi_tib, glue('{d}{s}.feather',
                                    d = bfi_dir,
                                    s = site_name))
}

#tcw: STATUS=READY
#. handle_errors
process_3_ms819 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  tcw <- try(get_gee_standard(network = network,
                              domain = domain,
                              gee_id = 'Oxford/MAP/TCW_5km_Monthly',
                              band = 'Mean',
                              prodname = 'tcw',
                              rez = 5000,
                              site_boundary = site_boundary))

  if(is.null(tcw)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site_name)))
  }

  if(class(tcw) == 'try-error'){
    return(generate_ms_err(glue('error in retrieving {s}',
                                s = site_name)))
  }

  if(tcw$type == 'ee_extract'){
    tcw <- tcw$table %>%
      mutate(var = substr(var, 4, nchar(var))) %>%
      mutate(datetime = ymd(datetime)) %>%
      mutate(year = year(datetime))
  } else{
    tcw <- tcw$table %>%
      mutate(datetime = ymd(paste0(substr(datetime, 1, 7), '_01'))) %>%
      mutate(year = year(datetime))
  }

  tcw_means <- tcw %>%
    filter(var == 'tcw_median') %>%
    group_by(site_name, year) %>%
    summarise(vh_tcw_max = max(val, na.rm = TRUE),
              vh_tcw_min = min(val, na.rm = TRUE),
              vh_tcw_mean = mean(val, na.rm = TRUE),
              vh_tcw_sd_year = sd(val, na.rm = TRUE)) %>%
    pivot_longer(cols = c('tcw_max', 'tcw_min', 'tcw_mean', 'tcw_sd_year'),
                 names_to = 'var',
                 values_to = 'val')

  tcw_sd <- tcw %>%
    filter(var == 'tcw_sd') %>%
    group_by(site_name, year) %>%
    summarise(val = mean(val, na.rm = TRUE)) %>%
    mutate(var = 'tcw_sd_space')

  tcw_final <- rbind(tcw_means, tcw_sd) %>%
    select(year, site_name, var, val)

  tcw <- tcw %>%
    select(datetime, site_name, var, val)


  dir <- glue('data/{n}/{d}/ws_traits/tcw/',
              n = network,
              d = domain)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  raw_path <- glue('{d}raw_{s}.feather',
               d = dir,
               s = site_name)
  sum_path <- glue('{d}sum_{s}.feather',
                   d = dir,
                   s = site_name)

  tcw <- append_unprod_prefix(tcw, prodname_ms)
  write_feather(tcw, raw_path)
  
  tcw_final <- append_unprod_prefix(tcw_final, prodname_ms)
  write_feather(tcw_final, sum_path)

  return()
}

#et_ref: STATUS=READY
#. handle_errors
process_3_ms820 <- function(network, domain, prodname_ms, site_name,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == !!site_name)

  final <- try(get_gee_standard(network = network,
                                domain = domain,
                                gee_id = 'IDAHO_EPSCOR/GRIDMET',
                                band = 'eto',
                                prodname = 'et_ref',
                                rez = 4000,
                                site_boundary = site_boundary,
                                batch = TRUE))


  if(is.null(final)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site_name)))
  }

  if(class(final) == 'try-error'){
    return(generate_ms_err(glue('error in retrieving {s}',
                                s = site_name)))
  }

  final <- final$table %>%
    mutate(datetime = substr(datetime, 0, 8)) %>%
    mutate(datetime = ymd(datetime))

  if(all(is.na(final$val)) || all(final$val == 0)){
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site_name)))
  }

  final_ <- final %>%
    filter(var == 'et_ref_median') %>%
    mutate(year = year(datetime)) %>%
    group_by(site_name, year) %>%
    summarise(et_ref_mean = mean(val, na.rm = TRUE),
              et_ref_sd_year = sd(val, na.rm = TRUE)) %>%
    pivot_longer(cols = c('et_ref_mean', 'et_ref_sd_year'),
                 names_to = 'var',
                 values_to = 'val')

  temp_sd <- final %>%
    filter(var == 'et_ref_sd') %>%
    mutate(year = year(datetime)) %>%
    group_by(site_name, year) %>%
    summarise(val = mean(val, na.rm = TRUE)) %>%
    mutate(var = 'et_ref_sd_space')

  final_sum <- rbind(final_, temp_sd) %>%
    select(year, site_name, var, val)

  final <- final %>%
    select(datetime, site_name, var, val)

  dir <- glue('data/{n}/{d}/ws_traits/et_ref/',
              n = network, d = domain)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  path_sum <- glue('data/{n}/{d}/ws_traits/{v}/sum_{s}.feather',
                   n = network, d = domain, v = 'et_ref', s = site_name)
  path_raw <- glue('data/{n}/{d}/ws_traits/{v}/raw_{s}.feather',
                   n = network, d = domain, v = 'et_ref', s = site_name)

  final <- append_unprod_prefix(final, prodname_ms)
  write_feather(final, path_raw)
  
  final_sum <- append_unprod_prefix(final_sum, prodname_ms)
  write_feather(final_sum, path_sum)

  return()
}