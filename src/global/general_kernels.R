# general kernels ####

#npp: STATUS=READY
#. handle_errors
process_3_ms005 <- function(network, domain, prodname_ms, site,
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
        mutate(year = as.numeric(year(date))) %>%
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
process_3_ms006 <- function(network, domain, prodname_ms, site,
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
process_3_ms007 <- function(network, domain, prodname_ms, site,
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
process_3_ms008 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == site)

    if(prodname_ms == 'tree_cover__ms008') {
        var <- try(get_gee_standard(network = network,
                                    domain = domain,
                                    gee_id = 'MODIS/006/MOD44B',
                                    band = 'Percent_Tree_Cover',
                                    prodname = 'tree_cover',
                                    rez = 500,
                                    ws_prodname = site_boundary))
    }

    if(prodname_ms == 'veg_cover__ms008') {
        var <- try(get_gee_standard(network=network,
                                domain=domain,
                                gee_id='MODIS/006/MOD44B',
                                band='Percent_NonTree_Vegetation',
                                prodname='veg_cover',
                                rez=500,
                                ws_prodname=site_boundary))
    }

    if(prodname_ms == 'bare_cover__ms008') {
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
process_3_ms009 <- function(network, domain, prodname_ms, site,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == site)

  if(prodname_ms == 'prism_precip__ms009') {
    final <- try(get_gee_standard(network = network,
                                  domain = domain,
                                  gee_id = 'OREGONSTATE/PRISM/AN81d',
                                  band = 'ppt',
                                  prodname = 'prism_precip',
                                  rez = 4000,
                                  ws_prodname = site_boundary,
                                  batch = TRUE))
  }

  if(prodname_ms == 'prism_temp_mean__ms009') {
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
process_3_ms010 <- function(network, domain, prodname_ms, site,
                            boundaries) {

    site_boundary <- boundaries %>%
        filter(site_name == site)

    if(prodname_ms == 'start_season__ms010') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                      time='start_season', ws_prodname=site_boundary, site_name=site))

    }

    if(prodname_ms == 'max_season__ms010') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                         time='max_season', ws_prodname=site_boundary, site_name=site))

    }

    if(prodname_ms == 'end_season__ms010') {

        sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                         time='end_season', ws_prodname=site_boundary, site_name=site))
    }

    if(prodname_ms == 'season_length__ms010') {

      sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms,
                       time='length_season', ws_prodname=site_boundary, site_name=site))
    }

    return()
}

#terrain: STATUS=READY
#. handle_errors
process_3_ms011 <- function(network, domain, prodname_ms, site,
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

    site_terrain <- tibble(site_name = site,
                           year = NA,
                           var = c('slope_mean', 'slope_sd', 'area'),
                           val = c(slope_mean, slope_sd, area))

    write_feather(site_terrain, glue('data/{n}/{d}/ws_traits/terrain/{s}.feather',
                                     n = network,
                                     d = domain,
                                     s = site))
}
