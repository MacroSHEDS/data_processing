# general kernels ####

#npp: STATUS=READY
#. handle_errors
process_3_ms005 <- function(network, domain, prodname_ms, site,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == site)

  npp <- get_gee_standard(network=network,
                          domain=domain,
                          gee_id='UMT/NTSG/v2/LANDSAT/NPP',
                          band='annualNPP',
                          prodname='npp',
                          rez=30,
                          ws_prodname=site_boundary)

  if(is.null(npp)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site)))
  }

  npp_final <- npp %>%
    mutate(year = year(date)) %>%
    select(-date)

  dir <- glue('data/{n}/{d}/ws_traits/npp/',
               n = network, d = domain)

  path <- glue('data/{n}/{d}/ws_traits/npp/{s}.feather',
              n = network, d = domain, s = site)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  write_feather(npp_final, path)

  return()
}

#gpp: STATUS=READY
#. handle_errors
process_3_ms006 <- function(network, domain, prodname_ms, site,
                            boundaries) {

  site_boundary <- boundaries %>%
    filter(site_name == site)

  gpp <- get_gee_standard(network = network,
                          domain = domain,
                          gee_id = 'UMT/NTSG/v2/LANDSAT/GPP',
                          band = 'GPP',
                          prodname = 'gpp',
                          rez = 30,
                          ws_prodname = site_boundary)

  if(is.null(gpp)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site)))
  }

  gpp_sum <- gpp %>%
    mutate(year = year(date)) %>%
    filter(var == 'gpp_median') %>%
    group_by(site_name, year, var) %>%
    summarise(gpp_sum = sum(val, na.rm = TRUE),
              count = n()) %>%
    mutate(gpp_sum = (gpp_sum/(count*16))*365) %>%
    select(-count)

  path <- glue('data/{n}/{d}/ws_traits/gpp.feather',
               n = network, d = domain)

  gpp_sd_year <- gpp %>%
    mutate(year = year(date)) %>%
    filter(var == 'gpp_median') %>%
    group_by(site_name, year, var) %>%
    summarise(val = sd(val, na.rm = TRUE)) %>%
    mutate(var = 'gpp_sd_year')

  gpp_sd <- gpp %>%
    mutate(year = year(date)) %>%
    filter(var == 'gpp_sd') %>%
    group_by(site_name, year, var) %>%
    summarise(val = mean(val, na.rm = TRUE)) %>%
    mutate(var = 'gpp_sd_space')

  gpp_final <- rbind(gpp_sum, gpp_sd_year, gpp_sd)

  dir <- glue('data/{n}/{d}/ws_traits/gpp/',
               n = network, d = domain)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  path <- glue('data/{n}/{d}/ws_traits/gpp/{s}.feather',
               n = network, d = domain, s = site)

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
    lai <- get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD15A2H',
                            band='Lai_500m',
                            prodname='lai',
                            rez=500,
                            ws_boundry=ws_boundry)

    if(is.null(lai)) {
      return(generate_ms_err(glue('No data was retrived for {s}',
                                        s = site)))
    }

    lai_means <- lai %>%
      mutate(year = year(date)) %>%
      filter(var == 'lai_median') %>%
      group_by(site_name, year) %>%
      summarise(lai_max = max(lai_median, na.rm = TRUE),
                lai_min = min(lai_median, na.rm = TRUE),
                lai_mean = median(lai_median, na.rm = TRUE),
                lai_sd_year = sd(lai_median, na.rm = TRUE),
                lai_sd_space = mean(lai_median, na.rm = TRUE))

    path <- glue('data/{n}/{d}/ws_traits/lai.feather',
                 n = network, d = domain)

    lai_sd <- lai %>%
      mutate(year = year(date)) %>%
      filter(var == 'lai_sd') %>%
      group_by(site_name, year) %>%
      summarise(val = mean(val, na.rm = TRUE)) %>%
      mutate(var = 'lai_sd_space')

    lai_final <- rbind(lai_means, lai_sd)

    dir <- glue('data/{n}/{d}/ws_traits/lai/',
                n = network, d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('data/{n}/{d}/ws_traits/lai/{s}.feather',
                 n = network, d = domain, s = site)

    write_feather(lai_final, path)
  }

  if(prodname_ms == 'fpar__ms007') {
    fpar <- get_gee_standard(network=network,
                             domain=domain,
                             gee_id='MODIS/006/MOD15A2H',
                             band='Fpar_500m',
                             prodname='fpar',
                             rez=500,
                             ws_boundry=ws_boundry)

    if(is.null(fpar)) {
      return(generate_ms_exception(glue('No data was retrived for {s}',
                                        s = site)))
    }

    fpar_means <- fpar %>%
      mutate(year = year(date)) %>%
      filter(var == 'fpar_median') %>%
      group_by(site_name, year) %>%
      summarise(fpar_max = max(fpar_median, na.rm = TRUE),
                fpar_min = min(fpar_median, na.rm = TRUE),
                fpar_mean = median(fpar_median, na.rm = TRUE),
                fpar_sd_year = sd(fpar_median, na.rm = TRUE),
                fpar_sd_space = mean(fpar_median, na.rm = TRUE))

    path <- glue('data/{n}/{d}/ws_traits/fpar.feather',
                 n = network, d = domain)

    fpar_sd <- fpar %>%
      mutate(year = year(date)) %>%
      filter(var == 'fpar_sd') %>%
      group_by(site_name, year) %>%
      summarise(val = mean(val, na.rm = TRUE)) %>%
      mutate(var = 'fpar_sd_space')

    fpar_final <- rbind(fpar_means, fpar_sd)

    dir <- glue('data/{n}/{d}/ws_traits/fpar/',
                n = network, d = domain)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    path <- glue('data/{n}/{d}/ws_traits/fpar/{s}.feather',
                 n = network, d = domain, s = site)

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
    var <- get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_Tree_Cover',
                            prodname='tree_cover',
                            rez=500,
                            ws_prodname=site_boundary)
  }

  if(prodname_ms == 'veg_cover__ms008') {
    var <- get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_NonTree_Vegetation',
                            prodname='veg_cover',
                            rez=500,
                            ws_prodname=site_boundary)
  }

  if(prodname_ms == 'bare_cover__ms008') {
    var <- get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_NonVegetated',
                            prodname='bare_cover',
                            rez=500,
                            ws_prodname=site_boundary)
  }

  if(is.null(var)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site)))
  }

  type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]

  var_final <- var %>%
    mutate(year = year(date)) %>%
    select(-date)

  path <- glue('data/{n}/{d}/ws_traits/{v}.feather',
               n = network, d = domain, v = type)

  dir <- glue('data/{n}/{d}/ws_traits/{v}/',
              n = network, d = domain, v = type)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  path <- glue('data/{n}/{d}/ws_traits/{v}/{s}.feather',
               n = network, d = domain, v = type, s = site)

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
    final <- get_gee_large(network=network,
                             domain=domain,
                             gee_id='OREGONSTATE/PRISM/AN81d',
                             band='ppt',
                             prodname='prism_precip',
                             rez=4000,
                             start = 1980,
                             ws_prodname=site_boundary)
  }

  if(prodname_ms == 'prism_temp_mean__ms009') {
    final <- get_gee_large(network=network,
                     domain=domain,
                     gee_id='OREGONSTATE/PRISM/AN81d',
                     band='tmean',
                     prodname='prism_temp_mean',
                     rez=4000,
                     start = 1980,
                     ws_prodname=site_boundary)
  }

  if(is.null(final)) {
    return(generate_ms_exception(glue('No data was retrived for {s}',
                                      s = site)))
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

  if(prodname_ms == 'season_length__ms010'){

    dir_start <- glue('data/{n}/{d}/ws_traits/start_season/{s}.feather',
                     n = network,
                     d = domain,
                     s = site)

    dir_end <- glue('data/{n}/{d}/ws_traits/end_season/{s}.feather',
                    n = network,
                    d = domain,
                    s = site)

    dir.create(glue('data/{n}/{d}/ws_traits/season_length/',
                    n = network,
                    d = domain))

    if(file.exists(dir_start) && file.exists(dir_end)){

      start_tib <- read_feather(dir_start) %>%
        mutate(var = ifelse(var == 'sos_mean', 'mean', 'sd')) %>%
        rename(start = val)

      end_tib <- read_feather(dir_end) %>%
        mutate(var = ifelse(var == 'eos_mean', 'mean', 'sd')) %>%
        rename(end = val)

      both_tib <- full_join(start_tib, end_tib, by = c('site_name', 'year', 'var')) %>%
        mutate(val = ifelse(var == 'mean', end-start, (start+end)/2)) %>%
        select(-start, -end) %>%
        mutate(var = ifelse(var == 'mean', 'season_length_mean', 'season_length_sd'))

        write_feather(both_tib, dir_end <- glue('data/{n}/{d}/ws_traits/season_length/{s}.feather',
                                                n = network,
                                                d = domain,
                                                s = site))
    } else {
      return(generate_ms_exception(glue('either start or end of season file is missing for {s}',
                                 s = site)))
    }
  }
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
                         domain = domain,
                         year = NA,
                         var = c('slope_mean', 'slope_sd', 'area'),
                         val = c(slope_mean, slope_sd, area))

  write_feather(site_terrain, glue('data/{n}/{d}/ws_traits/terrain/{s}.feather',
                                   n = network,
                                   d = domain,
                                   s = site))
}
