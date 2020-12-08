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
  
  gpp_sum <- gpp %>%
    mutate(year = year(date)) %>%
    filter(var == 'gpp_median') %>%
    group_by(site_name, year, var) %>%
    summarise(gpp_sum = sum(val, na.rm = TRUE),
              count = n()) %>%
    mutate(val = (gpp_sum/(count*16))*365) %>%
    select(-count, -gpp_sum) %>%
    mutate(var = 'gpp_sum')
  
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
                            ws_prodname=site_boundary)
    
    lai_means <- lai %>%
      mutate(year = year(date)) %>%
      filter(var == 'lai_median') %>%
      group_by(site_name, year) %>%
      summarise(lai_max = max(val, na.rm = TRUE),
                lai_min = min(val, na.rm = TRUE),
                lai_mean = median(val, na.rm = TRUE),
                lai_sd_year = sd(val, na.rm = TRUE)) %>%
      pivot_longer(cols = c('lai_max', 'lai_min', 'lai_mean', 'lai_sd_year',),
                   names_to = 'var', values_to = 'val')
    
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
                             ws_prodname=site_boundary)
    
    fpar_means <- fpar %>%
      mutate(year = year(date)) %>%
      filter(var == 'fpar_median') %>%
      group_by(site_name, year) %>%
      summarise(fpar_max = max(val, na.rm = TRUE),
                fpar_min = min(val, na.rm = TRUE),
                fpar_mean = median(val, na.rm = TRUE),
                fpar_sd_year = sd(val, na.rm = TRUE)) %>%
      pivot_longer(cols = c('fpar_max', 'fpar_min', 'fpar_mean', 'fpar_sd_year'),
                   names_to = 'var', values_to = 'val')
    
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
  
  type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]
  
  var_final <- var %>%
    mutate(year = year(date)) %>%
    select(-date) 
  
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
  
  type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]
  
  dir <- glue('data/{n}/{d}/ws_traits/{v}/',
              n = network, d = domain, v = type)
  
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  
  path <- glue('data/{n}/{d}/ws_traits/{v}/{s}.feather',
               n = network, d = domain, v = type, s = site)
  
  write_feather(final, path)
  
  return()
}

#start_season; end_season; max_season: STATUS=READY 
#. handle_errors 
process_3_ms010 <- function(network, domain, prodname_ms, site,
                            boundaries) {
  
  site_boundary <- boundaries %>% 
    filter(site_name == site)
 
  if(prodname_ms == 'start_season__ms010') {
    sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                  time='start_season', ws_prodname=site_boundary))
  }
 
 if(prodname_ms == 'max_season__ms010') {
   sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                          time='max_season', ws_prodname=site_boundary))
 }
 
 if(prodname_ms == 'end_season__ms010') {
   sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                          time='end_season', ws_prodname=site_boundary))
 }
 
}

