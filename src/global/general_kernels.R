# general kernels #### 

#npp: STATUS=READY 
#. handle_errors
process_3_ms005 <- function(network, domain, prodname_ms, ws_boundry) {
  
  npp <- get_gee_standard(network=network, 
                          domain=domain, 
                          gee_id='UMT/NTSG/v2/LANDSAT/NPP', 
                          band='annualNPP', 
                          prodname='npp', 
                          rez=30,
                          ws_boundry=ws_boundry)
  
  npp_final <- npp %>%
    mutate(year = year(date)) %>%
    select(-date) 
  
  path <- glue('data/{n}/{d}/ws_traits/npp.feather',
               n = network, d = domain)
  
  write_feather(npp_final, path)
  
  return()
}

#gpp: STATUS=READY 
#. handle_errors
process_3_ms006 <- function(network, domain, prodname_ms, ws_boundry) {
  
  gpp <- get_gee_standard(network=network, 
                          domain=domain, 
                          gee_id='UMT/NTSG/v2/LANDSAT/GPP', 
                          band='GPP', 
                          prodname='gpp', 
                          rez=30,
                          ws_boundry=ws_boundry)
  
  gpp_final <- gpp %>%
    mutate(year = year(date)) %>%
    group_by(site_name, year) %>%
    summarise(gpp_sum = sum(gpp_median, na.rm = TRUE),
              gpp_sd_year = sd(gpp_median, na.rm = TRUE),
              gpp_sd_space = mean(gpp_sd, na.rm = TRUE),
              count = n()) %>%
    mutate(gpp_sum = (gpp_sum/(count*16))*365) %>%
    select(-count)
  
  path <- glue('data/{n}/{d}/ws_traits/gpp.feather',
               n = network, d = domain)
  
  write_feather(gpp_final, path)
  
  
  return()
}

#lai; fpar: STATUS=READY 
#. handle_errors
process_3_ms007 <- function(network, domain, prodname_ms, ws_boundry) {
  
  if(prodname_ms == 'lai__ms007') {
    lai <- get_gee_standard(network=network, 
                            domain=domain, 
                            gee_id='MODIS/006/MOD15A2H', 
                            band='Lai_500m', 
                            prodname='lai', 
                            rez=500,
                            ws_boundry=ws_boundry)
    
    lai_final <- lai %>%
      mutate(year = year(date)) %>%
      group_by(site_name, year) %>%
      summarise(lai_max = max(lai_median, na.rm = TRUE),
                lai_min = min(lai_median, na.rm = TRUE),
                lai_mean = median(lai_median, na.rm = TRUE),
                lai_sd_year = sd(lai_median, na.rm = TRUE),
                lai_sd_space = mean(lai_median, na.rm = TRUE))
    
    path <- glue('data/{n}/{d}/ws_traits/lai.feather',
                 n = network, d = domain)
    
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
    
    fpar_final <- fpar %>%
      mutate(year = year(date)) %>%
      group_by(site_name, year) %>%
      summarise(fpar_max = max(fpar_median, na.rm = TRUE),
                fpar_min = min(fpar_median, na.rm = TRUE),
                fpar_mean = median(fpar_median, na.rm = TRUE),
                fpar_sd_year = sd(fpar_median, na.rm = TRUE),
                fpar_sd_space = mean(fpar_median, na.rm = TRUE))
    
    path <- glue('data/{n}/{d}/ws_traits/fpar.feather',
                 n = network, d = domain)
    
    write_feather(fpar_final, path)
  }
  return()
}

#tree_cover; veg_cover; bare_cover: STATUS=READY 
#. handle_errors
process_3_ms008 <- function(network, domain, prodname_ms, ws_boundry) {
  
  if(prodname_ms == 'tree_cover__ms008') {
    var <- get_gee_standard(network=network, 
                            domain=domain, 
                            gee_id='MODIS/006/MOD44B', 
                            band='Percent_Tree_Cover', 
                            prodname='tree_cover', 
                            rez=500,
                            ws_boundry=ws_boundry)
  }
  
  if(prodname_ms == 'veg_cover__ms008') {
    var <- get_gee_standard(network=network, 
                            domain=domain, 
                            gee_id='MODIS/006/MOD44B', 
                            band='Percent_NonTree_Vegetation', 
                            prodname='veg_cover', 
                            rez=500,
                            ws_boundry=ws_boundry)
  }
  
  if(prodname_ms == 'bare_cover__ms008') {
    var <- get_gee_standard(network=network, 
                            domain=domain, 
                            gee_id='MODIS/006/MOD44B', 
                            band='Percent_NonVegetated', 
                            prodname='bare_cover', 
                            rez=500,
                            ws_boundry=ws_boundry)
  }
  
  type <- str_split_fixed(prodname_ms, '__', n = Inf)[,1]
  
  var_final <- var %>%
    mutate(year = year(date)) %>%
    select(-date) 
  
  path <- glue('data/{n}/{d}/ws_traits/{v}.feather',
               n = network, d = domain, v = type)
  
  write_feather(var_final, path)
  
  return()
}

#prism_precip; prism_temp_mean: STATUS=READY 
#. handle_errors
process_3_ms009 <- function(network, domain, prodname_ms, ws_boundry) {
  
  if(prodname_ms == 'prism_precip__ms009') {
    get_gee_standard(network=network, 
                             domain=domain, 
                             gee_id='OREGONSTATE/PRISM/AN81d', 
                             band='ppt', 
                             prodname='prism_precip', 
                             rez=4000,
                             ws_boundry=ws_boundry)
  }
  
  if(prodname_ms == 'prism_temp_mean__ms009') {
    get_gee_standard(network=network, 
                     domain=domain, 
                     gee_id='OREGONSTATE/PRISM/AN81d', 
                     band='tmean', 
                     prodname='prism_temp_mean', 
                     rez=4000,
                     ws_boundry=ws_boundry)
  }
  return()
}

#start_season; end_season; max_season: STATUS=READY 
#. handle_errors 
process_3_ms010 <- function(network, domain, prodname_ms, ws_boundry) {
 
  if(prodname_ms == 'start_season__ms010') {
    sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                  time='start_season', ws_boundry=ws_boundry))
  }
 
 if(prodname_ms == 'max_season__ms010') {
   sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                          time='max_season', ws_boundry=ws_boundry))
 }
 
 if(prodname_ms == 'end_season__ms010') {
   sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                          time='end_season', ws_boundry=ws_boundry))
 }
 
}

