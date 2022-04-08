
#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)
    
    url = 'https://portal.edirepository.org/nis/dataviewer?packageid=edi.621.1&entityid=1dbe749e1863c3e1ec63d10dfbce6e65'
    
    last_mod_dt <- ymd_hms('1999-09-09 09:09:09') %>% # using fake date to force retrieve. last modified date not available in url. 
      with_tz('UTC')
    
    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)
    
    if(last_mod_dt > set_details$last_mod_dt){
      
      download.file(url = url,
                    destfile = rawfile,
                    cacheOK = FALSE,
                    method = 'curl')
      
      deets_out$url <- url
      deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                    tzone = 'UTC'))
      deets_out$last_mod_dt = last_mod_dt
      
      loginfo(msg = paste('Updated', set_details$component),
              logger = logger_module)
      
      return(deets_out)
    }
    
    loginfo(glue('Nothing to do for {p}',
                 p = set_details$prodname_ms),
            logger = logger_module)
    
    return(deets_out)

}

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain) {
    
    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)
    
    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)
    
    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    url = 'https://portal.edirepository.org/nis/dataviewer?packageid=edi.618.1&entityid=cb6494ed6f404616a97de8f80edd883d'
                      
   last_mod_dt <- ymd_hms('1999-09-09 09:09:09') %>% # using fake date to force retrieve. last modified date not available in url. 
     with_tz('UTC')
   
   deets_out <- list(url = NA_character_,
                     access_time = NA_character_,
                     last_mod_dt = NA_character_)
   
   if(last_mod_dt > set_details$last_mod_dt){
     
     download.file(url = url,
                   destfile = rawfile,
                   cacheOK = FALSE,
                   method = 'curl')
     
     deets_out$url <- url
     deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                   tzone = 'UTC'))
     deets_out$last_mod_dt = last_mod_dt
     
     loginfo(msg = paste('Updated', set_details$component),
             logger = logger_module)
     
     return(deets_out)
   }
   
   loginfo(glue('Nothing to do for {p}',
                p = set_details$prodname_ms),
           logger = logger_module)
   
   return(deets_out)                      
  
    
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- function(set_details, network, domain) {
    
    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)
    
    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)
    
    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)
        
    url = 'https://portal.edirepository.org/nis/dataviewer?packageid=edi.618.1&entityid=cb6494ed6f404616a97de8f80edd883d'
                      
    last_mod_dt <- ymd_hms('1999-09-09 09:09:09') %>% # using fake date to force retrieve. last modified date not available in url. 
      with_tz('UTC')
    
    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)
    
    if(last_mod_dt > set_details$last_mod_dt){
      
      download.file(url = url,
                    destfile = rawfile,
                    cacheOK = FALSE,
                    method = 'curl')
      
      deets_out$url <- url
      deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                    tzone = 'UTC'))
      deets_out$last_mod_dt = last_mod_dt
      
      loginfo(msg = paste('Updated', set_details$component),
              logger = logger_module)
      
      return(deets_out)
    }
    
    loginfo(glue('Nothing to do for {p}',
                 p = set_details$prodname_ms),
            logger = logger_module)
    
    return(deets_out)
    
}

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- download_from_googledrive

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)
    
    all_chem <- read.csv(rawfile, colClasses = 'character') %>%
                      rename(Ca = Ca_ueq_L, Mg = Mg_ueq_L, K = K_ueq_L,
                             Na = Na_ueq_L, Cl = Cl_ueq_L, SO4 = SO4_ueq_L, NO3 = NO3_ueq_L,
                             NH4 = NH4_ueq_L, Al = Al_ppb, DOC = DOC_mg_L, DIC = DIC_mg_L,
                             TN = total_N_ppm, TP = total_P_ppb, Si = Si_mg_L, ANC = ANC_ueq_L,
                             sp = sp_conductance_us_cm, #q = discharge_L_sec, 
                             H = H_ueq_L, HCO3 = HCO3_ueq_L, pH = pH_closed_cell, TN_flag = total_N_flag,
                             TP_flag = total_P_flag) %>%
                      filter(site != 'EB3') # removing due to low sample numbers
    all_chem[all_chem==''] <- NA         

    d <- ms_read_raw_csv(preprocessed_tibble = all_chem,
                         datetime_cols = list('year' = '%Y', 'month' = '%m',
                                              'day' = '%e', 'hour' = '%H'),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'watershed',
                         data_cols =  c('Ca' = 'Ca',
                                        'Mg' = 'Mg',
                                        'K' = 'K',
                                        'Na' = 'Na',
                                        'Cl' = 'Cl',
                                        'SO4' = 'SO4',
                                        'NO3' = 'NO3',
                                        'NH4' = 'NH4',
                                        'DOC' = 'DOC',
                                        'DIC' = 'DIC',
                                        'Si' = 'Si',
                                        'ANC'= 'ANC',
                                        #'q' = 'discharge',
                                        'H' = 'H',
                                        'HCO3' = 'HCO3',
                                        'Al' = 'Al',
                                        'TN' = 'TN',
                                        'TP' = 'TP',
                                        'sp' = 'spCond',
                                        'pH' = 'pH'
                                        ), 
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#_flag',
                         is_sensor = FALSE)
    
    d$`GN_Al__|dat` <- d$`GN_Al__|dat`/1000 #convert from ppb to ppm
    d$`GN_TP__|dat` <- d$`GN_TP__|dat`/1000 #convert from ppb to ppm
    
    d <- ms_cast_and_reflag(d,
                            variable_flags_to_drop = 'DROP',
                            variable_flags_dirty = c('DL', 'RL'),
                            varflag_col_pattern = '#V#__|flg')
    
    d <- ms_conversions(d, convert_units_from = c('Ca' = 'ueq/l', 'Mg' = 'ueq/l', 'K' = 'ueq/l', 
                                                  'Na' = 'ueq/l', 'Cl' = 'ueq/l', 'SO4' = 'ueq/l',
                                                  'NO3' = 'ueq/l', 'NH4' = 'ueq/l', 'ANC' = 'ueq/l',
                                                  'HCO3' = 'ueq/l'), 
                           convert_units_to = c('Ca' = 'mg/l', 'Mg' = 'mg/l', 'K' = 'mg/l',
                                                'Na' = 'mg/l', 'Cl' = 'mg/l', 'SO4' = 'mg/l',
                                                'NO3' = 'mg/l', 'NH4' = 'mg/l', 'ANC' = 'eq/l',
                                                'HCO3' = 'mg/l')) 
    

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {
    
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- ms_read_raw_csv(filepath = rawfile,
                       datetime_cols = list('year' = '%Y', 'month' = '%m',
                                            'day' = '%e'),
                       datetime_tz = 'US/Eastern',
                       site_code_col = 'site',
                       data_cols =  c('vol_converted_to_depth_mm' = 'precipitation'
                       ), 
                       data_col_pattern = '#V#',
                       set_to_NA = '',
                       is_sensor = FALSE)
  d$site_code[d$site_code=='AERSU'] <- 'Summit_met'
  d$site_code[d$site_code=='AERCA'] <- 'Mid_met'
  d$site_code[d$site_code=='AEREA'] <- 'EB_met'
  
  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)
  
  
  d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
  
  d <- synchronize_timestep(d)
  
  sites <- unique(d$site_code)
  
  for(s in 1:length(sites)){
    
    d_site <- d %>%
      filter(site_code == !!sites[s])
    
    write_ms_file(d = d_site,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = sites[s],
                  level = 'munged',
                  shapefile = FALSE)
  }
  
  return()
    
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {
    
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- ms_read_raw_csv(filepath = rawfile,
                       datetime_cols = list('year' = '%Y', 'month' = '%m',
                                            'day' = '%e'),
                       datetime_tz = 'US/Eastern',
                       site_code_col = 'site',
                       data_cols =  c('Ca_ueq_L' = 'Ca',
                                      'Mg_ueq_L' = 'Mg',
                                      'K_ueq_L' = 'K',
                                      'Na_ueq_L' = 'Na',
                                      'Cl_ueq_L' = 'Cl',
                                      'SO4_ueq_L' = 'SO4',
                                      'NO3_ueq_L' = 'NO3',
                                      'NH4_ueq_L' = 'NH4',
                                      'Si_mg_L' = 'Si',
                                      'anc_ueq_L'= 'ANC',
                                      'Al_ppb' = 'Al',
                                      #'Al_org_ppb' = ,
                                      'spec_conductance_us_cm' = 'spCond',
                                      'pH_air_eqll' = 'pH'
                                      #'apparent_color_PCU' = ,
                                      #'true_color_PCU'
                       ),
                       data_col_pattern = '#V#',
                       set_to_NA = '',
                       is_sensor = FALSE,
                       alt_site_code = list('Summit_met' = 'AERSU',
                                            'Mid_met' = 'AERCA',
                                            'EB_met' = 'AEREA'))
  
  d$`GN_Al__|dat` <- d$`GN_Al__|dat`/1000 #convert from ppb to ppm
  
  # d$site_code[d$site_code=='AERSU'] <- 'Summit_met'
  # d$site_code[d$site_code=='AERCA'] <- 'Mid_met'
  # d$site_code[d$site_code=='AEREA'] <- 'EB_met'
  
  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)
  
  d <- ms_conversions(d, convert_units_from = c('Ca' = 'ueq/l', 'Mg' = 'ueq/l', 'K' = 'ueq/l', 
                                                'Na' = 'ueq/l', 'Cl' = 'ueq/l', 'SO4' = 'ueq/l',
                                                'NO3' = 'ueq/l', 'NH4' = 'ueq/l', 'ANC' = 'ueq/l'), 
                      convert_units_to = c('Ca' = 'mg/l', 'Mg' = 'mg/l', 'K' = 'mg/l',
                                           'Na' = 'mg/l', 'Cl' = 'mg/l', 'SO4' = 'mg/l',
                                           'NO3' = 'mg/l', 'NH4' = 'mg/l', 'ANC' = 'eq/l')) 
  
  d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
  
  d <- synchronize_timestep(d)
  
  sites <- unique(d$site_code)
  
  for(s in 1:length(sites)){
    
    d_site <- d %>%
      filter(site_code == !!sites[s])
    
    write_ms_file(d = d_site,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = sites[s],
                  level = 'munged',
                  shapefile = FALSE)
  }
  
  return()
}

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {
  
  rawdir = glue('data/{n}/{d}/raw/{p}/{s}/bbwm watersheds georefed.zip',
                n = network,
                d = domain,
                p = prodname_ms,
                s = site_code)
  
  temp_path <- tempdir()
  
  zipped_files <- unzip(zipfile = rawdir,
                        exdir = temp_path,
                        overwrite = TRUE)
  
  #projstring <- choose_projection(unprojected = TRUE)
  
  d <- sf::st_read(glue('{tp}/bbwm watersheds georefed.shx',
                        tp = temp_path),
                   stringsAsFactors = FALSE,
                   quiet = TRUE) %>%
    mutate(site_code = c('WB', 'EB'), 
           area = area/10000) 
  
  sites <- unique(d$site_code)
  
  for(s in 1:length(sites)){
    
    d_site <- d %>%
      filter(site_code == !!sites[s])
    
    write_ms_file(d = d_site,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = sites[s],
                  level = 'munged',
                  shapefile = TRUE)
  }
  
  return()
}

#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {
    
    pull_usgs_discharge(network = network,
                        domain = domain,
                        prodname_ms = prodname_ms,
                        sites = c('WB' = '01022295', 'EB' = '01022294'),
                        time_step = c('daily', 'daily'))
    
    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#precip_gauge_locations
#. handle_errors
process_2_ms003 <- precip_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_precip_pchem_pflux
