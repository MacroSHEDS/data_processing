#retrieval kernels ####

#stream_chemistry_gwynns: STATUS=READY
#. handle_errors
process_0_700 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#stream_chemistry_club: STATUS=READY
#. handle_errors
process_0_900 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#stream_chemistry_gwynns_up: STATUS=READY
#. handle_errors
process_0_800 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_3110 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#munge kernels ####

#stream_chemistry_gwynns: STATUS=READY
#. handle_errors
process_1_700 <- function(network, domain, prodname_ms, site_name,
                          component) {
  # site_name=site_name; component=in_comp
  
  rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n=network, d=domain, p=prodname_ms, s=site_name, c=component)

  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          datetime_cols = list('Date' = '%Y-%m-%d',
                                               'time' = '%H:%M'),
                          datetime_tz = 'US/Eastern',
                          site_name_col = 'Site',
                          alt_site_name = list('GFCP' = 'GFCPisco',
                                               'GFGL' = 'GFGLisco',
                                               'GFVN' = 'GFVNisco',
                                               'GRGF' = 'grgf',
                                               'RGHT' = 'RGHTisco'),
                          data_cols = c('Cl', 'NO3'='NO3_N', 'PO4'='PO4_P', 'SO4', 'TN', 'TP', 
                                        'temperature'='temp', 'dox'='DO', 'pH', 
                                        'Turbidity'='turbid', 'Ecoli', 
                                        'conductivity'='spCond', 'Ca', 'HCO3', 
                                        'K', 'Mg', 'Na'),
                          set_to_NA = -999.99,
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
  
  d <- pivot_longer(data = d,
                    cols = ends_with('__|dat'),
                    names_pattern = '^(.+?)__\\|(dat)$',
                    names_to = c('var', '.value')) %>%
    rename(val = dat) %>%
    mutate(ms_status = 0)
  
  d <- remove_all_na_sites(d)
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  # Turbidity is in NTU the ms_vars is in FNU, nut sure these are comparable 
  d <- ue(ms_conversions(d,
                         convert_units_from = c(PO4_P = 'ug/l',
                                                SO4 = 'ug/l'),
                         convert_units_to = c(PO4_P = 'mg/l',
                                              SO4 = 'mg/l')))
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
  
}

#stream_chemistry_club: STATUS=READY
#. handle_errors
process_1_900 <- function(network, domain, prodname_ms, site_name,
                          component) {
  # site_name=site_name; component=in_comp
  
  rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n=network, d=domain, p=prodname_ms, s=site_name, c=component)
  
  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          datetime_cols = list('Date' = '%Y-%m-%d',
                                               'time' = '%H:%M'),
                          datetime_tz = 'US/Eastern',
                          site_name_col = 'Site',
                          data_cols = c('Cl', 'NO3'='NO3_N', 'PO4'='PO4_P', 'SO4', 'TN', 'TP', 
                                        'temperature'='temp', 'dox'='DO', 'ph'='pH', 
                                        'Turbidity'='turbid', 'Ecoli'),
                          set_to_NA = c(-999.990, -999.99, -999),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
  
  d <- pivot_longer(data = d,
                    cols = ends_with('__|dat'),
                    names_pattern = '^(.+?)__\\|(dat)$',
                    names_to = c('var', '.value')) %>%
    rename(val = dat) %>%
    mutate(ms_status = 0)
  
  d <- remove_all_na_sites(d)
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  d <- ue(ms_conversions(d,
                         convert_units_from = c(PO4_P = 'ug/l'),
                         convert_units_to = c(PO4_P = 'mg/l')))
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
}


#stream_chemistry_gwynns_up: STATUS=READY
#. handle_errors
process_1_800 <- function(network, domain, prodname_ms, site_name,
                          component) {
  
  rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n=network, d=domain, p=prodname_ms, s=site_name, c=component)
  
  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          datetime_cols = list('date' = '%Y-%m-%d'),
                          datetime_tz = 'US/Eastern',
                          site_name_col = 'site',
                          data_cols = c('chloride'='Cl', 'nitrate'='NO3_N', 'phosphate'='PO4_P', 
                                        'sulfate'='SO4', 'nitrogen_total'='TN', 
                                        'phosphorus_total'='TP'),
                          set_to_NA = c(-999.990, -999.99, -999),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
  
  d <- pivot_longer(data = d,
                    cols = ends_with('__|dat'),
                    names_pattern = '^(.+?)__\\|(dat)$',
                    names_to = c('var', '.value')) %>%
    rename(val = dat) %>%
    mutate(ms_status = 0)
  
  d <- remove_all_na_sites(d)
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  d <- ue(ms_conversions(d,
                         convert_units_from = c(PO4_P = 'ug/l',
                                                TP = 'ug/l', 
                                                SO4 = 'ug/l'),
                         convert_units_to = c(PO4_P = 'mg/l',
                                              TP = 'mg/l',
                                              SO4 = 'mg/l')))
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
  
}

#precipitation: STATUS=READY
#. handle_errors
process_1_3110 <- function(network, domain, prodname_ms, site_name,
                           component) {
  
  rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n=network, d=domain, p=prodname_ms, s=site_name, c=component)
  
  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          datetime_cols = list('Date_Time_EST' = '%Y-%m-%d %H:%M'),
                          datetime_tz = 'US/Eastern',
                          site_name_col = 'Rain_Gauge_ID',
                          data_cols = c('Precipitation_mm'='precipitation_ns'),
                          data_col_pattern = '#V#',
                          is_sensor = TRUE))
  

  # Baltimore precip is recorded at 8 sites where there are two tipping-bucket 
  # rain gauges each. It appears that only rain events are reported, as the 
  # dataset has no 0 values (waiting on response from BES). To correct for this
  # both gauges at each site are averaged and 0 values are filled in for all
  # minutes where there is not a record. 
  
  
  d <- d %>%
    rename(val = 3) %>%
    mutate(site_name = str_split_fixed(site_name, '_', n = Inf)[,1]) %>%
    group_by(datetime, site_name) %>%
    summarise(val = mean(val, na.rm = TRUE))
  
  rain_gauges <- unique(d$site_name)
  
  final <- tibble()
  for(i in 1:length(rain_gauges)) {
    
    onesite <- d %>%
      filter(site_name == rain_gauges[i])
    
    dates <- seq.POSIXt(min(onesite$datetime), max(onesite$datetime), by = 'min')
    
    with_0s <- tibble(datetime = dates)
    
    fin <- full_join(with_0s, onesite, by = 'datetime') %>%
      mutate(val = ifelse(is.na(val), 0, val)) %>%
      mutate(site_name = rain_gauges[i]) 
    
    final <- rbind(final, fin)
  }
  
  d <- final %>%
    mutate(var = 'IS_precipitation_ns',
           ms_status = 0)
    
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
}

#derive kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms012 <- function(network, domain, prodname_ms) {
  
  chem_files <- list_munged_files(network = network,
                                  domain = domain,
                                  prodname_ms = c('stream_chemistry_gwynns__700', 
                                                  'stream_chemistry_club__900',
                                                  'stream_chemistry_gwynns_up__800'))
  
  dir <- glue('data/{n}/{d}/derived/{p}',
              n = network,
              d = domain, 
              p = prodname_ms)
  
  dir.create(dir, showWarnings = FALSE)
  
  site_feather <- str_split_fixed(chem_files, '/', n = Inf)[,6]
  sites <- unique(str_split_fixed(site_feather, '[.]', n = Inf)[,1])
  
  for(i in 1:length(sites)) {
    site_files <- grep(sites[i], chem_files, value = TRUE)
    
    sile_full <- map_dfr(site_files, read_feather)
    
    ue(write_ms_file(d = sile_full,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     site_name = sites[i],
                     level = 'derived',
                     shapefile = FALSE,
                     link_to_portal = TRUE))
  }
}
