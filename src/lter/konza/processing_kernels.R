#retrieval kernels ####

#discharge_N04D: STATUS=READY
#. handle_errors
process_0_7 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}


#discharge_N20B: STATUS=READY
#. handle_errors
process_0_8 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#discharge_N01B: STATUS=READY
#. handle_errors
process_0_9 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#discharge_N02B: STATUS=READY
#. handle_errors
process_0_10 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_50 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  
  return()
}

#stream_conductivity: STATUS=READY
#. handle_errors
process_0_51 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  return()
}


#stream_suspended_sediments: STATUS=READY
#. handle_errors
process_0_20 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')

  return()
}

#stream_water_quality: STATUS=READY
#. handle_errors
process_0_21 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  return()
}

#stream_temperature: STATUS=READY
#. handle_errors
process_0_16 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_43 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_4 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.csv')
  return()
}

#guage_locations: STATUS=READY
#. handle_errors
process_0_230 <- function(set_details, network, domain){
  
  download_raw_file(network = network, 
                    domain = domain, 
                    set_details = set_details, 
                    file_type = '.zip')
  return()
}

#munge kernels ####

#discharge_N04D: STATUS=READY
#. handle_errors
process_1_7 <- function(network, domain, prodname_ms, site_name,
                           components){
  
  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)
  
  look <- read_csv(rawfile1)
  
  #look carefully at warnings from ms_read_raw_csv.
  #they may indicate insufficiencies
  d <- ue(ms_read_raw_csv(filepath = rawfile1,
                          datetime_col = list(name = 'DATE_TIME',
                                              format = '%Y-%m-%d %H:%M:%S',
                                              tz = 'Etc/GMT-8'),
                          site_name_col = 'SITECODE',
                          data_cols =  c(INST_Q = 'discharge'),
                          data_col_pattern = '#V#',
                          sensor_vs_analytical = c(INST_Q = 's'),
                          summary_flagcols = c('ESTCODE', 'EVENT_CODE')))
  
  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             summary_flags_clean = list(
                               ESTCODE = c('A', 'E'),
                               EVENT_CODE = c(NA, 'WEATHR')),
                             summary_flags_dirty = list(
                               ESTCODE = c('Q', 'S', 'P'),
                               EVENT_CODE = c('INSREM', 'MAINTE'))))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
  return(d)
}

