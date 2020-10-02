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

#stream_chemistry_263: STATUS=READY
#. handle_errors
process_0_950 <- function(set_details, network, domain){
  
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
                          data_cols = c('Cl', 'NO3'='NO3_N', 'PO4'='PO4_P', 'SO4', 'TN', 'TP', 
                                        'temperature'='temp', 'dox'='DO', 'pH', 
                                        'Turbidity'='turbid', 'Ecoli', 
                                        'conductivity'='spCond', 'Ca', 'HCO3', 
                                        'K', 'Mg', 'Na'),
                          set_to_NA = -999.99,
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
  
  ## Units not correct, Need to wait on response from BES
  
  #P04_P in micrograms/L 
  #SO4 in micrograms/L
  #TP in micrograms 
  #DO (dox) in mg/l and micrograms/liter
  #Turbidity in NTU
  #ecoli in CFU/100ml 
  # Ca probably mirograms says Mg/l but also micrograms per liter 
  # HCO3 in micrograms and mg/l
  # K says ng/L and micrograms
  # Mg says ng/L and micrograms/L 
  # Na says ng/L and micrograms/L 
  
  
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
  
## Need reply from BES for unit converstion 
  
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
  
}

#stream_chemistry_263: STATUS=READY
#. handle_errors
process_1_950 <- function(network, domain, prodname_ms, site_name,
                          component) {
  
  rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                 n=network, d=domain, p=prodname_ms, s=site_name, c=component)
  
  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          datetime_cols = list('Date' = '%Y-%m-%d',
                                               'time' = '%H:%M'),
                          datetime_tz = 'US/Eastern',
                          site_name_col = 'site',
                          data_cols = c('Cl', 'NO3'='NO3_N', 'PO4'='PO4_P', 'SO4', 
                                        'TN', 'TP', 'dox'='DO', 'pH', 
                                        'Turbid'='turbid', 'Ecoli'),
                          set_to_NA = c(-999.990, -999.99, -9999),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
  
  
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
                          site_name_col = 'site',
                          data_cols = c('Precipitation_mm'='precipitation'),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
}
