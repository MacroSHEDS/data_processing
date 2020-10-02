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
  
  look <- read_csv(rawfile)
  
  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          date_col = list(name = 'Date',
                                          format = '%Y-%m-%d',
                                          tz = 'US/Eastern'),
                          time_col = list(name = 'time',
                                          tz = 'US/Eastern'),
                          site_name_col = 'Site',
                          data_cols = c('Cl', 'NO3_N'='NO3', 'PO4_P'='PO4', 'SO4', 'TN', 'TP', 
                                        'temperature'='temp', 'dox'='DO', 'pH', 
                                        'Turbidity'='turbid', 'Ecoli', 
                                        'conductivity'='spCond', 'Ca', 'HCO3', 
                                        'K', 'Mg', 'Na'),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
  
  #Temporary but remove when ms_read_raw_csv can handed alternate NA values 
  d[d==-999.99] <- NA
  
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
  
  look <- read_csv(rawfile)
  
  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          date_col = list(name = 'Date',
                                          format = '%Y-%m-%d',
                                          tz = 'US/Eastern'),
                          time_col = list(name = 'time',
                                          tz = 'US/Eastern'),
                          site_name_col = 'Site',
                          data_cols = c('Cl', 'NO3_N'='NO3', 'PO4_P'='PO4', 'SO4', 'TN', 'TP', 
                                        'temperature'='temp', 'dox'='DO', 'ph'='pH', 
                                        'Turbidity'='turbid', 'Ecoli'),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))
  
  #Temporary but remove when ms_read_raw_csv can handed alternate NA values 
  d[d==-999.99] <- NA
  
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
