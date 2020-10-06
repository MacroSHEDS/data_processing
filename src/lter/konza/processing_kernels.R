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
  
  d <- read.csv(rawfile1, colClasses = "character") 
  
  d <- d %>%
    mutate(num_t = nchar(RecHour)) %>%
    mutate(num_d = nchar(Recday)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', RecHour),
                            num_t == 2 ~ paste0('01', RecHour),
                            num_t == 3 ~ paste0('0', RecHour),
                            num_t == 4 ~ as.character(RecHour),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(Recday)), as.character(Recday))) %>%
    select(-num_t, -RecHour, -num_d, -Recday)
  
  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'Watershed',
                          alt_site_name = list('N04D' = 'n04d'),
                          data_cols =  c('Discharge' = 'discharge'),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'QualFlag',
                          is_sensor = TRUE))
  
  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             variable_flags_to_drop = NA,
                             variable_flags_clean = NA,
                             summary_flags_dirty = list('QualFlag' = 1),
                             summary_flags_to_drop = list('QualFlag' = 6)))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  # Convert from cm/s to liters/s
  d <- d %>%
    mutate(val = val*1000)
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
  return(d)
}

#discharge_N20B: STATUS=READY
#. handle_errors
process_1_8 <- function(network, domain, prodname_ms, site_name,
                        components){
  
  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)
  
  d <- read.csv(rawfile1, colClasses = "character") 
  
  d <- d %>%
    mutate(num_t = nchar(RecHour)) %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', RecHour),
                            num_t == 2 ~ paste0('01', RecHour),
                            num_t == 3 ~ paste0('0', RecHour),
                            num_t == 4 ~ as.character(RecHour),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_t, -RecHour, -num_d, -RecDay)
  
  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'Watershed',
                          alt_site_name = list('N20B' = 'n20b'),
                          data_cols =  c('Discharge' = 'discharge'),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'QualFlag',
                          is_sensor = TRUE))
  
  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             variable_flags_to_drop = NA,
                             variable_flags_clean = NA,
                             summary_flags_dirty = list('QualFlag' = 1),
                             summary_flags_to_drop = list('QualFlag' = 6)))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  # Convert from cm/s to liters/s
  d <- d %>%
    mutate(val = val*1000)
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
  return(d)
}

#discharge_N01B: STATUS=READY
#. handle_errors
process_1_9 <- function(network, domain, prodname_ms, site_name,
                        components){
  
  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)
  
  d <- read.csv(rawfile1, colClasses = "character") 
  
  d <- d %>%
    mutate(num_t = nchar(RecHour)) %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', RecHour),
                            num_t == 2 ~ paste0('01', RecHour),
                            num_t == 3 ~ paste0('0', RecHour),
                            num_t == 4 ~ as.character(RecHour),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_t, -RecHour, -num_d, -RecDay)
  
  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'Watershed',
                          alt_site_name = list('N01B' = 'n01b'),
                          data_cols =  c('Discharge' = 'discharge'),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'QualFlag',
                          is_sensor = TRUE))
  
  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             variable_flags_to_drop = NA,
                             variable_flags_clean = NA,
                             summary_flags_dirty = list('QualFlag' = 1),
                             summary_flags_to_drop = list('QualFlag' = 6)))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  # Convert from cm/s to liters/s
  d <- d %>%
    mutate(val = val*1000)
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
  return(d)
}

#discharge_N02B: STATUS=READY
#. handle_errors
process_1_10 <- function(network, domain, prodname_ms, site_name,
                        components){
  
  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)
  
  d <- read.csv(rawfile1, colClasses = "character") 
  
  d <- d %>%
    mutate(num_t = nchar(RecHour)) %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', RecHour),
                            num_t == 2 ~ paste0('01', RecHour),
                            num_t == 3 ~ paste0('0', RecHour),
                            num_t == 4 ~ as.character(RecHour),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_t, -RecHour, -num_d, -RecDay)
  
  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'Watershed',
                          alt_site_name = list('N02B' = 'n02b'),
                          data_cols =  c('Discharge' = 'discharge'),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'QualFlag',
                          is_sensor = TRUE))
  
  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             variable_flags_to_drop = NA,
                             variable_flags_clean = NA,
                             summary_flags_dirty = list('QualFlag' = 1),
                             summary_flags_to_drop = list('QualFlag' = 6)))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  # Convert from cm/s to liters/s
  d <- d %>%
    mutate(val = val*1000)
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
  return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_50 <- function(network, domain, prodname_ms, site_name,
                         components) {
  
  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)
  
  d <- read.csv(rawfile1, colClasses = "character") 
  
  d <- d %>%
    mutate(RecTime = ifelse(RecTime == '.', 1200, RecTime)) %>%
    mutate(num_t = nchar(RecTime)) %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', RecTime),
                            num_t == 2 ~ paste0('01', RecTime),
                            num_t == 3 ~ paste0('0', RecTime),
                            num_t == 4 ~ as.character(RecTime),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_t, -RecTime, -num_d, -RecDay) 
  
  TP_codes <- c('TP below Det limit', 'TP<Det limit', 'tp< det limit', 'TP below Det limit', 
    'no3 and tp < det limit', 'tp < det limit')
  NO3_codes <- c('No3 Below det limit', 'NO3 < det limit', 'NO3<det limit', 'no3 < det limit', 
    'no3 below det limit', 'no3 and tp < det limit')
  NH4_codes <- c('NH4 <det limit', 'nh4<det limit')
  
  d_comments <- d$COMMENTS
  
  tp <- grepl(paste(TP_codes,collapse="|"), d_comments) 
  
  no3 <-  grepl(paste(NO3_codes,collapse="|"), d_comments) 
  
  nh4 <-  grepl(paste(NH4_codes,collapse="|"), d_comments) 
  
  d <- d %>%
    mutate(TP_code = tp, 
           NO3_code = no3,
           NH4_code = nh4,
           SRP_code = FALSE,
           TN_code = FALSE,
           DOC_code = FALSE,
           check = 1
           ) %>%
    filter(WATERSHED %in% c('n04d', 'n02b', 'n20b', 'n01b', 'nfkc', 'hokn',
                            'sfkc', 'tube', 'kzfl', 'shan', 'hikx'))
  
  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'WATERSHED',
                          alt_site_name = list('N04D' = 'n04d',
                                              'N02B'='n02b',
                                              'N20B' = 'n20b',
                                              'N01B' = 'n01b',
                                              'NFKC' = 'nfkc',
                                              'HOKN' = 'hokn',
                                              'SFKC' = 'sfkc',
                                              'TUBE' = 'tube',
                                              'KZFL' = 'kzfl',
                                              'SHAN' = 'shan',
                                              'HIKX' = 'hikx'),
                          data_cols =  c('NO3', 'NH4'='NH4_N', 'TN', 'SRP', 'TP', 'DOC'),
                          data_col_pattern = '#V#',
                          var_flagcol_pattern = '#V#_code',
                          summary_flagcols = 'check',
                          set_to_NA = '.',
                          is_sensor = FALSE))
  
  d <- ue(ms_cast_and_reflag(d,
                             variable_flags_clean = 'FALSE',
                             variable_flags_dirty = 'TRUE',
                              summary_flags_to_drop = list('check' = '2'),
                              summary_flags_clean = list('check' = '1')
                             ))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))

  d <- ue(ms_conversions(d,
                         convert_units_from = c(NO3 = 'ug/l', NH4_N = 'ug/l', 
                                                TN = 'ug/l', SRP = 'ug/l',
                                                TP = 'ug/l'),
                         convert_units_to = c(NO3 = 'mg/l', NH4_N = 'mg/l', 
                                              TN = 'mg/l', SRP = 'ug/l',
                                              TP = 'mg/l')))
  
  # Issue when all of one variable is NA in a site (SHAN and GN_NH4_N)
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
}

#stream_conductivity: STATUS=READY
#. handle_errors
process_1_51 <- function(network, domain, prodname_ms, site_name,
                         components) {
  
  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)
  
  d <- read.csv(rawfile1, colClasses = "character") 
  
  d <- d %>%
    mutate(RecTime = ifelse(RecTime == '.', 1200, RecTime)) %>%
    mutate(num_t = nchar(RecTime)) %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', RecTime),
                            num_t == 2 ~ paste0('01', RecTime),
                            num_t == 3 ~ paste0('0', RecTime),
                            num_t == 4 ~ as.character(RecTime),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_t, -RecTime, -num_d, -RecDay) 
  
  TP_codes <- c('TP below Det limit', 'TP<Det limit', 'tp< det limit', 'TP below Det limit', 
                'no3 and tp < det limit', 'tp < det limit')
  NO3_codes <- c('No3 Below det limit', 'NO3 < det limit', 'NO3<det limit', 'no3 < det limit', 
                 'no3 below det limit', 'no3 and tp < det limit')
  NH4_codes <- c('NH4 <det limit', 'nh4<det limit')
  
  d_comments <- d$COMMENTS
  
  tp <- grepl(paste(TP_codes,collapse="|"), d_comments) 
  
  no3 <-  grepl(paste(NO3_codes,collapse="|"), d_comments) 
  
  nh4 <-  grepl(paste(NH4_codes,collapse="|"), d_comments) 
  
  d <- d %>%
    mutate(TP_code = tp, 
           NO3_code = no3,
           NH4_code = nh4,
           SRP_code = FALSE,
           TN_code = FALSE,
           DOC_code = FALSE,
           check = 1
    ) %>%
    filter(WATERSHED %in% c('n04d', 'n02b', 'n20b', 'n01b', 'nfkc', 'hokn',
                            'sfkc', 'tube', 'kzfl', 'shan', 'hikx'))
  
  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'WATERSHED',
                          alt_site_name = list('N04D' = 'n04d',
                                               'N02B'='n02b',
                                               'N20B' = 'n20b',
                                               'N01B' = 'n01b',
                                               'NFKC' = 'nfkc',
                                               'HOKN' = 'hokn',
                                               'SFKC' = 'sfkc',
                                               'TUBE' = 'tube',
                                               'KZFL' = 'kzfl',
                                               'SHAN' = 'shan',
                                               'HIKX' = 'hikx'),
                          data_cols =  c('NO3', 'NH4'='NH4_N', 'TN', 'SRP', 'TP', 'DOC'),
                          data_col_pattern = '#V#',
                          var_flagcol_pattern = '#V#_code',
                          summary_flagcols = 'check',
                          set_to_NA = '.',
                          is_sensor = FALSE))
  
  d <- ue(ms_cast_and_reflag(d,
                             variable_flags_clean = 'FALSE',
                             variable_flags_dirty = 'TRUE',
                             summary_flags_to_drop = list('check' = '2'),
                             summary_flags_clean = list('check' = '1')
  ))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  d <- ue(ms_conversions(d,
                         convert_units_from = c(NO3 = 'ug/l', NH4_N = 'ug/l', 
                                                TN = 'ug/l', SRP = 'ug/l',
                                                TP = 'ug/l'),
                         convert_units_to = c(NO3 = 'mg/l', NH4_N = 'mg/l', 
                                              TN = 'mg/l', SRP = 'ug/l',
                                              TP = 'mg/l')))
  
  # Issue when all of one variable is NA in a site (SHAN and GN_NH4_N)
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
}

