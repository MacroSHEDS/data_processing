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

#precip_gauge_locations; stream_gauge_locations: STATUS=READY
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

#precip_gauge_locations; stream_gauge_locations: STATUS=READY
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
                        component){

  rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
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
                        component){

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
                        component){

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
                         component){

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
                         component){

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile1,
                  colClasses = 'character',
                  quote = '')

    d <- d %>%
        as_tibble()
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
                                                'N02B' = 'n02b',
                                                'N20B' = 'n20b',
                                                'N01B' = 'n01b',
                                                'NFKC' = 'nfkc',
                                                'HOKN' = 'hokn',
                                                'SFKC' = 'sfkc',
                                                'TUBE' = 'tube',
                                                'KZFL' = 'kzfl',
                                                'SHAN' = 'shan',
                                                'HIKX' = 'hikx'),
                            data_cols =  c('NO3', 'NH4'='NH4_N', 'TN', 'SRP',
                                           'TP', 'DOC'),
                            data_col_pattern = '#V#',
                            var_flagcol_pattern = '#V#_code',
                            summary_flagcols = 'check',
                            set_to_NA = '.',
                            is_sensor = FALSE))

    d <- ue(ms_cast_and_reflag(d,
                               variable_flags_clean = 'FALSE',
                               variable_flags_dirty = 'TRUE',
                               summary_flags_to_drop = list('check' = '2'),
                               summary_flags_clean = list('check' = '1')))

    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))

    d <- remove_all_na_sites(d)

    d <- ue(ms_conversions(d,
                            convert_molecules = c('NO3', 'SO4', 'PO4', 'SiO2',
                                                  'NH4', 'NH3'),
                            convert_units_from = c(NO3 = 'ug/l', NH4_N = 'ug/l',
                                                   TN = 'ug/l', SRP = 'ug/l',
                                                   TP = 'ug/l'),
                            convert_units_to = c(NO3 = 'mg/l', NH4_N = 'mg/l',
                                                 TN = 'mg/l', SRP = 'mg/l',
                                                 TP = 'mg/l')))

    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))

    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))

}

#stream_conductivity: STATUS=READY
#. handle_errors
process_1_51 <- function(network, domain, prodname_ms, site_name,
                         component) {

  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  d <- ue(ms_read_raw_csv(filepath = rawfile1,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'RecDay' = '%d',
                                               'RecTime' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'Site',
                          alt_site_name = list('N04D' = 'n04d',
                                               'N02B'='n02b',
                                               'N20B' = 'n20b',
                                               'N01B' = 'n01b'),
                          data_cols =  c('Conduct' = 'spCond'),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE))

    d <- d %>%
      rename(val = 3) %>%
      mutate(var = 'GN_spCond',
           ms_status = 0) %>%
      filter(site_name %in% c('N04D', 'N02B', 'N20B', 'N01B'))

  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))

  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))

  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))

}

#stream_suspended_sediments: STATUS=READY
#. handle_errors
process_1_20 <- function(network, domain, prodname_ms, site_name,
                         component) {

  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  d <- read.csv(rawfile1, colClasses = "character")

  d <- d %>%
    mutate(Rectime = ifelse(Rectime == '.', 1200, Rectime)) %>%
    mutate(num_t = nchar(Rectime)) %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(time = case_when(num_t == 1 ~ paste0('010', Rectime),
                            num_t == 2 ~ paste0('01', Rectime),
                            num_t == 3 ~ paste0('0', Rectime),
                            num_t == 4 ~ as.character(Rectime),
                            is.na(num_t) ~ '1200')) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_t, -Rectime, -num_d, -RecDay)

  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'Watershed',
                          data_cols =  c('TSS', 'VSS'),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'comments',
                          is_sensor = FALSE))

  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             variable_flags_to_drop = NA,
                             variable_flags_clean = NA,
                             summary_flags_to_drop = list(comments = 'bad'),
                             summary_flags_dirty = list(comments = 'remove')))

  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))

  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))

  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
}

#stream_water_quality: STATUS=PENDING
#. handle_errors
process_1_21 <- function(network, domain, prodname_ms, site_name,
                         component) {
  # Meta data says precip chem in everything (so idk what that's about)

  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  d <- read.csv(rawfile1, colClasses = "character")

  d <- d %>%
    mutate(site = case_when(component == 'ASW011' ~ 'N02B',
                            component == 'ASW012' ~ 'N04D'))

  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('Date' = '%m/%d/%Y',
                                               'Time' = '%H:%M:%S'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'site',
                          data_cols =  c('Spcond' = 'SpCond', ),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'comments',
                          is_sensor = FALSE))
}

#stream_temperature: STATUS=READY
#. handle_errors
process_1_21 <- function(network, domain, prodname_ms, site_name,
                         component) {

  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  d <- read.csv(rawfile1, colClasses = "character")
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_43 <- function(network, domain, prodname_ms, site_name,
                         component) {

  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  d <- read.csv(rawfile1, colClasses = "character")

  d <- d %>%
    mutate(num_d = nchar(RecDay)) %>%
    mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
    select(-num_d, -RecDay)  %>%
    filter(Watershed != '001d',
           Watershed != 'n01d',
           Watershed != '')

  d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                          datetime_cols = list('RecYear' = '%Y',
                                               'RecMonth' = '%m',
                                               'day' = '%d'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'Watershed',
                          alt_site_name = list('002C' = c('R20B', '001c', 'r20b', '001c'),
                                               '020B' = '020b',
                                               'HQ02' = c('00HQ', '00hq', 'hq'),
                                               'N4DF' = c('N04D', 'n04d'),
                                               'N01B' = 'n01b'),
                          data_cols =  c('NO3'='NO3_N', 'NH4'='NH4_N', 'TPN'='TPsN',
                                         'SRP', 'TPP'),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'Comments',
                          is_sensor = FALSE))

  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             variable_flags_to_drop = NA,
                             variable_flags_clean = NA,
                             summary_flags_to_drop = list(Comments = 'bad'),
                             summary_flags_dirty = list(Comments = 'remove')))

  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))

  d <- ue(ms_conversions(d,
                         convert_molecules = c('NO3', 'SO4', 'PO4', 'SiO2',
                                               'NH4', 'NH3'),
                         convert_units_from = c(NO3 = 'ug/l', NH4_N = 'ug/l',
                                                TPsN = 'ug/l', SRP = 'ug/l',
                                                TPP = 'ug/l'),
                         convert_units_to = c(NO3 = 'mg/l', NH4_N = 'mg/l',
                                              TPsN = 'mg/l', SRP = 'mg/l',
                                              TPP = 'mg/l')))

  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))

  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
}

#precipitation: STATUS=READY
#. handle_errors
process_1_4 <- function(network, domain, prodname_ms, site_name,
                         component) {

  rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  d <- read.csv(rawfile1, colClasses = "character")

  d <- ue(ms_read_raw_csv(filepath = rawfile1,
                          datetime_cols = list('RecDate' = '%m/%d/%Y'),
                          datetime_tz = 'US/Central',
                          site_name_col = 'watershed',
                          alt_site_name = list('HQ02' = 'HQ'),
                          data_cols =  c('ppt' = 'precipitation'),
                          data_col_pattern = '#V#',
                          summary_flagcols = 'Comments',
                          is_sensor = FALSE))

  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA,
                             variable_flags_to_drop = NA,
                             variable_flags_clean = NA,
                             summary_flags_to_drop = list(Comments = 'bad'),
                             summary_flags_dirty = list(Comments = 'remove')))

  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))

  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))

  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
}

#precip_gauge_locations; stream_gauge_locations: STATUS=READY
#. handle_errors
process_1_230 <- function(network, domain, prodname_ms, site_name,
                        component) {

  rawzip <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  rawpath <- glue('data/{n}/{d}/raw/{p}/{s}',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_name,
                  c = component)

  zipped_files <- unzip(zipfile = rawzip,
                        exdir = rawpath,
                        overwrite = TRUE)

  projstring <- ue(choose_projection(unprojected = TRUE))

  if(prodname_ms == 'precip_gauge_locations__230') {
  gauges <- sf::st_read(paste0(rawpath, '/', component)) %>%
    mutate(site_name = case_when(RAINGAUGE == 'PPTSE' ~'002C',
                                 RAINGAUGE == 'PPT4B' ~ '004B',
                                 RAINGAUGE == 'PPTUB' ~ '020B',
                                 RAINGAUGE == 'PPTK4' ~ 'K01B',
                                 RAINGAUGE == 'PPTN1B' ~ 'N01B',
                                 RAINGAUGE == 'PPTN2B' ~ 'N02B',
                                 RAINGAUGE == 'PPTN4FL' ~ 'N4DF',
                                 RAINGAUGE == 'PPTN4PC' ~ 'N4DU',
                                 RAINGAUGE == 'PPTUA' ~ 'R01A',
                                 RAINGAUGE == 'PPTHQ2' ~ 'HQ02')) %>%
    filter(! is.na(site_name)) %>%
    select(site_name, geometry) %>%
    sf::st_transform(projstring) %>%
    arrange(site_name)
  } else {
    gauges <- sf::st_read(paste0(rawpath, '/', component)) %>%
      filter(! is.na(DATES_SAMP),
             STATION != 'ESH',
             STATION != 'ESF') %>%
      select(site_name = STATION, geometry) %>%
      sf::st_transform(projstring) %>%
      arrange(site_name)
  }

  unlink(zipped_files)

  return(gauges)
}

#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms011 <- function(network, domain, prodname_ms) {

  q_files <- list_munged_files(network = network,
                    domain = domain,
                    prodname_ms = c('discharge_N04D__7', 'discharge_N20B__8',
                                    'discharge_N01B__9', 'discharge_N02B__10'))

  for(i in 1:length(q_files)) {
    site_path <- str_split_fixed(q_files[i], '/', n = Inf)[1,6]
    site <- str_split_fixed(site_path, '[.]', n = Inf)[,1]


    sile_full <- read_feather(q_files[i])

    ue(write_ms_file(d = sile_full,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     site_name = site,
                     level = 'derived',
                     shapefile = FALSE,
                     link_to_portal = TRUE))
  }
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms012 <- function(network, domain, prodname_ms) {

  chem_files <- list_munged_files(network = network,
                               domain = domain,
                               prodname_ms = c('stream_chemistry__50',
                                               'stream_conductivity__51',
                                               'stream_suspended_sediments__20'))

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

#precipitation: STATUS=PENDING
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

  ue(precip_idw(precip_prodname = 'precipitation__4',
                wb_prodname = NEED,
                pgauge_prodname = 'precip_gauge_locations__230',
                precip_prodname_out = prodname_ms))

  return()
}

#precip_chemistry: STATUS=PENDING
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

  ue(pchem_idw(pchem_prodname = 'precip_chemistry__43',
               precip_prodname = 'precipitation__4',
               wb_prodname = NEED,
               pgauge_prodname = 'precip_gauge_locations__230',
               pchem_prodname_out = prodname_ms))

  return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms){

  chemprod <- 'stream_chemistry__ms012'
  qprod <- 'discharge__ms011'

  chemfiles <- ue(list_derived_files(network = network,
                                    domain = domain,
                                    prodname_ms = chemprod))
  qfiles <- ue(list_derived_files(network = network,
                                 domain = domain,
                                 prodname_ms = qprod))

  flux_sites <- generics::intersect(
    ue(fname_from_fpath(qfiles, include_fext = FALSE)),
    ue(fname_from_fpath(chemfiles, include_fext = FALSE)))

  for(s in flux_sites){

    flux <- sw(ue(calc_inst_flux(chemprod = chemprod,
                                 qprod = qprod,
                                 level = 'derived',
                                 site_name = s)))

    ue(write_ms_file(d = flux,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     site_name = s,
                     level = 'derived',
                     shapefile = FALSE,
                     link_to_portal = TRUE))
  }

  return()
}

#precip_flux_inst: STATUS=PENDING
#. handle_errors
process_2_ms004 <- function(network, domain, prodname_ms){

  ue(flux_idw(pchem_prodname = 'precip_chemistry__43',
              precip_prodname = 'precipitation__4',
              wb_prodname = NEED,
              pgauge_prodname = 'precip_gauge_locations__230',
              flux_prodname_out = prodname_ms))

  return()
}

