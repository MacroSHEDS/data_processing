#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_20 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_174 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = '.csv')

  return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_90 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = '.csv')

  return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_14 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = '.csv')

  return()
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_156 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = '.csv')

  return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_182 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = '.csv')

  return()
}

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_20 <- function(network, domain, prodname_ms, site_code, component){

    if(component %in% c('All Sites Basic Field Stream Chemistry Data',
                        'LUQ LTER method detection limits')){
        return(tibble())
    }

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        rename_with(tolower) %>%
        mutate(sample_time = str_pad(sample_time,
                                     width = 4,
                                     pad = '0')) %>%
        rename_with(~case_when(!!!cases), .cols = everything())

    #these codes legit missing: tempcode, phcode, condcode, tsscode, tdpcode, turbiditycode
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('sample_date' = '%Y-%m-%d',
                                              'sample_time' = '%H%M'),
                         datetime_tz = 'Etc/GMT-4',
                         site_code_col = 'sample_id',
                         alt_site_code = list('QT' = c('QT', 'QT1')),
                         data_cols =  c('temp' = 'temp',
                                        'ph'='pH',
                                        'cond' = 'spCond',
                                        'cl' = 'Cl',
                                        'no3.n' = 'NO3_N',
                                        'so4.s' = 'SO4_S',
                                        'na' = 'Na',
                                        'k' = 'K',
                                        'mg' = 'Mg',
                                        'ca' = 'Ca',
                                        'nh4.n' = 'NH4_N',
                                        'po4.p' = 'PO4_P',
                                        'doc' = 'DOC',
                                        'dic' = 'DIC',
                                        'tdn' = 'TDN',
                                        'sio2' = 'SiO2',
                                        'don' = 'DON',
                                        'tss' = 'TSS',
                                        'tdp' = 'TDP',
                                        'turbidity' = 'turbid_FNU'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#code',
                         set_to_NA = '-9999',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d, variable_flags_bdl = 'BDL')

    d <- ms_conversions(d,
                        convert_units_from = c(NO3_N = 'ug/l',
                                               NH4_N = 'ug/l',
                                               PO4_P = 'ug/l',
                                               TDP = 'ug/l'),
                        convert_units_to = c(NO3_N = 'mg/l',
                                             NH4_N = 'mg/l',
                                             PO4_P = 'mg/l',
                                             TDP = 'mg/l'))

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_174 <- function(network, domain, prodname_ms, site_code, component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        rename_with(tolower) %>%
        mutate(sample_time = str_pad(sample_time, width = 4, side = 'left')) %>%
        rename_with(~case_when(!!!cases), .cols = everything())

    #these codes legit missing: tempcode, phcode, condcode, tsscode, tdpcode, turbiditycode
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('sample_date' = '%Y-%m-%d',
                                              'sample_time' = '%H%M'),
                         datetime_tz = 'Etc/GMT-4',
                         site_code_col = 'sample_id',
                         alt_site_code = list('Bisley_Tower' = 'RCB',
                                              'El_Verde' = 'RCEV'),
                         data_cols =  c('temp' = 'temp',
                                        'ph'='pH',
                                        'cond' = 'spCond',
                                        'cl' = 'Cl',
                                        'no3.n' = 'NO3_N',
                                        'so4.s' = 'SO4_S',
                                        'na' = 'Na',
                                        'k' = 'K',
                                        'mg' = 'Mg',
                                        'ca' = 'Ca',
                                        'nh4.n' = 'NH4_N',
                                        'po4.p' = 'PO4_P',
                                        'doc' = 'DOC',
                                        'dic' = 'DIC',
                                        'tdn' = 'TDN',
                                        'sio2' = 'SiO2',
                                        'don' = 'DON',
                                        'tss' = 'TSS',
                                        'tdp' = 'TDP',
                                        'turbidity' = 'turbid_FNU'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#code',
                         set_to_NA = '-9999',
                         is_sensor = FALSE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_bdl = 'BDL',
                            keep_empty_rows = TRUE)

    d <- ms_conversions(d,
                        convert_units_from = c(NO3_N = 'ug/l',
                                               NH4_N = 'ug/l',
                                               PO4_P = 'ug/l',
                                               TDP = 'ug/l'),
                        convert_units_to = c(NO3_N = 'mg/l',
                                             NH4_N = 'mg/l',
                                             PO4_P = 'mg/l',
                                             TDP = 'mg/l'))

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_90 <- function(network, domain, prodname_ms, site_code, component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'Bisley_Tower')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c(DATE = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-4',
                         site_code_col = 'site',
                         data_cols =  c(Precipitationmm = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999', '.', ''),
                         is_sensor = TRUE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_14 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'El_Verde') %>%
        rename_with(tolower) %>%
        rename_with(~sub('rainfall\\.+mm\\.?', 'rainfall_mm', .))

    if('field.comments' %in% names(d)){

        d <- d %>%
            mutate(hour = if_else(nchar(hour) == 0,
                                  '1200',
                                  hour),
                   hour = stringr::str_pad(hour,
                                           width = 4,
                                           pad = '0'))

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c(date = '%Y-%m-%d',
                                                  hour = '%H%M'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'site',
                             data_cols = c(rainfall_mm = 'precipitation'),
                             summary_flagcols = 'field.comments',
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999', ''),
                             is_sensor = TRUE,
                             keep_empty_rows = TRUE)

        d <- ms_cast_and_reflag(
            d,
            summary_flags_dirty = list('Field.Comments' = c('Cuarentena por Pandemia COVID 19',
                                                            'Hurricane rain storm event at the Station was 21.40 inches recorded at the Rain Collector at the roof of the Station. All other rain collectors overflowed and did not capture the event correctly.',
                                                            'Inicia Cuarentena por Pandemia COVID 19')),
            summary_flags_to_drop = list('Field.Comments' = c('BAD',  'DATA NOT COLLECTED')),
            varflag_col_pattern = NA,
            keep_empty_rows = TRUE
        )

    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c(date = '%Y-%m-%d'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'site',
                             data_cols = c(rainfall_mm = 'precipitation'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999', ''),
                             is_sensor = TRUE,
                             keep_empty_rows = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                keep_empty_rows = TRUE)
    }

    return(d)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_156 <- function(network, domain, prodname_ms, site_code, component){

    #info on luqillo data
    #https://www.sas.upenn.edu/lczodata/content/quebrada-three-bisley-q3

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    if(prodname_ms == 'stream_chemistry__156'){

        d <- read.csv(rawfile, colClasses = 'character') %>%
            select(DATE, meanTempq1, meanTempq2, meanTempq3) %>%
            pivot_longer(cols = c(meanTempq1, meanTempq2, meanTempq3))

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c(DATE = '%Y-%m-%d'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'name',
                             alt_site_code = list('Q1' = 'meanTempq1',
                                                  'Q2' = 'meanTempq2',
                                                  'Q3' = 'meanTempq3'),
                             data_cols =  c('value' = 'temp'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999', ''),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    } else {

        Q1A <- filter(site_data, site_code == 'Q1')$ws_area_ha
        Q2A <- filter(site_data, site_code == 'Q2')$ws_area_ha
        Q3A <- filter(site_data, site_code == 'Q3')$ws_area_ha

        d <- read.csv(rawfile, colClasses = 'character') %>%
            select(DATE, Q1, Q2, Q3) %>%
            mutate(Q1 = as.character(runoff_to_discharge(Q1, Q1A)),
                   Q2 = as.character(runoff_to_discharge(Q2, Q2A)),
                   Q3 = as.character(runoff_to_discharge(Q3, Q3A))) %>%
            pivot_longer(cols = c(Q1, Q2, Q3))

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c(DATE = '%Y-%m-%d'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'name',
                             data_cols =  c('value' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999', ''),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
    }

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_182 <- function(network, domain, prodname_ms, site_code, component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

      d <- read.csv(rawfile,
                    colClasses = 'character',
                    fileEncoding = 'ISO-8859-1')

      colnms <- colnames(d)
      if(
          ! grepl('datetime', colnms[1], ignore.case = TRUE) ||
          ! grepl('stage|level', colnms[2], ignore.case = TRUE) ||
          ! grepl('discharge', colnms[3], ignore.case = TRUE) ||
          ! grepl('notes', colnms[4], ignore.case = TRUE)
      ) stop('there has been a change')

      d <- d %>%
          select(datetime = 1,
                 discharge = 3,
                 notes = 4) %>%
          mutate(site = case_when(grepl('A', component) ~ 'QPA',
                                  grepl('B', component) ~ 'QPB',
                                  grepl('Prieta', component) ~ 'QP')) %>%
          filter(! datetime == '1900-01-00 00:00') #sensor error

      d <- ms_read_raw_csv(preprocessed_tibble = d,
                           datetime_cols = c(datetime = '%Y-%m-%d %H:%M'),
                           datetime_tz = 'Etc/GMT-4',
                           site_code_col = 'site',
                           data_cols =  'discharge',
                           summary_flagcols = 'notes',
                           data_col_pattern = '#V#',
                           set_to_NA = c('-9999', '', 'nan'),
                           is_sensor = TRUE)

      d <- ms_cast_and_reflag(
          d,
          varflag_col_pattern = NA,
          summary_flags_clean = list('notes' = c(NA, '', 'Checked correct', 'Good')),
          summary_flags_to_drop = list('notes' = c('Sensor error', 'Error', 'Bad'))
      )

    #m^3 to liters
    d <- mutate(d, val = val * 1000)

    return(d)
}

#derive kernels ####

#usgs_discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    pull_usgs_discharge(network = network,
                        domain = domain,
                        prodname_ms = prodname_ms,
                        sites = c('QG' = '50074950', 'QS' = '50063440',
                                  'RI' = '50075000', 'MPR' = '50065500',
                                  'RS' = '50067000', 'RES4' = '50063800',
                                  'QT' = '50063500'),
                        time_step = c('daily', 'sub_daily', 'daily',
                                      'daily', 'daily', 'daily',
                                      'daily'))
}

#discharge: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__156',
                                           #'discharge__182',
                                           'usgs_discharge__ms001'))
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__20',
                                           'stream_chemistry__156'))

  return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms005 <- stream_gauge_from_site_data

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms006 <- derive_stream_flux

#precipitation: STATUS=READY
#. handle_errors
process_2_ms007 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('precipitation__90',
                                           'precipitation__14'))
    return()
}

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms008 <- derive_precip_pchem_pflux
