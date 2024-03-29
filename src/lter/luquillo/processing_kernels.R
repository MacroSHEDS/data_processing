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
process_1_20 <- function(network, domain, prodname_ms, site_code,
                         component){

    if(component %in% c('All Sites Basic Field Stream Chemistry Data',
                        'LUQ LTER method detection limits')){
        return(tibble())
    }
    
    # look <- read_csv('data/lter/luquillo/raw/stream_chemistry__20/sitename_NA/All Sites Basic Field Stream Chemistry Data.csv')

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)
    
    d <- read.csv(rawfile,
                  colClasses = 'character') %>%
        mutate(Sample_Time = ifelse(nchar(Sample_Time) == 3, paste0(0, Sample_Time), Sample_Time)) %>%
        rename(NO3.NCode = NitrateCode,
               SO4.SCode = SulfateCode,
               ClCode = ChlorideCode,
               NaCode = SodiumCode,
               KCode = PotassiumCode,
               MgCode = MagnesiumCode,
               CaCode = CalciumCode,
               NH4.NCode = NH4Code,
               PO4.PCode = PO4Code)
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Sample_Date' = '%Y-%m-%d',
                                              'Sample_Time' = '%H%M'
                                              ),
                         datetime_tz = 'Etc/GMT-4',
                         site_code_col = 'Sample_ID',
                         alt_site_code = list('QT' = c('QT', 'QT1')),
                         data_cols =  c('Temp' = 'temp',
                                        'pH'='pH',
                                        'Cond' = 'spCond',
                                        'Cl',
                                        'NO3.N' = 'NO3_N',
                                        'SO4.S' = 'SO4_S',
                                        'Na',
                                        'K',
                                        'Mg',
                                        'Ca',
                                        'NH4.N' = 'NH4_N',
                                        'PO4.P' = 'PO4_P',
                                        'DOC',
                                        'DIC',
                                        'TDN',
                                        'SiO2',
                                        'DON',
                                        'TSS',
                                        'TDP'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#Code',
                         set_to_NA = '-9999',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_bdl = 'BDL')

    d <- ms_conversions(d,
                        convert_units_from = c(NO3_N = 'ug/l',
                                               NH4_N = 'ug/l',
                                               PO4_P = 'ug/l',
                                               TDP = 'ug/l'),
                        convert_units_to = c(NO3_N = 'mg/l',
                                             NH4_N = 'mg/l',
                                             PO4_P = 'mg/l',
                                             TDP = 'ug/l'))

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_174 <- function(network, domain, prodname_ms, site_code,
                          component){

  # Luquillo's metadata says NO3_N units are in mg/l but they are very high so
  # I am assuming they must be in ug/l, will check with Luquillo.
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile,
                  colClasses = 'character') %>%
      mutate(Sample_Time = ifelse(nchar(Sample_Time) == 3, paste0(0, Sample_Time), Sample_Time)) %>%
        rename(NO3.NCode = NitrateCode,
               SO4.SCode = SulfateCode,
               ClCode = ChlorideCode,
               NaCode = SodiumCode,
               KCode = PotassiumCode,
               MgCode = MagnesiumCode,
               CaCode = CalciumCode,
               NH4.NCode = NH4Code,
               PO4.PCode = PO4Code)


    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Sample_Date' = '%Y-%m-%d',
                                              'Sample_Time' = '%H%M'),
                         datetime_tz = 'Etc/GMT-4',
                         site_code_col = 'Sample_ID',
                         alt_site_code = list('Bisley_Tower' = 'RCB',
                                              'El_Verde' = 'RCEV'),
                         data_cols =  c('pH'='pH',
                                        'Cond' = 'spCond',
                                        'Cl',
                                        'NO3.N' = 'NO3_N',
                                        'SO4.S' = 'SO4_S',
                                        'Na',
                                        'K',
                                        'Mg',
                                        'Ca',
                                        'NH4.N' = 'NH4_N',
                                        'PO4.P' = 'PO4_P',
                                        'DOC',
                                        'DIC',
                                        'TDN',
                                        'TDP',
                                        'SiO2'),
                         data_col_pattern = '#V#',
                         set_to_NA = '-9999',
                         var_flagcol_pattern = '#V#Code',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_bdl = 'BDL')

    d <- ms_conversions(d,
                        convert_units_from = c(NH4_N = 'ug/l',
                                               PO4_P = 'ug/l',
                                               NO3_N = 'ug/l',
                                               TDP = 'ug/l'),
                        convert_units_to = c(NH4_N = 'mg/l',
                                             PO4_P = 'mg/l',
                                             NO3_N = 'mg/l',
                                             TDP = 'mg/l'))

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_90 <- function(network, domain, prodname_ms, site_code,
                         component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile,
                  colClasses = 'character') %>%
      mutate(site = 'Bisley_Tower') %>%
      mutate(month = str_split_fixed(DATE, '/', n = Inf)[,1],
             day = str_split_fixed(DATE, '/', n = Inf)[,2],
             year = str_split_fixed(DATE, '/', n = Inf)[,3]) %>%
      mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day)) %>%
      mutate(year = ifelse(nchar(year) >= 5, str_split_fixed(year, ' ', n = Inf)[,1], year))


    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('day' = '%d',
                                              'month' = '%m',
                                              'year' = '%Y'),
                         datetime_tz = 'Etc/GMT-4',
                         site_code_col = 'site',
                         data_cols =  c('Precipitationmm'='precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = '-9999',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_14 <- function(network, domain, prodname_ms, site_code,
                         component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    if(!component == 'El Verde Field Station Rainfall in Millimeters (1975-1989)'){
        p_name <- 'RAINFALL..MM.'
    } else{
        p_name <- 'Rainfall.mm.'
    }
    
      d <- read.csv(rawfile,
                    colClasses = 'character')

    d <- d %>%
      mutate(site = 'El_Verde') %>%
      mutate(month = str_split_fixed(DATE, '/', n = Inf)[,1],
             day = str_split_fixed(DATE, '/', n = Inf)[,2],
             year = str_split_fixed(DATE, '/', n = Inf)[,3]) %>%
      mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day)) %>%
      mutate(year = ifelse(nchar(year) >= 5, str_split_fixed(year, ' ', n = Inf)[,1], year))


    precip_name_def <- c('precipitation')
    names(precip_name_def) <- p_name

    if('Field.Comments' %in% names(d)){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('day' = '%d',
                                                  'month' = '%m',
                                                  'year' = '%Y'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'site',
                             data_cols =  precip_name_def,
                             summary_flagcols = 'Field.Comments',
                             data_col_pattern = '#V#',
                             set_to_NA = '-9999',
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list('Field.Comments' = c('Cuarentena por Pandemia COVID 19')),
                                summary_flags_to_drop = list('Field.Comments' = c('BAD', 
                                                                                  'DATA NOT COLLECTED')),
                                varflag_col_pattern = NA)
    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('day' = '%d',
                                                  'month' = '%m',
                                                  'year' = '%Y'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'site',
                             data_cols =  precip_name_def,
                             data_col_pattern = '#V#',
                             set_to_NA = '-9999',
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)
    }

    return(d)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_156 <- function(network, domain, prodname_ms, site_code,
                          component){

    #info on luqillo data
    #https://www.sas.upenn.edu/lczodata/content/quebrada-three-bisley-q3

    if(prodname_ms == 'stream_chemistry__156'){

        rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                       n = network,
                       d = domain,
                       p = prodname_ms,
                       s = site_code,
                       c = component)

        d <- read.csv(rawfile,
                      colClasses = 'character') %>%
          select(DATE, meanTempq1, meanTempq2, meanTempq3) %>%
          pivot_longer(cols = c(meanTempq1, meanTempq2, meanTempq3)) %>%
          mutate(month = str_split_fixed(DATE, '/', n = Inf)[,1],
                 day = str_split_fixed(DATE, '/', n = Inf)[,2],
                 year = str_split_fixed(DATE, '/', n = Inf)[,3]) %>%
          mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day)) %>%
          mutate(year = ifelse(nchar(year) >= 5, str_split_fixed(year, ' ', n = Inf)[,1], year))


        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('day' = '%d',
                                                  'month' = '%m',
                                                  'year' = '%Y'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'name',
                             alt_site_code = list('Q1' = 'meanTempq1',
                                                  'Q2' = 'meanTempq2',
                                                  'Q3' = 'meanTempq3'),
                             data_cols =  c('value'='temp'),
                             data_col_pattern = '#V#',
                             set_to_NA = '-9999',
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                              varflag_col_pattern = NA)
    } else{

        rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                       n = network,
                       d = domain,
                       p = prodname_ms,
                       s = site_code,
                       c = component)

        #Converting from mm/day to leters/s
        d <- read.csv(rawfile,
                      colClasses = 'character') %>%
          select(DATE, Q1, Q2, Q3) %>%
          mutate(Q1 = (((as.numeric(Q1)/1000000)*0.067)*1000000000000)/86400,
                 Q2 = (((as.numeric(Q2)/1000000)*0.0634)*1000000000000)/86400,
                 Q3 = (((as.numeric(Q3)/1000000)*0.35)*1000000000000)/86400) %>%
          mutate(Q1 = as.character(Q1),
                 Q2 = as.character(Q2),
                 Q3 = as.character(Q3)) %>%
          pivot_longer(cols = c(Q1, Q2, Q3)) %>%
          mutate(month = str_split_fixed(DATE, '/', n = Inf)[,1],
                 day = str_split_fixed(DATE, '/', n = Inf)[,2],
                 year = str_split_fixed(DATE, '/', n = Inf)[,3]) %>%
          mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day)) %>%
          mutate(year = ifelse(nchar(year) >= 5, str_split_fixed(year, ' ', n = Inf)[,1], year))

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('day' = '%d',
                                                  'month' = '%m',
                                                  'year' = '%Y'),
                             datetime_tz = 'Etc/GMT-4',
                             site_code_col = 'name',
                             data_cols =  c('value'='discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = '-9999',
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)
    }

    return(d)
}

#discharge: STATUS=PAUSED
#. handle_errors
process_1_182 <- function(network, domain, prodname_ms, site_code,
                          component){

    # There seems to be something wrong with this data product. around 2010
    # they switch from one rating curve to another and it seems that the
    # values are too/unreal
      rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code,
                     c = component)

      d <- read.csv(rawfile,
                    colClasses = 'character') %>%
        mutate(month = str_split_fixed(datetime, '/', n = Inf)[,1],
               day = str_split_fixed(datetime, '/', n = Inf)[,2],
               year = str_split_fixed(datetime, '/', n = Inf)[,3]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day)) %>%
        mutate(time =  str_split_fixed(year, ' ', n = Inf)[,2],
               year =  str_split_fixed(year, ' ', n = Inf)[,1]) %>%
        mutate(q = ifelse(Discharge.FNS.formula.discharge..m3.sec....1.095...1.585...stage...0.578...stage...stage == '',
                          discharge.McDowell...0.0005...stage..8.8846..35.314666,
                          Discharge.FNS.formula.discharge..m3.sec....1.095...1.585...stage...0.578...stage...stage)) %>%
        mutate(code = ifelse(Notes == '', Discharge.QA.Code.from.DB, Notes)) %>%
        mutate(site = 'QP')

      d <- ms_read_raw_csv(preprocessed_tibble = d,
                           datetime_cols = list('day' = '%d',
                                                'month' = '%m',
                                                'year' = '%Y',
                                                'time' = '%H:%M'),
                           datetime_tz = 'Etc/GMT-4',
                           site_code_col = 'site',
                           data_cols =  c('q'='discharge'),
                           summary_flagcols = 'code',
                           data_col_pattern = '#V#',
                           set_to_NA = '-9999',
                           is_sensor = TRUE)

      d <- ms_cast_and_reflag(d,
                              varflag_col_pattern = NA,
                              summary_flags_clean = list('code' = c('Checked correct',
                                                                    'Good')),
                              summary_flags_dirty = list('code' = c('Sensor error',
                                                                    'Bad')))

    #m3 to liters
    d <- d %>%
        mutate(val = val*1000)

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
