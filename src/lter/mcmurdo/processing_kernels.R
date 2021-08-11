source('src/lter/mcmurdo/domain_helpers.R')

#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_9001 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9030 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9002 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9003 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9007 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9009 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9010 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9011 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9013 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_9014 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9015 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9018 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9022 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9021 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9027 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9016 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9029 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9024 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9023 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9017 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_24 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_20 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_21 <- retrieve_mcmurdo

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_9030 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9002 <- function(network, domain, prodname_ms, site_code,
                           component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
               day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
               year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
        mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
               time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month),
               year = ifelse(year > 50, paste0(19, year), paste(20, year))) %>%
        mutate(year = str_replace(year, ' ', '')) %>%
        mutate(date = paste(day, month, year, sep = '-'))

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    if(grepl('discharge', prodname_ms)) {


            d <- ms_read_raw_csv(preprocessed_tibble = d,
                                 datetime_cols = list('date' = '%d-%m-%Y',
                                                      'time' = '%H:%M'),
                                 datetime_tz = 'Antarctica/McMurdo',
                                 site_code_col = 'STRMGAGEID',
                                 data_cols =  c('DISCHARGE_RATE' = 'discharge'),
                                 summary_flagcols = 'DISCHARGE_QLTY',
                                 data_col_pattern = '#V#',
                                 is_sensor = TRUE,
                                 sampling_type = 'I')

            d <- ms_cast_and_reflag(d,
                                    summary_flags_dirty = list('DISCHARGE_QLTY' = c('poor',
                                                                                    'Qmu',
                                                                                    'Qsed')),
                                    summary_flags_to_drop = list('DISCHARGE_QLTY' = 'UNUSABLE'),
                                    varflag_col_pattern = NA)

    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('WATER_TEMP' = 'temp',
                                            'CONDUCTIVITY' = 'spCond'),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#_QLTY',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                         'Qmu','WTmu','SCmu','WTsed',
                                                         'SCsed','Qsed'),
                                variable_flags_to_drop = c('UNUSABLE'),
                                variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                         'fair', 'Good', 'good'))
    }

    return(d)
}

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9003 <- function(network, domain, prodname_ms, site_code,
                           component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
               day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
               year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
        mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
               time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month),
               year = ifelse(year > 50, paste0(19, year), paste(20, year))) %>%
        mutate(year = str_replace(year, ' ', '')) %>%
        mutate(date = paste(day, month, year, sep = '-'))

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    if(grepl('discharge', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('DSCHRGE_RATE' = 'discharge'),
                             summary_flagcols = 'DSCHRGE_QLTY',
                             data_col_pattern = '#V#',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list('DSCHRGE_QLTY' = c('poor',
                                                                              'Qmu',
                                                                              'Qsed')),
                                summary_flags_to_drop = list('DSCHRGE_QLTY' = 'UNUSABLE'),
                                varflag_col_pattern = NA)

    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('WATER_TEMP' = 'temp',
                                            'CONDUCTIVITY' = 'spCond'),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#_QLTY',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                         'Qmu','WTmu','SCmu','WTsed',
                                                         'SCsed','Qsed'),
                                variable_flags_to_drop = c('UNUSABLE'),
                                variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                         'fair', 'Good', 'good'))
    }

    return(d)
}

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9007 <- function(network, domain, prodname_ms, site_code,
                           component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
               day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
               year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
        mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
               time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month),
               year = ifelse(year > 50, paste0(19, year), paste(20, year))) %>%
        mutate(year = str_replace(year, ' ', '')) %>%
        mutate(date = paste(day, month, year, sep = '-'))

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    if(grepl('discharge', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('DSCHRGE_RATE' = 'discharge'),
                             summary_flagcols = 'DSCHRGE_QLTY',
                             data_col_pattern = '#V#',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list('DSCHRGE_QLTY' = c('poor',
                                                                              'Qmu',
                                                                              'Qsed')),
                                summary_flags_to_drop = list('DSCHRGE_QLTY' = 'UNUSABLE'),
                                varflag_col_pattern = NA)

    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('WATER_TEMP' = 'temp',
                                            'CONDUCTIVITY' = 'spCond'),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#_QLTY',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                         'Qmu','WTmu','SCmu','WTsed',
                                                         'SCsed','Qsed'),
                                variable_flags_to_drop = c('UNUSABLE'),
                                variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                         'fair', 'Good', 'good'))
    }

    return(d)
}

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9009 <- function(network, domain, prodname_ms, site_code,
                           component) {

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
               day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
               year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
        mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
               time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month),
               year = ifelse(year > 50, paste0(19, year), paste(20, year))) %>%
        mutate(year = str_replace(year, ' ', '')) %>%
        mutate(date = paste(day, month, year, sep = '-'))

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    d <- d %>%
        rename(CONDUCTIVITY_QLTY = CONDUCTIVITY__QLTY)

    if(grepl('discharge', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('DISCHARGE_RATE' = 'discharge'),
                             summary_flagcols = 'DISCHARGE_QLTY',
                             data_col_pattern = '#V#',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list('DISCHARGE_QLTY' = c('poor',
                                                                                'Qmu',
                                                                                'WTmu',
                                                                                'SCmu',
                                                                                'WTsed',
                                                                                'SCsed',
                                                                                'Qsed')),
                                summary_flags_to_drop = list('DISCHARGE_QLTY' = 'UNUSABLE'),
                                varflag_col_pattern = NA)

    } else {


        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('WATER_TEMP' = 'temp',
                                            'CONDUCTIVITY' = 'spCond'),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#_QLTY',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                         'Qmu','WTmu','SCmu','WTsed',
                                                         'SCsed','Qsed'),
                                variable_flags_to_drop = c('UNUSABLE'),
                                variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                         'fair', 'Good', 'good'))
    }

    return(d)
}

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9010 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9011 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9013 <- munge_mcmurdo_discharge

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_9014 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9015 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9018 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9022 <- function(network, domain, prodname_ms, site_code,
                           component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
               day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
               year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
        mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
               time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month),
               year = ifelse(year > 50, paste0(19, year), paste(20, year))) %>%
        mutate(year = str_replace(year, ' ', '')) %>%
        mutate(date = paste(day, month, year, sep = '-'))

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    if(grepl('discharge', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('DISCHARGE_RATE' = 'discharge'),
                             summary_flagcols = 'DISCHG_COM',
                             data_col_pattern = '#V#',
                             is_sensor = TRUE,
                             sampling_type = 'I')

            d <- ms_cast_and_reflag(d,
                                    summary_flags_dirty = list('DISCHG_COM' = c('poor',
                                                                                    'Qmu',
                                                                                    'Qsed')),
                                    summary_flags_to_drop = list('DISCHG_COM' = 'UNUSABLE'),
                                    varflag_col_pattern = NA)

    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('WATER_TEMP' = 'temp',
                                            'CONDUCTIVITY' = 'spCond'),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#_QLTY',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                         'Qmu','WTmu','SCmu','WTsed',
                                                         'SCsed','Qsed'),
                                variable_flags_to_drop = c('UNUSABLE'),
                                variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                         'fair', 'Good', 'good'))
    }

    return(d)
}

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9021 <- function(network, domain, prodname_ms, site_code,
                           component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
               day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
               year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
        mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
            time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month),
               year = ifelse(year > 50, paste0(19, year), paste(20, year))) %>%
        mutate(year = str_replace(year, ' ', '')) %>%
        mutate(date = paste(day, month, year, sep = '-'))

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    code <- str_split_fixed(prodname_ms, '__', n=2)[1,2]

    if(grepl('discharge', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('DISCHARGE_RATE' = 'discharge'),
                             summary_flagcols = 'DIS_COMMENTS',
                             data_col_pattern = '#V#',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list('DIS_COMMENTS' = c('poor',
                                                                              'Qmu',
                                                                              'Qsed')),
                                summary_flags_to_drop = list('DIS_COMMENTS' = 'UNUSABLE'),
                                varflag_col_pattern = NA)

    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('WATER_TEMP' = 'temp',
                                            'CONDUCTIVITY' = 'spCond'),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#_QUALITY',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                         'Qmu','WTmu','SCmu','WTsed',
                                                         'SCsed','Qsed'),
                                variable_flags_to_drop = c('UNUSABLE'),
                                variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                             'fair', 'Good', 'good'))
    }

    return(d)
}

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9027 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9016 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9029 <- function(network, domain, prodname_ms, site_code,
                           component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code,
                   c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
               day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
               year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
        mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
               time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month),
               year = ifelse(year > 50, paste0(19, year), paste(20, year))) %>%
        mutate(year = str_replace(year, ' ', '')) %>%
        mutate(date = paste(day, month, year, sep = '-'))

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    code <- str_split_fixed(prodname_ms, '__', n=2)[1,2]

    if(grepl('discharge', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('DISCHARGE_RATE' = 'discharge'),
                             summary_flagcols = 'DISCHARGE_QLTY',
                             data_col_pattern = '#V#',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list('DISCHARGE_QLTY' = c('poor',
                                                                              'Qmu',
                                                                              'Qsed')),
                                summary_flags_to_drop = list('DISCHARGE_QLTY' = 'UNUSABLE'),
                                varflag_col_pattern = NA)
    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'STRMGAGEID',
                             data_cols =  c('WATER_TEMP' = 'temp'),
                             data_col_pattern = '#V#',
                             summary_flagcols = 'WATER_TEMP_QLTY',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list(WATER_TEMP_QLTY =
                                                               c('POOR', 'Poor', 'poor','poor',
                                                                 'Qmu','WTmu','SCmu','WTsed',
                                                                 'SCsed','Qsed')),
                                summary_flags_to_drop = list( WATER_TEMP_QLTY = 'UNUSABLE'),
                                summary_flags_clean = list(WATER_TEMP_QLTY =
                                                               c('GOOD', 'FAIR', 'Fair',
                                                                 'fair', 'Good', 'good')),
                                varflag_col_pattern = NA)
        }

    return(d)
}


#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9024 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9023 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9017 <- munge_mcmurdo_discharge

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_24 <- function(network, domain, prodname_ms, site_code,
                        component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)


    d <- read.csv(rawfile, colClasses = 'character', skip = 26) %>%
        rename(DOC=DOC..mg.L.) %>%
        filter(STRMGAGEID != '',
               STRMGAGEID != 'garwood',
               STRMGAGEID != 'miers',
               STRMGAGEID != 'delta_upper',
               STRMGAGEID != 'lizotte_mouth',
               STRMGAGEID != 'vguerard_lower')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE_TIME' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_code_col = 'STRMGAGEID',
                         data_cols =  c('DOC'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- filter_single_samp_sites(d)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_20 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)


    d <- read.csv(rawfile, colClasses = 'character', skip = 26) %>%
        filter(STRMGAGEID != '',
               STRMGAGEID != 'garwood',
               STRMGAGEID != 'miers',
               STRMGAGEID != 'delta_upper',
               STRMGAGEID != 'lizotte_mouth',
               STRMGAGEID != 'vguerard_lower') %>%
        rename(Li = Li..mg.L.,
               Na = Na..mg.L.,
               K = K..mg.L.,
               Mg = Mg..mg.L.,
               Ca = Ca..mg.L.,
               'F' = F..mg.L.,
               Cl = Cl..mg.L.,
               Br = Br..mg.L.,
               SO4 = SO4..mg.L.,
               Si = Si..mg.L.)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE_TIME' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_code_col = 'STRMGAGEID',
                         data_cols =  c('Li' = 'Li',
                                        'Na' = 'Na',
                                        'K' = 'K',
                                        'Mg' = 'Mg',
                                        'Ca' = 'Ca',
                                        'F' = 'F',
                                        'Cl' = 'Cl',
                                        'Br' = 'Br',
                                        'SO4' = 'SO4',
                                        'Si' = 'Si'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#.COMMENTS',
                         is_sensor = FALSE)

    dirty_string <- c('Not Enough water for the sample to be run fully',
                      'Not Quantified', 'ND', 'Br not quantified <0.04mg/',
                      'value outside calibration limit and not diluted to be within calibration limit',
                      'Br not quantified', 'not quantified', 'Not Quantified',
                      'not detected', 'ND < 0.04 mg/L', 'Br level below detection limit',
                      'F level below detection limit', 'F not quantified', '< 0.1 mg/L',
                      ' F not quantified <0.02mg/L', 'no detect', 'Ca not quantified',
                      'K not quantified', ' Li not quantified <0.001mg/L', 'Li not quantified',
                      'Li level below detection limit', 'ND <0.001')

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = dirty_string,
                            variable_flags_to_drop = 'REMOVE')

    d <- filter_single_samp_sites(d)

    d <- ms_conversions(d,
                        convert_units_from = c('SO4' = 'mg/l'),
                        convert_units_to = c('SO4' = 'mg/l'))

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_21 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)


    d <- read.csv(rawfile, colClasses = 'character', skip = 26) %>%
        filter(STRMGAGEID != '',
               STRMGAGEID != 'garwood',
               STRMGAGEID != 'miers',
               STRMGAGEID != 'delta_upper',
               STRMGAGEID != 'lizotte_mouth',
               STRMGAGEID != 'vguerard_lower',
               STRMGAGEID != 'uvg_f21') %>%
        rename(N.NO3 = N.NO3..ug.L.,
               N.NO2 = N.NO2..ug.L.,
               N.NH4 = N.NH4..ug.L.,
               SRP = SRP..ug.L.)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE_TIME' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_code_col = 'STRMGAGEID',
                         data_cols =  c('N.NO3' = 'NO3_N',
                                        'N.NO2' = 'NO2_N',
                                        'N.NH4' = 'NH4_N',
                                        'SRP' = 'SRP'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#.COMMENTS',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_clean = '',
                            variable_flags_to_drop = 'REMOVE')

    d <- filter_single_samp_sites(d)

    d <- ms_conversions(d,
                        convert_units_from = c('NO3_N' = 'ug/l',
                                               'NO2_N' = 'ug/l',
                                               'NH4_N' = 'ug/l',
                                               'SRP' = 'ug/l'),
                        convert_units_to = c('NO3_N' = 'mg/l',
                                             'NO2_N' = 'mg/l',
                                             'NH4_N' = 'mg/l',
                                             'SRP' = 'mg/l'))

    return(d)
}

#derive kernels ####

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- stream_gauge_from_site_data

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__9002',
                                            'discharge__9003',
                                            'discharge__9007',
                                            'discharge__9009',
                                            'discharge__9010',
                                            'discharge__9011',
                                            'discharge__9013',
                                            'discharge__9015',
                                            'discharge__9016',
                                            'discharge__9017',
                                            'discharge__9018',
                                            'discharge__9021',
                                            'discharge__9022',
                                            'discharge__9023',
                                            'discharge__9024',
                                            'discharge__9027',
                                            'discharge__9029',
                                            'discharge__9030'))

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__24',
                                            'stream_chemistry__20',
                                            'stream_chemistry__21',
                                            'stream_chemistry__9002',
                                            'stream_chemistry__9003',
                                            'stream_chemistry__9007',
                                            'stream_chemistry__9009',
                                            'stream_chemistry__9010',
                                            'stream_chemistry__9011',
                                            'stream_chemistry__9013',
                                            'stream_chemistry__9014',
                                            'stream_chemistry__9015',
                                            'stream_chemistry__9016',
                                            'stream_chemistry__9017',
                                            'stream_chemistry__9018',
                                            'stream_chemistry__9021',
                                            'stream_chemistry__9022',
                                            'stream_chemistry__9023',
                                            'stream_chemistry__9024',
                                            'stream_chemistry__9027',
                                            'stream_chemistry__9029',
                                            'stream_chemistry__9030'))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_stream_flux

