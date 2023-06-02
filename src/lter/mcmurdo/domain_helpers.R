retrieve_mcmurdo <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}


munge_mcmurdo_discharge <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # if(site_code %in% c('BOHNER', 'HUEY', 'SANTAFE', 'PRISCU', 'LAWSON', 'HOUSE')) {
    if(site_code %in% c('SANTAFE', 'PRISCU', 'HOUSE')) {

        d <- read.csv(rawfile, colClasses = 'character', skip = 36)
    } else {
        d <- read.csv(rawfile, colClasses = 'character')
    }

    if(prodcode_from_prodname_ms(prodname_ms) %in% c(9010, 9011, 9013, 9015, 9018,
                                                     9016, 9024, 9023, 9017)){
        d <- d %>%
            mutate(month = str_split_fixed(DATE_TIME, '/', n = Inf)[,1],
                   day = str_split_fixed(DATE_TIME, '/', n = Inf)[,2],
                   year_time = str_split_fixed(DATE_TIME, '/', n = Inf)[,3]) %>%
            mutate(year = str_split_fixed(year_time, ' ', n = Inf)[,1],
                   time = str_split_fixed(year_time, ' ', n = Inf)[,2]) %>%
            mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
                   month = ifelse(nchar(month) == 1, paste0(0, month), month)) %>%
            mutate(year = str_replace(year, ' ', '')) %>%
            mutate(date = paste(day, month, year, sep = '-'))
    } else{
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
    }

    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_')
    colnames(d) <- col_name

    code <- str_split_fixed(prodname_ms, '__', n=2)[1,2]

    if(grepl('discharge', prodname_ms)) {

        if(code %in% c(9021, 9002)) {

            d <- ms_read_raw_csv(preprocessed_tibble = d,
                                 datetime_cols = list('date' = '%d-%m-%Y',
                                                      'time' = '%H:%M'),
                                 datetime_tz = 'Antarctica/McMurdo',
                                 site_code_col = 'STRMGAGEID',
                                 data_cols =  c('DISCHARGE_RATE' = 'discharge'),
                                 summary_flagcols = 'DIS_COMMENTS',
                                 data_col_pattern = '#V#',
                                 is_sensor = TRUE,
                                 set_to_NA = '',
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
                                 data_cols =  c('DISCHARGE_RATE' = 'discharge'),
                                 summary_flagcols = 'DISCHARGE_QLTY',
                                 data_col_pattern = '#V#',
                                 is_sensor = TRUE,
                                 set_to_NA = '',
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
        }

    } else {

        if(code %in% c(9014, 9003, 9007, 9009, 9010, 9011, 9013, 9021, 9022, 9018,
                       9015, 9027, 9016, 9029, 9024, 9023)) {

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
                                 set_to_NA = '',
                                 sampling_type = 'I')

            d <- ms_cast_and_reflag(d,
                                    variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                             'Qmu','WTmu','SCmu','WTsed',
                                                             'SCsed','Qsed'),
                                    variable_flags_to_drop = c('UNUSABLE'),
                                    variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                             'fair', 'Good', 'good'))
        } else {

            d <- ms_read_raw_csv(preprocessed_tibble = d,
                                 datetime_cols = list('date' = '%d-%m-%Y',
                                                      'time' = '%H:%M'),
                                 datetime_tz = 'Antarctica/McMurdo',
                                 site_code_col = 'STRMGAGEID',
                                 data_cols =  c('WATER_TEMP' = 'temp',
                                                'CONDUCTANCE' = 'spCond'),
                                 data_col_pattern = '#V#',
                                 var_flagcol_pattern = '#V#_QLTY',
                                 is_sensor = TRUE,
                                 set_to_NA = '',
                                 sampling_type = 'I')

            d <- ms_cast_and_reflag(d,
                                    variable_flags_dirty = c('POOR', 'Poor', 'poor','poor',
                                                             'Qmu','WTmu','SCmu','WTsed',
                                                             'SCsed','Qsed'),
                                    variable_flags_to_drop = c('UNUSABLE'),
                                    variable_flags_clean = c('GOOD', 'FAIR', 'Fair',
                                                             'fair', 'Good', 'good'))

        }
    }

    return(d)
}

