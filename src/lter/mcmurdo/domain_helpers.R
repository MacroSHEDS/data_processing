retrieve_mcmurdo <- function(set_details, network, domain) {
    
    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')
    
    return()
}


munge_mcmurdo_discharge <- function(network, domain, prodname_ms, site_name,
                                          component){
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)
    
    if(site_name %in% c('BOHNER', 'HUEY', 'SANTAFE', 'PRISCU', 'LAWSON', 'HOUSE')) {
        
        d <- read.csv(rawfile, colClasses = 'character', skip = 36)
    } else {
        d <- read.csv(rawfile, colClasses = 'character')
    }
    
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
    
    if(grepl('discharge', prodname_ms)) {
        
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_name_col = 'STRMGAGEID',
                             data_cols =  c('DISCHARGE.RATE' = 'discharge'),
                             summary_flagcols = 'DISCHARGE.QLTY',
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)
        
        d <- ms_cast_and_reflag(d,
                                summary_flags_dirty = list('DISCHARGE.QLTY' = c('poor',
                                                                                'Qmu',
                                                                                'Qsed')),
                                summary_flags_to_drop = list('DISCHARGE.QLTY' = 'DROP'),
                                varflag_col_pattern = NA)
        
    } else {
        
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%d-%m-%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_name_col = 'STRMGAGEID',
                             data_cols =  c('WATER.TEMP' = 'temp',
                                            'CONDUCTANCE' = 'spCond'),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#.QLTY',
                             is_sensor = TRUE)
        
        d <- ms_cast_and_reflag(d,
                                variable_flags_dirty = c('POOR', 'Poor', 'poor'),
                                variable_flags_clean = c('GOOD', 'FAIR', 'Fair', 
                                                         'fair', 'Good', 'good'))
    }
    
    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)
    
    d <- synchronize_timestep(d,
                              desired_interval = '1 day', #set to '15 min' when we have server
                              impute_limit = 30)
    
    d <- apply_detection_limit_t(d, network, domain, prodname_ms) 
}
    