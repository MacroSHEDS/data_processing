retrieve_santa_barbara <- function(set_details, network, domain) {
    
    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')
    
    return()
}

munge_santa_barbara_precip <- function(network, domain, prodname_ms, site_name,
         component){
    
    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)
    
    sbc_gauge <- grepl('SBC ', component, fixed = T)
    fc_gauge <- grepl('SBCoFC ', component, fixed = T)
    
    if(sbc_gauge) {
        site <- str_split_fixed(component, 'Precipitation SBC', n = Inf)
    }
    
    if(fc_gauge) {
        site <- str_split_fixed(component, 'Precipitation SBCoFC', n = Inf)
    }
    
    if(component == 'UCSB 200 daily precipitation, 1951-ongoing') {
        site <- 'UCSB200'
    } else {
        
        site <- str_split_fixed(site[,2], ',', n = Inf) 
        site <- str_split_fixed(site[1,1], ' ', n = Inf)
        site <- paste(site[1,],collapse="")
    }
    
    d <- read.csv(rawfile1, colClasses = "character") %>%
        mutate(site_name = !!site)
    
    if(sbc_gauge && component != 'Precipitation SBC GV201, all years') {
        
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('timestamp_UTC' = '%Y-%m-%dT%H:%M'),
                             datetime_tz = 'UTC',
                             site_name_col = 'site_name',
                             data_cols =  c('precipitation_mm' = 'precipitation'),
                             data_col_pattern = '#V#',
                             set_to_NA = '-999',
                             is_sensor = TRUE)
    } else {
        
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('timestamp_utc' = '%Y-%m-%dT%H:%M'),
                             datetime_tz = 'UTC',
                             site_name_col = 'site_name',
                             data_cols =  c('precipitation_mm' = 'precipitation'),
                             data_col_pattern = '#V#',
                             set_to_NA = '-999',
                             is_sensor = TRUE)
    }
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)
    
    d <- synchronize_timestep(d,
                              desired_interval = '1 day', #set to '15 min' when we have server
                              impute_limit = 30)
    
    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
    
    return(d)
}

munge_santa_barbara_discharge <- function(network, domain, prodname_ms, site_name,
                           component){
    
    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)
    
    site <- str_split_fixed(component, ' ', n = Inf)[1,]
    site <- str_split_fixed(site[3], ',', n = Inf)[1,1]
    
    d <- read.csv(rawfile1, colClasses = "character") %>%
        mutate(site_name = !!site)
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('timestamp_utc' = '%Y-%m-%dT%H:%M'),
                         datetime_tz = 'UTC',
                         site_name_col = 'site_name',
                         data_cols =  c('discharge_lps' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = '-999',
                         is_sensor = TRUE)
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)
    
    d <- synchronize_timestep(d,
                              desired_interval = '1 day', #set to '15 min' when we have server
                              impute_limit = 30)
    
    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
    
    return(d)
}