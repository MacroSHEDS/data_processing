#retrieval kernels ####

#stream_chemistry_luquillo: STATUS=READY
#. handle_errors
process_0_20 <- function(set_details, network, domain){
    
  raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                       wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
                       s = set_details$site_name)
  
  dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
  
  download.file(url = set_details$url,
                destfile = glue(raw_data_dest, '/', set_details$component, '.csv'),
                cacheOK = FALSE, method = 'curl')
  
 # file.rename( glue(raw_data_dest, '/', set_details$component),  glue(raw_data_dest, '/', set_details$component, '.csv'))
  
  return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_156 <- function(set_details, network, domain) {
    
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
                         s = set_details$site_name)
    
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    
    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest, '/', set_details$component, '.csv'),
                  cacheOK = FALSE, method = 'curl')
}

#munge kernels ####

#stream_chemistry_luquillo: STATUS=READY
#. handle_errors
process_1_20 <- function(network, domain, prodname_ms, site_name,
                        component){
    
    if(component == 'All Sites Basic Field Stream Chemistry Data'){
        loginfo('Skipping redundant download', logger=logger_module)
        return(generate_blacklist_indicator())
    }
    
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)
  
    raw <- read_csv(rawfile)
  
    d <- raw %>%
        mutate(Sample_Time = ifelse(Sample_Time == '.', 1200, Sample_Time)) %>%
        mutate(num_t = nchar(Sample_Time)) %>%
        mutate(time = case_when(num_t == 1 ~ paste0('010', Sample_Time),
                                num_t == 2 ~ paste0('01', Sample_Time),
                                num_t == 3 ~ paste0('0', Sample_Time),
                                num_t == 4 ~ as.character(Sample_Time),
                                is.na(Sample_Time) ~ '1200')) %>%
        select(-num_t, -Sample_Time)
  
    # add TDP thiamin diphosphate to 
  
    d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                            datetime_cols = c(time = '%H%M',
                                              Sample_Date = '%m/%d/%Y'),
                            datetime_tz = 'Etc/GMT-4',
                            site_name_col = 'Sample_ID',
                            data_cols = c(Temp = 'temp',
                                          'pH' = 'pH',
                                          Cond = 'spCond',
                                          Cl = 'Cl',
                                          NO3 = 'NO3_N',
                                          'SO4-S' = 'SO4_S',
                                          'PO4-P' = 'PO4_S',
                                          Na = 'Na',
                                          K = 'K',
                                          Mg = 'Mg',
                                          Ca = 'Ca',
                                          'NH4-N' = 'NH4_N',
                                          DOC = 'DOC',
                                          DIC = 'DIC',
                                          TDN = 'TDN',
                                          SiO2 = 'SiO2',
                                          DON = 'DON',
                                          TSS = 'TSS'),
                            set_to_NA = c('-9999', '9999.00'),
                            data_col_pattern = '#V#',
                            is_sensor = FALSE))
    
    d <- ue(ms_cast_and_reflag(d,
                               varflag_col_pattern = NA))
  
    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))
    
    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))
  
    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
    return(d)
}


#discharge: STATUS=READY
#. handle_errors
process_1_156 <- function(network, domain, prodname_ms, site_name,
                         component){
    
    if(component %in% c('Daily Mean Temperature and Streamflow data from Bisley Quebrada 1, 2, 3',
                        'Daily Temperature data from Bisley Quebrada 1, 2, 3')) {
        loginfo('Skipping redundant download', logger=logger_module)
        return(generate_blacklist_indicator())
    }
    
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)
    
    raw <- read_csv(rawfile)
    
    d <- raw %>%
        mutate(Sample_Time = ifelse(Sample_Time == '.', 1200, Sample_Time)) %>%
        mutate(num_t = nchar(Sample_Time)) %>%
        mutate(time = case_when(num_t == 1 ~ paste0('010', Sample_Time),
                                num_t == 2 ~ paste0('01', Sample_Time),
                                num_t == 3 ~ paste0('0', Sample_Time),
                                num_t == 4 ~ as.character(Sample_Time),
                                is.na(Sample_Time) ~ '1200')) %>%
        select(-num_t, -Sample_Time)
    
    # add TDP thiamin diphosphate to 
    
    d <- ue(ms_read_raw_csv(preprocessed_tibble = d,
                            datetime_cols = c(time = '%H%M',
                                              Sample_Date = '%m/%d/%Y'),
                            datetime_tz = 'Etc/GMT-4',
                            site_name_col = 'Sample_ID',
                            data_cols = c(Temp = 'temp',
                                          'pH' = 'pH',
                                          Cond = 'spCond',
                                          Cl = 'Cl',
                                          NO3 = 'NO3_N',
                                          'SO4-S' = 'SO4_S',
                                          'PO4-P' = 'PO4_S',
                                          Na = 'Na',
                                          K = 'K',
                                          Mg = 'Mg',
                                          Ca = 'Ca',
                                          'NH4-N' = 'NH4_N',
                                          DOC = 'DOC',
                                          DIC = 'DIC',
                                          TDN = 'TDN',
                                          SiO2 = 'SiO2',
                                          DON = 'DON',
                                          TSS = 'TSS'),
                            set_to_NA = c('-9999', '9999.00'),
                            data_col_pattern = '#V#',
                            is_sensor = FALSE))
    
    d <- ue(ms_cast_and_reflag(d,
                               varflag_col_pattern = NA))
    
    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))
    
    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))
    
    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
    
    return(d)
}
