retrieve_plum <- function(set_details, network, domain) {
    
    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)
    
    return()
}

fish_brook <- c(109, 110)
cart_creek <- c(111, 112,113, 148, 149, 150, 151, 171, 172, 225, 226, 388, 
                433, 437, 389, 434, 391, 435, 391, 438, 436, 390, 439, 509,
                525, 526, 510, 527)
saw_mill_brook <- c(117, 118, 152, 153, 154, 155, 176, 177, 178, 227, 228, 
                    396, 449, 397, 450, 398, 485, 399, 486, 515, 516)
bear_meadow <- c(156, 157, 158, 173, 174, 229, 230, 392, 393, 394, 395)
upper_ipswich <- c(114, 115, 116)
parker_dam <- c(487, 488, 489, 490, 506, 507, 508)
ipswich_dam <- c(444, 445, 446, 447, 513, 514, 448, 532, 533, 534)
governors_academy <- c(68, 69, 70, 140, 141, 142, 143)
mbl_marshview <- c(67, 179, 180, 181, 239, 359, 385, 417, 423, 496, 542)

munge_plum_combined <- function(network, domain, prodname_ms, site_name,
                                component){
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
    
    code <- prodcode_from_prodname_ms(prodname_ms)
    
    site <- case_when(code %in% fish_brook ~ 'fish_brook',
              code %in% cart_creek ~ 'cart_creek',
              code %in% saw_mill_brook ~ 'saw_mill_brook',
              code %in% bear_meadow ~ 'bear_meadow', 
              code %in% upper_ipswich ~ 'upper_ipswich',
              code %in% parker_dam ~ 'parker_dam', 
              code %in% ipswich_dam ~ 'ipswich_dam')
    
    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = !!site)
    
    if(grepl('stream_chemistry', prodname_ms)) {
        
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date' = '%Y-%m-%d',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'US/Eastern',
                             site_name_col = 'site_name',
                             data_cols =  c('Temp' = 'temp',
                                            'SpCond' = 'spCond',
                                            'DOConc' = 'DO', 
                                            'pH' = 'pH'),
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)
        
        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)
        
        d <- ms_conversions(d, 
                            convert_units_from = c('spCond' = 'mS/cm'),
                            convert_units_to = c('spCond' = 'uS/cm'))
    
    } else {
        
        if('Flow' %in% names(d)) {
            
            d <- d %>%
                rename(Discharge = Flow)
        }
            
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date' = '%Y-%m-%d',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'US/Eastern',
                             site_name_col = 'site_name',
                             data_cols =  c('Discharge' = 'discharge'),
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)
        
        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)
        
        }
    #     
    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    # 
    # d <- synchronize_timestep(d)
    # 
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms) 
    
    return(d)
}

munge_plum_temp_q <- function(network, domain, prodname_ms, site_name,
                                component){
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
    
    code <- prodcode_from_prodname_ms(prodname_ms)
    
    site <- case_when(code %in% fish_brook ~ 'fish_brook',
                      code %in% cart_creek ~ 'cart_creek',
                      code %in% saw_mill_brook ~ 'saw_mill_brook',
                      code %in% bear_meadow ~ 'bear_meadow',
                      code %in% upper_ipswich ~ 'upper_ipswich',
                      code %in% parker_dam ~ 'parker_dam',
                      code %in% ipswich_dam ~ 'ipswich_dam')
    
    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = !!site)
    
    if(grepl('stream_chemistry', prodname_ms)) {
        
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date' = '%Y-%m-%d',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'US/Eastern',
                             site_name_col = 'site_name',
                             data_cols =  c('Temp' = 'temp'),
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)
        
    } else {
        
        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date' = '%Y-%m-%d',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'US/Eastern',
                             site_name_col = 'site_name',
                             data_cols =  c('Discharge' = 'discharge'),
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)
        
    }
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    # 
    # d <- synchronize_timestep(d)
    # 
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms) 
    
    return(d)
}

munge_plum_temp_cond <- function(network, domain, prodname_ms, site_name,
                              component) {
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
    
    code <- prodcode_from_prodname_ms(prodname_ms)
    
    site <- case_when(code %in% fish_brook ~ 'fish_brook',
                      code %in% cart_creek ~ 'cart_creek',
                      code %in% saw_mill_brook ~ 'saw_mill_brook',
                      code %in% bear_meadow ~ 'bear_meadow',
                      code %in% upper_ipswich ~ 'upper_ipswich',
                      code %in% parker_dam ~ 'parker_dam',
                      code %in% ipswich_dam ~ 'ipswich_dam')
    
    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = !!site)
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE' = '%Y-%m-%d',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'site_name',
                         data_cols =  c('Temperature' = 'temp',
                                        'Sp_Cond' = 'spCond'),
                         summary_flagcols = 'Flag',
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)
    
    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('Flag' = 2),
                            summary_flags_to_drop = list('Flag' = 'DROP'),
                            varflag_col_pattern = NA)
    
    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    # 
    # d <- synchronize_timestep(d)
    # 
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms) 
    
    return(d)
}

munge_plum_temp_cond_cart <- function(network, domain, prodname_ms, site_name,
                                 component) {
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
    
    code <- prodcode_from_prodname_ms(prodname_ms)
    
    site <- case_when(code %in% fish_brook ~ 'fish_brook',
                      code %in% cart_creek ~ 'cart_creek',
                      code %in% saw_mill_brook ~ 'saw_mill_brook',
                      code %in% bear_meadow ~ 'bear_meadow',
                      code %in% upper_ipswich ~ 'upper_ipswich',
                      code %in% parker_dam ~ 'parker_dam',
                      code %in% ipswich_dam ~ 'ipswich_dam')
    
    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = !!site)
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'site_name',
                         data_cols =  c('Temp' = 'temp',
                                        'Sp_Cond' = 'spCond'),
                         summary_flagcols = 'Flag',
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)
    
    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('Flag' = 2),
                            summary_flags_to_drop = list('Flag' = 'DROP'),
                            varflag_col_pattern = NA)
    # 
    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    # 
    # d <- synchronize_timestep(d)
    # 
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms) 
    
    return(d)
}

munge_plum_temp_do <- function(network, domain, prodname_ms, site_name,
                                 component) {
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
    
    code <- prodcode_from_prodname_ms(prodname_ms)
    
    site <- case_when(code %in% fish_brook ~ 'fish_brook',
                      code %in% cart_creek ~ 'cart_creek',
                      code %in% saw_mill_brook ~ 'saw_mill_brook',
                      code %in% bear_meadow ~ 'bear_meadow',
                      code %in% upper_ipswich ~ 'upper_ipswich',
                      code %in% parker_dam ~ 'parker_dam',
                      code %in% ipswich_dam ~ 'ipswich_dam')
    
    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = !!site)
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'site_name',
                         data_cols =  c('Temp' = 'temp',
                                        'DO_Concentration' = 'DO'),
                         summary_flagcols = 'Flag',
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)
    
    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('Flag' = 2),
                            summary_flags_to_drop = list('Flag' = 'DROP'),
                            varflag_col_pattern = NA)
    
    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    # 
    # d <- synchronize_timestep(d)
    # 
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms) 
    
    return(d)
}

munge_precip <- function(network, domain, prodname_ms, site_name,
                         component){
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
    
    code <- prodcode_from_prodname_ms(prodname_ms)
    
    site <- case_when(code %in% governors_academy ~ 'governors_academy',
                      code %in% mbl_marshview ~ 'mbl_marshview')
    
    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = !!site)  %>%
        mutate(hour = str_split_fixed(Time, ':', n = Inf)[,1]) %>%
        mutate(minute = str_split_fixed(Time, ':', n = Inf)[,2]) %>%
        mutate(day = str_split_fixed(Date, '-', n = Inf)[,1]) %>%
        mutate(month = str_split_fixed(Date, '-', n = Inf)[,2]) %>%
        mutate(year = str_split_fixed(Date, '-', n = Inf)[,3]) %>%
        mutate(hour = ifelse(nchar(hour) == 1, paste0(0, hour), hour)) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day)) %>%
        mutate(Date = paste(day, month, year, sep = '-'),
               Time = paste(hour, minute, sep = ':'))
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%d-%b-%Y',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'site_name',
                         data_cols =  c('Precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    # 
    # d <- synchronize_timestep(d)
    # 
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

munge_precip_alt <- function(network, domain, prodname_ms, site_name,
                         component){
    
    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
    
    code <- prodcode_from_prodname_ms(prodname_ms)
    
    site <- case_when(code %in% governors_academy ~ 'governors_academy',
                      code %in% mbl_marshview ~ 'mbl_marshview')
    
    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = !!site)  
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'site_name',
                         data_cols =  c('Precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    # 
    # d <- synchronize_timestep(d)
    # 
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}
