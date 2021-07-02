
#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_2918 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_2919 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2783 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_3064 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_3065 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_7241 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_3639 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2435 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2888 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2889 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}


#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_2918 <- function(network, domain, prodname_ms, site_code,
                           components){

    csv_file <- grep('.csv', components, value = TRUE)
    metadata_file <- grep('.txt', components, value = TRUE)

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = csv_file)

    metadata_path <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = metadata_file)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'GGL') %>%
        mutate(nchar = nchar(DATE_TIME)) %>%
        mutate(DATE_TIME = ifelse(nchar %in% c(6, 7, 8), paste0(DATE_TIME, ' 00:00'), DATE_TIME)) %>%
        mutate(date = str_split_fixed(DATE_TIME, ' ', n = Inf)[,1]) %>%
        mutate(time = str_split_fixed(DATE_TIME, ' ', n = Inf)[,2]) %>%
        mutate(hour = str_split_fixed(time, ':', n = Inf)[,1],
               minute = str_split_fixed(time, ':', n = Inf)[,2]) %>%
        mutate(hour = ifelse(nchar(hour) == 1, paste0(0, hour), hour)) %>%
        mutate(month = str_split_fixed(date, '/', n = Inf)[,1],
               day = str_split_fixed(date, '/', n = Inf)[,2],
               year = str_split_fixed(date, '/', n = Inf)[,3]) %>%
        mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
               month = ifelse(nchar(month) == 1, paste0(0, month), month)) %>%
        mutate(date_time = paste0(month, '/', day, '/', year, ' ', hour, ':', minute))


    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date_time' = '%m/%d/%y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('DISCHARGE.m.3.10..3..s' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    metadata <- read_file(metadata_path)

    meta_check <- str_split_fixed(metadata, '\\t', n = Inf)
    error_check <- grepl(x = meta_check, pattern = 'NOTE: There is an error in method of computing discharge from salt injections that needs to account for')

    if(TRUE %in% error_check){
        d <- d %>%
            mutate(ms_status = 1)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_2919 <- function(network, domain, prodname_ms, site_code,
                           components) {

    csv_file <- grep('.csv', components, value = TRUE)
    metadata_file <- grep('.txt', components, value = TRUE)

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = csv_file)

    metadata_path <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = site_code,
                          c = metadata_file)

    d <- read.csv(rawfile, colClasses = 'character') %>%
             mutate(site = 'GGU') %>%
             mutate(nchar = nchar(DATE_TIME)) %>%
             mutate(DATE_TIME = ifelse(nchar %in% c(8, 9), paste0(DATE_TIME, ' 00:00'), DATE_TIME)) %>%
             mutate(date = str_split_fixed(DATE_TIME, ' ', n = Inf)[,1]) %>%
             mutate(time = str_split_fixed(DATE_TIME, ' ', n = Inf)[,2]) %>%
             mutate(hour = str_split_fixed(time, ':', n = Inf)[,1],
                    minute = str_split_fixed(time, ':', n = Inf)[,2]) %>%
             mutate(hour = ifelse(nchar(hour) == 1, paste0(0, hour), hour)) %>%
             mutate(month = str_split_fixed(date, '/', n = Inf)[,1],
                    day = str_split_fixed(date, '/', n = Inf)[,2],
                    year = str_split_fixed(date, '/', n = Inf)[,3]) %>%
             mutate(day = ifelse(nchar(day) == 1, paste0(0, day), day),
                    month = ifelse(nchar(month) == 1, paste0(0, month), month)) %>%
             mutate(date_time = paste0(month, '/', day, '/', year, ' ', hour, ':', minute))


    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date_time' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('DISCHARGE.m.3.10..3..s' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    metadata <- read_file(metadata_path)

    meta_check <- str_split_fixed(metadata, '\\t', n = Inf)
    error_check <- grepl(x = meta_check, pattern = 'NOTE: There is an error in method of computing discharge from salt injections that needs to account for')

    if(TRUE %in% error_check){
        d <- d %>%
            mutate(ms_status = 1)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_2783 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(Location %in% c('BC_SW_6', 'BC_SW_2', 'BC_SW_4', 'BC_SW_12',
                              'BC_SW_14', 'BC_SW_17', 'BC_SW_18', 'BC_SW_20',
                              'BT_SW_0'))

    d <- d %>%
        mutate(Si..umol.L. = ((as.numeric(Si..umol.L.)*28.0855)/1000)) %>%
        mutate(Si = ifelse(is.na(Si.ICP..ppm.)|Si.ICP..ppm.=='', Si..umol.L., Si.ICP..ppm.)) %>%
        mutate(Si = as.character(Si))

    #Assuming IN means TIN
    #Skipped d180, not sure unit
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%y',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'Location',
                         data_cols =  c('Lab.pH' = 'pH',
                                        'Water.Temp.C.' = 'temp',
                                        'Lab.Cond..uS.cm.' = 'spCond',
                                        'H..uEQ.L.' = 'H',
                                        'Ca...uEQ.L.' = 'Ca',
                                        'K..ppm.' = 'K',
                                        'Mg...ppm.' = 'Mg',
                                        'Na..ppm.' = 'Na',
                                        'NH4..uEQ.L.' = 'NH4',
                                        'Cl..uEQ.L.' = 'Cl',
                                        'NO3..uEQ.L.' = 'NO3',
                                        'SO4..uEQ.L.' = 'SO4',
                                        'Si' = 'Si',
                                        'PO4.3...uEQ.L.' = 'PO4',
                                        'ALK.mg.L.' = 'alk',
                                        'TDN..umol.L.' = 'TDN',
                                        'DON..umol.L.' = 'DON',
                                        'IN..umol.L.' = 'TIN',
                                        'TP..umol.L.' = 'TP',
                                        'TDP..umol.L.' = 'TDP',
                                        'PP..umol.L.' = 'TPP',
                                        'DOP..umol.L.' = 'DOP',
                                        'IP..umol.L.' = 'TIP',
                                        'DOC.mg.C.L' = 'DOC',
                                        'TN..umol.L.' = 'TN',
                                        'PN..umol.L.' = 'TPN',
                                        'Chl.a..ug.L.' = 'CHL',
                                        'Phaeophytin.ug.L.' = 'pheophy',
                                        'Mn..ppm.' = 'Mn',
                                        'Fe..ppm.' = 'Fe',
                                        'Al..ppm.' = 'Al',
                                        'd180.mill' = 'd18O'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#.CTS',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = c('u', '<0.63', '<0.09', 'U',
                                                     '<0.145',
                                                     'charge balance difference > 10%',
                                                     '<0.364', 'DL', 'EQCL', 'NP',
                                                     'NV', 'QNS', 'T ERR'),
                            variable_flags_to_drop = 'DROP')

    d <- ms_conversions(d,
                        convert_units_from = c('H' = 'ueq/l',
                                               'NH4' = 'ueq/l',
                                               'Ca' = 'ueq/l',
                                               'Cl' = 'ueq/l',
                                               'NO3' = 'ueq/l',
                                               'SO4' = 'ueq/l',
                                               'PO4' = 'ueq/l',
                                               'TDN' = 'umol/l',
                                               'DON' = 'umol/l',
                                               'TIN' = 'umol/l',
                                               'TP' = 'umol/l',
                                               'TDP' = 'umol/l',
                                               'TPP' = 'umol/l',
                                               'DOP' = 'umol/l',
                                               'TIP' = 'umol/l',
                                               'TN' = 'umol/l',
                                               'TPN' = 'umol/l',
                                               'CHL' = 'ug/l',
                                               'pheophy' = 'ug/l'),
                        convert_units_to = c('H' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             'SO4' = 'mg/l',
                                             'PO4' = 'mg/l',
                                             'TDN' = 'mg/l',
                                             'DON' = 'mg/l',
                                             'TIN' = 'mg/l',
                                             'TP' = 'mg/l',
                                             'TDP' = 'mg/l',
                                             'TPP' = 'mg/l',
                                             'DOP' = 'mg/l',
                                             'TIP' = 'mg/l',
                                             'TN' = 'mg/l',
                                             'TPN' = 'mg/l',
                                             'CHL' = 'mg/l',
                                             'pheophy' = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_3064 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(Location %in% c('BT_SW_0', 'BT_SW_O', 'BT_SW_o'))

    d <- d %>%
        mutate(Si..umol.L. = ((as.numeric(Si..umol.L.)*28.0855)/1000)) %>%
        mutate(Si = ifelse(is.na(Si.ICP..ppm.)|Si.ICP..ppm.=='', Si..umol.L., Si.ICP..ppm.)) %>%
        mutate(Si = as.character(Si))

    #Assuming IN means TIN
    #Skipped d180, not sure unit
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%y',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'Location',
                         alt_site_code = list('BT_SW_0' = c('BT_SW_0', 'BT_SW_O', 'BT_SW_o')),
                         data_cols =  c('Lab.pH' = 'pH',
                                        'Water.Temp.C.' = 'temp',
                                        'Lab.Cond..uS.cm.' = 'spCond',
                                        'H..uEQ.L.' = 'H',
                                        'Ca...uEQ.L.' = 'Ca',
                                        'K..ppm.' = 'K',
                                        'Mg...ppm.' = 'Mg',
                                        'Na..ppm.' = 'Na',
                                        'NH4..uEQ.L.' = 'NH4',
                                        'Cl..uEQ.L.' = 'Cl',
                                        'NO3..uEQ.L.' = 'NO3',
                                        'SO4..uEQ.L.' = 'SO4',
                                        'Si' = 'Si',
                                        'PO4.3...uEQ.L.' = 'PO4',
                                        'ALK.mg.L.' = 'alk',
                                        'TDN..umol.L.' = 'TDN',
                                        'DON..umol.L.' = 'DON',
                                        'IN..umol.L.' = 'TIN',
                                        'TP..umol.L.' = 'TP',
                                        'TDP..umol.L.' = 'TDP',
                                        'PP..umol.L.' = 'TPP',
                                        'DOP..umol.L.' = 'DOP',
                                        'IP..umol.L.' = 'TIP',
                                        'DOC.mg.C.L' = 'DOC',
                                        'TN..umol.L.' = 'TN',
                                        'PN..umol.L.' = 'TPN',
                                        'Chl.a..ug.L.' = 'CHL',
                                        'Phaeophytin.ug.L.' = 'pheophy',
                                        'Mn..ppm.' = 'Mn',
                                        'Fe..ppm.' = 'Fe',
                                        'Al..ppm.' = 'Al',
                                        'd180.mill' = 'd18O'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#.CTS',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = c('u', '<0.63', '<0.09', 'U',
                                                     '<0.145',
                                                     'charge balance difference > 10%',
                                                     '<0.364', 'DL', 'EQCL', 'NP',
                                                     'NV', 'QNS', 'T ERR'),
                            variable_flags_to_drop = 'DROP')

    d <- ms_conversions(d,
                        convert_units_from = c('H' = 'ueq/l',
                                               'NH4' = 'ueq/l',
                                               'Ca' = 'ueq/l',
                                               'Cl' = 'ueq/l',
                                               'NO3' = 'ueq/l',
                                               'SO4' = 'ueq/l',
                                               'PO4' = 'ueq/l',
                                               'TDN' = 'umol/l',
                                               'DON' = 'umol/l',
                                               'TIN' = 'umol/l',
                                               'TP' = 'umol/l',
                                               'TDP' = 'umol/l',
                                               'TPP' = 'umol/l',
                                               'DOP' = 'umol/l',
                                               'TIP' = 'umol/l',
                                               'TN' = 'umol/l',
                                               'TPN' = 'umol/l',
                                               'CHL' = 'ug/l',
                                               'pheophy' = 'ug/l'),
                        convert_units_to = c('H' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             'SO4' = 'mg/l',
                                             'PO4' = 'mg/l',
                                             'TDN' = 'mg/l',
                                             'DON' = 'mg/l',
                                             'TIN' = 'mg/l',
                                             'TP' = 'mg/l',
                                             'TDP' = 'mg/l',
                                             'TPP' = 'mg/l',
                                             'DOP' = 'mg/l',
                                             'TIP' = 'mg/l',
                                             'TN' = 'mg/l',
                                             'TPN' = 'mg/l',
                                             'CHL' = 'mg/l',
                                             'pheophy' = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_3065 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(Location %in% c('GGL_SW_0_ISCO', 'GGU_SW_0_ISCO', 'GGU_SW_0',
                               'GGU_SPW_1', 'GGU_SPW_2', 'GGL_SW_0'))

    d <- d %>%
        mutate(Si..umol.L. = ((as.numeric(Si..umol.L.)*28.0855)/1000)) %>%
        mutate(Si = ifelse(is.na(Si.ICP..ppm.)|Si.ICP..ppm.=='', Si..umol.L., Si.ICP..ppm.)) %>%
        mutate(Si = as.character(Si))

    #Assuming IN means TIN
    #Skipped d180, not sure unit
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%y',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'Location',
                         alt_site_code = list('GGL' = c('GGL_SW_0_ISCO', 'GGL_SW_0'),
                                              'GGU' = c('GGU_SW_0_ISCO', 'GGU_SW_0'),
                                              'GGU_SPW1' = 'GGU_SPW_1',
                                              'GGU_SPW2' = 'GGU_SPW_2'),
                         data_cols =  c('Lab.pH' = 'pH',
                                        'Water.Temp.C.' = 'temp',
                                        'Lab.Cond..uS.cm.' = 'spCond',
                                        'H..uEQ.L.' = 'H',
                                        'Ca...uEQ.L.' = 'Ca',
                                        'K..ppm.' = 'K',
                                        'Mg...ppm.' = 'Mg',
                                        'Na..ppm.' = 'Na',
                                        'NH4..uEQ.L.' = 'NH4',
                                        'Cl..uEQ.L.' = 'Cl',
                                        'NO3..uEQ.L.' = 'NO3',
                                        'SO4..uEQ.L.' = 'SO4',
                                        'Si' = 'Si',
                                        'PO4.3...uEQ.L.' = 'PO4',
                                        'ALK.mg.L.' = 'alk',
                                        'TDN..umol.L.' = 'TDN',
                                        'DON..umol.L.' = 'DON',
                                        'IN..umol.L.' = 'TIN',
                                        'TP..umol.L.' = 'TP',
                                        'TDP..umol.L.' = 'TDP',
                                        'PP..umol.L.' = 'TPP',
                                        'DOP..umol.L.' = 'DOP',
                                        'IP..umol.L.' = 'TIP',
                                        'DOC.mg.C.L' = 'DOC',
                                        'TN..umol.L.' = 'TN',
                                        'PN..umol.L.' = 'TPN',
                                        'Chl.a..ug.L.' = 'CHL',
                                        'Phaeophytin.ug.L.' = 'pheophy',
                                        'Mn..ppm.' = 'Mn',
                                        'Fe..ppm.' = 'Fe',
                                        'Al..ppm.' = 'Al',
                                        'd180.mill' = 'd18O'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#.CTS',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = c('u', '<0.63', '<0.09', 'U',
                                                     '<0.145',
                                                     'charge balance difference > 10%',
                                                     '<0.364', 'DL', 'EQCL', 'NP',
                                                     'NV', 'QNS', 'T ERR'),
                            variable_flags_to_drop = 'DROP')

    d <- ms_conversions(d,
                        convert_units_from = c('H' = 'ueq/l',
                                               'NH4' = 'ueq/l',
                                               'Ca' = 'ueq/l',
                                               'Cl' = 'ueq/l',
                                               'NO3' = 'ueq/l',
                                               'SO4' = 'ueq/l',
                                               'PO4' = 'ueq/l',
                                               'TDN' = 'umol/l',
                                               'DON' = 'umol/l',
                                               'TIN' = 'umol/l',
                                               'TP' = 'umol/l',
                                               'TDP' = 'umol/l',
                                               'TPP' = 'umol/l',
                                               'DOP' = 'umol/l',
                                               'TIP' = 'umol/l',
                                               'TN' = 'umol/l',
                                               'TPN' = 'umol/l',
                                               'CHL' = 'ug/l',
                                               'pheophy' = 'ug/l'),
                        convert_units_to = c('H' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             'SO4' = 'mg/l',
                                             'PO4' = 'mg/l',
                                             'TDN' = 'mg/l',
                                             'DON' = 'mg/l',
                                             'TIN' = 'mg/l',
                                             'TP' = 'mg/l',
                                             'TDP' = 'mg/l',
                                             'TPP' = 'mg/l',
                                             'DOP' = 'mg/l',
                                             'TIP' = 'mg/l',
                                             'TN' = 'mg/l',
                                             'TPN' = 'mg/l',
                                             'CHL' = 'mg/l',
                                             'pheophy' = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_7241 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'GGL') %>%
        mutate(date = str_split_fixed(Date.Time..UTC.7, ' ', n = Inf)[,1]) %>%
        mutate(time = str_split_fixed(Date.Time..UTC.7, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0(0, time), time))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('GGL_SW_0.Specific.Conductance..uS.cm.' = 'spCond',
                                        'GGL_SW_0.Temp..C.' = 'temp'),
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

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_3639 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = case_when(Location %in% c('GGU_P_OPEN', 'GGU_P_Open') ~ 'GGL_SF_Met',
                                Location %in% c('BT_P_OPEN', 'BT_P_Open', 'BT_P_Met_Open',
                                               'BT_Met_P_Open', 'Bet_Met_P_Open') ~ 'BT_Met')) %>%
        filter(!is.na(site))

    d <- d %>%
        mutate(Si..umol.L. = round(((as.numeric(Si..umol.L.)*28.0855)/1000), 3)) %>%
        mutate(Si = ifelse(is.na(Si.ICP..ppm.), Si..umol.L., Si.ICP..ppm.))

    #Assuming IN means TIN
    #Skipped d180, not sure unit
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%y',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('Lab.pH' = 'pH',
                                        'Lab.Cond..uS.cm.' = 'spCond',
                                        'H..uEQ.L.' = 'H',
                                        'Ca...uEQ.L.' = 'Ca',
                                        'K..ppm.' = 'K',
                                        'Mg...ppm.' = 'Mg',
                                        'Na..ppm.' = 'Na',
                                        'NH4..uEQ.L.' = 'NH4',
                                        'Cl..uEQ.L.' = 'Cl',
                                        'NO3..uEQ.L.' = 'NO3',
                                        'SO4..uEQ.L.' = 'SO4',
                                        'Si' = 'Si',
                                        'PO4.3...uEQ.L.' = 'PO4',
                                        'ALK.mg.L.' = 'alk',
                                        'TDN..umol.L.' = 'TDN',
                                        'DON..umol.L.' = 'DON',
                                        'IN..umol.L.' = 'TIN',
                                        'TP..umol.L.' = 'TP',
                                        'TDP..umol.L.' = 'TDP',
                                        'PP..umol.L.' = 'TPP',
                                        'DOP..umol.L.' = 'DOP',
                                        'IP..umol.L.' = 'TIP',
                                        'DOC.mg.C.L' = 'DOC',
                                        'TN..umol.L.' = 'TN',
                                        'PN..umol.L.' = 'TPN',
                                        'Chl.a..ug.L.' = 'CHL',
                                        'Phaeophytin.ug.L.' = 'pheophy',
                                        'Mn..ppm.' = 'Mn',
                                        'Fe..ppm.' = 'Fe',
                                        'Al..ppm.' = 'Al',
                                        'd180.mill' = 'd18O'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#.CTS',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = c('u', '<0.63', '<0.09', 'U',
                                                     '<0.145',
                                                     'charge balance difference > 10%',
                                                     '<0.364', 'DL', 'EQCL', 'NP',
                                                     'NV', 'QNS', 'T ERR'),
                            variable_flags_to_drop = 'DROP')

    d <- ms_conversions(d,
                        convert_units_from = c('H' = 'ueq/l',
                                               'NH4' = 'ueq/l',
                                               'Ca' = 'ueq/l',
                                               'Cl' = 'ueq/l',
                                               'NO3' = 'ueq/l',
                                               'SO4' = 'ueq/l',
                                               'PO4' = 'ueq/l',
                                               'TDN' = 'umol/l',
                                               'DON' = 'umol/l',
                                               'TIN' = 'umol/l',
                                               'TP' = 'umol/l',
                                               'TDP' = 'umol/l',
                                               'TPP' = 'umol/l',
                                               'DOP' = 'umol/l',
                                               'TIP' = 'umol/l',
                                               'TN' = 'umol/l',
                                               'TPN' = 'umol/l',
                                               'CHL' = 'ug/l',
                                               'pheophy' = 'ug/l'),
                        convert_units_to = c('H' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             'SO4' = 'mg/l',
                                             'PO4' = 'mg/l',
                                             'TDN' = 'mg/l',
                                             'DON' = 'mg/l',
                                             'TIN' = 'mg/l',
                                             'TP' = 'mg/l',
                                             'TDP' = 'mg/l',
                                             'TPP' = 'mg/l',
                                             'DOP' = 'mg/l',
                                             'TIP' = 'mg/l',
                                             'TN' = 'mg/l',
                                             'TPN' = 'mg/l',
                                             'CHL' = 'mg/l',
                                             'pheophy' = 'mg/l'))

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

#precipitation: STATUS=READY
#. handle_errors
process_1_2435 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'BT_Met') %>%
        mutate(date = str_split_fixed(DATE_TIME, ' ', n = Inf)[,1]) %>%
        mutate(time = str_split_fixed(DATE_TIME, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0(0, time), time))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('RAIN.GAGE.MM.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
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

#precipitation: STATUS=READY
#. handle_errors
process_1_2888 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'GGL_NF_Met') %>%
        mutate(date = str_split_fixed(DATE_TIME, ' ', n = Inf)[,1]) %>%
        mutate(time = str_split_fixed(DATE_TIME, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0(0, time), time))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('RAIN.GAGE.MM.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
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

#precipitation: STATUS=READY
#. handle_errors
process_1_2889 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'GGL_SF_Met') %>%
        mutate(date = str_split_fixed(DATE_TIME, ' ', n = Inf)[,1]) %>%
        mutate(time = str_split_fixed(DATE_TIME, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0(0, time), time))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('RAIN.GAGE.MM.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
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

#derive kernels ####

#cdnr_discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    #retrieve data from colorado department of natural resources

    sm(pull_cdnr_discharge(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           sites = list('BC_SW_20' = c('start_year' = 1996,
                                                       'abbrv' = 'BOCMIDCO'),
                                        'BC_SW_4' = c('start_year' = 1996,
                                                      'abbrv' = 'BOCOROCO'))))

    return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms002 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- stream_gauge_from_site_data

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms004 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__2783',
                                           'stream_chemistry__3064',
                                           'stream_chemistry__3065',
                                           'stream_chemistry__7241'))

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_2_ms005 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__2918',
                                           'discharge__2919',
                                           'cdnr_discharge__ms001'))

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_2_ms006 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('precipitation__2435',
                                           'precipitation__2888',
                                           'precipitation__2889'))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms007 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms008 <- derive_precip_pchem_pflux
