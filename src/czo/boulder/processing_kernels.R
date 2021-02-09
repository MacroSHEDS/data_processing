
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
process_0_2785 <- function(set_details, network, domain) {

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
process_1_2918 <- function(network, domain, prodname_ms, site_name,
                           components){

    csv_file <- grep('.csv', components, value = TRUE)
    metadata_file <- grep('.txt', components, value = TRUE)

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = csv_file)

    metadata_path <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
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
                         site_name_col = 'site',
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
process_1_2919 <- function(network, domain, prodname_ms, site_name,
                           components) {

    csv_file <- grep('.csv', components, value = TRUE)
    metadata_file <- grep('.txt', components, value = TRUE)

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = csv_file)

    metadata_path <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = site_name,
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
                         site_name_col = 'site',
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
process_1_2785 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = case_when(grepl('GGL_SW_0', NLOC_ID) ~ 'GGL',
                                grepl('GGU_SW_0', NLOC_ID) ~ 'GGU',
                                grepl('GGU_SPW_1', NLOC_ID) ~ 'GGU_SPW1',
                                grepl('GGU_SPW_2', NLOC_ID) ~ 'GGU_SPW2',
                                NLOC_ID == 'BC_SW_6' ~ 'BC_SW_6',
                                NLOC_ID == 'BC_SW_2' ~ 'BC_SW_2',
                                NLOC_ID == 'BC_SW_4' ~ 'BC_SW_4',
                                NLOC_ID == 'BC_SW_12' ~ 'BC_SW_12',
                                NLOC_ID == 'BC_SW_14' ~ 'BC_SW_14',
                                NLOC_ID == 'BC_SW_17' ~ 'BC_SW_17',
                                NLOC_ID == 'BC_SW_18' ~ 'BC_SW_18',
                                NLOC_ID == 'BC_SW_20' ~ 'BC_SW_20',
                                NLOC_ID == 'BT_SW_0' ~ 'BT_SW_0')) %>%
        filter(!is.na(site)) %>%
        filter(SAMPLE_TYPE %in% c('SW', 'SPW')) %>%
        filter(X == "") %>%
        filter(X.1 == "") %>%
        mutate(TIME = ifelse(nchar(TIME) == 7, paste0(0, TIME), TIME))

    d[d == 'null'] <- NA

    d <- d %>%
        mutate(Si..umol.L. = round(((as.numeric(Si..umol.L.)*28.0855)/1000), 3)) %>%
        mutate(Si = ifelse(is.na(Si.ICP.ppm.), Si..umol.L., Si.ICP.ppm.))


    #Assuming IN means TIN
    #Skipped d180, not sure unit
    #Skipped Tritium.TU., not sure what unit it is in
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE_COLLECTION' = '%m/%e/%y',
                                              'TIME' = '%I:%M %p'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('Lab.pH' = 'pH',
                                        'Lab.Cond.uS.cm.' = 'spCond',
                                        'ALK.mg.L.' = 'alk',
                                        'H..uEQ.L.' = 'H',
                                        'NH4..uEQ.L.' = 'NH4',
                                        'Ca...uEQ.L.' = 'Ca',
                                        'Mg...uEQ.L.' = 'Mg',
                                        'Na..uEQ.L.' = 'Na',
                                        'K..uEQ.L.' = 'K',
                                        'Cl..uEQ.L.' = 'Cl',
                                        'NO3..uEQ.L.' = 'NO3',
                                        'SO4..uEQ.L.' = 'SO4',
                                        'PO4.3..uEQ.L.' = 'PO4',
                                        'SUM.' = 'cationCharge',
                                        'SUM..1' = 'anionCharge',
                                        'Charge.Bal' = 'ionBalance',
                                        'TDN.umol.L.' = 'TDN',
                                        'DON.umol.L.' = 'DON',
                                        'IN.umol.L.' = 'TIN',
                                        'TP.umol.L.' = 'TP',
                                        'TDP.umol.L.' = 'TDP',
                                        'PP.umol.L.' = 'TPP',
                                        'DOP.umol.L.' = 'DOP',
                                        'IP.umol.L.' = 'TIP',
                                        'DOC.mg.C.L' = 'DOC',
                                        'TN.umol.L.' = 'TN',
                                        'PN.umol.L.' = 'TPN',
                                        'Water.Temp.C.' = 'temp',
                                        'Chl.a.ug.L.' = 'CHL',
                                        'Phaeophytin.ug.L.' = 'pheophy',
                                        'Mn..umol.L.' = 'Mn',
                                        'Si' = 'Si',
                                        'Fe..umol.L.' = 'Fe',
                                        'Al..uEQ.L.' = 'Al'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#.CTS',
                         summary_flagcols = 'COMMENTS',
                         is_sensor = FALSE)

    d <- d %>%
        mutate(COMMENTS = ifelse(is.na(COMMENTS) | COMMENTS == 'precipitation event based auto sampling', 'DIRTY', 'CLEAN'))

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = c('u', '<0.63', '<0.09', 'U', '<0.145',
                                                     'charge balance difference > 10%',
                                                     '<0.364', 'DL', 'EQCL', 'NP', 'NV', 'QNS',
                                                     'T ERR'),
                            variable_flags_to_drop = 'DROP',
                            summary_flags_clean = list('COMMENTS' = 'CLEAN'),
                            summary_flags_dirty = list('COMMENTS' = 'DIRTY'))

    d <- ms_conversions(d,
                        convert_units_from = c('H' = 'ueq/l',
                                               'NH4' = 'ueq/l',
                                               'Ca' = 'ueq/l',
                                               'Mg' = 'ueq/l',
                                               'Na' = 'ueq/l',
                                               'K' = 'ueq/l',
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
                                               'DOC' = 'umol/l',
                                               'TN' = 'umol/l',
                                               'TPN' = 'umol/l',
                                               'CHL' = 'ug/l',
                                               'pheophy' = 'ug/l',
                                               'Mn' = 'umol/l',
                                               'Fe' = 'umol/l',
                                               'Al' = 'ueq/l'),
                        convert_units_to = c('H' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'K' = 'mg/l',
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
                                             'DOC' = 'mg/l',
                                             'TN' = 'mg/l',
                                             'TPN' = 'mg/l',
                                             'CHL' = 'mg/l',
                                             'pheophy' = 'mg/l',
                                             'Mn' = 'mg/l',
                                             'Fe' = 'mg/l',
                                             'Al' = 'mg/l'))

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
process_1_7241 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
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
                         site_name_col = 'site',
                         data_cols =  c('GGL_SW_0.Specific.Conductance..uS.cm.' = 'spCond',
                                        'GGL_SW_0.Temp..C.' = 'temp'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_3639 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(SAMPLE_TYPE %in% c('P')) %>%
        mutate(site = case_when(NLOC_ID %in% c('GGU_P_OPEN', 'GGU_P_Open') ~ 'GGL_SF_Met',
                                NLOC_ID %in% c('BT_P_OPEN', 'BT_P_Open', 'BT_P_Met_Open',
                                               'BT_Met_P_Open', 'Bet_Met_P_Open') ~ 'BT_Met')) %>%
        filter(!is.na(site)) %>%
        filter(X == "") %>%
        filter(X.1 == "") %>%
        mutate(TIME = ifelse(nchar(TIME) == 7, paste0(0, TIME), TIME))

    d[d == 'null'] <- NA

    d <- d %>%
        mutate(Si..umol.L. = round(((as.numeric(Si..umol.L.)*28.0855)/1000), 3)) %>%
        mutate(Si = ifelse(is.na(Si.ICP.ppm.), Si..umol.L., Si.ICP.ppm.))


    #Assuming IN means TIN
    #Skipped d180, not sure unit
    #Skipped Tritium.TU., not sure what unit it is in
    #Check ion balance
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE_COLLECTION' = '%m/%e/%y',
                                              'TIME' = '%I:%M %p'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('Lab.pH' = 'pH',
                                        'Lab.Cond.uS.cm.' = 'spCond',
                                        'ALK.mg.L.' = 'alk',
                                        'H..uEQ.L.' = 'H',
                                        'NH4..uEQ.L.' = 'NH4',
                                        'Ca...uEQ.L.' = 'Ca',
                                        'Mg...uEQ.L.' = 'Mg',
                                        'Na..uEQ.L.' = 'Na',
                                        'K..uEQ.L.' = 'K',
                                        'Cl..uEQ.L.' = 'Cl',
                                        'NO3..uEQ.L.' = 'NO3',
                                        'SO4..uEQ.L.' = 'SO4',
                                        'PO4.3..uEQ.L.' = 'PO4',
                                        'SUM.' = 'cationCharge',
                                        'SUM..1' = 'anionCharge',
                                        'Charge.Bal' = 'ionBalance',
                                        'TDN.umol.L.' = 'TDN',
                                        'DON.umol.L.' = 'DON',
                                        'IN.umol.L.' = 'TIN',
                                        'TP.umol.L.' = 'TP',
                                        'TDP.umol.L.' = 'TDP',
                                        'PP.umol.L.' = 'TPP',
                                        'DOP.umol.L.' = 'DOP',
                                        'IP.umol.L.' = 'TIP',
                                        'DOC.mg.C.L' = 'DOC',
                                        'TN.umol.L.' = 'TN',
                                        'PN.umol.L.' = 'TPN',
                                        'Water.Temp.C.' = 'temp',
                                        'Chl.a.ug.L.' = 'CHL',
                                        'Phaeophytin.ug.L.' = 'pheophy',
                                        'Mn..umol.L.' = 'Mn',
                                        'Si' = 'Si',
                                        'Fe..umol.L.' = 'Fe',
                                        'Al..uEQ.L.' = 'Al'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#.CTS',
                         summary_flagcols = 'COMMENTS',
                         is_sensor = FALSE)

    d <- d %>%
        mutate(COMMENTS = ifelse(is.na(COMMENTS) | COMMENTS == 'precipitation event based auto sampling', 'DIRTY', 'CLEAN'))

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = c('u', '<0.63', '<0.09', 'U', '<0.145',
                                                     'charge balance difference > 10%',
                                                     '<0.364', 'DL', 'EQCL', 'NP', 'NV', 'QNS',
                                                     'T ERR'),
                            variable_flags_to_drop = 'DROP',
                            summary_flags_clean = list('COMMENTS' = 'CLEAN'),
                            summary_flags_dirty = list('COMMENTS' = 'DIRTY'))

    d <- ms_conversions(d,
                        convert_units_from = c('H' = 'ueq/l',
                                               'NH4' = 'ueq/l',
                                               'Ca' = 'ueq/l',
                                               'Mg' = 'ueq/l',
                                               'Na' = 'ueq/l',
                                               'K' = 'ueq/l',
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
                                               'DOC' = 'umol/l',
                                               'TN' = 'umol/l',
                                               'TPN' = 'umol/l',
                                               'CHL' = 'ug/l',
                                               'pheophy' = 'ug/l',
                                               'Mn' = 'umol/l',
                                               'Fe' = 'umol/l',
                                               'Al' = 'ueq/l'),
                        convert_units_to = c('H' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'K' = 'mg/l',
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
                                             'DOC' = 'mg/l',
                                             'TN' = 'mg/l',
                                             'TPN' = 'mg/l',
                                             'CHL' = 'mg/l',
                                             'pheophy' = 'mg/l',
                                             'Mn' = 'mg/l',
                                             'Fe' = 'mg/l',
                                             'Al' = 'mg/l'))

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2435 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
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
                         site_name_col = 'site',
                         data_cols =  c('RAIN.GAGE.MM.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2888 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
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
                         site_name_col = 'site',
                         data_cols =  c('RAIN.GAGE.MM.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2889 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
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
                         site_name_col = 'site',
                         data_cols =  c('RAIN.GAGE.MM.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'null',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

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
                     input_prodname_ms = c('stream_chemistry__7241',
                                           'stream_chemistry__2785'))

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

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms007 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms008 <- derive_precip_pchem_pflux
