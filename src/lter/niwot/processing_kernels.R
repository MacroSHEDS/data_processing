source('src/lter/niwot/domain_helpers.R', local=TRUE)

#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_213 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_103 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_107 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_108 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_109 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_110 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_112 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_113 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_9 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_160 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_162 <- retrieve_niwot

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_163 <- retrieve_niwot

#discharge: STATUS=READY
#. handle_errors
process_0_102 <- retrieve_niwot

#discharge: STATUS=READY
#. handle_errors
process_0_111 <- retrieve_niwot

#discharge: STATUS=READY
#. handle_errors
process_0_74 <- retrieve_niwot

#discharge: STATUS=READY
#. handle_errors
process_0_105 <- retrieve_niwot

#precipitation: STATUS=READY
#. handle_errors
process_0_416 <- retrieve_niwot

#precipitation: STATUS=READY
#. handle_errors
process_0_414 <- retrieve_niwot

#precipitation: STATUS=READY
#. handle_errors
process_0_415 <- retrieve_niwot

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_213 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                             'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'samp_loc',
                         alt_site_name = list('SODDIE' = 'SODDIE STREAM'),
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_103 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_107 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('GREEN1' = 'GREEN LAKE 1'),
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_108 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('GREEN4' = c('GREEN LAKE 4',
                                                           'GREEN LAKE 4 WATERFALL')),
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_109 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('GREEN5' = c('GREEN LAKE 5')),
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_110 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('ALBION_INLET' = c('ALBION INLET')),
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_112 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_113 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_9 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         alt_site_name = list('SADDLE_007' = 'SADDLE STREAM 007'),
                         site_name_col = 'samp_loc',
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <-ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_160 <- function(network, domain, prodname_ms, site_name,
                        component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         alt_site_name = list('SADDLE' = 'SADDLE STREAM'),
                         site_name_col = 'local_site',
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_162 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_163 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = c('ROCK_GLACIER' = 'GREEN LAKE 5 ROCK GLACIER'),
                         data_cols =  c('pH' = 'pH',
                                        'cond' = 'spCond',
                                        'ANC' = 'ANC',
                                        'H.' = 'H',
                                        'NH4.' = 'NH4',
                                        'Ca..' = 'Ca',
                                        'Mg..' = 'Mg',
                                        'Na.' = 'Na',
                                        'K.' = 'K',
                                        'Cl.' = 'Cl',
                                        'NO3.' = 'NO3',
                                        'SO4..' = 'SO4',
                                        'PO4...' = 'PO4',
                                        'Si' = 'Si',
                                        'cat_sum' = 'cationCharge',
                                        'an_sum' = 'anionCharge',
                                        'chg_bal' = 'ionBalance',
                                        'TN' = 'TN',
                                        'TDN' = 'TDN',
                                        'PN' = 'TPN',
                                        'IN' = 'TIN',
                                        'TP' = 'TP',
                                        'TDP' = 'TDP',
                                        'PP' = 'TPP',
                                        'DOP' = 'DOP',
                                        'IP' = 'TIP',
                                        'd18O' = 'd18O',
                                        'TOC' = 'TOC',
                                        'DOC' = 'DOC',
                                        'POC' = 'POC'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('u', 'NP', 'DNS', 'QNS', 'trace'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               ANC = 'ueq/l',
                                               Ca = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               Mg = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'umol/l',
                                               TN = 'umol/l',
                                               TDN = 'umol/l',
                                               TPN = 'umol/l',
                                               DON = 'umol/l',
                                               TP = 'umol/l',
                                               TDP = 'umol/l',
                                               TPP = 'umol/l',
                                               DOP = 'umol/l',
                                               TIP = 'umol/l',
                                               TIN = 'ueq/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             ANC = 'eq/l',
                                             Ca = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             Mg = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             DON = 'mg/l',
                                             TP = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             TIN = 'mg/l',
                                             H = 'mg/l'))

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

#discharge: STATUS=READY
#. handle_errors
process_1_102 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                            datetime_cols = list('date' = '%Y-%m-%d'),
                            datetime_tz = 'US/Mountain',
                            site_name_col = 'local_site',
                            alt_site_name = list('ALBION' = 'alb'),
                            data_cols =  'discharge',
                            data_col_pattern = '#V#',
                            summary_flagcols = 'notes',
                            is_sensor = TRUE)

    flag_vals <- unique(d$notes)

    flag_vals <- flag_vals[flag_vals != 'NaN']

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list('notes' = 'NaN'),
                            summary_flags_dirty = list('notes' = flag_vals))

    # Convert from daily volume to l/s
    d <- d %>%
      mutate(val = (val*1000)/86400)

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

#discharge: STATUS=READY
#. handle_errors
process_1_111 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('MARTINELLI' = 'mar'),
                         data_cols =  'discharge',
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    # Convert from daily volume to l/s
    d <- d %>%
      mutate(val = (val*1000)/86400)

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

#discharge: STATUS=READY
#. handle_errors
process_1_74 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('SADDLE' = 'sdl'),
                         data_cols =  'discharge',
                         data_col_pattern = '#V#',
                         summary_flagcols = 'notes',
                         is_sensor = TRUE)

    flag_vals <- unique(d$notes)

    flag_vals <- flag_vals[flag_vals != 'NaN']

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list('notes' = 'NaN'),
                            summary_flags_dirty = list('notes' = flag_vals))

    # Convert from daily volume to l/s
    d <- d %>%
      mutate(val = (val*1000)/86400)

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

#discharge: STATUS=READY
#. handle_errors
process_1_105 <- function(network, domain, prodname_ms, site_name,
                         component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('GREEN4' = 'gl4'),
                         data_cols =  'discharge',
                         data_col_pattern = '#V#',
                         summary_flagcols = 'notes',
                         is_sensor = TRUE)

    flag_vals <- c('flow data interpolated and not based on stage records',
               'flow data preliminary and subject to change',
               'flow data estimated indirectly and not based on continuous level record',
               'record affected by failed interface board - use with care',
               'flow data estimated from interpolation or recession',
               'flow data estimated from interpolation or recession',
               'flow data estimated because water level was below channel sensors',
               'flow data estimated from interpolation',
               'flow data based upon intermittent estimates',
               'flow data estimated from field observations',
               'flow data estimated from recession and weekly observations',
               'flow data estimated from observations',
               'flow data estimated/interpolated by recession analysis',
               'flow data interpolated',
               'flow data estimated',
               'flow data estimated from intermittent observation',
               'flow data estimated by recession observation of 2 October',
               'flow data interpolated because there was no record due to a jammed float')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list('notes' = 'NaN'),
                            summary_flags_dirty = list('notes' = flag_vals))

    # Convert from daily volume to l/s
    d <- d %>%
      mutate(val = (val*1000)/86400)

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

#precipitation: STATUS=READY
#. handle_errors
process_1_416 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('saddle' = 'sdl'),
                         data_cols =  c('ppt_tot' = 'precipitation_ns'),
                         data_col_pattern = '#V#',
                        summary_flagcols = c('flag_ppt_tot', 'qdays'),
                         is_sensor = TRUE)

    d <- d %>%
      filter(qdays == 1)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list('flag_ppt_tot' = 'NaN'),
                            summary_flags_dirty = list('flag_ppt_tot' = c(1,2)))

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

#precipitation: STATUS=READY
#. handle_errors
process_1_414 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('C1' = 'c1'),
                         data_cols =  c('ppt_tot' = 'precipitation_ns'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'qdays',
                         is_sensor = TRUE)

    d <- d %>%
      filter(qdays == 1)

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

#precipitation: STATUS=READY
#. handle_errors
process_1_415 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('date' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'local_site',
                         alt_site_name = list('D1' = 'd1'),
                         data_cols =  c('ppt_tot' = 'precipitation_ns'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c('flag_ppt_tot', 'qdays'),
                         is_sensor = TRUE)

    d <- d %>%
      filter(qdays == 1)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list('flag_ppt_tot' = 'NaN'),
                            summary_flags_dirty = list('flag_ppt_tot' = c(1,2)))


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

#derive kernels ####

#rain_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- function(network, domain, prodname_ms) {

    rain_gauge_from_site_data(network = network,
                              domain = domain,
                              prodname_ms = 'rain_gauge_locations__ms004')

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_munged_products(network = network,
                            domain = domain,
                            prodname_ms = prodname_ms,
                            munged_prodname_ms = c('discharge__102',
                                                   'discharge__111',
                                                   'discharge__74',
                                                   'discharge__105'))

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms) {

    combine_munged_products(network = network,
                            domain = domain,
                            prodname_ms = prodname_ms,
                            munged_prodname_ms = c('stream_chemistry__213',
                                                   'stream_chemistry__103',
                                                   'stream_chemistry__107',
                                                   'stream_chemistry__108',
                                                   'stream_chemistry__109',
                                                   'stream_chemistry__110',
                                                   'stream_chemistry__112',
                                                   'stream_chemistry__113',
                                                   'stream_chemistry__9',
                                                   'stream_chemistry__160',
                                                   'stream_chemistry__162',
                                                   'stream_chemistry__163'))

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms) {

    precip_idw(precip_prodname = c('precipitation__416',
                                   'precipitation__414',
                                   'precipitation__415'),
               wb_prodname = 'ws_boundary__ms000',
               pgauge_prodname = 'rain_gauge_locations__ms004',
               precip_prodname_out = prodname_ms)

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms005 <- derive_stream_flux
