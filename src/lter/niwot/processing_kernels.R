#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_213 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)

  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_103 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)

  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_107 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)

  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_108 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)

  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_109 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)

  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_110 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_112 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_113 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)

  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_9 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_160 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_162 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_163 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_102 <- function(set_details, network, domain){

  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_111 <- function(set_details, network, domain){
  
  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_74 <- function(set_details, network, domain){
  
    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)
  return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_105 <- function(set_details, network, domain){
  
  download_raw_file(network = network,
                    domain = domain,
                    set_details = set_details,
                    file_type = NULL)
  return()
}

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
  
  d <- ue(ms_read_raw_csv(filepath = rawfile,
                          datetime_cols = list('date' = '%Y-%m-%d',
                                               'time' = '%H%M'),
                          datetime_tz = 'US/Mountain',
                          site_name_col = 'samp_loc',
                          alt_site_name = list('SODDIE' = 'SODDIE STREAM'),
                          data_cols =  c('pH' = 'pH',
                                         'cond' = 'SpCond',
                                         'ANC' = 'ANC',
                                         'acid' = 'acidity',
                                         'alkal' = 'alkalinity',
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
                                         'IN' = 'IN',
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
                          is_sensor = FALSE))
  
  d <- ue(ms_cast_and_reflag(d,
                             varflag_col_pattern = NA))
  
  d <- ue(carry_uncertainty(d,
                            network = network,
                            domain = domain,
                            prodname_ms = prodname_ms))
  
  d <- ms_conversions(d,
                      convert_units_from = c(NH4 = 'ueq/l',
                                             Na = 'ueq/l',
                                             K = 'ueq/l',
                                             Cl = 'ueq/l',
                                             NO3 = 'ueq/l',
                                             SO4 = 'ueq/l',
                                             PO4 = 'ueq/l',
                                             Si = 'mmol/l',
                                             TN = 'mmol/l',
                                             TDN = 'mmol/l',
                                             TPN = 'mmol/l',
                                             TDP = 'mmol/l',
                                             TPP = 'mmol/l',
                                             DOP = 'mmol/l',
                                             TIP = 'mmol/l',
                                             H = 'ueq/l'),
                      convert_units_to = c(NH4 = 'mg/l',
                                           Na = 'mg/l',
                                           K = 'mg/l',
                                           Cl = 'mg/l',
                                           NO3 = 'mg/l',
                                           SO4 = 'mg/l',
                                           PO4 = 'mg/l',
                                           Si = 'mg/l',
                                           TN = 'mg/l',
                                           TDN = 'mg/l',
                                           TPN = 'mg/l',
                                           TDP = 'mg/l',
                                           TPP = 'mg/l',
                                           DOP = 'mg/l',
                                           TIP = 'mg/l',
                                           H = 'mg/l'))
  
  d <- ue(synchronize_timestep(d,
                               desired_interval = '1 day', #set to '15 min' when we have server
                               impute_limit = 30))
  
  d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
  
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
    
    d <- ue(ms_read_raw_csv(filepath = rawfile,
                            datetime_cols = list('date' = '%Y-%m-%d',
                                                 'time' = '%H%M'),
                            datetime_tz = 'US/Mountain',
                            site_name_col = 'local_site',
                            data_cols =  c('pH' = 'pH',
                                           'cond' = 'SpCond',
                                           'ANC' = 'ANC',
                                           'acid' = 'acidity',
                                           'alkal' = 'alkalinity',
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
                                           'IN' = 'IN',
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
                            is_sensor = FALSE))
    
    d <- ue(ms_cast_and_reflag(d,
                               varflag_col_pattern = NA))
    
    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))
    
    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'mmol/l',
                                               TN = 'mmol/l',
                                               TDN = 'mmol/l',
                                               TPN = 'mmol/l',
                                               TDP = 'mmol/l',
                                               TPP = 'mmol/l',
                                               DOP = 'mmol/l',
                                               TIP = 'mmol/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             H = 'mg/l'))
    
    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))
    
    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
    
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
    
    look <- read_csv(rawfile)
    
    d <- ue(ms_read_raw_csv(filepath = rawfile,
                            datetime_cols = list('date' = '%Y-%m-%d',
                                                 'time' = '%H%M'),
                            datetime_tz = 'US/Mountain',
                            site_name_col = 'local_site',
                            alt_site_name = list('GREEN1' = 'GREEN LAKE 1'),
                            data_cols =  c('pH' = 'pH',
                                           'cond' = 'SpCond',
                                           'ANC' = 'ANC',
                                           'acid' = 'acidity',
                                           'alkal' = 'alkalinity',
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
                                           'IN' = 'IN',
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
                            is_sensor = FALSE))
    
    d <- ue(ms_cast_and_reflag(d,
                               varflag_col_pattern = NA))
    
    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))
    
    d <- ms_conversions(d,
                        convert_units_from = c(NH4 = 'ueq/l',
                                               Na = 'ueq/l',
                                               K = 'ueq/l',
                                               Cl = 'ueq/l',
                                               NO3 = 'ueq/l',
                                               SO4 = 'ueq/l',
                                               PO4 = 'ueq/l',
                                               Si = 'mmol/l',
                                               TN = 'mmol/l',
                                               TDN = 'mmol/l',
                                               TPN = 'mmol/l',
                                               TDP = 'mmol/l',
                                               TPP = 'mmol/l',
                                               DOP = 'mmol/l',
                                               TIP = 'mmol/l',
                                               H = 'ueq/l'),
                        convert_units_to = c(NH4 = 'mg/l',
                                             Na = 'mg/l',
                                             K = 'mg/l',
                                             Cl = 'mg/l',
                                             NO3 = 'mg/l',
                                             SO4 = 'mg/l',
                                             PO4 = 'mg/l',
                                             Si = 'mg/l',
                                             TN = 'mg/l',
                                             TDN = 'mg/l',
                                             TPN = 'mg/l',
                                             TDP = 'mg/l',
                                             TPP = 'mg/l',
                                             DOP = 'mg/l',
                                             TIP = 'mg/l',
                                             H = 'mg/l'))
    
    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))
    
    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
    
    return(d)
}



