
#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_2504 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_6686 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_2644 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_2497 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_4135 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2740 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2532 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2491 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2494 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2531 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2475 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2543 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_5491 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_5492 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2415 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2425 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_2504 <- function(network, domain, prodname_ms, site_name, component) {

    #CZO Site: https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2504/
    #Hydroshare: https://www.hydroshare.org/resource/c0e5094d1de54547a304d4dec3a7b3ff/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    if(!grepl('MCZOB', component)) {

        col_names <- names(d)
        site_name_cols <- col_names[!grepl('DateTime|time', col_names)]
        datetime_col <- col_names[grepl('DateTime|time', col_names)]

        d <- d %>%
            pivot_longer(cols = site_name_cols, names_to = 'sites', values_to = 'discharge') %>%
            filter(discharge != '[cfs]') %>%
            mutate(time = str_split_fixed(.data[[datetime_col]], ' ', n = Inf)[,2]) %>%
            mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
            mutate(date = str_split_fixed(.data[[datetime_col]], ' ', n = Inf)[,1])

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'US/Mountain',
                             site_name_col = 'sites',
                             data_cols =  'discharge',
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE)

    } else{

        d <- d %>%
            filter(Streamflow != 'cfs') %>%
            mutate(time = str_split_fixed(datetime, ' ', n = Inf)[,2]) %>%
            mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
            mutate(date = str_split_fixed(datetime, ' ', n = Inf)[,1]) %>%
            mutate(site = 'FLUME_MCZOB')

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'US/Mountain',
                             site_name_col = 'site',
                             data_cols =  c('Streamflow' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE)
    }

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    #cubic feet to liters
    d <- d %>%
        mutate(val = val*28.317)

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

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_6686 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/6686/
    #https://www.hydroshare.org/resource/7703d249f062428b8229c03ce072e5c6/

    #this product is only for one site (Bigelow) as opposed to discharge__2504
    #which has many sites in one product
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(StreamFlow != 'L/s') %>%
        mutate(time = str_split_fixed(DateTime, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
        mutate(date = str_split_fixed(DateTime, ' ', n = Inf)[,1]) %>%
        mutate(site = 'Bigelow')

    if(grepl('discharge', prodname_ms)){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'US/Mountain',
                             site_name_col = 'site',
                             data_cols =  c('StreamFlow' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
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

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'US/Mountain',
                             site_name_col = 'site',
                             data_cols =  c('WaterTemp' = 'temp'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)

        return(d)
    }
}

#discharge: STATUS=PENDING
#. handle_errors
process_1_2644 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2644/
    #https://www.hydroshare.org/resource/3c1fd54381764e5b8865609cb4127d63/

    #this product is only for one site (MarshallGulch), similar to discharge__6686
    return()
}

#discharge; stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_2497 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2497/
    #https://www.hydroshare.org/resource/9d7dd6ca40984607ad74a00ab0b5121b/

    #This product contains one site (OracleRidge) with both discharge and temp,
    # similar to discharge__2504
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_4135 <- function(network, domain, prodname_ms, site_name, component) {

    # CZO Site: https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/4135/
    # Hydroshare: https://www.hydroshare.org/resource/553c42d3a8ee40309b2d77071aa25f2e/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(time = str_split_fixed(DateTime, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
        mutate(date = str_split_fixed(DateTime, ' ', n = Inf)[,1]) %>%
        mutate(SiteCode = str_replace_all(SiteCode, '[/]', '_'))

    #SRP same as Ortho-P?
    #Most metals are reported as their isotope, not sure to keep istope form in
    #varible name or change to just element.
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'SiteCode',
                         alt_site_name = list('HistoryGrove' = c('FLUME_HG', 'FLUME_HG16', 'FLUME_HG_16'),
                                              'LowerJaramillo' = 'FLUME_LJ',
                                              'UpperJaramillo' = 'FLUME_UJ',
                                              'LowerLaJara' = c('FLUME_LLJ', 'FLUME_LLJ16', 'FLUME_LLJ_16'),
                                              'UpperRedondo' = 'FLUME_UR',
                                              'RedondoMeadow' = 'FLUME_RM',
                                              'UpperLaJara' = 'FLUME_ULJ'),
                         data_cols =  c('pH', 'Temp' = 'temp', 'EC' = 'spCond',
                                        'TIC', 'TOC', 'TN', 'F', 'Cl', 'NO2',
                                        'Br', 'NO3', 'SO4', 'PO4', 'Ca', 'Mg',
                                        'Na', 'K', 'Sr', 'Si', 'B', 'Be9' = 'Be',
                                        'Al27' = 'Al', 'Ti49', 'V51' = 'V',
                                        'Cr52' = 'Cr', 'Mn55' = 'Mn', 'Fe56' = 'Fe',
                                        'Co59' = 'Co', 'Ni60', 'Cu63' = 'Cu',
                                        'Zn64' = 'Zn', 'As75' = 'As', 'Se78',
                                        'Y89' = 'Y', 'Mo98' = 'Mo', 'Ag107' = 'Ag',
                                        'Cd114' = 'Cd', 'Sn118', 'Sb121' = 'Sb',
                                        'Ba138' = 'Ba', 'La139' = 'La', 'Ce140' = 'Ce',
                                        'Pr141' = 'Pr', 'Nd145', 'Sm147',
                                        'Eu153' = 'Eu', 'Gd157', 'Tb159' = 'Tb',
                                        'Dy164' = 'Dy', 'Ho165' = 'Ho', 'Er166' = 'Er',
                                        'Tm169' = 'Tm', 'Yb174' = 'Yb', 'Lu175' = 'Lu',
                                        'TI205' = 'Tl', 'Pb208' = 'Pb', 'U238' = 'U',
                                        'NH4.N' = 'NH4_N', 'Ortho.P' = 'SRP'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('Be' = 'ug/l',
                                               'Al' = 'ug/l',
                                               'Ti49' = 'ug/l',
                                               'V' = 'ug/l',
                                               'Cr' = 'ug/l',
                                               'Mn' = 'ug/l',
                                               'Fe' = 'ug/l',
                                               'Co' = 'ug/l',
                                               'Ni60' = 'ug/l',
                                               'Cu' = 'ug/l',
                                               'Zn' = 'ug/l',
                                               'As' = 'ug/l',
                                               'Se78' = 'ug/l',
                                               'Y' = 'ug/l',
                                               'Mo' = 'ug/l',
                                               'Ag' = 'ug/l',
                                               'Cd' = 'ug/l',
                                               'Sn' = 'ug/l',
                                               'Sb' = 'ug/l',
                                               'Ba' = 'ug/l',
                                               'La' = 'ng/l',
                                               'Ce' = 'ng/l',
                                               'Pr' = 'ng/l',
                                               'Nd145' = 'ng/l',
                                               'Sm147' = 'ng/l',
                                               'Eu' = 'ng/l',
                                               'Gd157' = 'ng/l',
                                               'Tb' = 'ng/l',
                                               'Dy' = 'ng/l',
                                               'Ho' = 'ng/l',
                                               'Er' = 'ng/l',
                                               'Tm' = 'ng/l',
                                               'Yb' = 'ng/l',
                                               'Lu' = 'ng/l',
                                               'Tl' = 'ug/l',
                                               'Pb' = 'ug/l',
                                               'U' = 'ng/l'),
                       convert_units_to = c('Be' = 'mg/l',
                                            'Al' = 'mg/l',
                                            'Ti49' = 'mg/l',
                                            'V' = 'mg/l',
                                            'Cr' = 'mg/l',
                                            'Mn' = 'mg/l',
                                            'Fe' = 'mg/l',
                                            'Co' = 'mg/l',
                                            'Ni60' = 'mg/l',
                                            'Cu' = 'mg/l',
                                            'Zn' = 'mg/l',
                                            'As' = 'mg/l',
                                            'Se78' = 'mg/l',
                                            'Y' = 'mg/l',
                                            'Mo' = 'mg/l',
                                            'Ag' = 'mg/l',
                                            'Cd' = 'mg/l',
                                            'Sn' = 'mg/l',
                                            'Sb' = 'mg/l',
                                            'Ba' = 'mg/l',
                                            'La' = 'mg/l',
                                            'Ce' = 'mg/l',
                                            'Pr' = 'mg/l',
                                            'Nd145' = 'mg/l',
                                            'Sm147' = 'mg/l',
                                            'Eu' = 'mg/l',
                                            'Gd157' = 'mg/l',
                                            'Tb' = 'mg/l',
                                            'Dy' = 'mg/l',
                                            'Ho' = 'mg/l',
                                            'Er' = 'mg/l',
                                            'Tm' = 'mg/l',
                                            'Yb' = 'mg/l',
                                            'Lu' = 'mg/l',
                                            'Tl' = 'mg/l',
                                            'Pb' = 'mg/l',
                                            'U' = 'mg/l'))

    #The remaing stemps in the normal data aquisition are not happening here becuase of issues that
    #arise in these steps when there is only one value for a site in the dataframe.
    #The remaining munge steps will happen in a derive kernel that combines all stream chemistry data

    # d <- carry_uncertainty(d,
    #                        network = network,
    #                        domain = domain,
    #                        prodname_ms = prodname_ms)
    #
    # d <- synchronize_timestep(d,
    #                           desired_interval = '1 day', #set to '15 min' when we have server
    #                           impute_limit = 30)
    #
    # d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_2740 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2740/
    #https://www.hydroshare.org/resource/3df05937abfc4cb59b8be04d674c4b48/

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2532 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2532/
    #https://www.hydroshare.org/resource/f5ae15fbdcc9425e847060f89da61557/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        pivot_longer(cols = c('B2_Rain1', 'B2_Rain2'), names_to = 'sites', values_to = 'precip') %>%
        mutate(time = str_split_fixed(DateTime, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
        mutate(date = str_split_fixed(DateTime, ' ', n = Inf)[,1]) %>%
        filter(DateTime != 'MST')

    #SRP same as Ortho-P?
    #Most metals are reported as their isotope, not sure to keep istope form in
    #varible name or change to just element.
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'sites',
                         data_cols =  c('precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
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

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2491 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/346cf577d01e4b2b90ff1cef86e789bb/
    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2494 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/1dc7b0975ae2469a96e3458075d3b75c/

    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2531 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2531/
    #https://www.hydroshare.org/resource/172de7fe091f48d98b1d380da5851932/

    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2475 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/0ba983afc62647dc8cfbd91058cfc56d/

    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2543 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2543/
    #https://www.hydroshare.org/resource/f7df7b07ea19477ab7ea701c34bc356b/
    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_5491 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/5491/
    #https://www.hydroshare.org/resource/4ab76a12613c493d82b2df57aa970c24/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d %>%
        mutate(time = str_split_fixed(DateTime, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
        mutate(date = str_split_fixed(DateTime, ' ', n = Inf)[,1]) %>%
        filter(SamplingMethod == 'PrcpColl')

    #SRP same as Ortho-P?
    #Most metals are reported as their isotope, not sure to keep istope form in
    #varible name or change to just element.
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'SiteCode',
                         data_cols =  c('pH', 'EC' = 'spCond',
                                        'TIC', 'TOC', 'TN', 'F.' = 'F', 'Cl.' = 'Cl',
                                        'NO2.' = 'NO2', 'Br.' = 'Br', 'NO3.' = 'NO3',
                                        'SO4', 'PO4', 'Be9' = 'Be', 'B11' = 'B',
                                        'Al27' = 'Al', 'Na23' = 'Na', 'Mg24' = 'Mg',
                                        'Si28' = 'Si', 'K39' = 'K', 'Ca40' = 'Ca',
                                        'Ti49', 'V51' = 'V', 'Cr52' = 'Cr',
                                        'Mn55' = 'Mn', 'Fe56' = 'Fe', 'Co59' = 'Co',
                                        'Ni60', 'Cu63' = 'Cu', 'Zn64' = 'Zn',
                                        'As75' = 'As', 'Se78', 'Y89' = 'Y', 'Mo98' = 'Mo',
                                        'Ag107' = 'Ag', 'Sr88' = 'Sr', 'Sn118',
                                        'Sb121' = 'Sb', 'Ba138' = 'Ba', 'La139' = 'La',
                                        'Ce140' = 'Ce', 'Pr141' = 'Pr', 'Nd145',
                                        'Sm147', 'Eu153' = 'Eu', 'Gd157', 'Tb159' = 'Tb',
                                        'Dy164' = 'Dy', 'Ho165' = 'Ho', 'Er166' = 'Er',
                                        'Tm169' = 'Tm', 'Yb174' = 'Yb', 'Lu175' = 'Lu',
                                        'Tl205' = 'Tl', 'Pb208' = 'Pb', 'U238' = 'U',
                                        'NH4.N' = 'NH4_N', 'Ortho.P' = 'SRP'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('Be' = 'ug/l',
                                               'Al' = 'ug/l',
                                               'Ti49' = 'ug/l',
                                               'V' = 'ug/l',
                                               'Cr' = 'ug/l',
                                               'Mn' = 'ug/l',
                                               'Fe' = 'ug/l',
                                               'Co' = 'ug/l',
                                               'Ni60' = 'ug/l',
                                               'Cu' = 'ug/l',
                                               'Zn' = 'ug/l',
                                               'As' = 'ug/l',
                                               'Se78' = 'ug/l',
                                               'Y' = 'ug/l',
                                               'Mo' = 'ug/l',
                                               'Ag' = 'ug/l',
                                               'Cd' = 'ug/l',
                                               'Sn118' = 'ug/l',
                                               'Sb' = 'ug/l',
                                               'Ba' = 'ug/l',
                                               'La' = 'ng/l',
                                               'Ce' = 'ng/l',
                                               'Pr' = 'ng/l',
                                               'Nd145' = 'ng/l',
                                               'Sm147' = 'ng/l',
                                               'Eu' = 'ng/l',
                                               'Gd157' = 'ng/l',
                                               'Tb' = 'ng/l',
                                               'Dy' = 'ng/l',
                                               'Ho' = 'ng/l',
                                               'Er' = 'ng/l',
                                               'Tm' = 'ng/l',
                                               'Yb' = 'ng/l',
                                               'Lu' = 'ng/l',
                                               'Tl' = 'ug/l',
                                               'Pb' = 'ug/l',
                                               'U' = 'ng/l'),
                        convert_units_to = c('Be' = 'mg/l',
                                             'Al' = 'mg/l',
                                             'Ti49' = 'mg/l',
                                             'V' = 'mg/l',
                                             'Cr' = 'mg/l',
                                             'Mn' = 'mg/l',
                                             'Fe' = 'mg/l',
                                             'Co' = 'mg/l',
                                             'Ni60' = 'mg/l',
                                             'Cu' = 'mg/l',
                                             'Zn' = 'mg/l',
                                             'As' = 'mg/l',
                                             'Se78' = 'mg/l',
                                             'Y' = 'mg/l',
                                             'Mo' = 'mg/l',
                                             'Ag' = 'mg/l',
                                             'Cd' = 'mg/l',
                                             'Sn118' = 'mg/l',
                                             'Sb' = 'mg/l',
                                             'Ba' = 'mg/l',
                                             'La' = 'mg/l',
                                             'Ce' = 'mg/l',
                                             'Pr' = 'mg/l',
                                             'Nd145' = 'mg/l',
                                             'Sm147' = 'mg/l',
                                             'Eu' = 'mg/l',
                                             'Gd157' = 'mg/l',
                                             'Tb' = 'mg/l',
                                             'Dy' = 'mg/l',
                                             'Ho' = 'mg/l',
                                             'Er' = 'mg/l',
                                             'Tm' = 'mg/l',
                                             'Yb' = 'mg/l',
                                             'Lu' = 'mg/l',
                                             'Tl' = 'mg/l',
                                             'Pb' = 'mg/l',
                                             'U' = 'mg/l'))

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

#precip_chemistry: STATUS=PENDING
#. handle_errors
process_1_5492 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/5492/
    #https://www.hydroshare.org/resource/38c0e61607e74c329ac798c8001bfa95/

    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2415 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/5ca1378090884e72a8dcb796d882bb07/

    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2425 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/1db7abcf0eb64ef49000c46a6133949d/

    return()
}
