
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
                             is_sensor = TRUE,
                             sampling_type = 'I')

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
                             is_sensor = TRUE,
                             sampling_type = 'I')
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
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)

        d <- carry_uncertainty(d,
                               network = network,
                               domain = domain,
                               prodname_ms = prodname_ms)

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
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)

        d <- carry_uncertainty(d,
                               network = network,
                               domain = domain,
                               prodname_ms = prodname_ms)

        return(d)
    }
}

#discharge: STATUS=READY
#. handle_errors
process_1_2644 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2644/
    #https://www.hydroshare.org/resource/3c1fd54381764e5b8865609cb4127d63/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(StreamFlow != 'L/s') %>%
        mutate(site = 'MarshallGulch')


    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('StreamFlow' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_2497 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2497/
    #https://www.hydroshare.org/resource/9d7dd6ca40984607ad74a00ab0b5121b/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(DateTime != 'MST') %>%
        mutate(site = 'OracleRidge')

    if(grepl('stream_chemistry', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('DateTime' = '%m/%d/%Y %H:%M'),
                             datetime_tz = 'US/Mountain',
                             site_name_col = 'site',
                             data_cols =  c('Temp' = 'temp'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE,
                             sampling_type = 'I')

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('DateTime' = '%m/%d/%Y %H:%M'),
                             datetime_tz = 'US/Mountain',
                             site_name_col = 'site',
                             data_cols =  c('Flow' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE,
                             sampling_type = 'I')
    }

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
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

    col_names <- colnames(d)
    units <- as.character(d[1,])

    names(units) <- col_names
    units <- units[! grepl(paste(c('DateTime', 'SiteCode', 'SampleCode', 'Sampling',
                                   'Method', 'SampleType', 'SampleMedium', 'pH',
                                   'ph.1','Temp', 'FI', 'HIX', 'SUVA', 'SamplingNote',
                                   'd13C.DIC', 'dD', 'd18O', 'EC', 'DO', 'Alkalinity',
                                   'UVA254'),
                                 collapse = '|'), col_names)]

    units <- units[! grepl('mg/L', units)]

    d <- d %>%
        mutate(SiteCode = str_replace_all(SiteCode, '[/]', '_')) %>%
        filter(! SiteCode %in% c('missing2', 'missing1', ''))

    col_names_to_vars <- c('pH', 'Temp' = 'temp', 'EC' = 'spCond',
                           'TIC', 'TOC', 'TN', 'F', 'Cl', 'NO2',
                           'Br', 'NO3', 'SO4', 'PO4', 'Ca', 'Mg',
                           'Na', 'K', 'Sr', 'Si28' = 'Si', 'B', 'Be9' = 'Be',
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
                           'NH4.N' = 'NH4_N', 'Ca40' = 'Ca', 'Mg24' = 'Mg',
                           'Na23' = 'Na', 'K39' = 'K', 'Sr88' = 'Sr', 'B11' = 'B',
                           'F.' = 'F', 'Cl.' = 'Cl', 'NO2.' = 'NO2', 'Br.' = 'Br',
                           'NO3.' = 'NO3', 'SO4.' = 'SO4', 'PO4.' = 'PO4')

    #Most metals are reported as their isotope, not sure to keep istope form in
    #varible name or change to just element.
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'SiteCode',
                         alt_site_name = list('HistoryGrove' = c('FLUME_HG', 'FLUME_HG16', 'FLUME_HG_16'),
                                              'LowerJaramillo' = 'FLUME_LJ',
                                              'UpperJaramillo' = 'FLUME_UJ',
                                              'LowerLaJara' = c('FLUME_LLJ', 'FLUME_LLJ16', 'FLUME_LLJ_16'),
                                              'UpperRedondo' = 'FLUME_UR',
                                              'RedondoMeadow' = 'FLUME_RM',
                                              'UpperLaJara' = 'FLUME_ULJ',
                                              'LowerRedondo' = 'FLUME_LR'),
                         data_cols = col_names_to_vars,
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    for(i in 1:length(units)){
        new_name <- col_names_to_vars[names(units[i]) == names(col_names_to_vars)]

        if(length(new_name) == 0) { next }

        names(units)[i] <- unname(new_name)
    }

    units <- units[names(units) %in% unname(col_names_to_vars)]

    units <- tolower(units)
    new_units_names <- names(units)
    units <- str_replace_all(units, 'umoles/l', 'umol/l')
    names(units) <- new_units_names

    new_units <- rep('mg/l', length(units))
    names(new_units) <- names(units)

    d <- ms_conversions(d,
                        convert_units_from = units,
                        convert_units_to = new_units)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    #d <- synchronize_timestep(d)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_2740 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2740/
    #https://www.hydroshare.org/resource/3df05937abfc4cb59b8be04d674c4b48/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    if(! c('EC', 'TN', 'TOC') %in% colnames(d)){
        return(NULL)
    }

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('DateTime' = '%m/%e/%Y'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'SiteCode',
                         alt_site_name = list('MarshallGulch' = 'MG_WEIR',
                                              'OracleRidge' = 'OR_low',
                                              'Bigelow' = 'BGZOB_Flume'),
                         data_cols =  c('pH', 'EC' = 'spCond', 'TIC', 'TOC', 'TN',
                                        'F.' = 'F', 'Cl.' = 'Cl', 'NO2.' = 'NO2',
                                        'NO3.' = 'NO3', 'SO4', 'PO4', 'Na23' = 'Na',
                                        'Mg24' = 'Mg', 'Al27' = 'Al', 'Si28' = 'Si',
                                        'K39' = 'K', 'Ca40' = 'Ca', 'Cr52' = 'Cr',
                                        'Mn55' = 'Mn', 'Fe56' = 'Fe', 'Co59' = 'Co',
                                        'Ni60' = 'Ni', 'Cu63' = 'Cu', 'Zn64' = 'Zn',
                                        'Y89' = 'Y', 'Cd111' = 'Cd', 'La139' = 'La',
                                        'Ce140' = 'Ce', 'Pr141' = 'Pr', 'Nd145',
                                        'Sm147', 'Eu153' = 'Eu', 'Gd157',
                                        'Tb159' = 'Tb', 'Dy164' = 'Dy', 'Ho165' = 'Ho',
                                        'Er166' = 'Er', 'Tm169' = 'Tm', 'Yb174' = 'Yb',
                                        'Lu175' = 'Lu', 'Pb208' = 'Pb'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('F' = 'umol/L',
                                               'Cl' = 'umol/L',
                                               'NO2' = 'umol/L',
                                               'NO3' = 'umol/L',
                                               'SO4' = 'umol/L',
                                               'PO4' = 'umol/L',
                                               'Na' = 'ug/L',
                                               'Mg' = 'ug/L',
                                               'Al' = 'ug/L',
                                               'Si' = 'ug/L',
                                               'K' = 'ug/L',
                                               'Ca' = 'ug/L',
                                               'Cr' = 'ug/L',
                                               'Mn' = 'ug/L',
                                               'Fe' = 'ug/L',
                                               'Co' = 'ug/L',
                                               'Ni' = 'ug/L',
                                               'Cu' = 'ug/L',
                                               'Zn' = 'ug/L',
                                               'Y' = 'ng/L',
                                               'Cd' = 'ug/L',
                                               'La' = 'ng/L',
                                               'Ce' = 'ng/L',
                                               'Pr' = 'ng/L',
                                               'Nd145' = 'ng/L',
                                               'Sm147' = 'ng/L',
                                               'Eu' = 'ng/L',
                                               'Gd157' = 'ng/L',
                                               'Tb' = 'ng/L',
                                               'Dy' = 'ng/L',
                                               'Ho' = 'ng/L',
                                               'Er' = 'ng/L',
                                               'Tm' = 'ng/L',
                                               'Yb' = 'ng/L',
                                               'Lu' = 'ng/L',
                                               'Pb' = 'ng/L'),
                        convert_units_to = c('F' = 'mg/L',
                                             'Cl' = 'mg/L',
                                             'NO2' = 'mg/L',
                                             'NO3' = 'mg/L',
                                             'SO4' = 'mg/L',
                                             'PO4' = 'mg/L',
                                             'Na' = 'mg/L',
                                             'Mg' = 'mg/L',
                                             'Al' = 'mg/L',
                                             'Si' = 'mg/L',
                                             'K' = 'mg/L',
                                             'Ca' = 'mg/L',
                                             'Cr' = 'mg/L',
                                             'Mn' = 'mg/L',
                                             'Fe' = 'mg/L',
                                             'Co' = 'mg/L',
                                             'Ni' = 'mg/L',
                                             'Cu' = 'mg/L',
                                             'Zn' = 'mg/L',
                                             'Y' = 'mg/L',
                                             'Cd' = 'mg/L',
                                             'La' = 'mg/L',
                                             'Ce' = 'mg/L',
                                             'Pr' = 'mg/L',
                                             'Nd145' = 'mg/L',
                                             'Sm147' = 'mg/L',
                                             'Eu' = 'mg/L',
                                             'Gd157' = 'mg/L',
                                             'Tb' = 'mg/L',
                                             'Dy' = 'mg/L',
                                             'Ho' = 'mg/L',
                                             'Er' = 'mg/L',
                                             'Tm' = 'mg/L',
                                             'Yb' = 'mg/L',
                                             'Lu' = 'mg/L',
                                             'Pb' = 'mg/L'))

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
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
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2491 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/346cf577d01e4b2b90ff1cef86e789bb/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'Burn_Met_Low')

    if(all(unique(d$Precipitation) == c('[mm]', '-9999'))){
        return(NULL)
    }

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('Precipitation' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2494 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/1dc7b0975ae2469a96e3458075d3b75c/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'Burn_Met_Up')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('Precipitation' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2531 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2531/
    #https://www.hydroshare.org/resource/172de7fe091f48d98b1d380da5851932/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        pivot_longer(cols = c('OR_Rain1', 'OR_Rain2', 'OR_Rain3'), names_to = 'sites', values_to = 'precip') %>%
        filter(DateTime != '[MST]') %>%
        mutate(sites = case_when(sites == 'OR_Rain1' ~ 'OR_PC1',
                                 sites == 'OR_Rain2' ~ 'OR_PC2',
                                 sites == 'OR_Rain3' ~ 'OR_PC3'))

    data_col <- names(d)[!grepl('DateTime|sites', names(d))]

    if(all(unique(d[[data_col]]) == c('[mm]', '-9999'))){
        return(NULL)
    }

    #SRP same as Ortho-P?
    #varible name or change to just element.
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'sites',
                         data_cols =  c('precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2475 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/0ba983afc62647dc8cfbd91058cfc56d/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'MCZOB_met') %>%
        filter(DateTime != '[MST]')

    if(all(unique(d$Precipitation) == c('-9999'))){
        return(NULL)
    }

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('Precipitation' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2543 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2543/
    #https://www.hydroshare.org/resource/f7df7b07ea19477ab7ea701c34bc356b/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    if(grepl('Winter', component)){
        d <- read.csv(rawfile, colClasses = 'character') %>%
            rename(precip = Schist_Winter) %>%
            mutate(sites = 'MG_Schist_Winter') %>%
            filter(DateTime != '[MST]')

    } else{

        d <- read.csv(rawfile, colClasses = 'character')
        names(d) <- str_replace(names(d), '\\.', '')

        pivot_names <- names(d)
        if(pivot_names[1] != 'DateTime'){
            names(d)[1] <- 'DateTime'
            pivot_names <- names(d)
        }

        pivot_names <- pivot_names[!pivot_names == 'DateTime']

        d <- d %>%
            pivot_longer(cols = pivot_names, names_to = 'sites', values_to = 'precip') %>%
            filter(DateTime != '[MST]')
    }

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         alt_site_name = list('MG_PC1' = 'Schist',
                                              'MG_PC2' = 'FernValley',
                                              'MG_Outlet' = 'Outlet',
                                              'MG_BurntMeadow' = 'BurntMeadow',
                                              'MG_DoubleRock' = 'DoubleRock',
                                              'MG_PC3' = 'Granite',
                                              'MG_Weir' = 'Weir',
                                              'MG_Lemmon' = 'MtLemmon'),
                         site_name_col = 'sites',
                         data_cols =  c('precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
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

    method_col <- names(d)[grepl('Method',  names(d))]

    d <- d %>%
        mutate(time = str_split_fixed(DateTime, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
        mutate(date = str_split_fixed(DateTime, ' ', n = Inf)[,1]) %>%
        filter(.data[[method_col]] == 'PrcpColl')

    d <- d %>%
        mutate(SiteCode = case_when(SiteCode == 'B2D-PGC' ~ 'B2D_PG',
                                    SiteCode == 'B2D-PRC' ~ 'B2D_PR',
                                    SiteCode == 'B2D-PSC' ~ 'B2D_PS',
                                    TRUE ~ SiteCode))

    if(! 'SO4' %in% names(d)){
        return(NULL)
    }

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
                                        'NH4.N' = 'NH4_N'),
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

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_5492 <- function(network, domain, prodname_ms, site_name, component) {

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/5491/
    #https://www.hydroshare.org/resource/4ab76a12613c493d82b2df57aa970c24/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    col_names <- colnames(d)
    units <- as.character(d[1,])

    names(units) <- col_names
    units <- units[! grepl(paste(c('DateTime', 'SiteCode', 'SampleCode', 'Sampling',
                                   'Method', 'SampleType', 'SampleMedium', 'pH',
                                   'ph.1','Temp', 'FI', 'HIX', 'SUVA', 'SamplingNote',
                                   'd13C.DIC', 'dD', 'd18O', 'EC', 'DO', 'Alkalinity',
                                   'UVA254', 'Offser', 'Cond'),
                                 collapse = '|'), col_names)]

    units <- units[! grepl('mg/L', units)]

    d <- d %>%
        filter(SiteCode %in% c('RainColl_Burn_Low_OC', 'RainColl_MCZOB'))

    col_names_to_vars <- c('pH', 'EC' = 'spCond', 'Cond' = 'spCond', 'TIC', 'TOC', 'TN', 'F', 'Cl',
                           'NO2', 'Br', 'NO3', 'SO4', 'PO4', 'Be9' = 'Be', 'B11' = 'B',
                           'Na23' = 'Na', 'Mg24' = 'Mg', 'Al27' = 'Al', 'Si28' = 'Si',
                           'P31' = 'P', 'K39' = 'K', 'Ca40' = 'Ca', 'Ti49',
                           'V52' = 'V', 'Cr52' = 'Cr', 'Mn55' = 'Mn', 'Fe56' = 'Fe',
                           'Co59' = 'Co', 'Ni60', 'Cu63' = 'Cu', 'Zn64' = 'Zn',
                           'As75' = 'As', 'Se78', 'Y89' = 'Y', 'Mo98' = 'Mo',
                           'Ag107' = 'Ag', 'Cd114' = 'Cd', 'Sn118',
                           'Sb121' = 'Sb', 'Ba138' = 'Ba', 'La139' = 'La',
                           'Ce140' = 'Ce', 'Pr141' = 'Pr', 'Nd145',
                           'Sm147' = 'Sm', 'Eu153' = 'Eu', 'Gd157',
                           'Tb159' = 'Tb', 'Dy164' = 'Dy', 'Ho165' = 'Ho',
                           'Er166' = 'Er', 'Tm169' = 'Tm', 'Yb174' = 'Yb',
                           'Lu175' = 'Lu', 'Tl205' = 'Tl', 'Pb208' = 'Pb',
                           'U238' = 'U', 'F.'= 'F', 'Cl.' = 'Cl', 'NO2.' = 'NO2',
                           'Br.' = 'Br', 'NO3.' = 'NO3', 'SO4.' = 'SO4',
                           'PO4.' = 'PO4')

    if(nrow(d) == 0){
        return(NULL)
    }

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'SiteCode',
                         alt_site_name = list('Burn_Met_Low' = 'RainColl_Burn_Low_OC',
                                              'MCZOB_met' = 'RainColl_MCZOB'),
                         data_cols =  col_names_to_vars,
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    for(i in 1:length(units)){
        new_name <- col_names_to_vars[names(units[i]) == names(col_names_to_vars)]

        if(length(new_name) == 0) { next }

        names(units)[i] <- unname(new_name)
    }

    units <- units[names(units) %in% unname(col_names_to_vars)]

    units <- tolower(units)
    new_units_names <- names(units)
    units <- str_replace_all(units, 'umoles/l', 'umol/l')
    names(units) <- new_units_names

    new_units <- rep('mg/l', length(units))
    names(new_units) <- names(units)

    d <- ms_conversions(d,
                        convert_units_from = units,
                        convert_units_to = new_units)
    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2415 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/5ca1378090884e72a8dcb796d882bb07/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'MC_flux_tower')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('PRECIP' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2425 <- function(network, domain, prodname_ms, site_name, component) {

    #https://www.hydroshare.org/resource/1db7abcf0eb64ef49000c46a6133949d/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'Ponderosa_flux_tower')

    precip_col <- names(d)[grepl('PREC|PRECIP', names(d))]

    precip_cat <- c('precipitation')
    names(precip_cat) <- precip_col

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  precip_cat,
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    return(d)

}

#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    files <- ms_list_files(network = network,
                           domain = domain,
                           prodname_ms = c('discharge__2504',
                                           'discharge__6686',
                                           'discharge__2644',
                                           'discharge__2497'))

    site_feather <- str_split_fixed(files, '/', n = Inf)[,6]
    sites <- unique(str_split_fixed(site_feather, '[.]', n = Inf)[,1])

    d <- tibble()
    for(i in 1:length(sites)) {
        site_files <- grep(sites[i], files, value = TRUE)

        site_full <- map_dfr(site_files, read_feather)

        d <- rbind(d, site_full)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(X = d,
                                 network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms,
                                 ignore_pred = TRUE)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_name == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = sites[i],
                      level = 'derived',
                      shapefile = FALSE)
    }

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

    files <- ms_list_files(network = network,
                           domain = domain,
                           prodname_ms = c('stream_chemistry__6686',
                                           'stream_chemistry__2497',
                                           'stream_chemistry__4135',
                                           'stream_chemistry__2740'))

    site_feather <- str_split_fixed(files, '/', n = Inf)[,6]
    sites <- unique(str_split_fixed(site_feather, '[.]', n = Inf)[,1])

    d <- tibble()
    for(i in 1:length(sites)) {
        site_files <- grep(sites[i], files, value = TRUE)

        site_full <- map_dfr(site_files, read_feather)

        d <- rbind(d, site_full)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(X = d,
                                 network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms,
                                 ignore_pred = TRUE)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_name == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = sites[i],
                      level = 'derived',
                      shapefile = FALSE)
    }

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms){

    precip_prodname_ms <- get_derive_ingredient(network = network,
                                                domain = domain,
                                                prodname = 'precipitation',
                                                ignore_derprod = TRUE,
                                                accept_multiple = TRUE)

    files <- ms_list_files(network = network,
                           domain = domain,
                           prodname_ms = precip_prodname_ms)

    site_feather <- str_split_fixed(files, '/', n = Inf)[,6]
    sites <- unique(str_split_fixed(site_feather, '[.]', n = Inf)[,1])

    d <- tibble()
    for(i in 1:length(sites)) {
        site_files <- grep(sites[i], files, value = TRUE)

        site_full <- map_dfr(site_files, read_feather)

        d <- rbind(d, site_full)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms, ignore_pred = TRUE)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_name == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = sites[i],
                      level = 'derived',
                      shapefile = FALSE)
    }

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_2_ms004 <- function(network, domain, prodname_ms){

    precip_prodname_ms <- get_derive_ingredient(network = network,
                                                domain = domain,
                                                prodname = 'precip_chemistry',
                                                ignore_derprod = TRUE,
                                                accept_multiple = TRUE)

    files <- ms_list_files(network = network,
                           domain = domain,
                           prodname_ms = precip_prodname_ms)

    site_feather <- str_split_fixed(files, '/', n = Inf)[,6]
    sites <- unique(str_split_fixed(site_feather, '[.]', n = Inf)[,1])

    d <- tibble()
    for(i in 1:length(sites)) {
        site_files <- grep(sites[i], files, value = TRUE)

        site_full <- map_dfr(site_files, read_feather)

        d <- rbind(d, site_full)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms, ignore_pred = TRUE)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_name == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = sites[i],
                      level = 'derived',
                      shapefile = FALSE)
    }

    return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms005 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms006 <- stream_gauge_from_site_data

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms007 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms008 <- derive_precip_pchem_pflux
