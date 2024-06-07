
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
process_1_2504 <- function(network, domain, prodname_ms, site_code, component){

    #CZO Site: https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2504/
    #Hydroshare: https://www.hydroshare.org/resource/c0e5094d1de54547a304d4dec3a7b3ff/

    if(component %in% c('Jemez_Methods.csv', 'Jemez_Sites.csv')) {
        return(NULL)
    }

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    if(!grepl('MCZOB', component)) {

        col_names <- names(d)
        site_code_cols <- col_names[!grepl('DateTime|time', col_names)]
        datetime_col <- col_names[grepl('DateTime|time', col_names)]

        d <- d %>%
            pivot_longer(cols = site_code_cols, names_to = 'sites', values_to = 'discharge') %>%
            filter(discharge != '[cfs]') %>%
            mutate(time = str_split_fixed(.data[[datetime_col]], ' ', n = Inf)[,2]) %>%
            mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
            mutate(date = str_split_fixed(.data[[datetime_col]], ' ', n = Inf)[,1])

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Etc/GMT-7',
                             site_code_col = 'sites',
                             data_cols =  'discharge',
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE,
                             sampling_type = 'I')

    } else {

        d <- d %>%
            filter(Streamflow != 'cfs') %>%
            mutate(time = str_split_fixed(datetime, ' ', n = Inf)[,2]) %>%
            mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
            mutate(date = str_split_fixed(datetime, ' ', n = Inf)[,1]) %>%
            mutate(site = 'FLUME_MCZOB')

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Etc/GMT-7',
                             site_code_col = 'site',
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

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_6686 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/6686/
    #https://www.hydroshare.org/resource/7703d249f062428b8229c03ce072e5c6/

    #this product is only for one site (Bigelow) as opposed to discharge__2504
    #which has many sites in one product
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(StreamFlow != 'L/s') %>%
        mutate(time = str_split_fixed(DateTime, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
        mutate(date = str_split_fixed(DateTime, ' ', n = Inf)[,1]) %>%
        mutate(site = 'Bigelow') %>%
        filter(DateTime != '')

    if(grepl('discharge', prodname_ms)){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Etc/GMT-7',
                             site_code_col = 'site',
                             data_cols =  c('StreamFlow' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999',
                                           ''),
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)

        d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

        return(d)

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('date' = '%m/%e/%Y',
                                                  'time' = '%H:%M'),
                             datetime_tz = 'Etc/GMT-7',
                             site_code_col = 'site',
                             data_cols =  c('WaterTemp' = 'temp'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999',
                                           ''),
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)

        d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

        return(d)
    }
}

#discharge: STATUS=READY
#. handle_errors
process_1_2644 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2644/
    #https://www.hydroshare.org/resource/3c1fd54381764e5b8865609cb4127d63/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(StreamFlow != 'L/s') %>%
        mutate(site = 'MarshallGulch')


    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c('StreamFlow' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_2497 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2497/
    #https://www.hydroshare.org/resource/9d7dd6ca40984607ad74a00ab0b5121b/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(DateTime != 'MST') %>%
        mutate(site = 'OracleRidge')

    if(grepl('stream_chemistry', prodname_ms)) {

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                             datetime_tz = 'Etc/GMT-7',
                             site_code_col = 'site',
                             data_cols =  c('Temp' = 'temp'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE,
                             sampling_type = 'I')

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                             datetime_tz = 'Etc/GMT-7',
                             site_code_col = 'site',
                             data_cols =  c('Flow' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                             is_sensor = TRUE,
                             sampling_type = 'I')
    }

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_4135 <- function(network, domain, prodname_ms, site_code, component){

    # CZO Site: https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/4135/
    # Hydroshare: https://www.hydroshare.org/resource/553c42d3a8ee40309b2d77071aa25f2e/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    col_names <- colnames(d)
    units <- as.character(d[1, ])

    d <- d %>%
        as_tibble() %>%
        mutate(SiteCode = str_replace_all(SiteCode, '[/]', '_')) %>%
        filter(! SiteCode %in% c('missing2', 'missing1', ''))

    ## standardize variable names

    priority <- rep(NA, ncol(d))
    for(i in seq_along(col_names)){

        cn <- col_names[i]
        ms_name <- purrr::keep(catalina_varname_priorities,  ~ cn %in% .x) %>%
            names()

        if(! length(ms_name)) next

        priority[i] <- which(catalina_varname_priorities[[ms_name]] == cn)
        colnames(d)[i] <- ms_name
    }

    ## choose among equivalent variables

    col_names_new <- colnames(d)
    dupers <- col_names_new[duplicated(col_names_new)]
    drop_these <- c()
    for(dup in dupers){

        dup_set <- which(col_names_new == dup)
        keep_this_one <- which.min(priority[dup_set])
        drop_these <- c(drop_these, dup_set[-keep_this_one])
    }

    d <- select(d, ! drop_these)
    if(length(drop_these)){
        units <- units[-drop_these]
        col_names_new <- col_names_new[-drop_these]
    }
    unit_names <- col_names_new

    ## determine which units need to be converted

    nonconvert_units <- grepl(paste(units_to_ignore,
                                    collapse = '|'),
                              colnames(d))

    units <- units[! nonconvert_units]
    unit_names <- unit_names[! nonconvert_units]

    more_nonconverts <- grepl('mg/L', units)

    units <- units[! more_nonconverts]
    unit_names <- unit_names[! more_nonconverts]
    names(units) <- unit_names

    if(any(nchar(units) == 0 | is.na(units))){
        message('unit problem')
        browser()
    }
    if(any(! grepl('/L$', units))){
        message('maybe unit problem')
        browser()
    }

    ## back to normal

    vars_encountered <- intersect(names(catalina_varname_priorities),
                                  colnames(d))

    d <- ms_read_raw_csv(
        preprocessed_tibble = d,
        datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
        datetime_tz = 'Etc/GMT-7',
        site_code_col = 'SiteCode',
        alt_site_code = list('HistoryGrove' = c('FLUME_HG', 'FLUME_HG16', 'FLUME_HG_16'),
                             'LowerJaramillo' = 'FLUME_LJ',
                             'UpperJaramillo' = 'FLUME_UJ',
                             'LowerLaJara' = c('FLUME_LLJ', 'FLUME_LLJ16', 'FLUME_LLJ_16'),
                             'UpperRedondo' = 'FLUME_UR',
                             'RedondoMeadow' = 'FLUME_RM',
                             'UpperLaJara' = 'FLUME_ULJ',
                             'LowerRedondo' = 'FLUME_LR'),
        data_cols = vars_encountered,
        data_col_pattern = '#V#',
        set_to_NA = errorcode_variants,
        is_sensor = FALSE
    )

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    new_units <- setNames(rep('mg/L',
                              length(units)),
                          names(units))

    units <- sub('moles', 'mol', units)
    d <- ms_conversions(d,
                        convert_units_from = units,
                        convert_units_to = new_units)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    #this happens in derive kernel for catalina
    # d <- synchronize_timestep(d)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_2740 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2740/
    #https://www.hydroshare.org/resource/3df05937abfc4cb59b8be04d674c4b48/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')
    col_names <- colnames(d)
    units <- as.character(d[1, ])

    d <- d %>%
        as_tibble() %>%
        mutate(SiteCode = str_replace_all(SiteCode, '[/]', '_')) %>%
        filter(! SiteCode %in% c('missing2', 'missing1', '', '-'))

    ## standardize variable names

    priority <- rep(NA, ncol(d))
    for(i in seq_along(col_names)){

        cn <- col_names[i]
        ms_name <- purrr::keep(catalina_varname_priorities,  ~ cn %in% .x) %>%
            names()

        if(! length(ms_name)) next

        priority[i] <- which(catalina_varname_priorities[[ms_name]] == cn)
        colnames(d)[i] <- ms_name
    }

    ## choose among equivalent variables

    col_names_new <- colnames(d)
    dupers <- col_names_new[duplicated(col_names_new)]
    drop_these <- c()
    for(dup in dupers){

        dup_set <- which(col_names_new == dup)
        keep_this_one <- which.min(priority[dup_set])
        drop_these <- c(drop_these, dup_set[-keep_this_one])
    }

    d <- select(d, ! drop_these)
    if(length(drop_these)){
        units <- units[-drop_these]
        col_names_new <- col_names_new[-drop_these]
    }
    unit_names <- col_names_new

    ## determine which units need to be converted

    nonconvert_units <- grepl(paste(units_to_ignore,
                                    collapse = '|'),
                              colnames(d))

    units <- units[! nonconvert_units]
    unit_names <- unit_names[! nonconvert_units]

    more_nonconverts <- grepl('mg/L', units)

    units <- units[! more_nonconverts]
    unit_names <- unit_names[! more_nonconverts]
    names(units) <- unit_names

    if(any(nchar(units) == 0 | is.na(units))) stop('unit problem')
    if(any(! grepl('/L$', units))) stop('maybe unit problem')

    ## back to normal

    vars_encountered <- intersect(names(catalina_varname_priorities),
                                  colnames(d))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'SiteCode',
                         alt_site_code = list('MarshallGulch' = 'MG_WEIR',
                                              'OracleRidge' = 'OR_low',
                                              'Bigelow' = 'BGZOB_Flume'),
                         data_cols =  vars_encountered,
                         data_col_pattern = '#V#',
                         set_to_NA = errorcode_variants,
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    new_units <- setNames(rep('mg/L',
                              length(units)),
                          names(units))

    units <- sub('moles', 'mol', units)

    if(length(units)){
        d <- ms_conversions(d,
                            convert_units_from = units,
                            convert_units_to = new_units)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    #this happens in derive kernel for catalina
    # d <- synchronize_timestep(d)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2532 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2532/
    #https://www.hydroshare.org/resource/f5ae15fbdcc9425e847060f89da61557/

    if(component %in% c('Catalina_Methods.csv', 'Catalina_Sites.csv')) {
        return(NULL)
    }

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        pivot_longer(cols = c('B2_Rain1', 'B2_Rain2'), names_to = 'sites', values_to = 'precip') %>%
        mutate(time = str_split_fixed(DateTime, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time)) %>%
        mutate(date = str_split_fixed(DateTime, ' ', n = Inf)[,1]) %>%
        filter(DateTime != 'MST')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'sites',
                         data_cols =  c('precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2491 <- function(network, domain, prodname_ms, site_code, component){

    #https://www.hydroshare.org/resource/346cf577d01e4b2b90ff1cef86e789bb/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'Burn_Met_Low')

    if(all(unique(d$Precipitation) == c('[mm]', '-9999'))){
        return(NULL)
    }

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c('Precipitation' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999',
                                       '[mm]'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2494 <- function(network, domain, prodname_ms, site_code, component){

    #https://www.hydroshare.org/resource/1dc7b0975ae2469a96e3458075d3b75c/

    if(component %in% c('Jemez_Sites.csv', 'Jemez_Methods.csv')) {
        return(NULL)
    }

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'Burn_Met_Up')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c('Precipitation' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999',
                                       '[mm]'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2531 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2531/
    #https://www.hydroshare.org/resource/172de7fe091f48d98b1d380da5851932/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    if(component %in% c('Catalina_Methods.csv', 'Catalina_Sites.csv')) {
        return(NULL)
    }

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

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'sites',
                         data_cols =  c('precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2475 <- function(network, domain, prodname_ms, site_code, component){

    #https://www.hydroshare.org/resource/0ba983afc62647dc8cfbd91058cfc56d/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    if(component %in% c('Jemez_Sites.csv', 'Jemez_Methods.csv')) {
        return(NULL)
    }

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'MCZOB_met') %>%
        filter(DateTime != '[MST]')

    if(all(unique(d$Precipitation) == c('-9999'))){
        return(NULL)
    }

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c('Precipitation' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2543 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/2543/
    #https://www.hydroshare.org/resource/f7df7b07ea19477ab7ea701c34bc356b/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
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
                         datetime_tz = 'Etc/GMT-7',
                         alt_site_code = list('MG_PC1' = 'Schist',
                                              'MG_PC2' = 'FernValley',
                                              'MG_Outlet' = 'Outlet',
                                              'MG_BurntMeadow' = 'BurntMeadow',
                                              'MG_DoubleRock' = 'DoubleRock',
                                              'MG_PC3' = 'Granite',
                                              'MG_Weir' = 'Weir',
                                              'MG_Lemmon' = 'MtLemmon'),
                         site_code_col = 'sites',
                         data_cols =  c('precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_5491 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/5491/
    #https://www.hydroshare.org/resource/4ab76a12613c493d82b2df57aa970c24/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')
    units <- as.character(d[1, ])

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

    # stringi::stri_enc_detect(x__)
    d[] <- lapply(d[], function(x){
        x_ = try(gsub(",", "", x), silent = TRUE)
        if(inherits(x_, 'try-error')){
            x <- iconv(x, from = "ISO-8859-1", to = "UTF-8")
        }
        x_ = gsub(",", "", x)
        return(x_)
    })

    if(! nrow(d)){
        return(NULL)
    }

    ## standardize variable names

    col_names <- colnames(d)
    d <- as_tibble(d)

    priority <- rep(NA, ncol(d))
    for(i in seq_along(col_names)){

        cn <- col_names[i]
        ms_name <- purrr::keep(catalina_varname_priorities,  ~ cn %in% .x) %>%
            names()

        if(! length(ms_name)) next

        priority[i] <- which(catalina_varname_priorities[[ms_name]] == cn)
        colnames(d)[i] <- ms_name
    }

    ## choose among equivalent variables

    col_names_new <- colnames(d)
    dupers <- col_names_new[duplicated(col_names_new)]
    drop_these <- c()
    for(dup in dupers){

        dup_set <- which(col_names_new == dup)
        keep_this_one <- which.min(priority[dup_set])
        drop_these <- c(drop_these, dup_set[-keep_this_one])
    }

    d <- select(d, ! drop_these)
    if(length(drop_these)){
        units <- units[-drop_these]
        col_names_new <- col_names_new[-drop_these]
    }
    unit_names <- col_names_new

    ## determine which units need to be converted

    nonconvert_units <- grepl(paste(units_to_ignore,
                                    collapse = '|'),
                              colnames(d))

    units <- units[! nonconvert_units]
    unit_names <- unit_names[! nonconvert_units]

    more_nonconverts <- grepl('mg/L', units)

    units <- units[! more_nonconverts]
    unit_names <- unit_names[! more_nonconverts]
    names(units) <- unit_names

    if(any(nchar(units) == 0 | is.na(units))){
        message('unit problem')
        browser()
    }
    if(any(! grepl('/L$', units))){
        message('maybe unit problem')
        browser()
    }

    ## back to normal

    vars_encountered <- intersect(names(catalina_varname_priorities),
                                  colnames(d))

    #Most metals are reported as their isotope, not sure to keep istope form in
    #varible name or change to just element.
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'SiteCode',
                         data_cols =  vars_encountered,
                         data_col_pattern = '#V#',
                         set_to_NA = errorcode_variants,
                         is_sensor = FALSE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    new_units <- setNames(rep('mg/L',
                              length(units)),
                          names(units))

    units <- sub('moles', 'mol', units)

    if(length(units)){
        d <- ms_conversions(d,
                            convert_units_from = units,
                            convert_units_to = new_units)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    #this happens in derive kernel for catalina
    # d <- synchronize_timestep(d)

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_5492 <- function(network, domain, prodname_ms, site_code, component){

    #https://czo-archive.criticalzone.org/catalina-jemez/data/dataset/5491/
    #https://www.hydroshare.org/resource/4ab76a12613c493d82b2df57aa970c24/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    col_names <- colnames(d)
    units <- as.character(d[1, ])

    d <- d %>%
        filter(SiteCode %in% c('RainColl_Burn_Low_OC', 'RainColl_MCZOB'))

    if(nrow(d) == 0){
        return(NULL)
    }

    ## standardize variable names

    col_names <- colnames(d)
    d <- as_tibble(d)

    priority <- rep(NA, ncol(d))
    for(i in seq_along(col_names)){

        cn <- col_names[i]
        ms_name <- purrr::keep(catalina_varname_priorities,  ~ cn %in% .x) %>%
            names()

        if(! length(ms_name)) next

        priority[i] <- which(catalina_varname_priorities[[ms_name]] == cn)
        colnames(d)[i] <- ms_name
    }

    ## choose among equivalent variables

    col_names_new <- colnames(d)
    dupers <- col_names_new[duplicated(col_names_new)]
    drop_these <- c()
    for(dup in dupers){

        dup_set <- which(col_names_new == dup)
        keep_this_one <- which.min(priority[dup_set])
        drop_these <- c(drop_these, dup_set[-keep_this_one])
    }

    d <- select(d, ! drop_these)
    if(length(drop_these)){
        units <- units[-drop_these]
        col_names_new <- col_names_new[-drop_these]
    }
    unit_names <- col_names_new

    ## determine which units need to be converted

    nonconvert_units <- grepl(paste(units_to_ignore,
                                    collapse = '|'),
                              colnames(d))

    units <- units[! nonconvert_units]
    unit_names <- unit_names[! nonconvert_units]

    more_nonconverts <- grepl('mg/L', units)

    units <- units[! more_nonconverts]
    unit_names <- unit_names[! more_nonconverts]
    names(units) <- unit_names

    if(any(nchar(units) == 0 | is.na(units))){
        message('unit problem')
        browser()
    }
    if(any(! grepl('/L$', units))){
        message('maybe unit problem')
        browser()
    }

    ## back to normal

    vars_encountered <- intersect(names(catalina_varname_priorities),
                                  colnames(d))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'SiteCode',
                         alt_site_code = list('Burn_Met_Low' = 'RainColl_Burn_Low_OC',
                                              'MCZOB_met' = 'RainColl_MCZOB'),
                         data_cols =  vars_encountered,
                         data_col_pattern = '#V#',
                         set_to_NA = errorcode_variants,
                         is_sensor = FALSE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    new_units <- setNames(rep('mg/L',
                              length(units)),
                          names(units))

    units <- sub('moles', 'mol', units)

    if(length(units)){
        d <- ms_conversions(d,
                            convert_units_from = units,
                            convert_units_to = new_units)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    #this happens in derive kernel for catalina
    # d <- synchronize_timestep(d)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2415 <- function(network, domain, prodname_ms, site_code, component){

    #https://www.hydroshare.org/resource/5ca1378090884e72a8dcb796d882bb07/

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'MC_flux_tower')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c('PRECIP' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999',
                                       'mm'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2425 <- function(network, domain, prodname_ms, site_code, component){

    #https://www.hydroshare.org/resource/1db7abcf0eb64ef49000c46a6133949d/
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'Ponderosa_flux_tower')

    precip_col <- names(d)[grepl('PREC|PRECIP', names(d))]

    precip_cat <- c('precipitation')
    names(precip_cat) <- precip_col

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DateTime' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  precip_cat,
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999',
                                       'mm'),
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

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
    for(i in 1:length(sites)){

        site_files <- grep(paste0('/', sites[i], '.feather'), files, value = TRUE)
        site_full <- map_dfr(site_files, read_feather)
        d <- rbind(d, site_full)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_code == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[i],
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
    for(i in 1:length(sites)){

		site_files <- grep(paste0('/', sites[i], '.feather'), files, value = TRUE)
        site_full <- map_dfr(site_files, read_feather)
        d <- rbind(d, site_full)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_code == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[i],
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
    for(i in 1:length(sites)){

		site_files <- grep(paste0('/', sites[i], '.feather'), files, value = TRUE)
        site_full <- map_dfr(site_files, read_feather)
        d <- rbind(d, site_full)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              admit_NAs = TRUE,
                              paired_p_and_pchem = FALSE,
                              allow_pre_interp = TRUE)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_code == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[i],
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
    for(i in 1:length(sites)){
		site_files <- grep(paste0('/', sites[i], '.feather'), files, value = TRUE)
        site_full <- map_dfr(site_files, read_feather)
        d <- rbind(d, site_full)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              admit_NAs = TRUE,
                              paired_p_and_pchem = FALSE,
                              allow_pre_interp = TRUE)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    for(i in 1:length(sites)) {

        site_full <- filter(d, site_code == !!sites[i])

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[i],
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
