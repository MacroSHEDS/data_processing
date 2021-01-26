retrieve_arctic <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

detrmin_arctic_site <- function(component){
    site <- ifelse(grepl('Oksrukuyik', component),
                   list('Oksrukuyik_Creek' = c('Oksrukuyik Creek',
                                               'Oksrukuyik')),
                   list('Kuparuk_River' = c('Kuparuk River',
                                            'Kuparuk')))

    if(grepl('Oksrukuyik', component)){
        names(site) <- 'Oksrukuyik_Creek'
    } else{
        names(site) <- 'Kuparuk_River'
    }

    return(site)
}

munge_discharge <- function(network, domain, prodname_ms, site_name,
                            component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    alt_site_names <- detrmin_arctic_site(component)

    if(grepl('discharge', prodname_ms)){

        look <- read.csv(rawfile, colClasses = 'character')

        q_name <- c('discharge')

        q_prodname <-  grep('Q..m3', colnames(look), value = TRUE)

        if(length(q_prodname) == 1){
            names(q_name) <- q_prodname
        } else{
            return(generate_ms_err('multiple q names'))
        }

        d <- ms_read_raw_csv(filepath = rawfile,
                             datetime_cols = list('Date' = '%d-%b-%y'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols =  q_name,
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)

        # Convert from cm/s to liters/s
        d <- d %>%
            mutate(val = val*1000)
    } else{

        look <- read.csv(rawfile, colClasses = 'character')

        temp_name <- c('temp')

        temp_prodname <-  grep('Temp', colnames(look), value = TRUE)

        if(length(temp_prodname) == 1){
            names(temp_name) <- temp_prodname
        } else{
            return(generate_ms_err('multiple temp names'))
        }

        d <- ms_read_raw_csv(filepath = rawfile,
                             datetime_cols = list('Date' = '%d-%b-%y'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols =  temp_name,
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)
    }


    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              desired_interval = '15 min',
                              impute_limit = 30)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

munge_discharge_temp <- function(network, domain, prodname_ms, site_name,
                                 component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    alt_site_names <- detrmin_arctic_site(component)

    if(grepl('discharge', prodname_ms)){

        look <- read.csv(rawfile, colClasses = 'character')

        q_name <- c('discharge')

        q_prodname <-  grep('Q..m3', colnames(look), value = TRUE)

        if(length(q_prodname) == 1){
            names(q_name) <- q_prodname
        } else{
            return(generate_ms_err('multiple q names'))
        }

        d <- ms_read_raw_csv(filepath = rawfile,
                             datetime_cols = list('Date' = '%d-%b-%y',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols =  q_name,
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)

        # Convert from cm/s to liters/s
        d <- d %>%
            mutate(val = val*1000)

    } else{

        look <- read.csv(rawfile, colClasses = 'character')

        temp_name <- c('temp')

        temp_prodname <-  grep('Temp', colnames(look), value = TRUE)

        if(length(temp_prodname) == 1){
            names(temp_name) <- temp_prodname
        } else{
            return(generate_ms_err('multiple q names'))
        }

        d <- ms_read_raw_csv(filepath = rawfile,
                             datetime_cols = list('Date' = '%d-%b-%y',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols = temp_name,
                             set_to_NA = c('-1111', '-1.111'),
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              desired_interval = '15 min',
                              impute_limit = 30)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

munge_toolik <- function(network, domain, prodname_ms, site_name,
                                 component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = 'Toolik_Inlet_Main')

    if(grepl('discharge', prodname_ms)){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date_Time' = '%m/%d/%y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'site_name',
                             data_cols =  c('Q_m3sec' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)

        # Convert from cm/s to liters/s
        d <- d %>%
            mutate(val = val*1000)

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date_Time' = '%m/%d/%y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'site_name',
                             data_cols =  c('Water_Temp_C' = 'temp',
                                            'Conductivity_uScm' = 'spCond'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111', '-9999'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              desired_interval = '15 min',
                              impute_limit = 30)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}


munge_toolik_2 <- function(network, domain, prodname_ms, site_name,
                           component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_name = 'Toolik_Inlet_Main')

    if(grepl('discharge', prodname_ms)){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date_Time' = '%d-%b-%Y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'site_name',
                             data_cols =  c('Q_m3sec' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)

        # Convert from cm/s to liters/s
        d <- d %>%
            mutate(val = val*1000)

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date_Time' = '%d-%b-%Y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'site_name',
                             data_cols =  c('Water_Temp_C' = 'temp',
                                            'Conductivity_uScm' = 'spCond'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111', '-9999'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                variable_flags_to_drop = NA,
                                variable_flags_clean = NA)
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              desired_interval = '15 min',
                              impute_limit = 30)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}
