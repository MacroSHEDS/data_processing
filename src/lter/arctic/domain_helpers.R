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

munge_toolik <- function(network, domain, prodname_ms, site_code,
                                 component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_code = 'Toolik_Inlet_Main')

    if(grepl('discharge', prodname_ms)){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c('Date_Time' = '%m/%d/%y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_code_col = 'site_code',
                             data_cols =  c('Q_m3sec' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)

        # Convert from cm/s to liters/s
        d <- d %>%
            mutate(val = val*1000)

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c('Date_Time' = '%m/%d/%y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_code_col = 'site_code',
                             data_cols =  c('Water_Temp_C' = 'temp',
                                            'Conductivity_uScm' = 'spCond'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111', '-9999'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)
    }
    return(d)
}

munge_toolik_2 <- function(network, domain, prodname_ms, site_code,
                           component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site_code = 'Toolik_Inlet_Main')

    if(grepl('discharge', prodname_ms)){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c('Date_Time' = '%d-%b-%Y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_code_col = 'site_code',
                             data_cols =  c('Q_m3sec' = 'discharge'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)

        # Convert from cm/s to liters/s
        d <- d %>%
            mutate(val = val*1000)

    } else{

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c('Date_Time' = '%d-%b-%Y %H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_code_col = 'site_code',
                             data_cols =  c('Water_Temp_C' = 'temp',
                                            'Conductivity_uScm' = 'spCond'),
                             data_col_pattern = '#V#',
                             set_to_NA = c('-1111', '-1.111', '-9999'),
                             is_sensor = TRUE)

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA)
    }

    return(d)
}
