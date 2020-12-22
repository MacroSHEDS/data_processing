source('src/lter/arctic/domain_helpers.R', local=TRUE)

#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_10502 <- retrieve_arctic

#discharge: STATUS=READY
#. handle_errors
process_0_10503 <- retrieve_arctic

#discharge: STATUS=READY
#. handle_errors
process_0_10504 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10505 <- retrieve_arctic

#discharge: STATUS=READY
#. handle_errors
process_0_1330 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1331 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1334 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1337 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1340 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1344 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1348 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1352 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1356 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1360 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1363 <- retrieve_arctic

#discharge: STATUS=READY
#. handle_errors
process_0_1367 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1370 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1316 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1320 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1324 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10512 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10513 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10514 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10515 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10516 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10517 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10518 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10519 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10520 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10521 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10521 <- retrieve_arctic

#discharge: STATUS=READY
#. handle_errors
process_0_1333 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1336 <- retrieve_arctic

#discharge: STATUS=READY
#. handle_errors
process_0_1339 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1342 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1346 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1350 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1354 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1358 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1362 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1365 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1368 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1372 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1318 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1322 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1326 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_1328 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10040 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10043 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10045 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10047 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10049 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10395 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10396 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10510 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10511 <- retrieve_arctic

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_10303 <- retrieve_arctic

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_10502 <- munge_discharge

#discharge: STATUS=READY
#. handle_errors
process_1_10503 <- munge_discharge

#discharge: STATUS=READY
#. handle_errors
process_1_10504 <- munge_discharge

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10505 <- munge_discharge_temp

#discharge: STATUS=READY
#. handle_errors
process_1_1330 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1331 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1334 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1337 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1340 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1344 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1348 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1352 <- munge_discharge_temp


#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1356 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1360 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1363 <- munge_discharge_temp

#discharge: STATUS=READY
#. handle_errors
process_1_1367 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1370 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1316 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1320 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1324 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10512 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10513 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10514 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10515 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10516 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10517 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10518 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10519 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10520 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10521 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10521 <- munge_discharge_temp

#discharge: STATUS=READY
#. handle_errors
process_1_1333 <- function(network, domain, prodname_ms, site_name,
                                              component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = list('Date' = '%d-%b-%y'),
                         datetime_tz = 'America/Anchorage',
                         site_name_col = 'Site',
                         alt_site_name = list('Oksrukuyik_Creek' = '5'),
                         data_cols =  c('Discharge2' = 'discharge'),
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
process_1_1336 <-  munge_discharge

#discharge: STATUS=READY
#. handle_errors
process_1_1339 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1342 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1346 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1350 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1354 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1358 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1362 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1365 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1368 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1372 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1318 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1322 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1326 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_1328 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10040 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10043 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10045 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10047 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10049 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_10395 <- function(network, domain, prodname_ms, site_name,
                            component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    alt_site_names <- detrmin_arctic_site(component)

    if(grepl('discharge', prodname_ms)){

        d <- read.csv(rawfile, colClasses = 'character')

        d[d == '-1111'] <- NA

        d <- d %>%
            rename(cr_q = `CR10X.Calc..Q..m3.sec.`,
                   hobo_q = `HOBO.Calc..Q..m3.sec.`) %>%
            mutate(cr_q = as.numeric(cr_q),
                   hobo_q = as.numeric(hobo_q)) %>%
            mutate(temp = (cr_q+hobo_q)/2) %>%
            mutate(discharge = case_when(is.na(cr_q) & !is.na(hobo_q) ~ hobo_q,
                                         ! is.na(cr_q) & is.na(hobo_q) ~ cr_q,
                                         is.na(cr_q) & is.na(hobo_q) ~ -1111,
                                         !is.na(cr_q) & !is.na(hobo_q) ~ temp)) %>%
            mutate_all(as.character)

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date' = '%d-%b-%y',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols =  'discharge',
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

        d <- read.csv(rawfile, colClasses = 'character')

        d[d == '-1111'] <- NA

        d <- d %>%
            rename(cr_temp = `CR10X.Temperature...C.`,
                   hobo_temp = `HOBO.Temperature...C.`) %>%
            mutate(cr_temp = as.numeric(cr_temp),
                   hobo_temp = as.numeric(hobo_temp)) %>%
            mutate(temp = (cr_temp+hobo_temp)/2) %>%
            mutate(temperature = case_when(is.na(cr_temp) & !is.na(hobo_temp) ~ hobo_temp,
                                         ! is.na(cr_temp) & is.na(hobo_temp) ~ cr_temp,
                                         is.na(cr_temp) & is.na(hobo_temp) ~ -1111,
                                         !is.na(cr_temp) & !is.na(hobo_temp) ~ temp)) %>%
            select(-temp) %>%
            mutate_all(as.character)

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date' = '%d-%b-%y',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols = c('temperature' = 'temp'),
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
                              desired_interval = '1 day', #set to '15 min' when we have server
                              impute_limit = 30)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10396 <- function(network, domain, prodname_ms, site_name,
                            component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    alt_site_names <- detrmin_arctic_site(component)

    if(grepl('discharge', prodname_ms)){

        d <- read.csv(rawfile, colClasses = 'character')

        d[d == '-1111'] <- NA

        d <- d %>%
            rename(cr_q = `CR10X.Calc..Q..m3.sec.`,
                   hobo_q = `HOBOCalc..Q..m3.sec.`) %>%
            mutate(cr_q = as.numeric(cr_q),
                   hobo_q = as.numeric(hobo_q)) %>%
            mutate(temp = (cr_q+hobo_q)/2) %>%
            mutate(discharge = case_when(is.na(cr_q) & !is.na(hobo_q) ~ hobo_q,
                                         ! is.na(cr_q) & is.na(hobo_q) ~ cr_q,
                                         is.na(cr_q) & is.na(hobo_q) ~ -1111,
                                         !is.na(cr_q) & !is.na(hobo_q) ~ temp)) %>%
            mutate_all(as.character)

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('Date' = '%d-%b-%y',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols =  'discharge',
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

        d <- ms_read_raw_csv(filepath = rawfile,
                             datetime_cols = list('Date' = '%d-%b-%y',
                                                  'Time' = '%H:%M'),
                             datetime_tz = 'America/Anchorage',
                             site_name_col = 'River',
                             alt_site_name = alt_site_names,
                             data_cols = c('HOBO.Temperature...C.' = 'temp'),
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
                              desired_interval = '1 day', #set to '15 min' when we have server
                              impute_limit = 30)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10510 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10511 <- munge_discharge_temp

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_1_10303 <- function(network, domain, prodname_ms, site_name,
                            component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    d <- d[4:nrow(d),]

    look <- d %>%
        group_by(River, Station) %>%
        summarise(count = n())

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

