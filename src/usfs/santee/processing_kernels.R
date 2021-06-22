
#retrieval kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_name)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2019-0033/RDS-2019-0033.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2019-0033/RDS-2019-0033.zip',
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(last_mod_dt)
    }

    loginfo(glue('Nothing to do for {p}',
            p = set_details$prodname_ms),
            logger = logger_module)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_name)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2019-0033/RDS-2019-0033.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2019-0033/RDS-2019-0033.zip',
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(last_mod_dt)
    }

    loginfo(glue('Nothing to do for {p}',
                 p = set_details$prodname_ms),
            logger = logger_module)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_name)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2019-0033/RDS-2019-0033.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2019-0033/RDS-2019-0033.zip',
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(last_mod_dt)
    }

    loginfo(glue('Nothing to do for {p}',
                 p = set_details$prodname_ms),
            logger = logger_module)

    return()
}

#munge kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Met5_hourly', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = list('Date_time_rain' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'Instr_ID',
                         data_cols =  c(Rainfall = 'precipitation'),
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

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_name)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_name == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)

    #rel_file_path <- paste0(temp_dir, '/', 'Data/HF00301_v4.csv')
    rel_file_path1 <- paste0(temp_dir, '/', 'Data/Flow/WS77_daily_flow_1964-2000.csv')
    rel_file_path2 <- paste0(temp_dir, '/', 'Data/Flow/WS77_10-15min_flow_2003-2019.csv')

    look <- read.csv(rel_file_path2, colClasses = 'character')

    d_h <- ms_read_raw_csv(filepath = rel_file_path1,
                           datetime_cols = list('Date' = '%m/%e/%Y'),
                           datetime_tz = 'US/Eastern',
                           site_name_col = 'Location',
                           data_cols =  c(Dailyflow = 'discharge'),
                           data_col_pattern = '#V#',
                           is_sensor = TRUE)

    d_h <- ms_cast_and_reflag(d_h,
                              varflag_col_pattern = NA)

    d_m <- ms_read_raw_csv(filepath = rel_file_path2,
                         datetime_cols = list('Date_time' = '%m/%e/%Y %H:%M'),
                         datetime_tz = 'America/Los_Angeles',
                         site_name_col = 'Location',
                         data_cols =  c(Flow_liter = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d_m <- ms_cast_and_reflag(d_m,
                              varflag_col_pattern = NA)

    # Combine historical and modern Q
    d <- rbind(d_m, d_h)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)


    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_name)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_name == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file1 <- paste0(temp_dir, '/Data/Water_quality/WS77_water_chemistry_1976-1994.csv')
    relevant_file2 <- grep('Data/Water_quality/WS77_water_chemistry_2003', fils, value = T)
    relevant_file2 <- paste0(temp_dir, '/', relevant_file2)

    d_h <- ms_read_raw_csv(filepath = relevant_file1,
                         datetime_cols = c(Date = '%m/%e/%Y'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'Location',
                         data_cols =  c(pH='pH',
                                        NO3_NO2_N_mgL='NO3_NO2_N',
                                        NH4_N_mgL='NH4_N',
                                        PO4_P_mgL='PO4_P',
                                        Cl_mgL='Cl',
                                        K_mgL = 'K',
                                        Na_mgL = 'Na',
                                        Ca_mgL = 'Ca',
                                        Mg_mgL = 'Mg',
                                        SO4_S_mgL = 'SO4_S',
                                        TKN_mgL = 'TKN',
                                        SiO3_mgL = 'SiO3',
                                        HCO3_mgL = 'HCO3',
                                        TN_mgL = 'TN'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE)

    d_h <- ms_cast_and_reflag(d_h,
                              varflag_col_pattern = NA)

    d_h <- ms_conversions(d_h,
                          convert_units_from = c(SiO3 = 'mg/l'),
                          convert_units_to = c(SiO3 = 'mg/l'))

    d_m <- ms_read_raw_csv(filepath = relevant_file2,
                           datetime_cols = c(Date_time = '%m/%e/%Y %H:%M:%S'),
                           datetime_tz = 'US/Eastern',
                           site_name_col = 'Location',
                           data_cols =  c(TDN_mgL = 'TDN',
                                          TDP_mgL = 'TDP',
                                          NH4_N_mgL = 'NH4_N',
                                          NO3_NO2_N_mgL = 'NO3_NO2_N',
                                          Cl_mgL = 'Cl',
                                          Ca_mgL = 'Ca',
                                          K_mgL = 'K',
                                          Mg_mgL = 'Mg',
                                          Na_mgL = 'Na',
                                          P_mgL = 'P',
                                          DOC_mgL = 'DOC',
                                          Br_mgL = 'Br',
                                          SO4_mgL = 'SO4',
                                          PO4_mgL = 'PO4',
                                          SiO2_mgL = 'SiO2',
                                          Temp_C = 'temp',
                                          pH = 'pH',
                                          Conductivity = 'spCond',
                                          DO_mgL = 'DO',
                                          DO_per_sat = 'DO_sat'),
                           data_col_pattern = '#V#',
                           is_sensor = FALSE)

    d_m <- ms_cast_and_reflag(d_m,
                              varflag_col_pattern = NA)

    d_m <- ms_conversions(d_m,
                          convert_units_from = c(SiO2 = 'mg/l'),
                          convert_units_to = c(SiO2 = 'mg/l'))

    # spCond ms/cm to us/cm
    d_m <- d_m %>%
        mutate(val = ifelse(var == 'spCond', val*1000, val))

    d <- rbind(d_h, d_m)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_name)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_name == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms002 <- precip_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_precip_pchem_pflux
