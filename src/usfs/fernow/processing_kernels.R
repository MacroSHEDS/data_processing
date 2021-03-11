
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

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2011-0014/RDS-2011-0014.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0014/RDS-2011-0014.zip',
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

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2011-0015/RDS-2011-0015.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0015/RDS-2011-0015.zip',
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

#precip_chemistry: STATUS=READY
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

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2011-0016/RDS-2011-0016.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0016/RDS-2011-0016.zip',
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
process_0_VERSIONLESS004 <- function(set_details, network, domain) {

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

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2011-0017/RDS-2011-0017.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0017/RDS-2011-0017.zip',
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

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    #DATETIME is messed up cuz of one digit thing
    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = list('Date..mm.dd.yyyy.' = '%m/%d/%Y'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'Watershed',
                         data_cols =  c('Precipitation..mm.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'I')

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
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = list('Date..mm.dd.yyyy.' = '%m/%e/%Y'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'Watershed',
                         data_cols =  c('Discharge..mm.' = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    # Fernow area values from metadeta. Needed to convert from mm/day to L/s
    fernow_areas <- tibble(site_name = c('WS-1', 'WS-2', 'WS-3', 'WS-4', 'WS-5',
                                         'WS-6', 'WS-7', 'WS-10', 'WS-13'),
                           area = c(30.11, 15.5, 34.27, 38.73, 36.41, 22.34,
                                    24.22, 15.21, 14.24)) %>%
        mutate(area = area*1e+10)

    d <- left_join(d, fernow_areas, by = 'site_name') %>%
        mutate(val = ((val*area)/1e+6)/86400)

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

#precip_chemistry: STATUS=READY
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

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = list('Date..mm.dd.yyyy.' = '%m/%e/%y'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'Weather.Station',
                         data_cols =  c('pH' = 'pH',
                                        'Electrical.Conductivity..uS.cm.' = 'spCond',
                                        # there are to ANC in ms_vars check
                                        'Acid.Neutralizing.Capacity..ueq.L.' = 'ANC',
                                        'Calcium..mg.L.' = 'Ca',
                                        'Magnesium..mg.L.' = 'Mg',
                                        'Sodium..mg.L.' = 'Na',
                                        'Potassium..mg.L.' = 'K',
                                        'Ammonium..mg.L.' = 'NH4',
                                        'Chloride..mg.L.' = 'Cl',
                                        'Sulfate..mg.L.' = 'SO4',
                                        'Nitrate..mg.L.' = 'NO3'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'G')

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = list('Date..mm.dd.yyyy.' = '%m/%e/%Y'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'Watershed',
                         data_cols =  c('pH' = 'pH',
                                        'Electrical.Conductivity..uS.cm.' = 'spCond',
                                        'Alkalinity..mg.CaCO3.L.' = 'alk',
                                        # there are to ANC in ms_vars check
                                        'Acid.Neutralizing.Capacity..ueq.L.' = 'ANC',
                                        'Calcium..mg.L.' = 'Ca',
                                        'Magnesium..mg.L.' = 'Mg',
                                        'Sodium..mg.L.' = 'Na',
                                        'Potassium..mg.L.' = 'K',
                                        'Chloride..mg.L.' = 'Cl',
                                        'Sulfate..mg.L.' = 'SO4',
                                        'Nitrate..mg.L.' = 'NO3'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('NO3' = 'mg/l'),
                        convert_units_to = c('NO3' = 'mg/l'))

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
process_2_ms003 <- precip_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_precip_pchem_pflux

#precipitation: STATUS=READY
#. handle_errors
process_2_ms900 <- function(network, domain, prodname_ms){

    dir.create('data/usfs/fernow/derived/precipitation__ms900/')
    file.copy(from = list.files('data/usfs/fernow/munged/precipitation__VERSIONLESS001/',
                                full.names = TRUE),
              to = 'data/usfs/fernow/derived/precipitation__ms900',
              overwrite = TRUE)
}


