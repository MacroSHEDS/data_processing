#retrieval kernels ####

#CUSTOMprecipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0014/RDS-2011-0014_Data.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(length(last_mod_dt) == 0){
        last_mod_dt <- NA
    }

    if(is.na(last_mod_dt) || last_mod_dt > set_details$last_mod_dt){

        download.file(url = url,
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        deets_out$url <- url
        deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                      tzone = 'UTC'))
        deets_out$last_mod_dt = last_mod_dt

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(deets_out)
    }

    loginfo(glue('Nothing to do for {p}',
            p = set_details$prodname_ms),
            logger = logger_module)

    return(deets_out)
}

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0015/RDS-2011-0015.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(length(last_mod_dt) == 0){
        last_mod_dt <- NA
    }

    if(is.na(last_mod_dt) || last_mod_dt > set_details$last_mod_dt){

        download.file(url = url,
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        deets_out$url <- url
        deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                      tzone = 'UTC'))
        deets_out$last_mod_dt = last_mod_dt

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(deets_out)
    }

    loginfo(glue('Nothing to do for {p}',
            p = set_details$prodname_ms),
            logger = logger_module)

    return(deets_out)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0016/RDS-2011-0016.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(length(last_mod_dt) == 0){
        last_mod_dt <- NA
    }

    if(is.na(last_mod_dt) || last_mod_dt > set_details$last_mod_dt){

        download.file(url = url,
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        deets_out$url <- url
        deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                      tzone = 'UTC'))
        deets_out$last_mod_dt = last_mod_dt

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(deets_out)
    }

    loginfo(glue('Nothing to do for {p}',
            p = set_details$prodname_ms),
            logger = logger_module)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2011-0017/RDS-2011-0017.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(length(last_mod_dt) == 0){
        last_mod_dt <- NA
    }

    if(is.na(last_mod_dt) || last_mod_dt > set_details$last_mod_dt){

        download.file(url = url,
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        deets_out$url <- url
        deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                      tzone = 'UTC'))
        deets_out$last_mod_dt = last_mod_dt

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(deets_out)
    }

    loginfo(glue('Nothing to do for {p}',
            p = set_details$prodname_ms),
            logger = logger_module)

    return(deets_out)
}

#munge kernels ####

#CUSTOMprecipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- file.path(tempdir(), 'fernow')
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = c('Date..mm.dd.yyyy.' = '%m/%e/%Y'),
                         datetime_tz = 'Etc/GMT+5',
                         site_code_col = 'Watershed',
                         data_cols =  c('Precipitation..mm.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'I',
                         keep_empty_rows = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = FALSE)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- file.path(tempdir(), 'fernow')
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = c('Date..mm.dd.yyyy.' = '%m/%e/%Y'),
                         datetime_tz = 'Etc/GMT+5',
                         site_code_col = 'Watershed',
                         data_cols =  c('Discharge..mm.' = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    # Fernow area values from metadeta. Needed to convert from mm/day to L/s
    fernow_areas <- tibble(site_code = c('WS-1', 'WS-2', 'WS-3', 'WS-4', 'WS-5',
                                         'WS-6', 'WS-7', 'WS-10', 'WS-13'),
                           area = c(30.11, 15.5, 34.27, 38.73, 36.41, 22.34,
                                    24.22, 15.21, 14.24)) %>%
        mutate(area = area*1e+10)

    d <- left_join(d, fernow_areas, by = 'site_code') %>%
        mutate(val = ((val*area)/1e+6)/86400)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- file.path(tempdir(), 'fernow')
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = c('Date..mm.dd.yyyy.' = '%m/%e/%y'),
                         datetime_tz = 'Etc/GMT+5',
                         site_code_col = 'Weather.Station',
                         data_cols =  c('pH' = 'pH',
                                        'Electrical.Conductivity..uS.cm.' = 'spCond',
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
                         set_to_NA = c('', '.'),
                         is_sensor = FALSE,
                         sampling_type = 'G',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    d <- ms_conversions(d,
                        convert_units_from = c(ANC = 'ueq/l'),
                        convert_units_to = c(ANC = 'eq/l'))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              admit_NAs = TRUE,
                              allow_pre_interp = TRUE)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- file.path(tempdir(), 'fernow')
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = c('Date..mm.dd.yyyy.' = '%m/%e/%Y'),
                         datetime_tz = 'Etc/GMT+5',
                         site_code_col = 'Watershed',
                         data_cols =  c('pH' = 'pH',
                                        'Electrical.Conductivity..uS.cm.' = 'spCond',
                                        'Alkalinity..mg.CaCO3.L.' = 'alk',
                                        'Acid.Neutralizing.Capacity..ueq.L.' = 'ANC',
                                        'Calcium..mg.L.' = 'Ca',
                                        'Magnesium..mg.L.' = 'Mg',
                                        'Sodium..mg.L.' = 'Na',
                                        'Potassium..mg.L.' = 'K',
                                        'Chloride..mg.L.' = 'Cl',
                                        'Sulfate..mg.L.' = 'SO4',
                                        'Nitrate..mg.L.' = 'NO3'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c(ANC = 'ueq/l'),
                        convert_units_to = c(ANC = 'eq/l'))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
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

#CUSTOMprecip_flux_inst: STATUS=READY
#. handle_errors
process_2_ms004 <- function(network, domain, prodname_ms){

    chemprod <- 'precip_chemistry__ms901'
    qprod <- 'CUSTOMprecipitation__VERSIONLESS001'

    chemfiles <- ms_list_files(network = network,
                               domain = domain,
                               prodname_ms = chemprod)

    qfiles <- ms_list_files(network = network,
                            domain = domain,
                            prodname_ms = qprod)

    flux_sites <- base::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        flux <- sw(calc_inst_flux(chemprod = chemprod,
                                  qprod = qprod,
                                  site_code = s))

        if(! is.null(flux)){

            write_ms_file(d = flux,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_code = s,
                          level = 'derived',
                          shapefile = FALSE)
        }
    }
}

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms008 <- stream_gauge_from_site_data
