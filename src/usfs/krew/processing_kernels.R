
#retrieval kernels ####

#precipitation: STATUS=READY
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

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2018-0028/RDS-2018-0028.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(last_mod_dt > set_details$last_mod_dt){

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

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2017-0037/RDS-2017-0037.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(last_mod_dt > set_details$last_mod_dt){

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

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2017-0040/RDS-2017-0040.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(last_mod_dt > set_details$last_mod_dt){

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

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- function(set_details, network, domain) {

    loginfo(glue('Nothing to do for {p}',
                 p = set_details$prodname_ms),
            logger = logger_module)

    return()
}

#munge kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('DAILY_CLIMATOLOGY', fils, value = T)

    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    all_met <- tibble()
    for(i in 1:length(rel_file_path)){
        site_code <- str_split_fixed(relevant_file[i], '/', n = Inf)[1,2]
        site_code <- str_split_fixed(site_code, '_', n = Inf)[1,1:2]
        site_code <- tolower(paste(site_code, collapse = '_'))

        met <- read.csv(rel_file_path[i], colClasses = 'character') %>%
            mutate(site = !!site_code)

        all_met <- rbind(all_met, met)
    }

    #DATETIME is messed up cuz of one digit thing
    d <- ms_read_raw_csv(preprocessed_tibble = all_met,
                         datetime_cols = list('Date' = '%m/%d/%y'),
                         datetime_tz = 'America/Los_Angeles',
                         site_code_col = 'site',
                         data_cols =  c('PRECIPITATION' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         set_to_NA = '-9999',
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

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)

    rel_file_path <- paste0(temp_dir, '/', 'Data/Discharge_15Min.csv')

    d_d <- read.csv(rel_file_path, colClasses = 'character') %>%
        pivot_longer(cols = contains('Rate'), names_to = 'site', values_to = 'Q') %>%
        select(Date, Time, site, Q) %>%
        mutate(site = substr(site, 0, 4))

    d_f <- read.csv(rel_file_path, colClasses = 'character') %>%
        pivot_longer(cols = contains('flag'), names_to = 'site', values_to = 'flag') %>%
        select(Date, Time, site, flag) %>%
        mutate(site = substr(site, 0, 4))

    d <- full_join(d_d, d_f, by = c('Date', 'Time', 'site'))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%e/%Y',
                                              'Time' = '%H:%M'),
                         datetime_tz = 'America/Los_Angeles',
                         site_code_col = 'site',
                         data_cols =  c('Q' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'flag',
                         set_to_NA = '-9999',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('flag' = 'e'),
                            summary_flags_to_drop = list('flag' = 'REMOVE'),
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

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
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    # Gab samples
    relevant_file1 <- grep('Grab_samples_final', fils, value = T)
    rel_file_path1 <- paste0(temp_dir, '/', relevant_file1)

    d <- ms_read_raw_csv(filepath = rel_file_path1,
                         datetime_cols = list('Date' = '%m/%e/%Y'),
                         datetime_tz = 'America/Los_Angeles',
                         site_code_col = 'WS',
                         data_cols =  c('CA' = 'Ca',
                                        'CL' = 'Cl',
                                        'EC' = 'spCond',
                                        'K' = 'K',
                                        'MG' = 'Mg',
                                        'NA.' = 'Na',
                                        'NH4' = 'NH4',
                                        'NO3' = 'NO3',
                                        'pH' = 'pH',
                                        'PO4' = 'PO4',
                                        'SO4' = 'SO4'),
                         data_col_pattern = '#V#',
                         set_to_NA = '-9999',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('Ca' = 'umol/l',
                                               'Cl' = 'umol/l',
                                               'K' = 'umol/l',
                                               'Mg' = 'umol/l',
                                               'Na' = 'umol/l',
                                               'NH4' = 'umol/l',
                                               'NO3' = 'umol/l',
                                               'PO4' = 'umol/l',
                                               'SO4' = 'umol/l'),
                        convert_units_to = c('Ca' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'K' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             'PO4' = 'mg/l',
                                             'SO4' = 'mg/l'))

    # Auto sampler
    relevant_file2 <- grep('ISCO_samples_final', fils, value = T)
    rel_file_path2 <- paste0(temp_dir, '/', relevant_file2)

    d_isco <- read.csv(rel_file_path2, colClasses = 'character')
    d_isco_names <- names(d_isco)

    flag_cols <- grepl('BDL_', d_isco_names)
    new_flag_names <- paste0('Lab_', str_split_fixed(d_isco_names, '_', n = Inf)[,2], '_flag')
    new_names <- c(d_isco_names[!flag_cols], new_flag_names[flag_cols])
    names(d_isco) <- new_names

    d_isco <- ms_read_raw_csv(preprocessed_tibble = d_isco,
                         datetime_cols = list('SampleDate' = '%m/%e/%Y',
                                              'SampleTime' = '%H:%M'),
                         datetime_tz = 'America/Los_Angeles',
                         site_code_col = 'SiteID',
                         data_cols =  c('pH' = 'pH',
                                        'EC' = 'spCond',
                                        'Temperature' = 'temp',
                                        'Lab_Chloride' = 'Cl',
                                        'Lab_Nitrate' = 'NO3',
                                        'Lab_Phosphate' = 'PO4',
                                        'Lab_Sulfate' = 'SO4',
                                        'Lab_Ammonium' = 'NH4',
                                        'Lab_Calcium' = 'Ca',
                                        'Lab_Magnesium' = 'Mg',
                                        'Lab_Potassium' = 'K',
                                        'Lab_Sodium' = 'Na'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#_flag',
                         set_to_NA = '-9999',
                         is_sensor = FALSE)

    d_isco <- ms_cast_and_reflag(d_isco,
                                 varflag_col_pattern = '#V#__|flg',
                                 variable_flags_dirty = c('Y', 'y'),
                                 variable_flags_to_drop = 'DROP')

    d_isco <- ms_conversions(d_isco,
                        convert_units_from = c('Ca' = 'umol/l',
                                               'Cl' = 'umol/l',
                                               'K' = 'umol/l',
                                               'Mg' = 'umol/l',
                                               'Na' = 'umol/l',
                                               'NH4' = 'umol/l',
                                               'NO3' = 'umol/l',
                                               'PO4' = 'umol/l',
                                               'SO4' = 'umol/l'),
                        convert_units_to = c('Ca' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'K' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'NH4' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             'PO4' = 'mg/l',
                                             'SO4' = 'mg/l'))

    d <- rbind(d, d_isco)
    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

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

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/discharge__VERSIONLESS002/sitename_NA/krew_discharge.zip',
                    n = network,
                    d = domain)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)

    rel_file_path <- paste0(temp_dir, '/', 'Data/Watersheds')

    projstring <- choose_projection(unprojected = TRUE)

    wb <- sf::st_read(rel_file_path,
                      stringsAsFactors = FALSE,
                      quiet = TRUE) %>%
        select(site_code = watershed,
               area = Area_ha,
               geometry = geometry) %>%
        sf::st_transform(projstring) %>%
        arrange(site_code)

    unlink(temp_dir)

    for(i in 1:nrow(wb)){

        new_wb <- wb[i,]

        site_code <- as_tibble(new_wb) %>%
            pull(site_code)

        write_ms_file(d = new_wb,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = site_code,
                      level = 'munged',
                      shapefile = TRUE,
                      link_to_portal = FALSE)
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
