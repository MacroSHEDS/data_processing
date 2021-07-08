
#retrieval kernels ####

#precipitation: STATUS=PENDING
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

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2018-0028/RDS-2018-0028.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2018-0028/RDS-2018-0028.zip',
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
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2020-0082/RDS-2020-0082.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2020-0082/RDS-2020-0082.zip',
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
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2020-0080/RDS-2020-0080.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2020-0080/RDS-2020-0080.zip',
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

#ws_boundary: STATUS=PENDING
#. handle_errors
process_0_VERSIONLESS004 <- function(set_details, network, domain) {

    loginfo(glue('Nothing to do for {p}',
                 p = set_details$prodname_ms),
            logger = logger_module)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS005 <- function(set_details, network, domain) {

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

    res <- httr::HEAD('https://www.fs.usda.gov/rds/archive/products/RDS-2020-0081/RDS-2020-0081.zip')
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = 'https://www.fs.usda.gov/rds/archive/products/RDS-2020-0081/RDS-2020-0081.zip',
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

#precipitation: STATUS=PENDING
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

    #rel_file_path <- paste0(temp_dir, '/', 'Data/HF00301_v4.csv')
    rel_file_path <- paste0(temp_dir, '/', 'Data/HF00301_v4.csv')

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = list('DATE_TIME' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'America/Los_Angeles',
                         site_code_col = 'SITECODE',
                         data_cols =  c(INST_Q = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         summary_flagcols = c('ESTCODE', 'EVENT_CODE'))

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(
                                ESTCODE = c('A', 'E'),
                                EVENT_CODE = c(NA, 'WEATHR')),
                            summary_flags_dirty = list(
                                ESTCODE = c('Q', 'S', 'P'),
                                EVENT_CODE = c('INSREM', 'MAINTE')))

    #convert cfs to liters/s
    d <- d %>%
        mutate(val = val * 28.317)

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

    relevant_file <- grep('Data/CF00301', fils, value = T)
    relevant_file <- relevant_file[! grepl('attributes', relevant_file)]
    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(PH='pH',
                                        COND='spCond',
                                        ALK='alk',
                                        SSED='suspSed',
                                        SI='Si',
                                        'UTP',
                                        'TDP',
                                        PARTP='TPP',
                                        PO4P='PO4_P',
                                        'UTKN',
                                        'TKN',
                                        PARTN='TPN',
                                        'DON',
                                        NH3N='NH3_N',
                                        NO3N='NO3_N',
                                        `NA`='Na',
                                        CA='Ca',
                                        MG='Mg',
                                        SO4S='SO4_S',
                                        CL='Cl',
                                        'K'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         var_flagcol_pattern = '#V#CODE',
                         summary_flagcols = c('TYPE'))

    d <- ms_cast_and_reflag(d,
                            variable_flags_to_drop = 'N',
                            variable_flags_clean =
                                c('A', 'E', 'D', 'DE', '*', 'D*'),
                            summary_flags_to_drop = list(
                                TYPE = c('N', 'S', 'YE', 'QB', 'QS', 'QL', 'QA')),
                            summary_flags_clean = list(TYPE = c('F', 'G')),
                            summary_flags_dirty = list(TYPE = c('P', 'G', 'C', 'A')))

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

#ws_boundary: STATUS=PENDING
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

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data/CP00301', fils, value = T)
    relevant_file <- relevant_file[! grepl('attributes', relevant_file)]
    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- ms_read_raw_csv(filepath = rel_file_path,
                         datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(PH='pH',
                                        COND='spCond',
                                        ALK='alk',
                                        SSED='suspSed',
                                        SI='Si',
                                        'UTP',
                                        'TDP',
                                        PARTP='TPP',
                                        PO4P='PO4_P',
                                        'UTKN',
                                        'TKN',
                                        PARTN='TPN',
                                        'DON',
                                        NH3N='NH3_N',
                                        NO3N='NO3_N',
                                        `NA`='Na',
                                        CA='Ca',
                                        MG='Mg',
                                        SO4S='SO4_S',
                                        CL='Cl',
                                        'K'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         var_flagcol_pattern = '#V#CODE',
                         summary_flagcols = c('TYPE'))

    d <- ms_cast_and_reflag(d,
                            variable_flags_to_drop = 'N',
                            variable_flags_clean =
                                c('A', 'E', 'D', 'DE', '*', 'D*'),
                            summary_flags_to_drop = list(
                                TYPE = c('N', 'S', 'YE', 'QB', 'QS', 'QL', 'QA')),
                            summary_flags_clean = list(TYPE = c('F', 'DF')))

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
