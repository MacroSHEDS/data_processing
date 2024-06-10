
#retrieval kernels ####

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

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2020-0082/RDS-2020-0082.zip'

    headers <- RCurl::getURL(url, nobody = 1L, header = 1L, httpheader = list('Accept-Encoding' = 'identity'))
    last_mod_dt <- str_match(headers, 'last-modified: (.*)')[, 2]
    last_mod_dt <- httr::parse_http_date(last_mod_dt) %>% with_tz('UTC')
    # res <- httr::HEAD(url)
    # last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
    #     as.POSIXct() %>%
    #     with_tz('UTC')

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

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2020-0080/RDS-2020-0080.zip'

    headers <- RCurl::getURL(url, nobody = 1L, header = 1L, httpheader = list('Accept-Encoding' = 'identity'))
    last_mod_dt <- str_match(headers, 'last-modified: (.*)')[, 2]
    last_mod_dt <- httr::parse_http_date(last_mod_dt) %>% with_tz('UTC')
    # res <- httr::HEAD(url)
    # last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
    #     as.POSIXct() %>%
    #     with_tz('UTC')

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

#ws_boundary: STATUS=PENDING
#. handle_errors
process_0_VERSIONLESS004 <- function(set_details, network, domain) {

    loginfo(glue('Nothing to do for {p}',
                 p = set_details$prodname_ms),
            logger = logger_module)

    return()
}

#precip_chemistry; precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS005 <- function(set_details, network, domain) {

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2020-0081/RDS-2020-0081.zip'

    if(set_details$prodname_ms == 'precipitation__VERSIONLESS005'){

        loginfo('skipping redundant download (pchem contains precip)',
                logger = logger_module)

        file.copy('data/usfs/suef/raw/documentation/documentation_precip_chemistry__VERSIONLESS005.txt',
                  'data/usfs/suef/raw/documentation/documentation_precipitation__VERSIONLESS005.txt')

        deets_out <- list(url = url,
                          access_time = held_data$precip_chemistry__VERSIONLESS005$sitename_NA$retrieve$held_version,
                          last_mod_dt = held_data$precip_chemistry__VERSIONLESS005$sitename_NA$retrieve$mtime)

        return(deets_out)
    }

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    headers <- RCurl::getURL(url, nobody = 1L, header = 1L, httpheader = list('Accept-Encoding' = 'identity'))
    last_mod_dt <- str_match(headers, 'last-modified: (.*)')[, 2]
    last_mod_dt <- httr::parse_http_date(last_mod_dt) %>% with_tz('UTC')

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

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component){

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
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(INST_Q = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         set_to_NA = '',
                         summary_flagcols = c('ESTCODE', 'EVENT_CODE'))

    d <- ms_cast_and_reflag(
        d,
        varflag_col_pattern = NA,
        summary_flags_clean = list(ESTCODE = c('A', 'E'),
                                   EVENT_CODE = c(NA, 'WEATHR')),
        summary_flags_dirty = list(ESTCODE = c('Q', 'S', 'P'),
                                   EVENT_CODE = c('INSREM', 'MAINTE'))
    )

    #convert cfs to liters/s
    d <- d %>%
        mutate(val = val * 28.317)

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component){

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

    d <- read.csv(rel_file_path, colClasses = 'character') %>%
        rename(NA.CODE = NACODE)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(PH='pH',
                                        COND='spCond',
                                        ALK='alk',
                                        SSED='suspSed',
                                        SI='Si',
                                        'UTP'='TP',
                                        'TDP',
                                        PARTP='TPP',
                                        PO4P='PO4_P',
                                        UTKN='TKN',
										# UTN='TN',
                                        TKN='TDKN',
                                        PARTN='TPN',
                                        'DON',
                                        NH3N='NH3_N',
                                        NO3N='NO3_N',
                                        NA.='Na',
                                        CA='Ca',
                                        MG='Mg',
                                        SO4S='SO4_S',
                                        CL='Cl',
                                        'K'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#CODE',
                         summary_flagcols = c('TYPE'))

    d <- ms_cast_and_reflag(
        d,
        variable_flags_bdl = '*',
        variable_flags_to_drop = 'N',
        variable_flags_dirty = c('Q', 'D*', 'C', 'D', 'DE', 'DQ', 'DC'),
        variable_flags_clean = c('A', 'E'),
        summary_flags_to_drop = list(TYPE = c('N', 'YE')),
        summary_flags_dirty = list(TYPE = c('C', 'S', 'A', 'P', 'B')),
        summary_flags_clean = list(TYPE = c('QB', 'QS', 'QL', 'QA', 'F', 'G'))
    )

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- filter(d, site_code == !!sites[s])

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
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component){

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

#precip_chemistry; precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = ifelse(prodname_ms == 'precipitation__VERSIONLESS005',
                               'precip_chemistry__VERSIONLESS005',
                               prodname_ms),
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data/CP00301', fils, value = T)
    relevant_file <- relevant_file[! grepl('attributes', relevant_file)]
    rel_file_path <- paste0(temp_dir, '/', relevant_file)

    d <- read.csv(rel_file_path, colClasses = 'character') %>%
        rename(NaCODE = NACODE, Na = NA.)

    if(prodname_ms == 'precip_chemistry__VERSIONLESS005'){

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'Etc/GMT-8',
                             site_code_col = 'SITECODE',
                             data_cols =  c(PH='pH',
                                            COND='spCond',
                                            ALK='alk',
                                            SSED='suspSed',
                                            SI='Si',
                                            'UTP'='TP',
                                            'TDP',
                                            PARTP='TPP',
                                            PO4P='PO4_P',
                                            UTKN='TKN',
                                            # UTN='TN',
                                            TKN='TDKN',
                                            PARTN='TPN',
                                            'DON',
                                            NH3N='NH3_N',
                                            NO3N='NO3_N',
                                            'Na',
                                            CA='Ca',
                                            MG='Mg',
                                            SO4S='SO4_S',
                                            CL='Cl',
                                            'K'),
                             data_col_pattern = '#V#',
                             is_sensor = FALSE,
                             set_to_NA = '',
                             var_flagcol_pattern = '#V#CODE',
                             summary_flagcols = c('TYPE'),
                             keep_empty_rows = TRUE)

        d <- ms_cast_and_reflag(
            d,
            variable_flags_bdl = '*',
            variable_flags_to_drop = 'sentinel',
            variable_flags_dirty = c('Q', 'D*', 'C', 'D', 'DE', 'DQ', 'DC'),
            variable_flags_clean = c('A', 'E', 'N'),
            summary_flags_to_drop = list(TYPE = c('YE', 'S')),
            summary_flags_dirty = list(TYPE = c('C', 'A', 'P', 'B')),
            summary_flags_clean = list(TYPE = c('N', 'QB', 'QS', 'QL', 'QA', 'F', 'G')),
            keep_empty_rows = TRUE
        )

        d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
        d <- synchronize_timestep(d,
                                  admit_NAs = TRUE,
                                  paired_p_and_pchem = TRUE,
                                  allow_pre_interp = TRUE)

    } else { #precip

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'Etc/GMT-8',
                             site_code_col = 'SITECODE',
                             data_cols =  c(PRECIP_CM = 'precipitation'),
                             data_col_pattern = '#V#',
                             is_sensor = FALSE,
                             set_to_NA = '',
                             summary_flagcols = c('TYPE', 'PCODE'),
                             keep_empty_rows = TRUE)

        d <- ms_cast_and_reflag(
            d,
            varflag_col_pattern = NA,
            summary_flags_to_drop = list(TYPE = c('YE', 'S'),
                                         PCODE = 'sentinel'),
            summary_flags_dirty = list(TYPE = 'P',
                                       PCODE = 'E'),
            keep_empty_rows = TRUE
        )

        d$val <- d$val * 10 #cm to mm

        d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
        d <- synchronize_timestep(d,
                                  admit_NAs = TRUE,
                                  paired_p_and_pchem = TRUE,
                                  allow_pre_interp = FALSE)
    }

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

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms006 <- stream_gauge_from_site_data
