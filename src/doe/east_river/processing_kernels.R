
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

    rawfile <- glue('{rd}/{c}.txt',
                    rd = raw_data_dest,
                    c = set_details$component)

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
    #
    #     login_escape <- sub(pattern = '@',
    #                         replacement = '%40',
    #                         x = set_details$orcid_login)
    #
    #     url_with_auth <- sub(pattern = '://',
    #                          replacement = paste0('://', login_escape, ':',
    #                                               set_details$orcid_pass, '@'),
    #                          x = set_details$url)
    #
    #     download.file(url = url_with_auth,
    #                   destfile = rawfile,
    #                   quiet = FALSE,
    #                   cacheOK = FALSE)
    #
    #     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
#
#     login_escape <- sub(pattern = '@',
#                         replacement = '%40',
#                         x = set_details$orcid_login)
#
#     url_with_auth <- sub(pattern = '://',
#                          replacement = paste0('://', login_escape, ':',
#                                               set_details$orcid_pass, '@'),
#                          x = set_details$url)
#
#     download.file(url = url_with_auth,
#                   destfile = rawfile,
#                   quiet = FALSE,
#                   cacheOK = FALSE)
#
#     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
    #
    #     login_escape <- sub(pattern = '@',
    #                         replacement = '%40',
    #                         x = set_details$orcid_login)
    #
    #     url_with_auth <- sub(pattern = '://',
    #                          replacement = paste0('://', login_escape, ':',
    #                                               set_details$orcid_pass, '@'),
    #                          x = set_details$url)
    #
    #     download.file(url = url_with_auth,
    #                   destfile = rawfile,
    #                   quiet = FALSE,
    #                   cacheOK = FALSE)
    #
    #     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
    #
    #     login_escape <- sub(pattern = '@',
    #                         replacement = '%40',
    #                         x = set_details$orcid_login)
    #
    #     url_with_auth <- sub(pattern = '://',
    #                          replacement = paste0('://', login_escape, ':',
    #                                               set_details$orcid_pass, '@'),
    #                          x = set_details$url)
    #
    #     download.file(url = url_with_auth,
    #                   destfile = rawfile,
    #                   quiet = FALSE,
    #                   cacheOK = FALSE)
    #
    #     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
    #
    #     login_escape <- sub(pattern = '@',
    #                         replacement = '%40',
    #                         x = set_details$orcid_login)
    #
    #     url_with_auth <- sub(pattern = '://',
    #                          replacement = paste0('://', login_escape, ':',
    #                                               set_details$orcid_pass, '@'),
    #                          x = set_details$url)
    #
    #     download.file(url = url_with_auth,
    #                   destfile = rawfile,
    #                   quiet = FALSE,
    #                   cacheOK = FALSE)
    #
    #     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS006 <- function(set_details, network, domain) {

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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
    #
    #     login_escape <- sub(pattern = '@',
    #                         replacement = '%40',
    #                         x = set_details$orcid_login)
    #
    #     url_with_auth <- sub(pattern = '://',
    #                          replacement = paste0('://', login_escape, ':',
    #                                               set_details$orcid_pass, '@'),
    #                          x = set_details$url)
    #
    #     download.file(url = url_with_auth,
    #                   destfile = rawfile,
    #                   quiet = FALSE,
    #                   cacheOK = FALSE)
    #
    #     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS007 <- function(set_details, network, domain) {

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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
    #
    #     login_escape <- sub(pattern = '@',
    #                         replacement = '%40',
    #                         x = set_details$orcid_login)
    #
    #     url_with_auth <- sub(pattern = '://',
    #                          replacement = paste0('://', login_escape, ':',
    #                                               set_details$orcid_pass, '@'),
    #                          x = set_details$url)
    #
    #     download.file(url = url_with_auth,
    #                   destfile = rawfile,
    #                   quiet = FALSE,
    #                   cacheOK = FALSE)
    #
    #     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS008 <- function(set_details, network, domain) {

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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          # username = set_details$orcid_login,
                          # password = set_details$orcid_pass,
                          skip = FALSE,
                          overwrite = TRUE)
    #
    #     login_escape <- sub(pattern = '@',
    #                         replacement = '%40',
    #                         x = set_details$orcid_login)
    #
    #     url_with_auth <- sub(pattern = '://',
    #                          replacement = paste0('://', login_escape, ':',
    #                                               set_details$orcid_pass, '@'),
    #                          x = set_details$url)
    #
    #     download.file(url = url_with_auth,
    #                   destfile = rawfile,
    #                   quiet = FALSE,
    #                   cacheOK = FALSE)
    #
    #     res <- httr::HEAD(url_with_auth)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#discharge; stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS009 <- function(set_details, network, domain){

    if(grepl('chemistry', prodname_ms)) return()

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

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          skip = FALSE,
                          overwrite = TRUE)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = set_details$url,
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#munge kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.txt',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.delim(rawfile, sep = ',') %>%
        mutate(site = 'KCOMKRET2') %>%
        as_tibble()

    #DATETIME is messed up cuz of one digit thing
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('dateTime' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c('PrecipRate' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    # Precipitation is reported in mm/min, it is a 15 minute time step so must
    # convert
    d <- d %>%
        mutate(val = val*15)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d,
                              admit_NAs = TRUE,
                              allow_pre_interp = TRUE)

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
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    unzip(rawfile,
          exdir = temp_dir)

    temp_dir_files <- list.files(temp_dir, full.names = TRUE, recursive = TRUE)
    rel_file <- grep('EastRiver', temp_dir_files, value = TRUE)

    one_site_files <- glue(temp_dir, '/onesite')
    dir.create(one_site_files)

    rel_file_1 <- grep('EastRiver_2014-08_2016-10', rel_file, value = TRUE)

    unzip(rel_file_1,
          exdir = one_site_files)

    all_sites_files <- list.files(one_site_files, full.names = TRUE)
    all_sites_files <- all_sites_files[grepl('.xlsx', all_sites_files)]
    all_sites_files <- grep('WG', all_sites_files, value = TRUE, invert = TRUE)

    all_sites <- tibble()
    for(i in 1:length(all_sites_files)){

        site_code <- str_match(string = all_sites_files[i],
                               pattern = '([^\\/\\\\]+)_[dD]ischarge.xlsx$')[, 2]

        one_site <- readxl::read_excel(all_sites_files[i], sheet = 'Corrected')

        if(is.na(as.POSIXct(pull(one_site[1, 1])))) stop('datetime issue')
        if(is.na(as.numeric(pull(one_site[1, 2])))) stop('value issue')

        time_col <- grep('time|Time', colnames(one_site), value = TRUE)
        q_col <- grep('cms', colnames(one_site), value = TRUE)
        one_site <- one_site %>%
            select('standard_time' = !!time_col, 'q' = !!q_col) %>%
            mutate(site = !!site_code)

        all_sites <- rbind(one_site, all_sites)
    }

    rel_file_2 <- grep('EastRiver_2017-10_2018-10', rel_file, value = TRUE)

    unlink(one_site_files, recursive = TRUE)

    unzip(rel_file_2,
          exdir = one_site_files)

    all_sites_files <- list.files(one_site_files, full.names = TRUE, recursive = TRUE)
    all_sites_files <- all_sites_files[grepl('.xlsx', all_sites_files)]

    one_site <- readxl::read_excel(all_sites_files, sheet = 'Corrected') %>%
        select(standard_time = `Standard Time`,
               q = `Q m3/s`) %>%
        mutate(site = 'PH')

    all_sites <- rbind(all_sites, one_site) %>%
        as_tibble()

    d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                         datetime_cols = c('standard_time' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         alt_site_code = east_river_site_name_map,
                         data_cols =  c('q' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = '-9999',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    # Discharge is in m^3/s, converting to L/s
    d <- d %>%
        mutate(val = val * 1000)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

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

    # rawfile='data/doe/east_river/raw/stream_chemistry__VERSIONLESS003/sitename_NA/east_river_isotopes.zip'
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile,
          exdir = temp_dir)

    zip_par_dir <- list.files(temp_dir, pattern = 'isotope')

    fp <- list.files(file.path(temp_dir, zip_par_dir),
                     recursive = TRUE,
                     full.names = TRUE)
    fp <- grep('depth|well', fp, value = TRUE, invert = TRUE)

    all_sites <- tibble()
    for(i in seq_along(fp)){

        fn <- basename(fp[i])

        if(grepl('splains|evans|blank|locations', fn)) next

        site_code <- str_extract(fn, '([^_]+_[^_]+)', group = 1)
        site_var <- str_extract(fn, '[^_]+_[^_]+_(.+)(?=\\.csv$)', group = 1)

        d_ <- read.csv(fp[i], colClasses = 'character')

        if(! colnames(d_)[2] %in% c('deltad', 'deltao18')) stop('var issue')
        if(is.na(as.Date(d_[2, 1]))) stop('date issue')
        if(is.na(as.numeric(d_[2, 2]))) stop('value issue')

        d_ <- d_ %>%
            rename(val = 2) %>%
            mutate(site = !!site_code,
                   var = !!site_var)

        all_sites <- rbind(all_sites, d_)
    }

    all_sites <- all_sites %>%
        filter(site %in% east_river_sites_of_interest,
               ! grepl('Y', date)) %>%
        mutate(val = as.numeric(val),
               site = toupper(site)) %>%
        filter(val > -1000) %>%
        group_by(date, site, var) %>%
        summarize(val = mean(val, na.rm = TRUE)) %>%
        ungroup() %>%
        pivot_wider(names_from = var,
                    values_from = val)

    d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                         datetime_cols = c('date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c(deltad = 'dD',
                                        deltao18 = 'd18O'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

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

    unlink(temp_dir, recursive = TRUE)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component){

    # rawfile='data/doe/east_river/raw/stream_chemistry__VERSIONLESS004/sitename_NA/east_river_cations.zip'
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile,
          exdir = temp_dir)

    zip_par_dir <- list.files(temp_dir, pattern = 'cation')

    fp <- list.files(file.path(temp_dir, zip_par_dir),
                     recursive = TRUE,
                     full.names = TRUE)
    fp <- grep('depth|well', fp, value = TRUE, invert = TRUE)

    all_sites <- tibble()
    for(i in seq_along(fp)){

        fn <- basename(fp[i])

        if(grepl('splains|evans|blank|locations', fn)) next

        site_code <- str_extract(fn, '([^_]+_[^_]+)', group = 1)
        site_var <- str_extract(fn, '[^_]+_[^_]+_(.+)(?=\\.csv$)', group = 1)

        d_ <- read.csv(fp[i], colClasses = 'character')

        if(! colnames(d_)[2] %in% names(east_river_cations)) stop('var issue')
        if(! d_[1, 2] %in% c('ppb')) stop('unit issue')
        if(is.na(as.Date(d_[2, 1]))) stop('date issue')
        if(is.na(as.numeric(d_[2, 2]))) stop('value issue')

        d_ <- d_ %>%
            rename(val = 2) %>%
            mutate(site = !!site_code,
                   var = !!site_var)

        all_sites <- rbind(all_sites, d_)
    }

    all_sites <- all_sites %>%
        filter(site %in% east_river_sites_of_interest,
               ! grepl('Y', date)) %>%
        mutate(val = as.numeric(val),
               site = toupper(site)) %>%
        filter(val > -1000) %>%
        group_by(date, site, var) %>%
        summarize(val = mean(val, na.rm = TRUE)) %>%
        ungroup() %>%
        pivot_wider(names_from = var,
                    values_from = val)

    d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                         datetime_cols = c('date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  east_river_cations,
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    # Units are ppb, converting to approx mg/L
    d <- mutate(d, val = val / 1000)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

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

    unlink(temp_dir, recursive = TRUE)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component){

    # rawfile='data/doe/east_river/raw/stream_chemistry__VERSIONLESS005/sitename_NA/east_river_anions.zip'
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()

    unzip(rawfile,
          exdir = temp_dir)

    zip_par_dir <- list.files(temp_dir, pattern = 'anion')

    fp <- list.files(file.path(temp_dir, zip_par_dir),
                     recursive = TRUE,
                     full.names = TRUE)
    fp <- grep('depth|well', fp, value = TRUE, invert = TRUE)

    all_sites <- tibble()
    for(i in seq_along(fp)){

        fn <- basename(fp[i])

        if(grepl('splains|evans|blank|locations', fn)) next

        site_code <- str_extract(fn, '([^_]+_[^_]+)', group = 1)
        site_var <- str_extract(fn, '[^_]+_[^_]+_(.+)(?=\\.csv$)', group = 1)

        d_ <- read.csv(fp[i], colClasses = 'character')

        if(! colnames(d_)[2] %in% east_river_anions) stop('var issue')
        if(! d_[1, 2] %in% c('µmol/L; µM')) stop('unit issue')
        if(is.na(as.Date(d_[2, 1]))) stop('date issue')
        if(is.na(as.numeric(d_[2, 2]))) stop('value issue')

        d_ <- d_ %>%
            rename(val = 2) %>%
            mutate(site = !!site_code,
                   var = !!site_var)

        all_sites <- rbind(all_sites, d_)
    }

    all_sites <- all_sites %>%
        filter(site %in% east_river_sites_of_interest,
               # var %in% east_river_anions,
               ! grepl('Y', date)) %>%
        mutate(val = as.numeric(val),
               site = toupper(site)) %>%
        filter(val > -1000) %>%
        group_by(date, site, var) %>%
        summarize(val = mean(val, na.rm = TRUE)) %>%
        ungroup() %>%
        pivot_wider(names_from = var,
                    values_from = val)

    if('nitrite' %in% colnames(all_sites)) stop('re-enable nitrate in funcs below')

    d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                         datetime_cols = c('date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c(chloride = 'Cl',
                                        nitrate = 'NO3',
                                        # nitrite = 'NO2',
                                        phosphate = 'PO4',
                                        fluoride = 'F',
                                        sulfate = 'SO4'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('Cl' = 'umol/l',
                                               'F' = 'umol/l',
                                               'NO3' = 'umol/l',
                                               # 'NO2' = 'umol/l',
                                               'PO4' = 'umol/l',
                                               'SO4' = 'umol/l'),
                        convert_units_to = c('Cl' = 'mg/l',
                                             'F' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             # 'NO2' = 'mg/l',
                                             'PO4' = 'mg/l',
                                             'SO4' = 'mg/l'))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

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

    unlink(temp_dir, recursive = TRUE)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component){

    # rawfile='data/doe/east_river/raw/stream_chemistry__VERSIONLESS006/sitename_NA/east_river_carbon.zip'
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()

    unzip(rawfile,
          exdir = temp_dir)

    zip_par_dir <- list.files(temp_dir, pattern = 'dic')

    fp <- list.files(file.path(temp_dir, zip_par_dir),
                     recursive = TRUE,
                     full.names = TRUE)
    fp <- grep('depth|well', fp, value = TRUE, invert = TRUE)

    all_sites <- tibble()
    for(i in seq_along(fp)){

        fn <- basename(fp[i])

        if(grepl('splains|evans|blank|locations', fn)) next

        site_code <- str_extract(fn, '([^_]+_[^_]+)', group = 1)
        site_var <- str_extract(fn, '[^_]+_[^_]+_(.+)(?=\\.csv$)', group = 1)

        d_ <- read.csv(fp[i], colClasses = 'character')

        if(! colnames(d_)[2] %in% c('dic', 'npoc')) stop('var issue')
        if(! d_[1, 2] %in% c('mg.L-1')) stop('unit issue')
        if(is.na(as.Date(d_[2, 1]))) stop('date issue')
        if(is.na(as.numeric(d_[2, 2]))) stop('value issue')

        d_ <- d_ %>%
            rename(val = 2) %>%
            mutate(site = !!site_code,
                   var = !!site_var)

        all_sites <- rbind(all_sites, d_)
    }

    all_sites <- all_sites %>%
        filter(site %in% east_river_sites_of_interest,
               ! grepl('Y', date)) %>%
        mutate(val = as.numeric(val),
               site = toupper(site)) %>%
        filter(val > -1000) %>%
        group_by(date, site, var) %>%
        summarize(val = mean(val, na.rm = TRUE)) %>%
        ungroup() %>%
        pivot_wider(names_from = var,
                    values_from = val)

    d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                         datetime_cols = c('date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c(dic = 'DIC',
                                        npoc = 'DOC'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

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

    unlink(temp_dir, recursive = TRUE)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS007 <- function(network, domain, prodname_ms, site_code, component){

    #data/doe/east_river/raw/stream_chemistry__VERSIONLESS007/sitename_NA/east_river_nitrogen.zip

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()

    unzip(rawfile,
          exdir = temp_dir)

    zip_par_dir <- list.files(temp_dir, pattern = 'ammonia')

    fp <- list.files(file.path(temp_dir, zip_par_dir),
                     recursive = TRUE,
                     full.names = TRUE)
    fp <- grep('depth|well', fp, value = TRUE, invert = TRUE)

    all_sites <- tibble()
    for(i in seq_along(fp)){

        fn <- basename(fp[i])

        if(grepl('splains|evans|blank|locations', fn)) next

        site_code <- str_extract(fn, '([^_]+_[^_]+)', group = 1)
        site_var <- str_extract(fn, '[^_]+_[^_]+_(.+)(?=\\.csv$)', group = 1)

        d_ <- read.csv(fp[i], colClasses = 'character')

        if(! colnames(d_)[2] %in% c('tdn', 'ammonia_n')) stop('var issue')
        if(! d_[1, 2] %in% c('ug.L-1', 'ppm')) stop('unit issue')
        if(is.na(as.Date(d_[2, 1]))) stop('date issue')
        if(is.na(as.numeric(d_[2, 2]))) stop('value issue')

           d_ <- d_ %>%
            rename(val = 2) %>%
            mutate(site = !!site_code,
                   var = !!site_var)

        all_sites <- rbind(all_sites, d_)
    }

    all_sites <- all_sites %>%
        filter(site %in% east_river_sites_of_interest,
               ! grepl('Y', date)) %>%
        mutate(val = as.numeric(val),
               site = toupper(site)) %>%
        filter(val > -1000) %>%
        group_by(date, site, var) %>%
        summarize(val = mean(val, na.rm = TRUE)) %>%
        ungroup() %>%
        pivot_wider(names_from = var,
                    values_from = val)

    d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                         datetime_cols = c('date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         data_cols =  c(tdn = 'TDN',
                                        ammonia_n = 'NH3_N'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('TDN' = 'ug/l'),
                        convert_units_to = c('TDN' = 'mg/l'))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

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

    unlink(temp_dir, recursive = TRUE)

    return()
}

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS008 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    unzip(rawfile,
          exdir = temp_dir)

    path <- paste(temp_dir, 'data', sep = '/')
    sheds <- sf::st_read(path, quiet = TRUE)
    projstring <- choose_projection(unprojected = TRUE)

    sitecodes <- names(east_river_site_name_map)
    sitenames <- unname(east_river_site_name_map)
    sheds <- sheds %>%
        filter(NAME %in% sitenames) %>%
        mutate(NAME = sitecodes[match(NAME, sitenames)]) %>%
        select(site_code = NAME, area = km2) %>%
        mutate(area = area * 100)

    sites <- unique(sheds$site_code)

    for(s in 1:length(sites)){

        d_site <- sheds %>%
            filter(site_code == !!sites[s]) %>%
            sf::st_transform(., crs = projstring)

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = TRUE)
    }

    unlink(temp_dir, recursive = TRUE)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS009 <- function(network, domain, prodname_ms, site_code, component){

    is_q_run <- grepl('discharge', prodname_ms)

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = ifelse(is_q_run, prodname_ms, 'discharge__VERSIONLESS009'),
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()

    zipped_files <- unzip(rawfile, list = TRUE)

    files_wanted <- zipped_files$Name[! grepl('Stream_discharge_data_collected_within_the_East_River_Colorado_for_the_Lawrence_Berkeley_National_Laboratory',
                                              zipped_files$Name)]
    unzip(rawfile,
          files = files_wanted,
          exdir = temp_dir)

    temp_dir_files <- list.files(temp_dir, full.names = TRUE)
    pdir <- grep('stream_discharge_data_wy', temp_dir_files, ignore.case = TRUE, value = TRUE)

    if(! length(pdir)) stop('major filename change')

    qfiles <- list.files(pdir, recursive = TRUE, full.names = TRUE, pattern = 'Daily')
    qfiles <- grep('NL|Graph', qfiles, value = TRUE, invert = TRUE)

    all_sites <- tibble()
    for(i in seq_along(qfiles)){

        qf <- qfiles[i]

        site_code <- str_extract(qf, '([^_]+)(?=_Mean_Daily)')

        nrow_header <- read_lines(qf, n_max = 1) %>%
            str_extract('\\d+') %>%
            as.numeric()

        d_ <- sm(read_csv(
            qf,
            skip = nrow_header - 1,
            col_select = any_of(c('Date', 'date', 'DateTime', 'Q',
                                  'mean_daily_Q', 'Daily_Mean_Q', 'Mean_daily_Q',
                                  'Mean_Daily_Q', 'mean daily Q (m3/s)',
                                  'Water_Temperature', 'Notes'))
        ))

        if(ncol(d_) != 3 && ! i %in% c(11, 15, 19, 23, 27, 28, 29)){
            print(read_lines(qf, n_max = 8))
            stop('column issue')
        }

        d_ <- rename_with(d_, tolower)
        d_ <- rename_with(d_, ~sub('mean daily q \\(m3/s\\)', 'mean_daily_q', .))
        d_ <- rename_with(d_, ~sub('daily_mean_q|^q$', 'mean_daily_q', .))
        d_ <- mutate(d_, across(any_of('datetime'), as.Date))
        d_ <- rename_with(d_, ~sub('date|datetime', 'date', .))

        if(! is.Date(d_$date[1])) d_$date <- as.Date(d_$date, format = '%m/%d/%y')

        d_$site <- site_code

        all_sites <- bind_rows(all_sites, d_)
    }

    all_sites <- filter(all_sites,
                        ! is.na(date),
                        ! site %in% c('EBC', 'GSB'))

    if(is_q_run){
        target_var <- c(mean_daily_q = 'discharge')
    } else {
        target_var <- c(water_temperature = 'temp')
    }

    d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                         datetime_cols = c('date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-7',
                         site_code_col = 'site',
                         alt_site_code = east_river_site_name_map,
                         data_cols =  target_var,
                         data_col_pattern = '#V#',
                         set_to_NA = '-9999',
                         summary_flagcols = 'notes',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(notes = c('N/A')),
                            summary_flags_to_drop = list(notes = 'sentinel'))

    if(is_q_run){
        d <- mutate(d, val = val * 1000) #m^3/s -> L/s
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__VERSIONLESS003',
                                           'stream_chemistry__VERSIONLESS004',
                                           'stream_chemistry__VERSIONLESS005',
                                           'stream_chemistry__VERSIONLESS006',
                                           'stream_chemistry__VERSIONLESS007'))

}

#discharge: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__VERSIONLESS002',
                                           'discharge__VERSIONLESS009'))
    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_stream_flux

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms005 <- stream_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms006 <- derive_precip_pchem_pflux
