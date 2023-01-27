source('src/webb/sleeper/domain_helpers.R')
source('src/webb/network_helpers.R')

## #retrieval kernels ####
## network = 'webb'
## domain = 'sleeper'

## set_details <- webb_pkernel_setup(network = network, domain = domain, prodcode = "VERSIONLESS001")

## prodname_ms <- set_details$prodname_ms
## site_code <- set_details$site_code
## component <- set_details$component
## url <- set_details$url

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {
    # START OF BLOCK YOU DONT CHANGE #
    # this sets the file path of the raw data, should always be this same format
    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    # this creates that directory, if it doesn't already exist
    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    # END OF BLOCK YOU DONT CHANGE #

    # create documentation file
    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    # download the data form the download link!
    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          skip = FALSE,
                          overwrite = TRUE)

    # this code records metadat about the date, time, and other details
    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#precip_chem: STATUS=READY
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
                          skip = FALSE,
                          overwrite = TRUE)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#discharge: STATUS=READY
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

    rawfile <- glue('{rd}/{c}.csv',
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

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#discharge: STATUS=READY
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

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    # call our dataRetrieval function
    q <- retrieve_usgs_sleeper_daily_q(set_details)

    # download it to the raw file locatin
    write_csv(q, file = rawfile)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#discharge: STATUS=READY
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

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    # call our dataRetrieval function
    q <- retrieve_usgs_sleeper_daily_q(set_details)

    # download it to the raw file locatin
    write_csv(q, file = rawfile)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
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
                          skip = FALSE,
                          overwrite = TRUE)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
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

    rawfile <- glue('{rd}/{c}.csv',
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

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}


#munge kernels ####
## IsDate <- function(mydate, date.format = "%m/%d/%y") {
##   tryCatch(!is.na(as.Date(mydate, date.format)),
##            error = function(err) {FALSE})
## }

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.delim(rawfile, sep = ',') %>%
        mutate(site = 'RW9') %>%
        as_tibble()

    # NOTE: return to basin-wide precip, and to 30% datetime loss
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%y'),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'site',
                         data_cols =  c('Precip..mm' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    # NOTE: return too all of these
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    # Precipitation is daily
    ## d <- d %>%
    ##     mutate(val = val*15)
    ## d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    # NOTE: not applicable
    ## d <- synchronize_timestep(d)

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

#precip_chem: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # temp_dir <- file.path(tempdir(), 'macrosheds_unzip_dir/')

    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    # unlink(paste0(temp_dir, '/*'))

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

    all_sites <- tibble()
    for(i in 1:length(all_sites_files)){

        site_code <- str_match(string = all_sites_files[i],
                               pattern = '([^\\/\\\\]+)_[dD]ischarge.xlsx$')[, 2]

        one_site <- readxl::read_excel(all_sites_files[i], sheet = 'Corrected')
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
                         datetime_cols = list('standard_time' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site',
                         data_cols =  c('q' = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    # Discharge is in m^3/s, converting to L/s
    d <- d %>%
        mutate(val = val*1000)

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

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {


    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # temp_dir <- file.path(tempdir(), 'macrosheds_unzip_dir')
    temp_dir <- tempdir()

    # dir.create(temp_dir,
    #            showWarnings = FALSE,
    #            recursive = TRUE)
    #
    # unlink(paste0(temp_dir, '/*'),
    #        recursive = TRUE,
    #        force = TRUE)

    unzip(rawfile,
          exdir = temp_dir)

    temp_dir_files <- list.files(temp_dir, recursive = TRUE)
    temp_dir_files_p <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)

    temp_dir_files_csv <- temp_dir_files[grepl('.csv', temp_dir_files)]
    temp_dir_files_p <- temp_dir_files_p[grepl('.csv', temp_dir_files)]

    removed_depth <- !grepl('depth|well', temp_dir_files_csv)

    file_names <- temp_dir_files_csv[removed_depth]
    file_paths <- temp_dir_files_p[removed_depth]

    all_sites <- tibble()
    for(i in 1:length(file_names)){

        if(file_names[i] == 'locations.csv') next

        site_info <- str_split_fixed(file_names[i], '_', n = Inf)

        if(length(site_info) == 1) next

        if(length(site_info) == 2){
            site_code <- site_info[1,1]
        } else{
            site_code <- site_info[1,1:(length(site_info)-1)]
            site_code <- paste(site_code, collapse = '_')
        }

        site_var <- site_info[1,length(site_info)]
        site_var <- paste(site_var, collapse = '_')
        site_var <- str_remove(site_var, '.csv')

        site_table <- read.csv(file_paths[i], colClasses = 'character')

        colname <- colnames(site_table)[2]

        site_table <- site_table %>%
            rename(val = !!colname) %>%
            mutate(site = !!site_code,
                   site_var = !!site_var,
                   site_var_unit = !!colname,
                   file_name = !!file_names[i])

        all_sites <- rbind(all_sites, site_table)
    }

    # Can't determin for sure where rockcreek is, removing
    # east_above_rustlers is listed as the same location as rutler, a seprate creek,
    #    removing for now
    # removeing feild blanks
    # EBC_ISCO == East_below_Copper
    # er and er_plm# are pizometers
    d <- all_sites %>%
        filter(site != 'rockcreek',
               site != 'filterblank',
               site != 'filter_blank',
               site != 'fieldblank',
               site != 'east_above_rustlers',
               site != 'er',
               site != 'er_plm1',
               site != 'er_plm4',
               site != 'er_plm6') %>%
        mutate(var = case_when(site_var == 'deltad' ~ 'deuterium',
                               site_var == 'deltao18' ~ 'd18O')) %>%
        mutate(site_code = case_when(site == 'avery' ~ 'Avery',
                                     site == 'benthette' ~ 'Benthette',
                                     site == 'bradley' ~ 'Bradley',
                                     site == 'copper' ~ 'Copper',
                                     site %in% c('east_below_copper', 'ebc_isco') ~ 'EBC',
                                     site == 'gothic' ~ 'Gothic',
                                     site == 'marmot' ~ 'Marmot',
                                     site == 'ph_isco' ~ 'PH',
                                     site == 'quigley' ~ 'Quigley',
                                     site == 'rock' ~ 'Rock',
                                     site == 'rustlers' ~ 'Rustlers',
                                     TRUE ~ site)) %>%
        mutate(datetime = as_datetime(utc_time, format = '%Y-%m-%d', tz = 'UTC')) %>%
        mutate(val = as.numeric(val),
               ms_status = 0) %>%
        select(datetime, site_code, val, var, ms_status) %>%
        filter(!is.na(val),
               !is.na(var)) %>%
        as_tibble()

    # Catch this new site name
    d$site_code[d$site_code == 'east_above_quigley'] <- 'EAQ'

    # need to add overwrite options
    d <- identify_sampling_bypass(d,
                                  is_sensor = FALSE,
                                  date_col = 'datetime',
                                  network = network,
                                  domain = domain,
                                  prodname_ms = prodname_ms,
                                  sampling_type = 'G')

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

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # temp_dir <- file.path(tempdir(), 'macrosheds_unzip_dir/')

    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    # unlink(paste0(temp_dir, '/*'))

    unzip(rawfile,
          exdir = temp_dir)

    temp_dir_files <- list.files(temp_dir, recursive = TRUE)
    temp_dir_files_p <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)

    removed_depth <- !grepl('depth|well', temp_dir_files)

    file_names <- temp_dir_files[removed_depth]
    file_paths <- temp_dir_files_p[removed_depth]

    all_sites <- tibble()
    for(i in 1:length(file_names)){

        if(file_names[i] == 'locations.csv') next

        site_info <- str_split_fixed(file_names[i], '_', n = Inf)

        if(length(site_info) == 1) next

        if(grepl('aqlithiumion_aslithium|aqberylliumion_asberyllium', file_names[i])){

            if(length(site_info) == 4){
                site_code <- site_info[1,1]
            } else{
                site_code <- site_info[1,1:(length(site_info)-3)]
                site_code <- paste(site_code, collapse = '_')
            }

        } else{
            if(length(site_info) == 3){
                site_code <- site_info[1,1]
            } else{
                 site_code <- site_info[1,1:(length(site_info)-2)]
                 site_code <- paste(site_code, collapse = '_')
            }
        }

        #site_code_alt <- paste(site_info[1,1:2], collapse = '_')
        site_var <- site_info[1,length(site_info)-1]
        site_unit <- str_remove(site_info[1,length(site_info)], '.csv')

        site_table <- read.csv(file_paths[i], colClasses = 'character')

        colname <- colnames(site_table)[2]

        site_table <- site_table %>%
            rename(val = !!colname) %>%
            mutate(site = !!site_code,
                   site_var = !!site_var,
                   site_unit = !!site_unit,
                   site_var_unit = !!colname,
                   file_name = !!file_names[i])

        all_sites <- rbind(all_sites, site_table)
    }

    # Can't determin for sure where rockcreek is, removing
    # east_above_rustlers is listed as the same location as rutler, a seprate creek,
    #    removing for now
    # removeing feild blanks
    # EBC_ISCO == East_below_Copper
    # cp#, er_plm#, and mdp# are all pizometers
    d <- all_sites %>%
        filter(site != 'rockcreek',
               site != 'filterblank',
               site != 'filter_blank',
               site != 'fieldblank',
               site != 'east_above_rustlers',
               site != 'cp1',
               site != 'cp2',
               site != 'cp3',
               site != 'cp4',
               site != 'cp5',
               site != 'cp6',
               site != 'er_plm1',
               site != 'er_plm4',
               site != 'er_plm6',
               site != 'mdp1',
               site != 'mdp2',
               site != 'mdp3') %>%
        mutate(var = case_when(site_var_unit == 'aluminum_ppb' ~ 'Al',
                               site_var_unit == 'antimony_ppb' ~ 'Sb',
                               site_var_unit == 'aqberylliumion_asberyllium_ppb' ~ 'Be',
                               site_var_unit == 'aqlithiumion_aslithium_ppb' ~ 'Li',
                               site_var_unit == 'arsenic_ppb' ~ 'As',
                               site_var_unit == 'barium_ppb' ~ 'Ba',
                               site_var_unit == 'boron_ppb' ~ 'B',
                               site_var_unit == 'calcium_ppb' ~ 'Ca',
                               site_var_unit == 'cadmium_ppb' ~ 'Cd',
                               site_var_unit == 'cesium_ppb' ~ 'Cs',
                               site_var_unit == 'chromium_ppb' ~ 'Cr',
                               site_var_unit == 'cobalt_ppb' ~ 'Co',
                               site_var_unit == 'copper_ppb' ~ 'Cu',
                               site_var_unit == 'europium_ppb' ~ 'Eu',
                               site_var_unit == 'germanium_ppb' ~ 'Ge',
                               site_var_unit == 'iron_ppb' ~ 'Fe',
                               site_var_unit == 'manganese_ppb' ~ 'Mn',
                               site_var_unit == 'lead_ppb' ~ 'Pb',
                               site_var_unit == 'magnesium_ppb' ~ 'Mg',
                               site_var_unit == 'molybdenum_ppb' ~ 'Mo',
                               site_var_unit == 'nickel_ppb' ~ 'Ni',
                               site_var_unit == 'phosphorus_ppb' ~ 'P',
                               site_var_unit == 'potassium_ppb' ~ 'K',
                               site_var_unit == 'rubidium_ppb' ~ 'Rb',
                               site_var_unit == 'selenium_ppb' ~ 'Se',
                               site_var_unit == 'silicon_ppb' ~ 'Si',
                               site_var_unit == 'silver_ppb' ~ 'Ag',
                               site_var_unit == 'sodium_ppb' ~ 'Na',
                               site_var_unit == 'strontium_ppb' ~ 'Sr',
                               site_var_unit == 'thorium_ppb' ~ 'Th',
                               site_var_unit == 'tin_ppb' ~ 'Sn',
                               site_var_unit == 'titanium_ppb' ~ 'Ti',
                               site_var_unit == 'uranium_ppb' ~ 'U',
                               site_var_unit == 'vanadium_ppb' ~ 'V',
                               site_var_unit == 'zinc_ppb' ~ 'Zn',
                               site_var_unit == 'zirconium_ppb' ~ 'Zr')) %>%
        mutate(site_code = case_when(site == 'avery' ~ 'Avery',
                                     site == 'benthette' ~ 'Benthette',
                                     site == 'bradley' ~ 'Bradley',
                                     site == 'copper' ~ 'Copper',
                                     site %in% c('east_below_copper', 'ebc_isco') ~ 'EBC',
                                     site == 'gothic' ~ 'Gothic',
                                     site == 'marmot' ~ 'Marmot',
                                     site == 'ph_isco' ~ 'PH',
                                     site == 'quigley' ~ 'Quigley',
                                     site == 'rock' ~ 'Rock',
                                     site == 'rustlers' ~ 'Rustlers',
                                     TRUE ~ site)) %>%
        mutate(datetime = as_datetime(utc_time, format = '%Y-%m-%d', tz = 'UTC')) %>%
        mutate(val = as.numeric(val),
               ms_status = 0) %>%
        select(datetime, site_code, val, var, ms_status) %>%
        as_tibble()

    # look <- d %>%
    #     group_by(datetime, site_code, var) %>%
    #     summarise(n = n())
    # Catch this new site name
    d$site_code[d$site_code == 'east_above_quigley'] <- 'EAQ'

    # need to add overwrite options
    d <- identify_sampling_bypass(d,
                                  is_sensor = FALSE,
                                  date_col = 'datetime',
                                  network = network,
                                  domain = domain,
                                  prodname_ms = prodname_ms,
                                  sampling_type = 'G')

    # 107482
    # Units are in ppb, converting to mg/L
    d <- d %>%
        mutate(val = val/1000)

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

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # temp_dir <- file.path(tempdir(), 'macrosheds_unzip_dir/')

    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    # unlink(paste0(temp_dir, '/*'))

    unzip(rawfile,
          exdir = temp_dir)

    temp_dir_files <- list.files(temp_dir, recursive = TRUE)
    temp_dir_files_p <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)

    removed_depth <- !grepl('depth|well', temp_dir_files)

    file_names <- temp_dir_files[removed_depth]
    file_paths <- temp_dir_files_p[removed_depth]

    all_sites <- tibble()
    for(i in 1:length(file_names)){

        if(file_names[i] == 'locations.csv') next

        site_info <- str_split_fixed(file_names[i], '_', n = Inf)

        if(length(site_info) == 1) next

        if(length(site_info) == 5){
            site_code <- site_info[1,1]
        } else{
            site_code <- site_info[1,1:(length(site_info)-4)]
            site_code <- paste(site_code, collapse = '_')
        }

        #site_code_alt <- paste(site_info[1,1:2], collapse = '_')
        site_var <- site_info[1,length(site_info)-3]
        site_unit <- str_remove(site_info[1,(length(site_info)-2):length(site_info)], '.csv')
        site_unit <- paste(site_unit, collapse = '_')

        site_table <- read.csv(file_paths[i], colClasses = 'character')

        colname <- colnames(site_table)[2]

        site_table <- site_table %>%
            rename(val = !!colname) %>%
            mutate(site = !!site_code,
                   site_var = !!site_var,
                   site_unit = !!site_unit,
                   site_var_unit = !!colname,
                   file_name = !!file_names[i])

        all_sites <- rbind(all_sites, site_table)
    }

    # Can't determin for sure where rockcreek is, removing
    # east_above_rustlers is listed as the same location as rutler, a seprate creek,
    #    removing for now
    # removeing feild blanks
    # EBC_ISCO == East_below_Copper
    # er and er_plm# are pizometers
    d <- all_sites %>%
        filter(site != 'rockcreek',
               site != 'filterblank',
               site != 'filter_blank',
               site != 'fieldblank',
               site != 'east_above_rustlers',
               site != 'er',
               site != 'er_plm1',
               site != 'er_plm6') %>%
        mutate(var = case_when(site_var == 'chloride' ~ 'Cl',
                               site_var == 'fluoride' ~ 'F',
                               site_var == 'nitrate' ~ 'NO3',
                               site_var == 'phosphate' ~ 'PO4',
                               site_var == 'sulfate' ~ 'SO4')) %>%
        mutate(site_code = case_when(site == 'avery' ~ 'Avery',
                                     site == 'benthette' ~ 'Benthette',
                                     site == 'bradley' ~ 'Bradley',
                                     site == 'copper' ~ 'Copper',
                                     site %in% c('east_below_copper', 'ebc_isco') ~ 'EBC',
                                     site == 'gothic' ~ 'Gothic',
                                     site == 'marmot' ~ 'Marmot',
                                     site == 'ph_isco' ~ 'PH',
                                     site == 'quigley' ~ 'Quigley',
                                     site == 'rock' ~ 'Rock',
                                     site == 'rustlers' ~ 'Rustlers',
                                     TRUE ~ site)) %>%
        mutate(datetime = as_datetime(utc_time, format = '%Y-%m-%d', tz = 'UTC')) %>%
        mutate(val = as.numeric(val),
               ms_status = 0) %>%
        select(datetime, site_code, val, var, ms_status) %>%
        filter(!is.na(val),
               !is.na(var)) %>%
        as_tibble()

    # Catch this new site name
    d$site_code[d$site_code == 'east_above_quigley'] <- 'EAQ'

    # need to add overwrite options
    d <- identify_sampling_bypass(d,
                                  is_sensor = FALSE,
                                  date_col = 'datetime',
                                  network = network,
                                  domain = domain,
                                  prodname_ms = prodname_ms,
                                  sampling_type = 'G')

    d <- ms_conversions(d,
                        convert_units_from = c('Cl' = 'umol/l',
                                               'F' = 'umol/l',
                                               'NO3' = 'umol/l',
                                               'PO4' = 'umol/l',
                                               'SO4' = 'umol/l'),
                        convert_units_to = c('Cl' = 'mg/l',
                                             'F' = 'mg/l',
                                             'NO3' = 'mg/l',
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

# JUMP: for stream chem work
set_details <- webb_pkernel_setup(network = network, domain = domain, prodcode = "VERSIONLESS006")
prodname_ms <- set_details$prodname_ms
site_code <- set_details$site_code
component <- set_details$component
url <- set_details$url

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # creating a temporary directory to unzip the folder in
    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)
    unzip(rawfile,
          exdir = temp_dir)

    # reading in the contents of the extracted folder
    file_names <- list.files(temp_dir, recursive = TRUE)
    file_paths <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)

    # there are many important files in here, but we are only trying to get
    # the raw stream chemistry data
    chem_fp <- file_paths[grepl('Chemistry.csv', file_paths)]

    # the original file has encoding isssues, and connot be read directly
    # here I use a readr function which "guesses" the most likely encodings
    # which is good to know but I ultimately didnt use
    d_encoding <- readr::guess_encoding(chem_fp)[[1,1]]

    # read in the csv ignoring column name encoding issues
    d <- read.csv(chem_fp, check.names = FALSE)
    d_old_names <- names(d)

    ## NOTE: special character was microgram symbol
    ## replacing with "u"
    d_new_names <- unname(sapply(d_old_names, function(x) gsub('\\\xb5', "u", x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub('<', "", x)))

    # then rename all columns with these new names
    colnames(d) <- d_new_names

    # filter to only stream site type
    d <- d %>%
      filter(Sample_Type == "ST")

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here to be just the old_var = new_var structure
    sleeper_aq_chem = c()

    for(i in 1:length(sleepers_stream_chem_var_info)) {
      og_name <- names(sleepers_stream_chem_var_info[i])
      ms_name <- sleepers_stream_chem_var_info[[i]][3]

      sleeper_aq_chem[og_name] = ms_name
    }

    # original data has a "summary" flag column, which lists simply a variable name -- meaning
    # that it is sctually a variable flag column of sorts. it will be easiest if we can
    # unpack this into a new column, for each variable, where the value = 1 if the original
    # flag column named it in a particular observation. one added issue is this "summary" column
    # has potential for mulitple variables at a time.

    # start by making new column for each chemistry variable
    data_frame <- transform(
              d, Na_flag = ifelse(col1==col3, col1+col2, col1+col3)
    )

    last_col <- colnames(d)[ncol(d)]

    ## df <- d %>%
    ##   mutate(across(Chemistry_Flag:all_of(!!last_col), ~ .x))

    df <- d %>%
      dplyr::mutate(
               across(Chemistry_Flag:all_of(!!last_col) & !ends_with("_Lab"),
                      .fns = list(varflag = ~ case_when(Chemistry_Flag == cur_column() ~ 1)),
                      .names = "{fn}_{col}"
                      ))

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date_Time' = "%Y-%m-%d %H:%M:%S"),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'Sample_Name',
                         data_cols =  sleeper_aq_chem,
                         data_col_pattern = '#V#',
                         # variable specific flag pattern
                         var_flagcol_pattern = '#V#_Lab',
                         # summary (all vars) flag colun name
                         summary_flagcols = 'Chemistry_Flag',
                         is_sensor = FALSE)

    d.l <- ms_cast_and_reflag(d,
                            ## characters that, if found in a variable's "flag" column
                            # will drop that row
                            ### comment out, no dropping from these flags
                            variable_flags_to_drop = 'DIRTY_',

                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            ### have to supply something, this never occurs
                            variable_flags_dirty   = c('DIRTY'),

                            # will turn the *ms_status* column to 0 (e.g. clean)
                            ### 1:18 for each lab
                            ## variable_flags_clean   = c(as.character(1:18)),

                            # will turn the *ms_status* column to 2
                            # (e.g. set value to detection limit divided by 2)
                            ### commenting out for now
                            ## variable_flags_bdl = list(Chemistry_Flag = c())

                            ## characters that, if found in the summary "flag" column
                            # will drop that row
                            summary_flags_to_drop  = list(Chemistry_Flag = c('DIRTY')),

                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            summary_flags_dirty    = list(Chemistry_Flag = unname(sleeper_aq_chem)),

                            # will turn the *ms_status* column to 0 (e.g. clean)
                            ## summary_flags_clean    = list(Chemistry_Flag = c('NA'))

                            # will turn the *ms_status* column to 2
                            # (e.g. set value to detection limit divided by 2)
                            ## summary_flags_bdl = list(Chemistry_Flag = c())
                            )



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
process_1_VERSIONLESS007 <- function(network, domain, prodname_ms, site_code, component) {

    # TDN is listed as g/l it appears but comparing to anoth east river
    # data prod, it looks like (and makes snese) that the unit is ug/l

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # temp_dir <- file.path(tempdir(), 'macrosheds_unzip_dir/')

    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    # unlink(paste0(temp_dir, '/*'))

    unzip(rawfile,
          exdir = temp_dir)

    temp_dir_files <- list.files(temp_dir, recursive = TRUE)
    temp_dir_files_p <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)

    removed_depth <- !grepl('depth|well', temp_dir_files)

    file_names <- temp_dir_files[removed_depth]
    file_paths <- temp_dir_files_p[removed_depth]

    all_sites <- tibble()
    for(i in 1:length(file_names)){

        if(file_names[i] == 'locations.csv') next

        site_info <- str_split_fixed(file_names[i], '_', n = Inf)

        if(length(site_info) == 1) next

        if(length(site_info) == 4){
            site_code <- site_info[1,1]
        } else{
            site_code <- site_info[1,1:(length(site_info)-3)]
            site_code <- paste(site_code, collapse = '_')
        }

        site_var <- site_info[1,length(site_info)-3]
        site_unit <- str_remove(site_info[1,(length(site_info)-2):length(site_info)], '.csv')
        site_unit <- paste(site_unit, collapse = '_')

        site_table <- read.csv(file_paths[i], colClasses = 'character')

        colname <- colnames(site_table)[2]

        site_table <- site_table %>%
            rename(val = !!colname) %>%
            mutate(site = !!site_code,
                   site_var = !!site_var,
                   site_unit = !!site_unit,
                   site_var_unit = !!colname,
                   file_name = !!file_names[i])

        all_sites <- rbind(all_sites, site_table)
    }

    # Can't determin for sure where rockcreek is, removing
    # east_above_rustlers is listed as the same location as rutler, a seprate creek,
    #    removing for now
    # removeing feild blanks
    # EBC_ISCO == East_below_Copper
    # er and er_plm# are pizometers
    d <- all_sites %>%
        filter(site != 'rockcreek',
               site != 'filterblank',
               site != 'filter_blank',
               site != 'fieldblank',
               site != 'east_above_rustlers',
               site != 'er',
               site != 'er_plm1',
               site != 'er_plm4',
               site != 'er_plm6') %>%
        mutate(var = case_when(site_unit == 'tdn_g_l' ~ 'TDN',
                               site_unit == 'ammonia_n_ppm' ~ 'NH3_N')) %>%
        mutate(site_code = case_when(site == 'avery' ~ 'Avery',
                                     site == 'benthette' ~ 'Benthette',
                                     site == 'bradley' ~ 'Bradley',
                                     site == 'copper' ~ 'Copper',
                                     site %in% c('east_below_copper', 'ebc_isco') ~ 'EBC',
                                     site == 'gothic' ~ 'Gothic',
                                     site == 'marmot' ~ 'Marmot',
                                     site == 'ph_isco' ~ 'PH',
                                     site == 'quigley' ~ 'Quigley',
                                     site == 'rock' ~ 'Rock',
                                     site == 'rustlers' ~ 'Rustlers',
                                     TRUE ~ site)) %>%
        mutate(datetime = as_datetime(utc_time, format = '%Y-%m-%d', tz = 'UTC')) %>%
        mutate(val = as.numeric(val),
               ms_status = 0) %>%
        select(datetime, site_code, val, var, ms_status) %>%
        filter(!is.na(val),
               !is.na(var)) %>%
        as_tibble()

    # Catch this new site name
    d$site_code[d$site_code == 'east_above_quigley'] <- 'EAQ'

    # need to add overwrite options
    d <- identify_sampling_bypass(d,
                                  is_sensor = FALSE,
                                  date_col = 'datetime',
                                  network = network,
                                  domain = domain,
                                  prodname_ms = prodname_ms,
                                  sampling_type = 'G')

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
