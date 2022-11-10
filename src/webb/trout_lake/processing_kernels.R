source('src/webb/sleeper/domain_helpers.R')

install.packages(c('dataRetrieval', 'geoknife', 'sbtools'))
library(dataRetrieval)


#retrieval kernels ####
#discharge: STATUS=READY
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
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = set_details$component)
  
  # call our dataRetrieval function
  q <- readNWISdv(siteNumber="05357215", parameterCd = "00060")
  
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
process_0_VERSIONLESS002 <- function(set_details, network, domain) {
  
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
  q <- readNWISdv(siteNumber="05357230", parameterCd = "00060")
  
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

    # call our dataRetrieval function
     q <- readNWISdv(siteNumber="05357225", parameterCd = "00060")

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
  q <- readNWISdv(siteNumber="05357245", parameterCd = "00060")
  
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

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {


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
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {

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

#stream_chemistry: STATUS=READY
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
        mutate(var = case_when(site_unit == 'dic_mg_l' ~ 'DIC',
                               site_unit == 'npoc_mg_l' ~ 'DOC')) %>%
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


#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__VERSIONLESS001',
                                           'discharge__VERSIONLESS002', 
                                           'discharge__VERSIONLESS003',
                                           'discharge__VERSIONLESS004'))
    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- stream_gauge_from_site_data

