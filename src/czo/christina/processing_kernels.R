
#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2407 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_1000 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2400 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2465 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_1001 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.dat')

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2565 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_2407 <- function(network, domain, prodname_ms, site_name, components) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = components)

    fils <- rawfile[!grepl('HEADER', rawfile)]

    all <- tibble()
    for(i in 1:length(fils)){

        check_lines <- read_file(fils[i])
        check_lines <- str_split_fixed(check_lines, '\\n|\\r\\r', n = Inf)
        data_line <- grep('\\\\data', check_lines)

        d <- sw(read_csv(fils[i], skip = data_line-3, col_types = cols(.default = "c")))

        if('sitecode' %in% names(d)){

            d <- d %>%
                rename(site = sitecode)
        }

        all <- rbind.fill(all, d)
    }

    all[all == '-9999'] <- NA

    col_names <- names(all)

    all_names <- c()
    for(i in 1:length(col_names)){
        col_split <- str_split_fixed(col_names[i], '_', n = Inf)

        col_split <- paste(col_split[1:length(col_split)-1], collapse = '_')

        all_names <- append(all_names, col_split)
    }

    name_consolidate <- tibble(new_name = all_names,
                               old_name = col_names) %>%
        filter(new_name != '')

     data_cold <- name_consolidate$old_name

    if('Sodium_dissolved_SWRCDX' %in% col_names && 'Sodium_dissolved_SWRCDX2' %in% col_names){

        all <- all %>%
            mutate(sodium = case_when(!is.na(Sodium_dissolved_SWRCDX) & !is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX2,
                                      !is.na(Sodium_dissolved_SWRCDX) & is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX,
                                      is.na(Sodium_dissolved_SWRCDX) & !is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX2))

        data_cold <- append(data_cold, 'sodium')

        name_consolidate <- name_consolidate %>%
            mutate(var = case_when(new_name == 'Sulfate_dissolved' ~ 'SO4',
                                         new_name == 'Sulfate' ~ 'SO4',
                                         new_name == 'Specific_conductance' ~ 'spCond',
                                         new_name == 'Temperature' ~ 'temp',
                                         new_name == 'Oxygen_dissolved' ~ 'DO',
                                         new_name == 'Carbon_dissolved_organic' ~ 'DOC',
                                         new_name == 'Alkalinity_total' ~ 'alk',
                                         new_name %in% c('Calcium_dissolved','Calcium') ~ 'Ca',
                                         new_name %in% c('Magnesium_dissolved','Magnesium') ~ 'Mg',
                                         new_name == 'Sodium_dissolved|Sodium|sodium' ~ 'Na',
                                         new_name %in% c('Potassium_dissolved','Potassium') ~ 'K',
                                         new_name %in% c('Chloride_dissolved','Chloride') ~ 'Cl',
                                         new_name == 'Nitrogen_nitrate_NO3' ~ 'NO3_N', #check
                                         new_name == 'Nitrogen_NH4' ~ 'NH4',
                                         new_name == 'Nitrogen_organic' ~ 'DON', #check
                                         new_name == 'Nitrogen_tot_diss' ~ 'TDN', #check
                                         new_name == 'Nitrogen_total_kjeldahl' ~ 'TKN',
                                         new_name == 'Nitrogen_total' ~ 'TN',
                                         new_name == 'Phosphorus_phosphate' ~ 'PO4_P',
                                         new_name %in% c('Phosphorus_total','Phosphorus_tot_dissolved') ~ 'TP',
                                         new_name == 'Phosphorus_tot_diss' ~ 'TDP',
                                         new_name == 'Turbidity' ~ 'turbid', #check
                                         new_name %in% c('Aluminum_dissolved','Aluminum') ~ 'Al',
                                         new_name == 'Arsenic_dissolved' ~ 'As',
                                         new_name == 'Boron_dissolved' ~ 'B',
                                         new_name == 'Chromium_dissolved' ~ 'Cr',
                                         new_name == 'Copper_dissolved' ~ 'Cu',
                                         new_name == 'Iron_dissolved' ~ 'Fe',
                                         new_name == 'Lead_dissolved' ~ 'Pb',
                                         new_name == 'Manganese_dissolved' ~ 'Mn',
                                         new_name %in% c('Silicon_dissolved','Silicon') ~ 'Si',
                                         new_name %in% c('Silica_dissolved','Silica_SiO2') ~ 'SiO2',
                                         new_name == 'Zinc_dissolved' ~ 'Zn',
                                         new_name == 'Phosphorus_phosphate_PO4' ~ 'PO4', # check may be PO4_P
                                         new_name == 'pH' ~ 'pH',
                                         new_name %in% c('Sodium_dissolved', 'Sodium') ~ 'Na',
                                         new_name == 'Nitrogen_nitrite_NO2' ~ 'NO2_N',
                                         new_name == 'Sulfur_dissolved' ~ 'S')) %>%
            add_row(new_name = 'sodium', old_name = 'sodium', var = 'Na')

        }

    all_ <- all %>%
        filter(site != '\\data') %>%
        pivot_longer(cols = data_cold, names_to = 'old_name') %>%
        left_join(name_consolidate, by = 'old_name') %>%
        filter(!is.na(value)) %>%
        select(-old_name) %>%
        rename(date = `date  time`) %>%
        filter(!is.na(var)) %>%
        group_by(site, date, var) %>%
        summarise(value = mean(as.numeric(value, na.tm = TRUE))) %>%
        ungroup() %>%
        mutate(time = str_split_fixed(date, ' ', n = Inf)[,2],
               date = str_split_fixed(date, ' ', n = Inf)[,1]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time),
               time = ifelse(time == '', '12:00', time)) %>%
        mutate(datetime = as_datetime(paste(date, time, sep = ' '), format = '%Y%m%d %H:%M', tz = 'America/New_York')) %>%
        mutate(datetime = with_tz(datetime, 'UTC')) %>%
        mutate(ms_status = 0) %>%
        select(site_name = site,
               var,
               val = value,
               datetime,
               ms_status) %>%
        filter(var != 'alk')

    d <- identify_sampling_bypass(df = all_,
                                  is_sensor = FALSE,
                                  domain = domain,
                                  network = network,
                                  prodname_ms = prodname_ms,
                                  sampling_type = 'G')

    d <- d %>%
        filter(var != 'GN_S')

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)

}

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_1000 <- function(network, domain, prodname_ms, site_name, components) {


    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = components)

    #look <- read.csv(rawfile, colClasses = 'character')

    all <- tibble()
    for(i in 1:length(rawfile)){

        check_lines <- read_file(rawfile[i])
        check_lines <- str_split_fixed(check_lines, '\\n|\\r\\r', n = Inf)

        # Get col names
        names_lines <- grep('COL[0-9]', check_lines)

        names_lines <- check_lines[1,names_lines]

        names_lines <- str_split_fixed(names_lines, 'value=', n = Inf)
        names_lines <- str_split_fixed(names_lines[,2], ',', n = Inf)[,1]
        col_names <- trimws(names_lines)

        data_line <- grep('\\\\data', check_lines)
        d <- sw(read_csv(rawfile[i], skip = data_line, col_types = cols(.default = 'c'),
                         col_names = col_names))

        all <- rbind.fill(all, d)
    }

    all[all == '-9999'] <- NA

    col_names <- names(all)

    all_names <- c()
    for(i in 1:length(col_names)){
        col_split <- str_split_fixed(col_names[i], '_', n = Inf)

        col_split <- paste(col_split[1:length(col_split)-1], collapse = '_')

        all_names <- append(all_names, col_split)
    }

    name_consolidate <- tibble(new_name = all_names,
                               old_name = col_names) %>%
        filter(new_name != '')

    data_cold <- name_consolidate$old_name

    if('Sodium_dissolved_SWRCDX' %in% col_names && 'Sodium_dissolved_SWRCDX2' %in% col_names){

        all <- all %>%
            mutate(sodium = case_when(!is.na(Sodium_dissolved_SWRCDX) & !is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX2,
                                      !is.na(Sodium_dissolved_SWRCDX) & is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX,
                                      is.na(Sodium_dissolved_SWRCDX) & !is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX2))

        data_cold <- append(data_cold, 'sodium')

        name_consolidate <- name_consolidate %>%
            mutate(var = case_when(new_name == 'Sulfate_dissolved' ~ 'SO4',
                                   new_name == 'Sulfate' ~ 'SO4',
                                   new_name == 'Specific_conductance' ~ 'spCond',
                                   new_name == 'Temperature' ~ 'temp',
                                   new_name == 'Oxygen_dissolved' ~ 'DO',
                                   new_name == 'Carbon_dissolved_organic' ~ 'DOC',
                                   new_name == 'Alkalinity_total' ~ 'alk',
                                   new_name %in% c('Calcium_dissolved','Calcium') ~ 'Ca',
                                   new_name %in% c('Magnesium_dissolved','Magnesium') ~ 'Mg',
                                   new_name == 'Sodium_dissolved|Sodium|sodium' ~ 'Na',
                                   new_name %in% c('Potassium_dissolved','Potassium') ~ 'K',
                                   new_name %in% c('Chloride_dissolved','Chloride') ~ 'Cl',
                                   new_name == 'Nitrogen_nitrate_NO3' ~ 'NO3_N', #check
                                   new_name == 'Nitrogen_NH4' ~ 'NH4',
                                   new_name == 'Nitrogen_organic' ~ 'DON', #check
                                   new_name == 'Nitrogen_tot_diss' ~ 'TDN', #check
                                   new_name == 'Nitrogen_total_kjeldahl' ~ 'TKN',
                                   new_name == 'Nitrogen_total' ~ 'TN',
                                   new_name == 'Phosphorus_phosphate' ~ 'PO4_P',
                                   new_name %in% c('Phosphorus_total','Phosphorus_tot_dissolved') ~ 'TP',
                                   new_name == 'Phosphorus_tot_diss' ~ 'TDP',
                                   new_name == 'Turbidity' ~ 'turbid', #check
                                   new_name %in% c('Aluminum_dissolved','Aluminum') ~ 'Al',
                                   new_name == 'Arsenic_dissolved' ~ 'As',
                                   new_name == 'Boron_dissolved' ~ 'B',
                                   new_name == 'Chromium_dissolved' ~ 'Cr',
                                   new_name == 'Copper_dissolved' ~ 'Cu',
                                   new_name == 'Iron_dissolved' ~ 'Fe',
                                   new_name == 'Lead_dissolved' ~ 'Pb',
                                   new_name == 'Manganese_dissolved' ~ 'Mn',
                                   new_name %in% c('Silicon_dissolved','Silicon') ~ 'Si',
                                   new_name %in% c('Silica_dissolved','Silica_SiO2') ~ 'SiO2',
                                   new_name == 'Zinc_dissolved' ~ 'Zn',
                                   new_name == 'Phosphorus_phosphate_PO4' ~ 'PO4', # check may be PO4_P
                                   new_name == 'pH' ~ 'pH',
                                   new_name %in% c('Sodium_dissolved', 'Sodium') ~ 'Na',
                                   new_name == 'Nitrogen_nitrite_NO2' ~ 'NO2_N',
                                   new_name == 'Sulfur_dissolved' ~ 'S')) %>%
            add_row(new_name = 'sodium', old_name = 'sodium', var = 'Na')

    }

    all_ <- all %>%
        filter(site != '\\data') %>%
        pivot_longer(cols = data_cold, names_to = 'old_name') %>%
        left_join(name_consolidate, by = 'old_name') %>%
        filter(!is.na(value)) %>%
        select(-old_name) %>%
        rename(date = `date  time`) %>%
        filter(!is.na(var)) %>%
        group_by(site, date, var) %>%
        summarise(value = mean(as.numeric(value, na.tm = TRUE))) %>%
        ungroup() %>%
        mutate(time = str_split_fixed(date, ' ', n = Inf)[,2],
               date = str_split_fixed(date, ' ', n = Inf)[,1]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time),
               time = ifelse(time == '', '12:00', time)) %>%
        mutate(datetime = as_datetime(paste(date, time, sep = ' '), format = '%Y%m%d %H:%M', tz = 'America/New_York')) %>%
        mutate(datetime = with_tz(datetime, 'UTC')) %>%
        mutate(ms_status = 0) %>%
        select(site_name = site,
               var,
               val = value,
               datetime,
               ms_status)

    d <- identify_sampling_bypass(df = all_,
                                  is_sensor = FALSE,
                                  domain = domain,
                                  network = network,
                                  prodname_ms = prodname_ms,
                                  sampling_type = 'G')

    d <- d %>%
        filter(var != 'GN_S')

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)

}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_2400 <- function(network, domain, prodname_ms, site_name, components) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = components)

    all <- tibble()
    for(i in 1:length(rawfile)){

        check_lines <- read_file(rawfile[i])
        check_lines <- str_split_fixed(check_lines, '\\n|\\r\\r', n = Inf)
        data_line <- grep('\\\\data', check_lines)

        d <- sw(read_csv(rawfile[i], skip = data_line, col_types = cols(.default = "c"),
                         col_names = F))

        all <- rbind(all, d)
    }

    all[all == '-9999'] <- NA

    names(all) <- c('SiteCode', 'date', 'Chlorophyll_1', 'Chlorophyll_2', 'Chlorophyll_3',
                    'Chlorophyll_4', 'Pheophytin_1', 'Pheophytin_2', 'Pheophytin_3',
                    'Pheophytin_4')


    d <- pivot_longer(all, cols = starts_with(c('C', 'P'))) %>%
        mutate(value = as.numeric(value)) %>%
        mutate(datetime = as_datetime(date, format = '%Y%m%d %H:%S', tz = 'US/Eastern')) %>%
        lubridate::with_tz(tzone = 'UTC') %>%
        mutate(name = case_when(name %in% c('Chlorophyll_1', 'Chlorophyll_2',
                                            'Chlorophyll_3', 'Chlorophyll_4') ~ 'CHL',
                                name %in% c('Pheophytin_1', 'Pheophytin_2',
                                            'Pheophytin_3', 'Pheophytin_4') ~ 'pheophy')) %>%
        group_by(SiteCode, datetime, name) %>%
        summarise(val = mean(value, na.rm = TRUE)) %>%
        ungroup() %>%
        filter(!is.na(val)) %>%
        rename(site_name = SiteCode, var = name) %>%
        arrange(site_name, var, datetime) %>%
        mutate(ms_status = 0)

    d <- identify_sampling_bypass(df = d,
                                  is_sensor = FALSE,
                                  domain = domain,
                                  network = network,
                                  prodname_ms = prodname_ms,
                                  sampling_type = 'G')

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)

}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_2465 <- function(network, domain, prodname_ms, site_name, components) {


    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = components)

    look <- read.csv(rawfile[1], colClasses = 'character')

    fils <- rawfile[!grepl('HEADER', rawfile)]
    all <- tibble()
    for(i in 1:length(rawfile)){

        check_lines <- read_file(rawfile[i])
        check_lines <- str_split_fixed(check_lines, '\\n|\\r\\r', n = Inf)
        data_line <- grep('\\\\data', check_lines)

        d <- sw(read_csv(rawfile[i], skip = data_line, col_types = cols(.default = "c"),
                         col_names = F))

        all <- rbind(all, d)
    }

    names(all) <- c('SiteCode', 'date', 'temperature')

    d <- ms_read_raw_csv(preprocessed_tibble = all,
                         datetime_cols = list('date' = '%Y%m%d %H:%S'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'SiteCode',
                         alt_site_name = list('WCCLAB' = 'WCCLABTEMP'),
                         data_cols = c(temperature = 'temp'),
                         set_to_NA = c('-9999'),
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

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_1001 <- function(network, domain, prodname_ms, site_name, component) {

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.dat',
                   n=network, d=domain, p=prodname_ms, s=site_name, c=component)


    fil <- str_split_fixed(component, '_', n = Inf)[1,4]
    if(fil == 'from'){
        return(NULL)
    }
    year <- as.numeric(str_split_fixed(fil, '[.]', n = Inf)[1,1])

    if(year >= 2013){

        check_lines <- read_file(rawfile)
        check_lines <- str_split_fixed(check_lines, '\\n|\\r\\r', n = Inf)
        data_line <- grep('\\\\data', check_lines)

        d <- read.csv(rawfile, colClasses = 'character', skip = data_line) %>%
            mutate(site = 'WCCLAB') %>%
            rename(discharge = 3,
                   Date.Time..EST. = 1)
    } else{
        d <- read.csv(rawfile, colClasses = 'character') %>%
            mutate(site = 'WCCLAB') %>%
            mutate(discharge = ifelse(Discharge..cfs. == '-9999', Discharge..cfs..from.Continuous.record, Discharge..cfs.))

    }

    d['date'] <- str_split_fixed(d$Date.Time..EST., ' ', n = Inf)[,1]

    d_times <- str_split(d$Date.Time..EST., ' ', n = Inf)

    d_time_co <- c()
    for(i in 1:length(d_times)){
        time <- d_times[[i]][length(d_times[[i]])]

        d_time_co <- append(d_time_co, time)
    }

    d['time'] <- d_time_co

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%Y%m%d',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'site',
                         data_cols = 'discharge',
                         set_to_NA = c('-9999'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- d %>%
        mutate(val = val*28.3168)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_2565 <- function(network, domain, prodname_ms, site_name, components) {


    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n=network, d=domain, p=prodname_ms, s=site_name, c=components)

    d <- read.delim(rawfile, colClasses = 'character',  sep = '\t', skip = 19) %>%
            mutate(site = 'SWRC_met')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Year' = '%Y',
                                              'Month' = '%m',
                                              'Date' = '%e',
                                              'Hour' = '%H'),
                         datetime_tz = 'US/Eastern',
                         site_name_col = 'site',
                         data_cols = c('Precipitation.mm.' = 'precipitation'),
                         set_to_NA = c('-9999'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#derive kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__2407',
                                           'stream_chemistry__1000',
                                           'stream_chemistry__2400',
                                           'stream_chemistry__2465'))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- precip_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_precip_pchem_pflux
