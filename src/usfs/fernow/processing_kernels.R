
#retrieval kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_0_14 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_15 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_16 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_17 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#munge kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_1_14 <- function(network, domain, prodname_ms, site_name, component) {

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
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    unlink(temp_dir, recursive = TRUE)

    return(d)
}


#discharge: STATUS=READY
#. handle_errors
process_1_15 <- function(network, domain, prodname_ms, site_name, component) {

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

    return(d)
}


#precip_chemistry: STATUS=READY
#. handle_errors
process_1_16 <- function(network, domain, prodname_ms, site_name, component) {

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

    d <- read.csv(rel_file_path, colClasses = 'character')
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('GGL_SW_0.Specific.Conductance..uS.cm.' = 'spCond',
                                        'GGL_SW_0.Temp..C.' = 'temp'),
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


#stream_chemistry: STATUS=READY
#. handle_errors
process_1_17 <- function(network, domain, prodname_ms, site_name, component) {

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

    d <- read.csv(rel_file_path, colClasses = 'character')
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

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)
}

#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_precip_pchem_pflux
