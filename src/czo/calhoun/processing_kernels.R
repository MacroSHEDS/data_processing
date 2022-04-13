
#retrieval kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_0_6421 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_6470 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge; precipitation: STATUS=PENDING
#. handle_errors
process_0_4680 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2851 <- function(set_details, network, domain) {

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component),
                  cacheOK = FALSE,
                  method = 'libcurl', mode = 'wb')

    return()
}

#munge kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_1_6421 <- function(network, domain, prodname_ms, site_code,
                           component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'reasearch_area_1') %>%
        mutate(date = str_split_fixed(timeStamp, ' ', n = Inf)[,1]) %>%
        mutate(time = str_split_fixed(timeStamp, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0(0, time), time))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'site',
                         data_cols =  c('precipitation..mm.' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_6470 <- function(network, domain, prodname_ms, site_code,
                           component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(site = 'weir_4') %>%
        mutate(date = str_split_fixed(timeStamp, ' ', n = Inf)[,1]) %>%
        mutate(time = str_split_fixed(timeStamp, ' ', n = Inf)[,2]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0(0, time), time))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'site',
                         data_cols =  c('discharge..L.s.' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = 'NaN',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    return(d)
}

#precipitation; discharge: STATUS=PENDING
#. handle_errors
process_1_4680 <- function(network, domain, prodname_ms, site_code,
                           components) {

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = components)

    rawfile <- glue('{rd}/{c}',
                    rd = rawdir,
                    c = components)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    if(prodname_ms == 'precipitation__4680'){

        rain_files <- zipped_files[grepl('Rain', zipped_files)]

        all_sites <- tibble()
        for(p in 1:length(rain_files)){

            site_code <- ifelse(grepl('Rain 9', rain_files[p]),
                                'rain_gauge9',
                                'rain_gauge11')

            site <- readxl::read_xlsx(rain_files[p]) %>%
                rename(date = 1,
                      rain = 2) %>%
                mutate(site = !!site_code) %>%
                select(date, rain, site)

            all_sites <- rbind(all_sites, site)
        }

        d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                             datetime_cols = list('date' = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'US/Eastern',
                             site_code_col = 'site',
                             data_cols =  c('rain' = 'precipitation'),
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)

    } else {

        q_files <- zipped_files[grepl('Stream', zipped_files)]

        all_sites <- tibble()
        for(p in 1:length(q_files)){

            site_raw <- str_split_fixed(q_files[p], '[/]', n = Inf)[1,8]

            site_n <- case_when(site_raw == 'Stream 2' ~ 'weir_2',
                              site_raw == 'Stream 3' ~ 'weir_3',
                              site_raw == 'Stream 4' ~ 'weir_4')

            if(p %in% c(16, 24)){

                site <- readxl::read_xlsx(q_files[p]) %>%
                    rename(date = 3,
                           q = 2) %>%
                    mutate(site = !!site_n) %>%
                    select(date, q, site)
            } else{

                site <- readxl::read_xlsx(q_files[p]) %>%
                    rename(date = 1,
                           q = 3) %>%
                    mutate(site = !!site_n) %>%
                    select(date, q, site)
            }

            all_sites <- rbind(all_sites, site)
        }

        all_sites <- all_sites %>%
            mutate(q = q*28.317)

        d <- ms_read_raw_csv(preprocessed_tibble = all_sites,
                             datetime_cols = list('date' = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'US/Eastern',
                             site_code_col = 'site',
                             data_cols =  c('q' = 'discharge'),
                             data_col_pattern = '#V#',
                             is_sensor = TRUE)
    }

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    unlink(zipped_files, recursive = FALSE)

    unlink('data/czo/calhoun/raw/precipitation__4680/sitename_NA/Stream/Stream 2/', recursive = FALSE)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_2851 <- function(network, domain, prodname_ms, site_code,
                           component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- readxl::read_xlsx(rawfile) %>%
        mutate_all(as.character)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'sample_location',
                         alt_site_code = list('Tyger_River_hwy49' = 'A17',
                                              'Isaacs_Creek_Bombing_Rang' = 'B16',
                                              'Enoree_River_Jones_Ford' = 'C15',
                                              'Johns_Creek' = 'D14',
                                              'Sparks_Creek' = 'E2',
                                              'Sparks_Creek_Seep' = 'F3',
                                              'Upper_Padgetts_Creek' = 'H4',
                                              'Padgetts_Creek_Old_Buncombe' = 'I13',
                                              'Padgetts_Creek_Sardis_Rd' = 'J5',
                                              'Ref4_Drain' = 'K6',
                                              'Tyger_River_Sardis_Rd' = 'L1',
                                              'Ref4_Below_Bowl' = 'M7',
                                              'weir_1' = 'N18',
                                              'weir_2' = 'R8',
                                              'weir_3' = 'O9',
                                              'weir_4' = 'P10',
                                              'Holcombes_Branch' = 'Q11'),
                         data_cols =  c('EC.S_m' = 'spCond',
                                        'pH' = 'pH',
                                        'TC.mg_L' = 'TC',
                                        'TN.mg_L' = 'TN',
                                        'Fluoride.mg_L' = 'F',
                                        'Chloride.mg_L' = 'Cl',
                                        'Nitrite.mg_L' = 'NO2',
                                        'Bromide.mg_L' = 'Br',
                                        'Sulfate.mg_L' = 'SO4',
                                        'Nitrate_mg_L' = 'NO3',
                                        'Phosphate_mg_L' = 'PO4',
                                        'Alkalinity_mg_LCaCO3' = 'alk',
                                        'Lithium_mg_l' = 'Li',
                                        'Sodium_mg_l' = 'Na',
                                        'Ammonium.mg_l' = 'NH4',
                                        'Potassium_mg_l' = 'K',
                                        'Magnesium_mg_l' = 'Mg',
                                        'Calcium_mg_l' = 'Ca',
                                        'Fe57_umol_l' = 'Fe',
                                        'Al_umol_l' = 'Al',
                                        'Mn_umol_l' = 'Mn',
                                        'sum-anions' = 'anionCharge',
                                        'sum-cations_IC' = 'cationCharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('n.a.'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('Fe' = 'umol/l',
                                               'Al' = 'umol/l',
                                               'Mn' = 'umol/l'),
                        convert_units_to = c('Fe' = 'mg/l',
                                             'Al' = 'mg/l',
                                             'Mn' = 'mg/l'))

    #converstion function is not set up for spCond units
    d <- d %>%
        mutate(val = ifelse(var == 'GN_spCond', val * 10000, val))

    return(d)
}

#derive kernels ####

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms001 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms002 <- stream_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms005 <- derive_precip_pchem_pflux

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_stream_flux
