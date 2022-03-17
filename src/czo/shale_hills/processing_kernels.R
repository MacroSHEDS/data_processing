
#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- download_from_googledrive

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- download_from_googledrive

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- download_from_googledrive

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    all_q_files <- list.files(rawfile, full.names = TRUE)

    # Shale Hills Stream
    #    1
    shs1_f <- grep('export', all_q_files, value = TRUE)

    shs1 <- read.csv(shs1_f, colClasses = 'character') %>%
        mutate(site = 'SH_weir')

    shs1 <- ms_read_raw_csv(preprocessed_tibble = shs1,
                         datetime_cols = list('TmStamp' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'America/New_York',
                         site_code_col = 'site',
                         data_cols =  c('Discharge' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = '-999.0',
                         summary_flagcols = 'QualCode',
                         is_sensor = TRUE)

    shs1 <- ms_cast_and_reflag(shs1,
                            varflag_col_pattern = NA,
                            summary_flags_dirty = list('QualCode' = 'E'),
                            summary_flags_to_drop = list('QualCode' = 'DROP'))

    # meters3 per day to meters3 per second
    shs1 <- shs1 %>%
        mutate(val = val/86400) %>%
        # This day look proplomatic
        filter(datetime < ymd_hm('2012-06-29 00:00') | datetime > ymd_hm('2012-06-29 23:50'))

    #    2
    shs2_f <- grep('SH_Discharge_Level_1', all_q_files, value = TRUE)

    shs2 <- read.csv(shs2_f, colClasses = 'character') %>%
        mutate(site = 'SH_weir')

    shs2 <- ms_read_raw_csv(preprocessed_tibble = shs2,
                            datetime_cols = list('TmStamp_UTC' = '%Y-%m-%d %H:%M:%S'),
                            datetime_tz = 'UTC',
                            site_code_col = 'site',
                            data_cols =  c('dischg_m3s' = 'discharge'),
                            data_col_pattern = '#V#',
                            set_to_NA = '-9999.0',
                            is_sensor = TRUE)

    shs2 <- ms_cast_and_reflag(shs2,
                               varflag_col_pattern = NA)

    shs2 <- shs2 %>%
        # 2018 data looks like it has issues
        filter(datetime < '2018-01-01')

    # Shavers Creek Above Lake
    sc_ablake_f <- grep('SCAL_Discharge', all_q_files, value = TRUE)

    sc_ablake <- read.csv(sc_ablake_f, colClasses = 'character') %>%
        mutate(site = 'SCAL')

    sc_ablake <- ms_read_raw_csv(preprocessed_tibble = sc_ablake,
                            datetime_cols = list('TmStamp_UTC' = '%Y-%m-%d %H:%M:%S'),
                            datetime_tz = 'UTC',
                            site_code_col = 'site',
                            data_cols =  c('dischg_m3s' = 'discharge'),
                            data_col_pattern = '#V#',
                            set_to_NA = '-9999.0',
                            is_sensor = TRUE)

    sc_ablake <- ms_cast_and_reflag(sc_ablake,
                               varflag_col_pattern = NA)

    # Shavers Creek Outlet
    sc_outlet_f <- grep('SC_outlet', all_q_files, value = TRUE)

    sc_outlet <- read.csv(sc_outlet_f, colClasses = 'character') %>%
        mutate(site = 'SCO')

    sc_outlet <- ms_read_raw_csv(preprocessed_tibble = sc_outlet,
                                 datetime_cols = list('TmStamp_UTC' = '%Y-%m-%d %H:%M:%S'),
                                 datetime_tz = 'UTC',
                                 site_code_col = 'site',
                                 data_cols =  c('dischg_m3s' = 'discharge'),
                                 data_col_pattern = '#V#',
                                 set_to_NA = '-9999.0',
                                 is_sensor = TRUE)

    sc_outlet <- ms_cast_and_reflag(sc_outlet,
                                    varflag_col_pattern = NA)

    # Garner Run Outlet
    gr_f <- grep('GR_Discharge', all_q_files, value = TRUE)

    gr <- read.csv(gr_f, colClasses = 'character') %>%
        mutate(site = 'GRO')

    gr <- ms_read_raw_csv(preprocessed_tibble = gr,
                                 datetime_cols = list('TmStamp_UTC' = '%Y-%m-%d %H:%M:%S'),
                                 datetime_tz = 'UTC',
                                 site_code_col = 'site',
                                 data_cols =  c('dischg_m3s' = 'discharge'),
                                 data_col_pattern = '#V#',
                                 set_to_NA = '-9999.0',
                                 is_sensor = TRUE)

    gr <- ms_cast_and_reflag(gr,
                             varflag_col_pattern = NA)

    d <- rbind(shs1, shs2, sc_ablake, sc_outlet, gr) %>%
        # m3/s to L/s
        mutate(val = val*1000)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

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

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    all_p_files <- list.files(rawfile, full.names = TRUE)

    SSHCZO_f <- grep('SSHCZOHourPrecipSH', all_p_files, value = TRUE)

    d <- read.csv(SSHCZO_f, colClasses = 'character') %>%
        mutate(site = 'Shale_Hills_precip')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                            datetime_cols = list('TmStamp' = '%Y-%m-%d %H:%M:%S'),
                            datetime_tz = 'America/New_York',
                            site_code_col = 'site',
                            data_cols =  c('Total_Precip_mm' = 'precipitation'),
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

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    chem_files <- list.files(rawfile, full.names = TRUE)

    h6_f <- grep('H6_hydro', chem_files, value = TRUE)
    h6 <- readxl::read_xlsx(h6_f)

    h6 <- ms_read_raw_csv(preprocessed_tibble = h6,
                         datetime_cols = list('Date' = '%Y-%m-%d'),
                         datetime_tz = 'America/New_York',
                         site_code_col = 'Site',
                         alt_site_code = list('SH_weir' = c('SSHCZO-WIER', 'SH-WEIR'),
                                              'GRO' = 'GR outlet', 'GR'),
                         data_cols =  c('Temperature (C )' = 'temp',
                                        'pH',
                                        'DO (%)' = 'DO_sat',
                                        'DO (mg L-1)' = 'DO',
                                        'TDS (mg -1)' = 'TDS',
                                        'F (umol L-1)' = 'F',
                                        'Cl- (umol L-1)' = 'Cl',
                                        'SO42- (umol L-1)' = 'SO4',
                                        'NO3-(umol L-1)' = 'NO3',
                                        'Al (umol L-1)' = 'Al',
                                        'Ca (umol L-1)' = 'Ca',
                                        'Fe (umol L-1)' = 'Fe',
                                        'K  (umol L-1)' = 'K',
                                        'Mg (umol L-1)' = 'Mg',
                                        'Mn (umol L-1)' = 'Mn',
                                        'Na  (umol L-1)' = 'Na',
                                        'Si  (umol L-1)' = 'Si',
                                        'Sr (umol L-1)' = 'Sr',
                                        'Ba (\u03bcmol L-1)' = 'Ba',
                                        'P (\u03bcmol L-1)' = 'P',
                                        'Zn (\u03bcmol L-1)' = 'Zn'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         convert_to_BDL_flag = c('BDL'))

    h6 <- ms_cast_and_reflag(h6,
                             varflag_col_pattern = NA)

    h6 <- ms_conversions(h6,
                         convert_units_from = c('F' = 'umol/l',
                                               'Cl' = 'umol/l',
                                               'SO4' = 'umol/l',
                                               'NO3' = 'umol/l',
                                               'Al' = 'umol/l',
                                               'Ca' = 'umol/l',
                                               'Fe' = 'umol/l',
                                               'K' = 'umol/l',
                                               'Mg' = 'umol/l',
                                               'Mn' = 'umol/l',
                                               'Na' = 'umol/l',
                                               'Si' = 'umol/l',
                                               'Sr' = 'umol/l',
                                               'Ba' = 'umol/l',
                                               'P' = 'umol/l',
                                               'Zn' = 'umol/l'),
                        convert_units_to = c('F' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'SO4' = 'mg/l',
                                             'NO3' = 'mg/l',
                                             'Al' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Fe' = 'mg/l',
                                             'K' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'Mn' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'Si' = 'mg/l',
                                             'Sr' = 'mg/l',
                                             'Ba' = 'mg/l',
                                             'P' = 'mg/l',
                                             'Zn' = 'mg/l'))

    doi_f2014 <- grep('SC_2014StreamwaterChemistry_DOI', chem_files, value = TRUE)
    doi2014 <- readxl::read_xlsx(doi_f2014, sheet = '3 Data', skip = 1) %>%
        rename(id = 1)
    doi_dates <- readxl::read_xlsx(doi_f2014, sheet = '2 Samples', skip = 1) %>%
        select(id = IDENTIFICATION, date = ...12) %>%
        mutate(date = as_date(as.numeric(date), origin = '1900-01-01')) %>%
        filter(!is.na(date))

    doi_names <- names(doi2014)
    doi_names <- doi_names[!grepl('[.][.][.]|PARAMETER', doi_names)]

    doi2014 <- doi2014 %>%
        select(doi_names)

    doi_com <- full_join(doi_dates, doi2014, by = 'id') %>%
        mutate(name = str_split_fixed(id, '_', n= Inf)[,1]) %>%
        filter(!is.na(date))

    d_2014 <- ms_read_raw_csv(preprocessed_tibble = doi_com,
                          datetime_cols = list('date' = '%Y-%m-%d'),
                          datetime_tz = 'America/New_York',
                          site_code_col = 'name',
                          alt_site_code = list('SH_weir' = c('SH')),
                          data_cols =  c('Temperature' = 'temp',
                                         'pH',
                                         'DO (%)' = 'DO_sat',
                                         'Dissolved Oxygen' = 'DO',
                                         'Specific Conductivity' = 'spCond',
                                         'Cl', 'SO4', 'NO3', 'Al', 'Ca',
                                         'Fe', 'K', 'Mg', 'Mn', 'Na', 'Si', 'Sr',
                                         'DOC'),
                          data_col_pattern = '#V#',
                          is_sensor = FALSE)

    d_2014 <- ms_cast_and_reflag(d_2014,
                             varflag_col_pattern = NA)

    d_2014 <- ms_conversions(d_2014,
                         convert_units_from = c('Cl' = 'umol/l',
                                                'SO4' = 'umol/l',
                                                'NO3' = 'umol/l',
                                                'Al' = 'umol/l',
                                                'Ca' = 'umol/l',
                                                'Fe' = 'umol/l',
                                                'K' = 'umol/l',
                                                'Mg' = 'umol/l',
                                                'Mn' = 'umol/l',
                                                'Na' = 'umol/l',
                                                'Si' = 'umol/l',
                                                'Sr' = 'umol/l'),
                         convert_units_to = c('Cl' = 'mg/l',
                                              'SO4' = 'mg/l',
                                              'NO3' = 'mg/l',
                                              'Al' = 'mg/l',
                                              'Ca' = 'mg/l',
                                              'Fe' = 'mg/l',
                                              'K' = 'mg/l',
                                              'Mg' = 'mg/l',
                                              'Mn' = 'mg/l',
                                              'Na' = 'mg/l',
                                              'Si' = 'mg/l',
                                              'Sr' = 'mg/l'))

    sw_data_f <- grep('Stream_Water_Data', chem_files, value = TRUE)

    sw_all <- tibble()
    for(i in 1:length(sw_data_f)){

        sw_data <- readxl::read_xlsx(sw_data_f[i], 'Data',
                                     col_types = 'text') %>%
            rename(Cl = starts_with('Cl-'),
                   NO3 = starts_with('NO3-'),
                   SO4 = starts_with('SO42-'),
                   'F' = starts_with('F ('),
                   Al = starts_with('Al+3'),
                   Ca = starts_with('Ca+2'),
                   K = starts_with('K+'),
                   Mg = starts_with('Mg+2'),
                   Na = starts_with('Na+'),
                   Si = starts_with('Si ('),
                   Sr = starts_with('Sr ('),
                   Fe = starts_with('Fe+3'),
                   Mn = starts_with('Mn+2'),
                   Ni = starts_with('Ni ('),
                   P = starts_with('P ('),
                   V = starts_with('V ('),
                   Zn = starts_with('Zn ('),
                   Ba = starts_with('Ba (')) %>%
            mutate(Sample_Date = as_date(as.numeric(Sample_Date), origin = '1900-01-01'))

        sw_data <- ms_read_raw_csv(preprocessed_tibble = sw_data,
                                   datetime_cols = list('Sample_Date' = '%Y-%m-%d'),
                                   datetime_tz = 'America/New_York',
                                   site_code_col = 'Sample Name',
                                   alt_site_code = list('SH_weir' = c('SW', 'SW_ISCO'),
                                                        'SH_headwaters' = 'SH',
                                                        'SH_middle' = 'SM'),
                                   data_cols =  c('DOC (ppm)' = 'DOC',
                                                  'Water Temp.  (°C)' = 'temp',
                                                  'pH', 'Cl', 'NO3', 'SO4', 'F',
                                                  'Al', 'Ca', 'K', 'Mg', 'Na',
                                                  'Si', 'Sr', 'Fe', 'Mn', 'Ni',
                                                  'P', 'V', 'Zn', 'Ba'),
                                   data_col_pattern = '#V#',
                                   is_sensor = FALSE)

        sw_data <- ms_cast_and_reflag(sw_data,
                                 varflag_col_pattern = NA)

        sw_all <- rbind(sw_all, sw_data)
    }

    sw_all <- ms_conversions(sw_all,
                             convert_units_from = c('Cl' = 'umol/l',
                                                    'NO3' = 'umol/l',
                                                    'SO4' = 'umol/l',
                                                    'F' = 'umol/l',
                                                    'Al' = 'umol/l',
                                                    'Ca' = 'umol/l',
                                                    'K' = 'umol/l',
                                                    'Mg' = 'umol/l',
                                                    'Na' = 'umol/l',
                                                    'Si' = 'umol/l',
                                                    'Sr' = 'umol/l',
                                                    'Fe' = 'umol/l',
                                                    'Mn' = 'umol/l',
                                                    'Ni' = 'umol/l',
                                                    'P' = 'umol/l',
                                                    'V' = 'umol/l',
                                                    'Zn' = 'umol/l',
                                                    'Ba' = 'umol/l'),
                             convert_units_to = c('Cl' = 'mg/l',
                                                  'NO3' = 'mg/l',
                                                  'SO4' = 'mg/l',
                                                  'F' = 'mg/l',
                                                  'Al' = 'mg/l',
                                                  'Ca' = 'mg/l',
                                                  'K' = 'mg/l',
                                                  'Mg' = 'mg/l',
                                                  'Na' = 'mg/l',
                                                  'Si' = 'mg/l',
                                                  'Sr' = 'mg/l',
                                                  'Fe' = 'mg/l',
                                                  'Mn' = 'mg/l',
                                                  'Ni' = 'mg/l',
                                                  'P' = 'mg/l',
                                                  'V' = 'mg/l',
                                                  'Zn' = 'mg/l',
                                                  'Ba' = 'mg/l'))

    d <- rbind(h6, d_2014, sw_all) %>%
        filter(! site_code %in% c('', 'GR Dam', 'GR1', 'GR2', 'GR3', 'GR4', 'GR5'))

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

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

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms001 <- precip_gauge_from_site_data

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_precip_pchem_pflux
