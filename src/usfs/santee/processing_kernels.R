
#retrieval kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- download_from_googledrive

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- download_from_googledrive

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- download_from_googledrive

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- download_from_googledrive

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS005 <- download_from_googledrive

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
    fils <- list.files(temp_dir, recursive = T, full.names = T)
    
    # HQ 
    #    historical
    hq_hist_path <- grep('hq_hist.csv', fils, value = T)
    
    hq_hist_d <- ms_read_raw_csv(filepath = hq_hist_path,
                                 datetime_cols = list('Date_instant' = '%Y-%m-%d'),
                                 datetime_tz = 'US/Eastern',
                                 site_code_col = 'Instr_ID',
                                 data_cols =  c(Rainfall_m = 'precipitation'),
                                 data_col_pattern = '#V#',
                                 is_sensor = TRUE)

    hq_hist_d <- ms_cast_and_reflag(hq_hist_d,
                                    varflag_col_pattern = NA)

    #    modern (all null values)
    # hq_modern_path <- grep('hq_daily', fils, value = T)
    # 
    # hq_modern_path <- ms_read_raw_csv(filepath = hq_modern_path,
    #                              datetime_cols = list('Date' = '%Y-%m-%d'),
    #                              datetime_tz = 'US/Eastern',
    #                              site_code_col = 'Instr_ID',
    #                              data_cols =  c(Rainfall_m = 'precipitation'),
    #                              data_col_pattern = '#V#',
    #                              is_sensor = TRUE)
    # 
    # hq_hist_d <- ms_cast_and_reflag(hq_hist_d,
    #                                 varflag_col_pattern = NA)
    # 
    # HQ pluvio guage
    pluvio_path <- grep('hq_pluvio_highrez', fils, value = T)
    pluvio_d <- read.csv(pluvio_path, colClasses = 'character') %>%
        mutate(site_code = 'SEFHQ_pluvio')
    
    pluvio_d <- ms_read_raw_csv(preprocessed_tibble = pluvio_d,
                                 datetime_cols = list('Date_time_' = '%m/%e/%Y %H:%M'),
                                 datetime_tz = 'US/Eastern',
                                 site_code_col = 'site_code',
                                 data_cols =  c(Rainfall_m = 'precipitation'),
                                 data_col_pattern = '#V#',
                                 is_sensor = TRUE)
    
    pluvio_d <- ms_cast_and_reflag(pluvio_d,
                                    varflag_col_pattern = NA)
    
    # lotti 
    #     high rez
    lotti_highrez_path <- grep('lotti_highrez', fils, value = T)

    lotti_highrez_d <- ms_read_raw_csv(filepath = lotti_highrez_path,
                                datetime_cols = list('Date_time_temp' = '%m/%e/%Y %H:%M'),
                                datetime_tz = 'US/Eastern',
                                site_code_col = 'Instr_ID',
                                data_cols =  c(Rainfall_m = 'precipitation'),
                                data_col_pattern = '#V#',
                                sampling_type = 'I',
                                is_sensor = TRUE)

    lotti_highrez_d <- ms_cast_and_reflag(lotti_highrez_d,
                                          varflag_col_pattern = NA)

    #    daily
    lotti_hist_path <- grep('lotti_hist', fils, value = T)

    lotti_hist_d <- ms_read_raw_csv(filepath = lotti_hist_path,
                                       datetime_cols = list('Date_temp' = '%Y-%m-%d'),
                                       datetime_tz = 'US/Eastern',
                                       site_code_col = 'Instr_ID',
                                       data_cols =  c(Rainfall_m = 'precipitation'),
                                       data_col_pattern = '#V#',
                                       sampling_type = 'I',
                                       is_sensor = TRUE)
    
    lotti_hist_d <- ms_cast_and_reflag(lotti_hist_d,
                                       varflag_col_pattern = NA)
    
    # met 25
    #    highrez
    met25_highrez_path <- grep('met25_highrez', fils, value = T)

    met25_highrez_d <- ms_read_raw_csv(filepath = met25_highrez_path,
                                    datetime_cols = list('Date_time_rain' = '%m/%d/%Y %H:%M'),
                                    datetime_tz = 'US/Eastern',
                                    site_code_col = 'Instr_ID',
                                    data_cols =  c(Rainfall_m = 'precipitation'),
                                    data_col_pattern = '#V#',
                                    sampling_type = 'I',
                                    is_sensor = TRUE)
    
    met25_highrez_d <- ms_cast_and_reflag(met25_highrez_d,
                                       varflag_col_pattern = NA)
    
    #    historical 
    met25_hist_path <- grep('met25_hist', fils, value = T)

    met25_hist_d <- ms_read_raw_csv(filepath = met25_hist_path,
                                       datetime_cols = list('Date_temp' = '%Y-%m-%d'),
                                       datetime_tz = 'US/Eastern',
                                       site_code_col = 'Instr_ID',
                                       data_cols =  c(Rainfall_m = 'precipitation'),
                                       data_col_pattern = '#V#',
                                       sampling_type = 'I',
                                       is_sensor = TRUE)
    
    met25_hist_d <- ms_cast_and_reflag(met25_hist_d,
                                          varflag_col_pattern = NA)
    
    # met5
    #     highrez
    met5_highrez_path <- grep('met25_highrez', fils, value = T)
    look <- read.csv(met5_highrez_path, colClasses = 'character')
    
    met5_highrez_d <- ms_read_raw_csv(filepath = met5_highrez_path,
                                    datetime_cols = list('Date_time_rain' = '%m/%e/%Y %H:%M'),
                                    datetime_tz = 'US/Eastern',
                                    site_code_col = 'Instr_ID',
                                    data_cols =  c(Rainfall_m = 'precipitation'),
                                    data_col_pattern = '#V#',
                                    sampling_type = 'I',
                                    is_sensor = TRUE)
    
    met5_highrez_d <- ms_cast_and_reflag(met5_highrez_d,
                                         varflag_col_pattern = NA)
    #    historical 
    met5_hist_path <- grep('met5_hist', fils, value = T)

    met5_hist_d <- ms_read_raw_csv(filepath = met5_hist_path,
                                    datetime_cols = list('Date_temp' = '%Y-%m-%d'),
                                    datetime_tz = 'US/Eastern',
                                    site_code_col = 'Instr_ID',
                                    data_cols =  c(Rainfall_m = 'precipitation'),
                                    data_col_pattern = '#V#',
                                    sampling_type = 'I',
                                    is_sensor = TRUE)
    
    met5_hist_d <- ms_cast_and_reflag(met5_hist_d,
                                       varflag_col_pattern = NA)
    
    # turkey
    turkey_highrez_path <- grep('turkey_highrez', fils, value = T)
    
    turkey_highrez_d <- ms_read_raw_csv(filepath = turkey_highrez_path,
                                   datetime_cols = list('Date_time_' = '%m/%e/%Y %H:%M'),
                                   datetime_tz = 'US/Eastern',
                                   site_code_col = 'Instr_ID',
                                   alt_site_code = list('TC_Met' = 'TC Met'),
                                   data_cols =  c(Rainfall_m = 'precipitation'),
                                   data_col_pattern = '#V#',
                                   sampling_type = 'I',
                                   is_sensor = TRUE)
    
    turkey_highrez_d <- ms_cast_and_reflag(turkey_highrez_d,
                                      varflag_col_pattern = NA)
    
    # It looks like the highrez files for lotti, met5 and met 25 are missing 
    # most of the data for every year, SFE was contacted 
    d <- rbind(hq_hist_d, pluvio_d, lotti_hist_d, lotti_hist_d, met5_hist_d, 
               turkey_highrez_d)

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

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/santee_q.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    
    fils <- list.files(paste0(temp_dir, '/', 'santee_q'), full.names = T)

    # ws77
    #    historical
    hist_paths <- grep('historical', fils, value = T)
    all_historical <- tibble()
    for(s in 1:length(hist_paths)){
        d_hist <- ms_read_raw_csv(filepath = hist_paths[s],
                                  datetime_cols = list('Date_' = '%Y-%m-%d'),
                                  datetime_tz = 'US/Eastern',
                                  site_code_col = 'Location',
                                  data_cols =  c(Dailyflow_ = 'discharge'),
                                  data_col_pattern = '#V#',
                                  is_sensor = TRUE)
        
        d_hist <- ms_cast_and_reflag(d_hist,
                                          varflag_col_pattern = NA)
        
        all_historical <- rbind(all_historical, d_hist)
    }
    #    modern
    modern_paths <- grep('10min|15min', fils, value = T)
    
    all_modern <- tibble()
    for(s in 1:length(modern_paths)){
        d_modern <- ms_read_raw_csv(filepath = modern_paths[s],
                                  datetime_cols = list('Data_time' = '%m/%e/%Y %H:%M'),
                                  datetime_tz = 'US/Eastern',
                                  site_code_col = 'Location',
                                  data_cols =  c(Flow_liter = 'discharge'),
                                  data_col_pattern = '#V#',
                                  is_sensor = TRUE)
        
        d_modern <- ms_cast_and_reflag(d_modern,
                                       varflag_col_pattern = NA)
        
        all_modern <- rbind(all_modern, d_modern)
    }

    # Combine historical and modern Q
    d <- rbind(all_historical, all_modern)

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

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/santee_waterqual.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(paste0(temp_dir, '/', 'santee_waterqual'), 
                       recursive = T, full.names = T)

    modern_fils <- grep('mod', fils, value = T)
    
    all_modern <- tibble()
    for(s in 1:length(modern_fils)){

        d_m <- ms_read_raw_csv(filepath = modern_fils[s],
                               datetime_cols = c(Date_time = '%m/%e/%Y %H:%M'),
                               datetime_tz = 'US/Eastern',
                               site_code_col = 'Location',
                               data_cols =  c(TN_mgL = 'TDN',
                                              TP_mgL = 'TDP',
                                              NH4_N_mgL = 'NH4_N',
                                              NO3_NO2_N_mgL = 'NO3_NO2_N',
                                              Cl_mgL = 'Cl',
                                              Ca_mgL = 'Ca',
                                              K_mgL = 'K',
                                              Mg_mgL = 'Mg',
                                              Na_mgL = 'Na',
                                              P_mgL = 'P',
                                              DOC_mgL = 'DOC',
                                              Br_mgL = 'Br',
                                              SO4_mgL = 'SO4',
                                              PO4_mgL = 'PO4',
                                              SiO2_mgL = 'SiO2',
                                              Temp_C = 'temp',
                                              pH = 'pH',
                                              Conductivity = 'spCond',
                                              DO_mgL = 'DO',
                                              DO_per_sat = 'DO_sat'),
                               data_col_pattern = '#V#',
                               is_sensor = FALSE)
        
        d_m <- ms_cast_and_reflag(d_m,
                                  varflag_col_pattern = NA)
        
        all_modern <- rbind(all_modern, d_m)
    }
    
    # spCond ms/cm to us/cm
    all_modern <- all_modern %>%
        mutate(val = ifelse(var == 'spCond', val*1000, val))
    
    all_modern <- ms_conversions(all_modern,
                                     convert_units_from = c(PO4 = 'mg/l'),
                                     convert_units_to = c(PO4 = 'mg/l'))
    
    historical_fils <- grep('hist', fils, value = T)
    
    all_historical <- tibble()
    for(s in 1:length(historical_fils)){
        
        d_h <- ms_read_raw_csv(filepath = historical_fils[s],
                               datetime_cols = c(Date_ = '%Y-%m-%d'),
                               datetime_tz = 'US/Eastern',
                               site_code_col = 'Location',
                               data_cols =  c(pH='pH',
                                              NO3_NO2_N_mgL='NO3_NO2_N',
                                              NH4_N_mgL='NH4_N',
                                              PO4_P_mgL='PO4_P',
                                              Cl_mgL='Cl',
                                              K_mgL = 'K',
                                              Na_mgL = 'Na',
                                              Ca_mgL = 'Ca',
                                              Mg_mgL = 'Mg',
                                              SO4_S_mgL = 'SO4_S',
                                              TKN_mgL = 'TKN',
                                              SiO3_mgL = 'SiO3',
                                              HCO3_mgL = 'HCO3',
                                              TN_mgL = 'TN',
                                              Conductivi = 'spCond',
                                              Temp_C = 'temp'),
                               data_col_pattern = '#V#',
                               set_to_NA = 'NULL',
                               is_sensor = FALSE)
        
        d_h <- ms_cast_and_reflag(d_h,
                                  varflag_col_pattern = NA)
        
        all_historical <- rbind(all_historical, d_h)
    }

    all_historical <- ms_conversions(all_historical,
                                     convert_units_from = c(SiO3 = 'mg/l'),
                                     convert_units_to = c(SiO3 = 'mg/l'))

    d <- rbind(all_historical, all_modern)

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

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {
    
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/santee-wetdry_Export.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)
    
    d <- read.csv(rawfile) %>%
        mutate(conver_fact = as.numeric(SampleArea)/(as.numeric(SampleWeight)/1e+6)) %>%
        mutate(NH4_N = as.character(as.numeric(NH4_N)*conver_fact),
               CL = as.character(as.numeric(CL)*conver_fact),
               Br = as.character(as.numeric(Br)*conver_fact),
               NO3_N = as.character(as.numeric(NO3_N)*conver_fact),
               O_PO4 = as.character(as.numeric(O_PO4)*conver_fact),
               SO4 = as.character(as.numeric(SO4)*conver_fact),
               K = as.character(as.numeric(K)*conver_fact),
               NA. = as.character(as.numeric(NA.)*conver_fact),
               CA = as.character(as.numeric(CA)*conver_fact),
               MG = as.character(as.numeric(MG)*conver_fact),
               TP = as.character(as.numeric(TP)*conver_fact)) %>%
        # This gauge is located near the HQ, the HQ dataset does not
        # include precip data past 2000. But the SEFHQ_pluvio is located near the 
        # HQ
        mutate(site = 'SEFHQ_pluvio')
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                           datetime_cols = c(Date = '%Y-%m-%d'),
                           datetime_tz = 'US/Eastern',
                           site_code_col = 'site',
                           data_cols =  c(NH4_N = 'NH4_N',
                                          CL = 'Cl',
                                          Br = 'Br',
                                          NO3_N = 'NO3_N',
                                          O_PO4 = 'PO4',
                                          SO4 = 'SO4',
                                          K = 'K',
                                          NA. = 'Na',
                                          CA = 'Ca',
                                          MG = 'Mg',
                                          TP = 'TP',
                                          PH = 'pH',
                                          Conductivity = 'spCond'),
                           data_col_pattern = '#V#',
                           is_sensor = FALSE)
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    d <- ms_conversions(d,
                          convert_units_from = c(PO4 = 'mg/l'),
                          convert_units_to = c(PO4 = 'mg/l'))
    
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

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {
    
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)
    
    # WS79 AND WS80 are miss labeled. Check this if the product is updated
    wb_paths <- list.files(rawfile, full.names = T)
    # wb_paths <- grep('WS79|WS80', wb_paths, value = TRUE)
    temp_dir <- tempdir()
    
    proj <- choose_projection(unprojected = TRUE)
    
    unlink(temp_dir, recursive = T)
    temp_dir <- tempdir()
    
    unzip(wb_paths[1], exdir = temp_dir)
    
    name <- str_split_fixed(wb_paths[1], 'WS', n = Inf)[1,2]
    name <- paste0('WS', str_split_fixed(name, '_| ', n = Inf)[1,1])
    
    name <- case_when(name == 'WS79' ~ 'WS80',
                      name == 'WS80' ~ 'WS79',
                      TRUE ~ name)
    
    ws1 <- st_read(temp_dir) %>%
        mutate(site_code = !!name) %>%
        select(site_code) %>%
        sf::st_transform(proj)
    
    unlink(temp_dir, recursive = TRUE)
    
    temp_dir <- tempdir()
    
    unzip(wb_paths[2], exdir = temp_dir)
    
    name <- str_split_fixed(wb_paths[2], 'WS', n = Inf)[1,2]
    name <- paste0('WS', str_split_fixed(name, '_| ', n = Inf)[1,1])
    
    name <- case_when(name == 'WS79' ~ 'WS80',
                      name == 'WS80' ~ 'WS79',
                      TRUE ~ name)
    
    ws2 <- st_read(temp_dir) %>%
        mutate(site_code = !!name) %>%
        select(site_code) %>%
        sf::st_cast(., to = 'POLYGON') %>%
        sf::st_transform(proj) %>%
        mutate(area = sf::st_area(geometry)) %>%
        filter(as.numeric(area) > 100) %>%
        select(-area)
    
    unlink(temp_dir, recursive = TRUE)
    
    temp_dir <- tempdir()
    
    unzip(wb_paths[3], exdir = temp_dir)
    
    name <- str_split_fixed(wb_paths[3], 'WS', n = Inf)[1,2]
    name <- paste0('WS', str_split_fixed(name, '_| ', n = Inf)[1,1])
    
    name <- case_when(name == 'WS79' ~ 'WS80',
                      name == 'WS80' ~ 'WS79',
                      TRUE ~ name)
    
    ws3 <- st_read(temp_dir) %>%
        mutate(site_code = !!name) %>%
        select(site_code) %>%
        sf::st_cast(., to = 'POLYGON') %>%
        sf::st_transform(proj)
    
    unlink(temp_dir, recursive = TRUE)
    
    temp_dir <- tempdir()
    
    unzip(wb_paths[4], exdir = temp_dir)
    
    name <- str_split_fixed(wb_paths[4], 'WS', n = Inf)[1,2]
    name <- paste0('WS', str_split_fixed(name, '_| ', n = Inf)[1,1])
    
    name <- case_when(name == 'WS79' ~ 'WS80',
                      name == 'WS80' ~ 'WS79',
                      TRUE ~ name)
    
    ws4 <- st_read(temp_dir) %>%
        mutate(site_code = !!name) %>%
        select(site_code) %>%
        sf::st_cast(., to = 'POLYGON') %>%
        sf::st_transform(proj)

    d <- rbind(ws1, ws2, ws3, ws4) %>%
        mutate(area = as.numeric(sf::st_area(geometry)/10000))

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
                      shapefile = TRUE)
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
