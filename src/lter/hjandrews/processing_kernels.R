#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_4341 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    fext <- ifelse(set_details$component %in% c('HF00401', 'HF00407'),
                   '.txt',
                   '.csv')

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  '.csv'),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#precipitation; precip_gauge_locations: STATUS=READY
#. handle_errors
process_0_5482 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    fext <- ifelse(set_details$component == 'MS00404',
                   '.txt',
                   '.csv')

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precipitation'){

        if(! set_details$component %in% c('MS00402', 'MS00403', 'MS00404')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_gauge_locations'){

        if(! set_details$component == 'MS00401'){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  fext),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#stream_chemistry; stream_flux_inst: STATUS=READY
#. handle_errors
process_0_4021 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_chemistry'){

        if(! set_details$component %in% c('CF00201', 'CF00202', 'CF00203', 'CF00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_flux_inst'){

        if(! set_details$component %in% c('CF00204', 'CF00205', 'CF00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  '.csv'),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#precip_chemistry; precip_flux_inst: STATUS=READY
#. handle_errors
process_0_4022 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_chemistry'){

        if(! set_details$component %in% c('CP00201', 'CP00203', 'CP00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_flux_inst'){

        if(! set_details$component %in% c('CP00202', 'CP00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  '.csv'),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#stream_gauge_locations; ws_boundary: STATUS=READY
#. handle_errors
process_0_3239 <- function(set_details, network, domain) {

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'ws_boundary'){

        if(! set_details$component %in% c('hf01402', 'hf01404')){
            loginfo('Skipping redundant or unneeded download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_gauge_locations'){

        if(! set_details$component == 'hf01403'){
            loginfo('Skipping redundant or unneeded download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    rawfile=glue(raw_data_dest,
                 '/',
                 set_details$component)
    download.file(url = set_details$url,
                  destfile = rawfile,
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#munge kernels ####

#discharge: STATUS=PENDING (just copied from hbef. see comment within)
#. handle_errors
process_1_4341 <- function(network, domain, prodname_ms, site_name,
                           components){

    #discharge for hj comes from two sources, lter portal and
    #http://andlter.forestry.oregonstate.edu/data/register/default.aspx?datapage=page2_static_ascii&enid=1519&entnum=5&dbcode=HF004

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.txt',
                   n=network,
                   d=domain,
                   p=prodname_ms,
                   s=site_name,
                   c=component)
    read_csv(rawfile)

    d = sw(read_csv(rawfile, progress=FALSE,
                    col_types=readr::cols_only(
                        DATETIME='c', #can't parse 24:00
                        WS='c',
                        Discharge_ls='d'))) %>%
        # Flag='c'))) %>% #all flags are acceptable for this product
        rename(site_name = WS,
               datetime = DATETIME,
               discharge = Discharge_ls) %>%
        mutate(
            datetime = with_tz(force_tz(as.POSIXct(datetime), 'US/Eastern'), 'UTC'),
            # datetime = with_tz(as_datetime(datetime, 'US/Eastern'), 'UTC'),
            site_name = paste0('w', site_name),
            ms_status = 0) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                  any_vars(! is.na(.))) %>%
        group_by(datetime, site_name) %>%
        summarize(
            discharge = mean(discharge, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup()

    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = '15 min',
                                 impute_limit = 30))

    return(d)
}

#precipitation; precip_gauge_locations: STATUS=READY
#. handle_errors
process_1_5482 <- function(network, domain, prodname_ms, site_name,
                         components){

    component <- ifelse(prodname_ms == 'precip_gauge_locations__5482',
                        'MS00401',
                        'MS00403')

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n=network,
                    d=domain,
                    p=prodname_ms,
                    s=site_name,
                    c=component)

    if(prodname_ms == 'precip_gauge_locations__5482'){

        projstring <- ue(choose_projection(unprojected = TRUE))

        d <- sw(read_csv(rawfile1, progress=FALSE,
                         col_types = readr::cols_only(
                             SITECODE = 'c',
                             LATITUDE = 'd',
                             LONGITUDE = 'd'))) %>%
             rename(site_name = SITECODE)

        sp::coordinates(d) <- ~LONGITUDE+LATITUDE
        d <- sf::st_as_sf(d)
        sf::st_crs(d) <- projstring #assuming. geodetic datum not given by lter

    } else {

        d = sw(read_csv(rawfile1, progress=FALSE,
                        col_types=readr::cols_only(
                            DATE = 'D',
                            SITECODE = 'c',
                            # PRECIP_METHOD = 'c', #method information
                            # QC_LEVEL = 'c', #derived, gapfilled, etc
                            PRECIP_TOT_DAY = 'd',
                            PRECIP_TOT_FLAG = 'c',
                            EVENT_CODE = 'c')))

        d = ue(sourceflags_to_ms_status(d,
                                        flagstatus_mappings = list(
                                            PRECIP_TOT_FLAG = c('A', 'E'),
                                            EVENT_CODE = NA)))

        d <- d %>%
            rename(datetime = DATE,
                   site_name = SITECODE,
                   precip = PRECIP_TOT_DAY) %>%
            mutate(datetime = lubridate::ymd(datetime, tz = 'UTC')) %>%
            filter_at(vars(-site_name, -datetime, -ms_status),
                      any_vars(! is.na(.))) %>%
            group_by(datetime, site_name) %>%
            summarize(
                precip = mean(precip, na.rm=TRUE),
                ms_status = numeric_any(ms_status)) %>%
            ungroup()

        d <- ue(synchronize_timestep(ms_df = d,
                                     desired_interval = '1 day',
                                     impute_limit = 30))
    }

    return(d)
}

#stream_chemistry; stream_flux_inst: STATUS=READY
#. handle_errors
process_1_4021 <- function(network, domain, prodname_ms, site_name,
                           components){

    component <- ifelse(grepl('chemistry', prodname_ms),
                        'CF00201',
                        'CF00205')

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d = sw(read_csv(rawfile1,
                    progress = FALSE,
                    col_types = readr::cols_only(
                        DATE_TIME='c', SITECODE='c', TYPE='c', MEAN_LPS='d',
                        PH='d', COND='d',
                        ALK='d', SSED='d', SI='d', UTP='d', TDP='d', PARTP='d',
                        PO4P='d', UTN='d', TDN='d', DON='d', PARTN='d', UTKN='d',
                        TKN='d', NH3N='d', NO3N='d', `NA` = 'd', K='d', CA='d',
                        MG='d', SO4S='d', CL='d', DOC='d', ANCA='d',
                        ALK_OUTPUT='d', SSED_OUTPUT='d', SI_OUTPUT='d',
                        UTP_OUTPUT='d', TDP_OUTPUT='d', PARTP_OUTPUT='d',
                        PO4P_OUTPUT='d', UTN_OUTPUT='d', TDN_OUTPUT='d',
                        DON_OUTPUT='d', PARTN_OUTPUT='d', UTKN_OUTPUT='d',
                        TKN_OUTPUT='d', NH3N_OUTPUT='d', NO3N_OUTPUT='d',
                        NA_OUTPUT = 'd', K_OUTPUT='d', CA_OUTPUT='d',
                        MG_OUTPUT='d', SO4S_OUTPUT='d', CL_OUTPUT='d',
                        DOC_OUTPUT='d'))) %>%
        filter(! TYPE  %in% c('N', 'S', 'YE', 'QB', 'QS', 'QL', 'QA'))

                        # PHCODE='c', COND='d', CONDCODE='c', ALK='d', ALKCODE='c',
                        # SSED='d', SSEDCODE='c', SI='d', SICODE='c', UTP='d',
                        # UTPCODE='c', TDP='d', TDPCODE='c', PARTP='d', PARTPCODE='c',
                        # PO4P='d', PO4PCODE='c', UTN='d', UTNCODE='c', TDN='d',
                        # TDNCODE='c', DON='d', DONCODE='c', PARTN='d',
                        # PARTNCODE='c', UTKN='d', UTKNCODE='c', TKN='d',
                        # TKNCODE='c', NH3N='d', NH3NCODE='c', NO3N='d',
                        # NO3NCODE='c', `NA`='d', NACODE='c', K='d', KCODE='c',
                        # CA='d', CACODE='c', MG='d', MGCODE='c', SO4S='d',
                        # SO4SCODE='c', CL='d', CLCODE='c', DOC='d', DOCCODE='c',
                        # PVOL='d', PVOLCODE='c', ANCA='d', ANCACODE='c'))) %>%

    d = ue(sourceflags_to_ms_status(d,
                                    flagstatus_mappings = list(TYPE = 'F')))
    d <- d %>%
        rename(site_name = SITECODE,
               datetime = DATE_TIME) %>%
        rename_all(dplyr::recode,
                   MEAN_LPS='discharge_ns', PH='pH', COND='spCond', ALK='alk',
                   SSED='suspSed', SI='Si', PARTP='TPP', PO4P='PO4_P',
                   PARTN='TPN', NH3N='NH3_N', NO3N='NO3_N', CA='Ca', MG='Mg',
                   SO4S='SO4_S', CL='Cl', ANCA='AnCaR', `NA`='Na',
                   ALK_OUTPUT='alk',
                   SSED_OUTPUT='suspSed', SI_OUTPUT='Si', PARTP_OUTPUT='TPP',
                   PO4P_OUTPUT='PO4_P', PARTN_OUTPUT='TPN', NH3N_OUTPUT='NH3_N',
                   NO3N_OUTPUT='NO3_N', CA_OUTPUT='Ca', MG_OUTPUT='Mg',
                   SO4S_OUTPUT='SO4_S', CL_OUTPUT='Cl',
                   UTP_OUTPUT='UTP', TDP_OUTPUT='TDP', UTN_OUTPUT='UTN',
                   TDN_OUTPUT='TDN', DON_OUTPUT='DON', UTKN_OUTPUT='UTKN',
                   TKN_OUTPUT='TKN', NA_OUTPUT='Na', K_OUTPUT='K',
                   DOC_OUTPUT='DOC') %>%
        mutate(
            datetime = with_tz(as_datetime(datetime,
                                           tz = 'Etc/GMT-8'),
                               tz = 'UTC'),
            ms_status = as.logical(ms_status)) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                  any_vars(! is.na(.))) %>%
        group_by(datetime, site_name) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status))

    d[is.na(d)] = NA

    #constant interval
    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = '1 hour', #set back to '15 min' when we have server
                                 impute_limit = 30))

    return(d)
}

#precip_chemistry; precip_flux_inst: STATUS=READY
#. handle_errors
process_1_4022 <- function(network, domain, prodname_ms, site_name,
                           components){

    component <- ifelse(grepl('chemistry', prodname_ms),
                        'CP00201',
                        'CP00202')

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    d = sw(read_csv(rawfile1,
                    progress = FALSE,
                    col_types = readr::cols_only(
                        DATE_TIME='c', SITECODE='c', TYPE='c', PRECIP_CM='d',
                        PH='d', COND='d',
                        ALK='d', SSED='d', SI='d', UTP='d', TDP='d', PARTP='d',
                        PO4P='d', UTN='d', TDN='d', DON='d', PARTN='d', UTKN='d',
                        TKN='d', NH3N='d', NO3N='d', `NA` = 'd', K='d', CA='d',
                        MG='d', SO4S='d', CL='d', DOC='d', ANCA='d',
                        ALK_INPUT='d', SSED_INPUT='d', SI_INPUT='d',
                        UTP_INPUT='d', TDP_INPUT='d', PARTP_INPUT='d',
                        PO4P_INPUT='d', UTN_INPUT='d', TDN_INPUT='d',
                        DON_INPUT='d', PARTN_INPUT='d', UTKN_INPUT='d',
                        TKN_INPUT='d', NH3N_INPUT='d', NO3N_INPUT='d',
                        NA_INPUT = 'd', K_INPUT='d', CA_INPUT='d',
                        MG_INPUT='d', SO4S_INPUT='d', CL_INPUT='d',
                        DOC_INPUT='d'))) %>%
        filter(! TYPE  %in% c('N', 'S', 'YE', 'QB', 'QS', 'QL', 'QA'))

                        # PHCODE='c', COND='d', CONDCODE='c', ALK='d', ALKCODE='c',
                        # SSED='d', SSEDCODE='c', SI='d', SICODE='c', UTP='d',
                        # UTPCODE='c', TDP='d', TDPCODE='c', PARTP='d', PARTPCODE='c',
                        # PO4P='d', PO4PCODE='c', UTN='d', UTNCODE='c', TDN='d',
                        # TDNCODE='c', DON='d', DONCODE='c', PARTN='d',
                        # PARTNCODE='c', UTKN='d', UTKNCODE='c', TKN='d',
                        # TKNCODE='c', NH3N='d', NH3NCODE='c', NO3N='d',
                        # NO3NCODE='c', `NA`='d', NACODE='c', K='d', KCODE='c',
                        # CA='d', CACODE='c', MG='d', MGCODE='c', SO4S='d',
                        # SO4SCODE='c', CL='d', CLCODE='c', DOC='d', DOCCODE='c',
                        # PVOL='d', PVOLCODE='c', ANCA='d', ANCACODE='c'))) %>%

    d = ue(sourceflags_to_ms_status(d,
                                    flagstatus_mappings = list(TYPE = 'F')))
    d <- d %>%
        rename(site_name = SITECODE,
               datetime = DATE_TIME) %>%
        rename_all(dplyr::recode,
                   PRECIP_CM='precipitation_ns', PH='pH', COND='spCond', ALK='alk',
                   SSED='suspSed', SI='Si', PARTP='TPP', PO4P='PO4_P',
                   PARTN='TPN', NH3N='NH3_N', NO3N='NO3_N', CA='Ca', MG='Mg',
                   SO4S='SO4_S', CL='Cl', ANCA='AnCaR', `NA`='Na',
                   ALK_INPUT='alk',
                   SSED_INPUT='suspSed', SI_INPUT='Si', PARTP_INPUT='TPP',
                   PO4P_INPUT='PO4_P', PARTN_INPUT='TPN', NH3N_INPUT='NH3_N',
                   NO3N_INPUT='NO3_N', CA_INPUT='Ca', MG_INPUT='Mg',
                   SO4S_INPUT='SO4_S', CL_INPUT='Cl',
                   UTP_INPUT='UTP', TDP_INPUT='TDP', UTN_INPUT='UTN',
                   TDN_INPUT='TDN', DON_INPUT='DON', UTKN_INPUT='UTKN',
                   TKN_INPUT='TKN', NA_INPUT='Na', K_INPUT='K',
                   DOC_INPUT='DOC') %>%
        mutate(
            datetime = with_tz(as_datetime(datetime,
                                           tz = 'Etc/GMT-8'),
                               tz = 'UTC'),
            ms_status = as.logical(ms_status)) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                  any_vars(! is.na(.))) %>%
        group_by(datetime, site_name) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status))

    d[is.na(d)] = NA

    #constant interval
    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = '1 day',
                                 impute_limit = 30))

    return(d)
}

#ws_boundary; stream_gauge_locations: STATUS=READY
#. handle_errors
process_1_3239 <- function(network, domain, prodname_ms, site_name,
                           components){

    component <- ifelse(prodname_ms == 'stream_gauge_locations__3239',
                        'hf01403',
                        'hf01402')

    rawdir1 = glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name)
    rawfile1 <- glue(rawdir1, '/', component)

    zipped_files <- unzip(zipfile = rawfile1,
                          exdir = rawdir1,
                          overwrite = TRUE)

    projstring <- ue(choose_projection(unprojected = TRUE))

    if(prodname_ms == 'stream_gauge_locations__3239'){

        d <- sf::st_read(rawdir1,
                         stringsAsFactors = FALSE,
                         quiet = TRUE) %>%
            select(site_name = SITECODE,
                   geometry = geometry) %>%
            sf::st_transform(projstring) %>%
            arrange(site_name) %>%
            sf::st_zm(drop = TRUE,
                      what = 'ZM')

    } else {

        d <- sf::st_read(rawdir1,
                         stringsAsFactors = FALSE,
                         quiet = TRUE) %>%
            select(site_name = WS_,
                   area = F_AREA,
                   geometry = geometry) %>%
            filter(! grepl('^[0-9][0-9]?a$', site_name)) %>% #remove areas below station
            mutate(  #for consistency with name elsewhere
                site_name = stringr::str_pad(site_name,
                                             width = 2,
                                             pad = '0'),
                site_name = paste0('GSWS', site_name),
                site_name = ifelse(site_name == 'GSWSMACK',
                                   'GSMACK',
                                   site_name),
                site_name = ifelse(site_name == 'GSWS04',
                                   'GSLOOK',
                                   site_name)) %>%
            sf::st_transform(projstring) %>%
            arrange(site_name)
    }

    unlink(zipped_files)

    return(d)
}

#derive kernels ####