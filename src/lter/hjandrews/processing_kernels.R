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

#discharge: STATUS=READY
#. handle_errors
process_1_4341 <- function(network, domain, prodname_ms, site_name,
                           components){

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/HF00401.txt',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name)

    #look carefully at warnings from ms_read_raw_csv.
    #they may indicate insufficiencies
    d <- ue(ms_read_raw_csv(filepath = rawfile1,
                            datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                            datetime_tz = 'Etc/GMT-8',
                            site_name_col = 'SITECODE',
                            data_cols =  c(INST_Q = 'discharge'),
                            data_col_pattern = '#V#',
                            is_sensor = TRUE,
                            summary_flagcols = c('ESTCODE', 'EVENT_CODE')))

    d <- ue(ms_cast_and_reflag(d,
                               varflag_col_pattern = NA,
                               summary_flags_clean = list(
                                   ESTCODE = c('A', 'E'),
                                   EVENT_CODE = c(NA, 'WEATHR')),
                               summary_flags_dirty = list(
                                   ESTCODE = c('Q', 'S', 'P'),
                                   EVENT_CODE = c('INSREM', 'MAINTE'))))

    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))

    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))

    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))

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
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

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

    } else if(prodname_ms == 'precipitation__5482'){

        d <- ue(ms_read_raw_csv(filepath = rawfile1,
                                datetime_cols = c(DATE = '%Y-%m-%d'),
                                datetime_tz = 'UTC',
                                site_name_col = 'SITECODE',
                                data_cols =  c(PRECIP_TOT_DAY = 'precip'),
                                data_col_pattern = '#V#',
                                is_sensor = FALSE,
                                summary_flagcols = c('PRECIP_TOT_FLAG',
                                                     'EVENT_CODE')))

        d <- ue(ms_cast_and_reflag(d,
                                   varflag_col_pattern = NA,
                                   summary_flags_clean = list(
                                       PRECIP_TOT_FLAG = c('A', 'E'),
                                       EVENT_CODE = c(NA, 'METHOD')),
                                   #METHOD indicates when methods change.
                                   summary_flags_dirty = list(
                                       PRECIP_TOT_FLAG = c('Q', 'C', 'U'),
                                       EVENT_CODE = c('INSREM', 'MAINTE'))))

        d <- ue(carry_uncertainty(d,
                                  network = network,
                                  domain = domain,
                                  prodname_ms = prodname_ms))

        d <- ue(synchronize_timestep(d,
                                     desired_interval = '1 day', #set to '15 min' when we have server
                                     impute_limit = 30))

        d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))
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

    #look carefully at warnings from ms_read_raw_csv.
    #they may indicate insufficiencies
    d <- ue(ms_read_raw_csv(filepath = rawfile1,
                            datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                            datetime_tz = 'Etc/GMT-8',
                            site_name_col = 'SITECODE',
                            data_cols =  c(PH='pH', COND='spCond', ALK='alk',
                                SSED='suspSed', SI='Si', PARTP='TPP', PO4P='PO4_P',
                                PARTN='TPN', NH3N='NH3_N', NO3N='NO3_N', CA='Ca',
                                MG='Mg', SO4S='SO4_S', CL='Cl', ANCA='AnCaR',
                                `NA`='Na', 'UTP', 'TDP', 'UTN', 'TDN', 'DON',
                                'UTKN', 'TKN', 'K', 'DOC'),
                            data_col_pattern = '#V#',
                            is_sensor = FALSE,
                            alt_datacol_pattern = '#V#_OUTPUT',
                            var_flagcol_pattern = '#V#CODE',
                            summary_flagcols = c('TYPE')))

    d <- ue(ms_cast_and_reflag(d,
                               variable_flags_to_drop = 'N',
                               variable_flags_clean =
                                   c('A', 'E', 'D', 'DE', '*', 'D*'),
                               summary_flags_to_drop = list(
                                   TYPE = c('N', 'S', 'YE', 'QB', 'QS', 'QL', 'QA')),
                               summary_flags_clean = list(TYPE = 'F')))

    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))

    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))

    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))

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

    d <- ue(ms_read_raw_csv(filepath = rawfile1,
                            datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                            datetime_tz = 'Etc/GMT-8',
                            site_name_col = 'SITECODE',
                            data_cols =  c(PH='pH', COND='spCond', ALK='alk',
                                           SSED='suspSed', SI='Si', PARTP='TPP', PO4P='PO4_P',
                                           PARTN='TPN', NH3N='NH3_N', NO3N='NO3_N', CA='Ca',
                                           MG='Mg', SO4S='SO4_S', CL='Cl', ANCA='AnCaR',
                                           `NA`='Na', 'UTP', 'TDP', 'UTN', 'TDN', 'DON',
                                           'UTKN', 'TKN', 'K', 'DOC'),
                                           # PRECIP_CM='precip_ns'),
                            data_col_pattern = '#V#',
                            is_sensor = FALSE,
                            alt_datacol_pattern = '#V#_INPUT',
                            var_flagcol_pattern = '#V#CODE',
                            summary_flagcols = c('TYPE')))

    d <- ue(ms_cast_and_reflag(d,
                               variable_flags_to_drop = 'N',
                               variable_flags_clean =
                                   c('A', 'E', 'D', 'DE', '*', 'D*'),
                               summary_flags_to_drop = list(
                                   TYPE = c('N', 'S', 'YE', 'QB', 'QS', 'QL', 'QA')),
                               summary_flags_clean = list(TYPE = c('F', 'DF'))))

    #HJAndrews does not collect precip and precip chemistry at the same
    #locations, so we here crudely localize pchem data to the nearest precip
    #gauges. we could do this more gracefully with idw.
    d <- d %>%
        mutate(site_name = case_when(
            site_name == 'RCADMN' ~ 'PRIMET',
            grepl('^RCHI..$', site_name, perl = TRUE) ~ 'CENMET',
            TRUE ~ '_ERR')) #may be tripped if they add a new dry dep gauge

    if(any(d$site_name == '_ERR')){
        stop(glue('hjandrews has added a new pchem gauge that we havent mapped',
                  ' to a location'))
    }

    d <- ue(carry_uncertainty(d,
                              network = network,
                              domain = domain,
                              prodname_ms = prodname_ms))

    d <- ue(synchronize_timestep(d,
                                 desired_interval = '1 day', #set to '15 min' when we have server
                                 impute_limit = 30))

    d <- ue(apply_detection_limit_t(d, network, domain, prodname_ms))

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

#precipitation: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    ue(precip_idw(precip_prodname = 'precipitation__5482',
                  wb_prodname = 'ws_boundary__3239',
                  pgauge_prodname = 'precip_gauge_locations__5482',
                  precip_prodname_out = prodname_ms))

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

        ue(pchem_idw(pchem_prodname = 'precip_chemistry__4022',
                     precip_prodname = 'precipitation__5482',
                     wb_prodname = 'ws_boundary__3239',
                     pgauge_prodname = 'precip_gauge_locations__5482',
                     pchem_prodname_out = prodname_ms))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms){

    chemprod <- 'stream_chemistry__4021'
    qprod <- 'discharge__4341'

    chemfiles <- ue(list_munged_files(network = network,
                                      domain = domain,
                                      prodname_ms = chemprod))
    qfiles <- ue(list_munged_files(network = network,
                                   domain = domain,
                                   prodname_ms = qprod))

    flux_sites <- generics::intersect(
        ue(fname_from_fpath(qfiles, include_fext = FALSE)),
        ue(fname_from_fpath(chemfiles, include_fext = FALSE)))

    for(s in flux_sites){

        flux <- sw(ue(calc_inst_flux(chemprod = chemprod,
                                     qprod = qprod,
                                     site_name = s)))

        ue(write_ms_file(d = flux,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_name = s,
                         level = 'derived',
                         shapefile = FALSE,
                         link_to_portal = TRUE))
    }

    return()
}

#precip_flux_inst: STATUS=READY
#. handle_errors
process_2_ms004 <- function(network, domain, prodname_ms){

    ue(flux_idw(pchem_prodname = 'precip_chemistry__4022',
                precip_prodname = 'precipitation__5482',
                wb_prodname = 'ws_boundary__3239',
                pgauge_prodname = 'precip_gauge_locations__5482',
                flux_prodname_out = prodname_ms))

    return()
}

#npp: STATUS=READY
#. handle_errors
process_2_ms005 <- function(network, domain, prodname_ms) {

    ue(get_gee_standard(network=network,
                        domain=domain,
                        gee_id='UMT/NTSG/v2/LANDSAT/NPP',
                        band='annualNPP',
                        prodname='npp',
                        rez=30,
                        ws_prodname='ws_boundary__3239'))
    return()
}

#gpp: STATUS=READY
#. handle_errors
process_2_ms006 <- function(network, domain, prodname_ms) {

    ue(get_gee_standard(network=network,
                        domain=domain,
                        gee_id='UMT/NTSG/v2/LANDSAT/GPP',
                        band='GPP',
                        prodname='gpp',
                        rez=30,
                        ws_prodname='ws_boundary__3239'))
    return()
}

#lai; fpar: STATUS=READY
#. handle_errors
process_2_ms007 <- function(network, domain, prodname_ms) {

    if(prodname_ms == 'lai__ms007') {
        ue(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD15A2H',
                            band='Lai_500m',
                            prodname='lai',
                            rez=500,
                            ws_prodname='ws_boundary__3239'))
    }

    if(prodname_ms == 'fpar__ms007') {
        ue(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD15A2H',
                            band='Fpar_500m',
                            prodname='fpar',
                            rez=500,
                            ws_prodname='ws_boundary__3239'))
    }
    return()
}

#tree_cover; veg_cover; bare_cover: STATUS=READY
#. handle_errors
process_2_ms008 <- function(network, domain, prodname_ms) {

    if(prodname_ms == 'tree_cover__ms008') {
        ue(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_Tree_Cover',
                            prodname='tree_cover',
                            rez=500,
                            ws_prodname='ws_boundary__3239'))
    }

    if(prodname_ms == 'veg_cover__ms008') {
        ue(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_NonTree_Vegetation',
                            prodname='veg_cover',
                            rez=500,
                            ws_prodname='ws_boundary__3239'))
    }

    if(prodname_ms == 'bare_cover__ms008') {
        ue(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='MODIS/006/MOD44B',
                            band='Percent_NonVegetated',
                            prodname='bare_cover',
                            rez=500,
                            ws_prodname='ws_boundary__3239'))
    }
    return()
}

#prism_precip; prism_temp_mean: STATUS=READY
#. handle_errors
process_2_ms009 <- function(network, domain, prodname_ms) {

    if(prodname_ms == 'prism_precip__ms009') {
        ue(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='OREGONSTATE/PRISM/AN81d',
                            band='ppt',
                            prodname='prism_precip',
                            rez=4000,
                            ws_prodname='ws_boundary__3239'))
    }

    if(prodname_ms == 'prism_temp_mean__ms009') {
        ue(get_gee_standard(network=network,
                            domain=domain,
                            gee_id='OREGONSTATE/PRISM/AN81d',
                            band='tmean',
                            prodname='prism_temp_mean',
                            rez=4000),
           ws_prodname='ws_boundary__3239')
    }
    return()
}
