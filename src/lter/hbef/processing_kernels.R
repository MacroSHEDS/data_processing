#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_1 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_13 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component, '.csv'),
        cacheOK=FALSE, method='curl')

    return()
}

#stream_chemistry; precip_chemistry: STATUS=READY
#. handle_errors
process_0_208 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    if(set_details$component == 'Analytical Methods'){
        fext <- '.pdf'
    } else {
        l1 <- all(grepl('precip', c(set_details$component,  prodname_ms)))
        l2 <- all(grepl('stream', c(set_details$component,  prodname_ms)))

        if(! sum(l1, l2)){
            loginfo('Skipping redundant download', logger=logger_module)
            return(generate_blacklist_indicator())
        }

        fext <- '.csv'
    }

    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component, fext),
        cacheOK=FALSE, method='curl')

    return()
}

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_0_107 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

    return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_0_100 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

    return()
}

#ws_boundary: STATUS=READY
#. handle_errors
process_0_94 <- function(set_details, network, domain) {

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    rawfile=glue(raw_data_dest, '/', set_details$component)
    download.file(url = set_details$url,
                  destfile = rawfile,
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_1 <- function(network, domain, prodname_ms, site_name,
    component){
    # site_name=site_name; component=in_comp

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
        n=network, d=domain, p=prodname_ms, s=site_name, c=component)

    d = sw(read_csv(rawfile, progress=FALSE,
        col_types=readr::cols_only(
            DATETIME='c', #can't parse 24:00
            WS='c',
            Discharge_ls='d'))) %>%
            # Flag='c'))) %>% #all flags are acceptable for this product
        rename(site_name = WS,
            datetime = DATETIME,
            discharge = Discharge_ls)

    detlim <- ue(identify_detection_limit(d$discharge))

    d <- d %>%
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

    d$discharge <- ue(apply_detection_limit(d$discharge, detlim))

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_13 <- function(network, domain, prodname_ms, site_name,
    component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
        n=network, d=domain, p=prodname_ms, s=site_name, c=component)

    d = sw(read_csv(rawfile, progress=FALSE,
        col_types=readr::cols_only(
            DATE='c', #can't parse 24:00
            rainGage='c',
            Precip='d'))) %>%
        # Flag='c'))) %>% #all flags are acceptable for this product
        rename(datetime = DATE,
            site_name = rainGage,
            precip = Precip)

    detlim <- ue(identify_detection_limit(d$precip))

    d <- d %>%
        mutate(
            datetime = with_tz(force_tz(as.POSIXct(datetime), 'US/Eastern'), 'UTC'),
            ms_status = 0) %>%
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

    d$precip <- ue(apply_detection_limit(d$precip, detlim))
}

#stream_chemistry; precip_chemistry: STATUS=READY
#. handle_errors
process_1_208 <- function(network, domain, prodname_ms, site_name,
    component){

    #this product includes precip chem and stream chem, which have the same
    #data structure. so for now, the component argument governs which one is
    #processed herein. once we work out rain interpolation, we might have to
    #add a conditional and some divergent processing here.

    if(component == 'Analytical Methods') return()

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
        n=network, d=domain, p=prodname_ms, s=site_name, c=component)

    d <- sw(read_csv(rawfile, col_types=readr::cols_only(
            site='c', date='c', timeEST='c', pH='n', DIC='n', spCond='n',
            temp='n', ANC960='n', ANCMet='n', precipCatch='n', flowGageHt='n',
            Ca='n', Mg='n', K='n', Na='n', TMAl='n', OMAl='n',
            Al_ICP='n', NH4='n', SO4='n', NO3='n', Cl='n', PO4='n',
            DOC='n', TDN='n', DON='n', SiO2='n', Mn='n', Fe='n',# notes='c',
            `F`='n', cationCharge='n', fieldCode='c', anionCharge='n',
            theoryCond='n', ionError='n', ionBalance='n'))) %>%
        rename(site_name = site) %>%
        rename_all(dplyr::recode, #essentially rename_if_exists
                   precipCatch='precipitation_ns',
                   flowGageHt='discharge_ns')

        detlims <- ue(identify_detection_limit(d))

    d <- d %>%
        mutate(site_name = ifelse(grepl('W[0-9]', site_name), #harmonize sitename conventions
            tolower(site_name), site_name)) %>%
        mutate(
            timeEST = ifelse(is.na(timeEST), '12:00', timeEST),
            datetime = lubridate::ymd_hm(paste(date, timeEST), tz = 'UTC'),
            ms_status = ifelse(is.na(fieldCode), FALSE, TRUE), #see summarize
            DIC = ue(convert_unit(DIC, 'uM', 'mM')),
            NH4_N = ue(convert_molecule(NH4, 'NH4', 'N')),
            NO3_N = ue(convert_molecule(NO3, 'NO3', 'N')),
            PO4_P = ue(convert_molecule(PO4, 'PO4', 'P'))) %>%
        select(-date, -timeEST, -PO4, -NH4, -NO3, -fieldCode) %>%
        filter_at(vars(-site_name, -datetime),
                  any_vars(! is.na(.))) %>%
        group_by(datetime, site_name) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status)) %>%
        select(-ms_status, everything())

    d[is.na(d)] = NA #replaces NaNs. is there a clean, pipey way to do this?

    intv <- ifelse(grepl('precip', prodname_ms),
                   '1 day',
                   '1 hour')
    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = intv,
                                 impute_limit = 30))

    d <- ue(apply_detection_limit(d, detlims))

    return(d)
}

#ws_boundary: STATUS=READY
#. handle_errors
process_1_94 <- function(network, domain, prodname_ms, site_name,
                         component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n=network, d=domain, p=prodname_ms, s=site_name)
    rawfile <- glue(rawdir, '/', component)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    projstring <- ue(choose_projection(unprojected = TRUE))

    wb <- sf::st_read(rawdir, stringsAsFactors = FALSE,
                      quiet = TRUE) %>%
        # mutate(WSHEDS0_ID = ifelse(is.na(WSHEDS0_ID), 0, WSHEDS0_ID)) %>%
        filter(!is.na(WS)) %>% #ignore encompassing, non-experimental watershed
        select(site_name = WSHEDS0_ID,
               area = AREA,
               perimeter = PERIMETER,
               geometry = geometry) %>%
        mutate(site_name = paste0('w', site_name)) %>%
        sf::st_transform(projstring) %>%
        arrange(site_name)

    unlink(zipped_files)

    for(i in 1:nrow(wb)){

        new_wb <- wb[i,]

        site_name <- as_tibble(new_wb) %>%
            pull(site_name)

        ue(write_ms_file(d = new_wb,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_name = site_name,
                         level = 'munged',
                         shapefile = TRUE,
                         link_to_portal = TRUE))
    }

    return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_1_100 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n=network, d=domain, p=prodname_ms, s=site_name)
    rawfile <- glue(rawdir, '/', component)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    projstring <- ue(choose_projection(unprojected = TRUE))

    rg_all <- sf::st_read(rawdir, stringsAsFactors = FALSE,
                          quiet = TRUE) %>%
        filter(! is.na(GAGE_NUM)) %>%
        select(site_name = ID,
               geometry = geometry) %>%
        sf::st_transform(projstring) %>%
        arrange(site_name)

    unlink(zipped_files)

    for(i in 1:nrow(rg_all)){

        rg <- rg_all[i,] %>%
            sf::st_zm(drop=TRUE, what='ZM') #drop Z dimension

        ue(write_ms_file(d = rg,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_name = rg$site_name,
                         level = 'munged',
                         shapefile = TRUE,
                         link_to_portal = TRUE))
    }

    return()
}

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_1_107 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n=network, d=domain, p=prodname_ms, s=site_name)
    rawfile <- glue(rawdir, '/', component)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    projstring <- ue(choose_projection(unprojected = TRUE))

    weirs_all <- sf::st_read(rawdir, stringsAsFactors = FALSE,
                             quiet = TRUE) %>%
        filter(! is.na(WEIR_NUM)) %>%
        select(site_name = WEIR_NUM,
               geometry = geometry) %>%
        mutate(site_name = paste0('w', site_name)) %>%
        sf::st_transform(projstring) %>%
        arrange(site_name)

    unlink(zipped_files)

    for(i in 1:nrow(weirs_all)){

        w <- weirs_all[i,] %>%
             sf::st_zm(drop=TRUE, what='ZM') #drop Z dimension

        ue(write_ms_file(d = w,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_name = w$site_name,
                         level = 'munged',
                         shapefile = TRUE,
                         link_to_portal = TRUE))
    }

    return()
}

#derive kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){ #13

    ue(precip_idw(precip_prodname = 'precipitation__13',
                  wb_prodname = 'ws_boundary__94',
                  pgauge_prodname = 'precip_gauge_locations__100',
                  precip_prodname_out = prodname_ms))

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){ #208

    ue(pchem_idw(pchem_prodname = 'precip_chemistry__208',
                 precip_prodname = 'precipitation__13',
                 wb_prodname = 'ws_boundary__94',
                 pgauge_prodname = 'precip_gauge_locations__100',
                 pchem_prodname_out = prodname_ms))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms){

    chemprod <- 'stream_chemistry__208'
    qprod <- 'discharge__1'

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
                                     # dt_round_interv = 'hours')))

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

    ue(flux_idw(pchem_prodname = 'precip_chemistry__208',
                precip_prodname = 'precipitation__13',
                wb_prodname = 'ws_boundary__94',
                pgauge_prodname = 'precip_gauge_locations__100',
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
                     ws_prodname='ws_boundary__94'))
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
                     ws_prodname='ws_boundary__94'))
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
                     ws_prodname='ws_boundary__94'))
    }
    
    if(prodname_ms == 'fpar__ms007') {
        ue(get_gee_standard(network=network, 
                         domain=domain, 
                         gee_id='MODIS/006/MOD15A2H', 
                         band='Fpar_500m', 
                         prodname='fpar', 
                         rez=500,
                         ws_prodname='ws_boundary__94'))
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
                         ws_prodname='ws_boundary__94'))
        }
    
    if(prodname_ms == 'veg_cover__ms008') {
        ue(get_gee_standard(network=network, 
                         domain=domain, 
                         gee_id='MODIS/006/MOD44B', 
                         band='Percent_NonTree_Vegetation', 
                         prodname='veg_cover', 
                         rez=500,
                         ws_prodname='ws_boundary__94'))
        }
        
    if(prodname_ms == 'bare_cover__ms008') {
        ue(get_gee_standard(network=network, 
                         domain=domain, 
                         gee_id='MODIS/006/MOD44B', 
                         band='Percent_NonVegetated', 
                         prodname='bare_cover', 
                         rez=500,
                         ws_prodname='ws_boundary__94'))
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
                         ws_prodname='ws_boundary__94'))
    }
    
    if(prodname_ms == 'prism_temp_mean__ms009') {
        ue(get_gee_standard(network=network, 
                         domain=domain, 
                         gee_id='OREGONSTATE/PRISM/AN81d', 
                         band='tmean', 
                         prodname='prism_temp_mean', 
                         rez=4000),
                         ws_prodname='ws_boundary__94')
    }
    return()
}

