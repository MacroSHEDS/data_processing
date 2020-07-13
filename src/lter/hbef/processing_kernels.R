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
}

#rain_gauge_locations: STATUS=READY
#. handle_errors
process_0_100 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')
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
            precip = Precip) %>%
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
}

#stream_chemistry; precip_chemistry: STATUS=READY
#. handle_errors
process_1_208 <- function(network, domain, prodname_ms, site_name,
    component){

    #this product includes precip chem and stream chem, which have the same
    #data structure. so for now, the component argument governs which one is
    #processed herein. once we work out rain interpolation, we might have to
    #add a conditional and some divergent processing here.

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
                   flowGageHt='discharge_ns') %>%
        mutate(site_name = ifelse(grepl('W[0-9]', site_name), #harmonize sitename conventions
            tolower(site_name), site_name)) %>%
        mutate(
            timeEST = ifelse(is.na(timeEST), '12:00', timeEST),
            datetime = lubridate::ymd_hm(paste(date, timeEST), tz = 'UTC'),
            ms_status = ifelse(is.na(fieldCode), FALSE, TRUE), #see summarize
            DIC = convert_unit(DIC, 'uM', 'mM'),
            NH4_N = convert_molecule(NH4, 'NH4', 'N'),
            NO3_N = convert_molecule(NO3, 'NO3', 'N'),
            PO4_P = convert_molecule(PO4, 'PO4', 'P')) %>%
        select(-date, -timeEST, -PO4, -NH4, -NO3, -fieldCode) %>%
        filter_at(vars(-site_name, -datetime),
                   any_vars(! is.na(.))) %>%
        group_by(datetime, site_name) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status)) %>%
        select(-ms_status, everything())

    d[is.na(d)] = NA #replaces NaNs. is there a clean, pipey way to do this?

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

    ws <- sf::st_read(rawdir,
                      quiet = TRUE) %>%
        filter(!is.na(WS))

    unlink(zipped_files)

    for(i in 1:nrow(ws)){

        new_ws <- ws[i,]

        site_name <- as_tibble(new_ws) %>%
            pull(WS) %>%
            as.character() %>%
            stringr::str_replace('WS', 'w') #harmonize hbef sitenames

        write_munged_file(d = new_ws,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_name = site_name,
                          shapefile = TRUE)

        create_portal_link(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           site_name = site_name,
                           dir = TRUE)
    }

    return()
}

#rain_gauge_locations: STATUS=READY
#. handle_errors
process_1_100 <- function(network, domain, prodname_ms, site_name,
                          component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n=network, d=domain, p=prodname_ms, s=site_name)
    rawfile <- glue(rawdir, '/', component)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    rg_all <- sf::st_read(rawdir,
                          quiet = TRUE) %>%
        filter(! is.na(GAGE_NUM))

    unlink(zipped_files)

    for(i in 1:nrow(rg_all)){

        rg <- rg_all[i,] %>%
            sf::st_zm(drop=TRUE, what='ZM') #drop Z dimension

        gage_id <- as_tibble(rg) %>%
            mutate(GAGE_NUM = paste0('rg', GAGE_NUM)) %>%
            pull(GAGE_NUM)

        write_munged_file(d = rg,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_name = gage_id,
                          shapefile = TRUE)

        create_portal_link(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           site_name = gage_id,
                           dir = TRUE)
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

    weirs_all <- sf::st_read(rawdir,
                             quiet = TRUE) %>%
        filter(! is.na(WEIR_NUM))

    unlink(zipped_files)

    for(i in 1:nrow(weirs_all)){

        w <- weirs_all[i,] %>%
             sf::st_zm(drop=TRUE, what='ZM') #drop Z dimension

        site_name <- as_tibble(w) %>%
            mutate(WEIR_NUM = paste0('w', WEIR_NUM)) %>%
            pull(WEIR_NUM)

        write_munged_file(d = w,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_name = site_name,
                          shapefile = TRUE)

        create_portal_link(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           site_name = site_name,
                           dir = TRUE)
    }

    return()
}

#derive kernels####

#precipitation: STATUS=READY
#. handle_errors
process_2_13 <- function(network, domain, prodname_ms){
    # network='lter'; domain='hbef'; prodname_ms='precipitation__13'; i=j=1

    projstring <- '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'#4326: not projected
    wbprod <- 'ws_boundary__94'
    rgprod <- 'rain_gauge_locations__100'
    sgprod <- 'stream_gauge_locations__107'
    # rain_location <- st_read('data_in/hbef_raingage')
    # watersheds <- st_read('data_in/hbef_wsheds')
    # rain_raw <- read_csv('data_in/hbef_precip.csv') %>%
    #     rename(date = 1, ID = 2, precip = 3)

    gaugefiles <- list_munged_files(network = network,
                                    domain = domain,
                                    prodname_ms = prodname_ms)
    raindata <- tibble()
    for(i in 1:length(gaugefiles)){
        raindata <- read_feather(gaugefiles[i]) %>%
            bind_rows(raindata)
    }
    rain_raw = raindata

    wb_paths <- list.files(glue('data/{n}/{d}/munged/{wb}',
                                   n = network,
                                   d = domain,
                                   wb = wbprod),
                              recursive = TRUE,
                              full.names = TRUE,
                              pattern = '*.shp')
    wbs <- lapply(wb_paths, function(x) sf::st_read(x, stringsAsFactors = FALSE))
    # wb <- sw(Reduce(sf::st_union, wbs)) %>%
    wb <- sw(Reduce(rbind, wbs)) %>%
        st_transform(projstring)
    watersheds <- wb
    # watersheds <- sf::st_read('~/Downloads/hmm/hbef_wsheds/hbef_wsheds.shp')
    rg_paths <- list.files(glue('data/{n}/{d}/munged/{rg}',
                                   n = network,
                                   d = domain,
                                   rg = rgprod),
                              recursive = TRUE,
                              full.names = TRUE,
                              pattern = '*.shp')
    rgs <- lapply(rg_paths, function(x) sf::st_read(x, stringsAsFactors = FALSE))
    rg <- sw(Reduce(rbind, rgs)) %>%
        st_transform(projstring)
    rain_location <- rg
    # rain_location <- sf::st_read('~/Downloads/hmm/hbef_raingage/hbef_raingage.shp')

    sg_paths <- list.files(glue('data/{n}/{d}/munged/{sg}',
                                   n = network,
                                   d = domain,
                                   sg = sgprod),
                              recursive = TRUE,
                              full.names = TRUE,
                              pattern = '*.shp')
    sgs <- lapply(sg_paths, function(x) sf::st_read(x, stringsAsFactors = FALSE))
    sg <- sw(Reduce(rbind, sgs)) %>%
        st_transform(projstring)
    # st_set_crs(2154)

    ## Summarise data
    rain_annual <- rain_raw %>%
        # mutate(date = ymd(date)) %>%
        filter(year(datetime) == 2004) %>%
        group_by(site_name) %>%
        summarise(annual = sum(precip, na.rm = TRUE))
    rain_join <- left_join(rain_location,
                           rain_annual,
                           by = c('ID' = 'site_name')) %>%
        st_transform(projstring) #redund
    #?
    dem <- elevatr::get_elev_raster(watersheds, z = 12)# %>%
        # raster::projectRaster(crs = projstring)
        # terra::rast(dem) %>%
        # terra::project(projstring)
    gs <- gstat(formula = annual~1,
                locations = rain_join)
                # nmax = 5,
                # set = list(idp = 0))

    idw <- raster::interpolate(dem, gs)
    idw_mask <- raster::mask(idw, watersheds)
    idw_trm <- raster::trim(idw_mask)
    mapview(idw, maxpixels =  2757188)
    mapview(idw)
    mapview(idw_mask)
    ## Interpolation
    # v = gstat::variogram(annual~1, rain_join)
    v = gstat::variogram(gs, locations = rain_join)
    m = gstat::fit.variogram(v, gstat::vgm(1, 'Sph'))
    ws_grid <- dem
    #May not work if you have an updated stars but not an updated sf
    ws_stars <- st_as_stars(ws_grid)
    interp = krige(formula = annual~1, rain_join, ws_stars, model = m)
    plot(interp)
    test <- st_as_sf(interp)
    mapview(test, zcol = 'var1.pred', lwd = 0)
    mapview(rain_join, zcol = 'annual')
    # IDW
    test <- dem
    gs <- gstat(formula=annual~1, locations=rain_join)
    idw <- interpolate(test, gs)
    idw_mask <- mask(idw, ws8)
    idw_trm <- trim(idw_mask)



    #this logic is temporary, and just gets precip into the format that works
    #with the portal. but once interp is working, we'll perform that here
    #instead

    mfiles <- list_munged_files(network = network,
                                domain = domain,
                                prodname_ms = prodname_ms)

    combined <- tibble()
    for(f in mfiles){
        d = read_feather(f)
        combined <- bind_rows(combined, d)
    }

    #this chunk will be replaced by create_portal_link when interp is ready
    prod_dir = glue('data/{n}/{d}/derived/{p}', n=network, d=domain,
                    p=prodname_ms)
    dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)
    site_file <- glue('data/{n}/{d}/derived/{p}/precip.feather',
         n = network,
         d = domain,
         p = prodname_ms)
    write_feather(combined, site_file)
    portal_site_file <- glue('../portal/data/{d}/precip.feather', d = domain)
    unlink(portal_site_file)
    invisible(sw(file.link(to = portal_site_file, from = site_file)))

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_2_208 <- function(network, domain, prodname_ms){

    #this logic is temporary, and just gets pchem into the format that works
    #with the portal. but once interp is working, we'll perform that here
    #instead

    mfiles <- list_munged_files(network = network,
                                domain = domain,
                                prodname_ms = prodname_ms)

    combined <- tibble()
    for(f in mfiles){
        d = read_feather(f)
        combined <- bind_rows(combined, d)
    }

    #this chunk will be replaced by create_portal_link when interp is ready
    prod_dir = glue('data/{n}/{d}/derived/{p}', n=network, d=domain,
                    p=prodname_ms)
    dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)
    site_file <- glue('data/{n}/{d}/derived/{p}/precip.feather',
                      n = network,
                      d = domain,
                      p = prodname_ms)
    write_feather(combined, site_file)
    portal_site_file <- glue('../portal/data/{d}/precip.feather', d = domain)
    unlink(portal_site_file)
    invisible(sw(file.link(to = portal_site_file, from = site_file)))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    chemprod <- 'stream_chemistry__208'
    qprod <- 'discharge__1'

    chemfiles <- list_munged_files(network = network,
                                   domain = domain,
                                   prodname_ms = chemprod)
    qfiles <- list_munged_files(network = network,
                                domain = domain,
                                prodname_ms = qprod)

    flux_sites <- generics::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        flux <- sw(calc_inst_flux(chemprod = chemprod,
                                  qprod = qprod,
                                  site_name = s,
                                  dt_round_interv = 'hours'))

        write_munged_file(d = flux,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_name = s)

        create_portal_link(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           site_name = s)
    }

    return()
}

#precip_flux_inst: STATUS=PENDING (must localize precip to stream sites first)
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

    chemprod <- 'precip_chemistry__208'
    qprod <- 'precipitation__13'

    chemfiles <- list_munged_files(network = network,
                                   domain = domain,
                                   prodname_ms = chemprod)
    qfiles <- list_munged_files(network = network,
                                domain = domain,
                                prodname_ms = qprod)

    flux_sites <- generics::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        flux <- sw(calc_inst_flux(chemprod = chemprod,
                                  qprod = qprod,
                                  site_name = s,
                                  dt_round_interv = 'hours'))

        write_munged_file(d = flux,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_name = s)

        create_portal_link(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           site_name = s)
    }

    return()
}

