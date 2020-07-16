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

#derive kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_2_13 <- function(network, domain, prodname_ms){
    # network='lter'; domain='hbef'; prodname_ms='precipitation__13'; i=j=1

    #EPSG code 4326; datum WGS84; not projected
    # projstring <- glue('+proj=longlat +datum=WGS84 +no_defs ',
    #                    '+ellps=WGS84 +towgs84=0,0,0')
    projstring <- '+proj=utm +zone=19 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'

    precip <- read_combine_feathers(network, domain, prodname_ms)
    wb <- read_combine_shapefiles(network, domain, 'ws_boundary__94')
    wb <- sf::st_transform(wb, projstring) %>%
        select(-WSHEDS0_ID)
    rg <- read_combine_shapefiles(network, domain, 'rain_gauge_locations__100')
    rg <- sf::st_transform(rg, projstring) %>%
        select(-X, -Y, -GAGE_NUM)
    # sg <- read_combine_shapefiles(network, domain, 'stream_gauge_locations__107')
    # sg <- sf::st_transform(sg, projstring)

    precip <- precip %>%
        mutate(datetime = lubridate::year(datetime)) %>% #temporary
        group_by(site_name, datetime) %>%
        summarize(
            precip = mean(precip, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup() %>%
        filter(site_name %in% rg$ID) %>% #may have to harmonize names
        tidyr::pivot_wider(names_from = site_name,
                           values_from = precip) %>%
        mutate(ms_status = as.logical(ms_status)) %>%
        group_by(datetime) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else numeric_any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status)) %>%
        arrange(datetime)

    precip_status <- precip$ms_status
    precip_dt <- precip$datetime
    precip$ms_status <- precip$datetime <- NULL
    precip <- as.matrix(precip)
        # left_join(rg, by = c('site_name' = 'ID'))
    dem <- sm(elevatr::get_elev_raster(wb, z = 12))

    for(j in 1:nrow(wb)){

        wbj <- slice(wb, j)
        dem_wbj <- raster::crop(dem, wbj)
        dem_wbj <- raster::mask(dem_wbj, wbj)

        #rows sum to 1
        #norm only applies to present gauges

        inv_distmat <- matrix(NA, nrow = length(dem_wbj), ncol = nrow(rg),
                          dimnames = list(NULL, rg$ID))
        for(k in 1:nrow(rg)){
            rgk <- slice(rg, k)
            # elevs <- raster::values(dem_wbj) #incorporate this
            inv_distmat[, k] <- raster::distanceFromPoints(dem_wbj, rgk) %>%
                raster::values(.)
        }

        ws_mean_precip <- rep(NA, nrow(precip))
        for(k in 1:nrow(precip)){
            precip_k <- t(precip[k, , drop = FALSE])
            inv_distmat_sub <- inv_distmat[, ! is.na(precip_k)]
            precip_k <- precip_k[! is.na(precip_k)]
            weightmat <- do.call(rbind, #avoids matrix transposition
                                 unlist(apply(inv_distmat_sub,
                                              1,
                                              function(x) list(x / sum(x))),
                                        recursive = FALSE))
            precip_k[is.na(precip_k)] <- 0 #allows matrix multiplication
            precip_interp <- weightmat %*% precip_k
            ws_mean_precip[k] <- mean(precip_interp)
        }

        #WRITE THIS
        tibble(datetime = precip_dt,
               site_name = wbj$WS,
               precip = ws_mean_precip,
               ms_status = precip_status)

    }

        mean_precip <- raster::values(idw_mask) %>%
            mean(., na.rm = TRUE)
    }

    raster::extract(dem, rg) #get elevation at point (NOT NEEDED, BUT RECORD IT)

    #NOW TRY THE INTERP WAY AND COMPARE TIME
    # for(w in wb$WS){ #find tidy way to do this
    for(dt in unique(rain_st$datetime)){
        rain_dt <- filter(rain_st, datetime == dt)


        if(length(unique(rain_dt$precip)) == 1){
            #ALL OUTPUTS EQUAL
        }
        ms_status <- as.numeric(any(rain_dt$ms_status == 1))

        idw <- rain_dt %>%
            gstat::gstat(formula = precip~1,
                         locations = .) %>%
            raster::interpolate(dem, .)

        rg$X


        #HOW CAN THIS BE MADE EFFICIENT?
        wb_sub <- filter(wb, WS == w)
        idw_mask <- raster::mask(idw, wb_sub) #trims itself by default
        mean_precip <- raster::values(idw_mask) %>%
            mean(., na.rm = TRUE)
    }

    #dont forget to interp final sg precip to a desirable interval?

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

    gc()
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

