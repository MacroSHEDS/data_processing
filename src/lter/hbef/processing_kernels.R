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
process_2_13 <- function(network, domain, prodname_ms){
    # network='lter'; domain='hbef'; prodname_ms='precipitation__13'; i=j=1

    #load precip data, watershed boundaries, rain gauge locations
    precip <- ue(read_combine_feathers(network = network,
                                       domain = domain,
                                       prodname_ms = prodname_ms))
    wb <- ue(read_combine_shapefiles(network = network,
                                     domain = domain,
                                     prodname_ms = 'ws_boundary__94'))
    rg <- ue(read_combine_shapefiles(network = network,
                                     domain = domain,
                                     prodname_ms = 'rain_gauge_locations__100'))

    #project based on average latlong of watershed boundaries
    bbox <- as.list(sf::st_bbox(wb))
    projstring <- ue(choose_projection(lat = mean(bbox$ymin, bbox$ymax),
                                       long = mean(bbox$xmin, bbox$xmax)))
    wb <- sf::st_transform(wb, projstring)
    rg <- sf::st_transform(rg, projstring)

    #get a DEM that encompasses all watersheds; add elev column to rain gauges
    dem <- sm(elevatr::get_elev_raster(wb, z = 12)) #res should adjust with area
    rg$elevation <- terra::extract(dem, rg)

    #clean precip and arrange for matrixification
    precip <- precip %>%
        filter(site_name %in% rg$site_name) %>%
        # mutate(datetime = lubridate::year(datetime)) %>%
        mutate(datetime = lubridate::as_date(datetime)) %>% #finer? coarser?
        group_by(site_name, datetime) %>%
        summarize(
            precip = mean(precip, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup() %>%
        tidyr::pivot_wider(names_from = site_name,
                           values_from = precip) %>%
        mutate(ms_status = as.logical(ms_status)) %>%
        group_by(datetime) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status)) %>%
        arrange(datetime)

    #interpolate precipitation volume and write watershed averages
    for(j in 1:nrow(wb)){

        wbj <- slice(wb, j)
        site_name <- wbj$site_name

        ws_mean_precip <- ue(shortcut_idw(encompassing_dem = dem,
                                          wshd_bnd = wbj,
                                          data_locations = rg,
                                          data_values = precip,
                                          stream_site_name = site_name,
                                          output_varname = 'precip',
                                          elev_agnostic = FALSE))

        #interp final precip to a desirable interval?
        ue(write_ms_file(ws_mean_precip,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_name = site_name,
                         level = 'derived',
                         shapefile = FALSE,
                         link_to_portal = TRUE))
    }

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_2_208 <- function(network, domain, prodname_ms){

    #load precip and pchem data, watershed boundaries, rain gauge locations
    pchem <- ue(read_combine_feathers(network = network,
                                      domain = domain,
                                      prodname_ms = prodname_ms))
    precip <- ue(read_combine_feathers(network = network,
                                       domain = domain,
                                       prodname_ms = 'precipitation__13'))
    wb <- ue(read_combine_shapefiles(network = network,
                                     domain = domain,
                                     prodname_ms = 'ws_boundary__94'))
    rg <- ue(read_combine_shapefiles(network = network,
                                     domain = domain,
                                     prodname_ms = 'rain_gauge_locations__100'))

    #project based on average latlong of watershed boundaries
    bbox <- as.list(sf::st_bbox(wb))
    projstring <- ue(choose_projection(lat = mean(bbox$ymin, bbox$ymax),
                                       long = mean(bbox$xmin, bbox$xmax)))
    wb <- sf::st_transform(wb, projstring)
    rg <- sf::st_transform(rg, projstring)

    #get a DEM that encompasses all watersheds; add elev column to rain gauges
    dem <- sm(elevatr::get_elev_raster(wb, z = 12)) #res should adjust with area
    rg$elevation <- terra::extract(dem, rg)

    #clean precip and arrange for matrixification
    precip <- precip %>%
        filter(site_name %in% rg$site_name) %>%
        # mutate(datetime = lubridate::year(datetime)) %>%
        mutate(datetime = lubridate::as_date(datetime)) %>% #finer? coarser?
        group_by(site_name, datetime) %>%
        summarize(
            precip = mean(precip, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup() %>%
        tidyr::pivot_wider(names_from = site_name,
                           values_from = precip) %>%
        mutate(ms_status = as.logical(ms_status)) %>%
        group_by(datetime) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status)) %>%
        arrange(datetime)

    #organize variables by those that can be flux converted and those that can't
    flux_vars <- ms_vars$variable_code[as.logical(ms_vars$flux_convertible)]
    pchem_vars_fluxable <- colnames(sw(select(pchem,
                                              -datetime,
                                              -site_name,
                                              -ms_status,
                                              one_of(flux_vars))))
    pchem_vars_unfluxable <- colnames(sw(select(pchem,
                                                -datetime,
                                                -site_name,
                                                -ms_status,
                                                -one_of(flux_vars))))

    #clean pchem one variable at a time, matrixify it, insert it into list
    nvars_fluxable <- length(pchem_vars_fluxable)
    pchem_setlist_fluxable <- as.list(rep(NA, nvars_fluxable))
    for(i in 1:nvars_fluxable){

        v <- pchem_vars_fluxable[i]

        #clean data and arrange for matrixification
        pchem_setlist_fluxable[[i]] <- pchem %>%
            select(datetime, site_name, !!v, ms_status) %>%
            filter(site_name %in% rg$site_name) %>%
            # mutate(datetime = lubridate::year(datetime)) %>%
            mutate(datetime = lubridate::as_date(datetime)) %>% #finer? coarser?
            group_by(site_name, datetime) %>%
            summarize(
                !!v := mean(!!sym(v), na.rm=TRUE),
                ms_status = numeric_any(ms_status)) %>%
            ungroup() %>%
            tidyr::pivot_wider(names_from = site_name,
                               values_from = !!sym(v)) %>%
            mutate(ms_status = as.logical(ms_status)) %>%
            group_by(datetime) %>%
            summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
            ungroup() %>%
            mutate(ms_status = as.numeric(ms_status)) %>%
            arrange(datetime)
    }

    #send vars into flux interpolator with precip, one at a time;
    #combine and write outputs by site
    for(i in 1:nrow(wb)){

        wbj <- slice(wb, i)
        site_name <- wbj$site_name

        ws_mean_conc <- ws_mean_flux <- tibble()
        for(j in 1:nvars_fluxable){

            v <- pchem_vars_fluxable[j]

            ws_means <- ue(shortcut_idw_concflux(encompassing_dem = dem,
                                                 wshd_bnd = wbj,
                                                 data_locations = rg,
                                                 precip_values = precip,
                                                 chem_values = pchem_setlist_fluxable[[j]],
                                                 stream_site_name = site_name))

            if(j == 1){
                datetime_out <- select(ws_means, datetime)
                site_name_out <- select(ws_means, site_name)
                ms_status_out <- ws_means$ms_status
            }

            ms_status_out <- bitwOr(ws_means$ms_status, ms_status_out)

            ws_mean_conc <- ws_means %>%
                select(concentration) %>%
                rename(!!v := concentration) %>%
                bind_cols(ws_mean_conc)

            ws_mean_flux <- ws_means %>%
                select(flux) %>%
                rename(!!v := flux) %>%
                bind_cols(ws_mean_flux)
        }

        #reassemble tibbles
        ws_mean_conc <- bind_cols(datetime_out, site_name_out, ws_mean_conc)
        ws_mean_flux <- bind_cols(datetime_out, site_name_out, ws_mean_flux)
        ws_mean_conc$ms_status <- ws_mean_flux$ms_status <- ms_status_out

        if(any(is.na(ws_mean_conc$datetime))){
            stop('NA datetime found in ws_mean_conc')
        }
        if(any(is.na(ws_mean_flux$datetime))){
            stop('NA datetime found in ws_mean_flux')
        }

        ue(write_ms_file(ws_mean_conc,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_name = site_name,
                         level = 'derived',
                         shapefile = FALSE,
                         link_to_portal = TRUE))

        ue(write_ms_file(ws_mean_flux,
                         network = network,
                         domain = domain,
                         prodname_ms = 'precip_flux_inst__ms002',
                         site_name = site_name,
                         level = 'derived',
                         shapefile = FALSE,
                         link_to_portal = TRUE))
    }

    #clean pchem one variable at a time, matrixify it, insert it into list
    nvars_unfluxable <- length(pchem_vars_unfluxable)
    pchem_setlist_unfluxable <- as.list(rep(NA, nvars_unfluxable))
    for(i in 1:nvars_unfluxable){

        v <- pchem_vars_unfluxable[i]

        #clean data and arrange for matrixification
        pchem_setlist_unfluxable[[i]] <- pchem %>%
            select(datetime, site_name, !!v, ms_status) %>%
            filter(site_name %in% rg$site_name) %>%
            # mutate(datetime = lubridate::year(datetime)) %>%
            mutate(datetime = lubridate::as_date(datetime)) %>% #finer? coarser?
            group_by(site_name, datetime) %>%
            summarize(
                !!v := mean(!!sym(v), na.rm=TRUE),
                ms_status = numeric_any(ms_status)) %>%
            ungroup() %>%
            tidyr::pivot_wider(names_from = site_name,
                               values_from = !!sym(v)) %>%
            mutate(ms_status = as.logical(ms_status)) %>%
            group_by(datetime) %>%
            summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
            ungroup() %>%
            mutate(ms_status = as.numeric(ms_status)) %>%
            arrange(datetime)
    }

    #send vars into regular idw interpolator WITHOUT precip, one at a time;
    #combine and write outputs by site
    for(i in 1:nrow(wb)){

        wbj <- slice(wb, i)
        site_name <- wbj$site_name

        ws_mean_d <- tibble()
        for(j in 1:nvars_unfluxable){

            v <- pchem_vars_unfluxable[j]

            ws_mean <- ue(shortcut_idw(encompassing_dem = dem,
                                       wshd_bnd = wbj,
                                       data_locations = rg,
                                       data_values = pchem_setlist_unfluxable[[j]],
                                       stream_site_name = site_name,
                                       output_varname = v,
                                       elev_agnostic = TRUE))

            if(j == 1){
                datetime_out <- select(ws_mean, datetime)
                site_name_out <- select(ws_mean, site_name)
                ms_status_out <- ws_mean$ms_status
            }

            ms_status_out <- bitwOr(ws_mean$ms_status, ms_status_out)

            ws_mean_d <- ws_mean %>%
                select(!!v) %>%
                bind_cols(ws_mean_d)
        }

        #reassemble tibbles
        ws_mean_d <- bind_cols(datetime_out, site_name_out, ws_mean_d)
        ws_mean_d$ms_status <- ms_status_out

        if(any(is.na(ws_mean_d$datetime))){
            stop('NA datetime found in ws_mean_d')
        }

        ue(write_ms_file(ws_mean_d,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_name = site_name,
                         level = 'derived',
                         shapefile = FALSE,
                         link_to_portal = TRUE))
    }

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

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
                                     site_name = s,
                                     dt_round_interv = 'hours')))

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

