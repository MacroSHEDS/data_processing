#junk functions, possibly useful for parts

# obsolete kernels ####

process_0_DP1.20093.001_api = function(d, set_details){

    data1_ind = intersect(grep("expanded", d$data$files$name),
        grep("fieldSuperParent", d$data$files$name))
    data2_ind = intersect(grep("expanded", d$data$files$name),
        grep("externalLabData", d$data$files$name))
    data3_ind = intersect(grep("expanded", d$data$files$name),
        grep("domainLabData", d$data$files$name))

    data1 = tryCatch({
        read.delim(d$data$files$url[data1_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data2 = tryCatch({
        read.delim(d$data$files$url[data2_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data3 = tryCatch({
        read.delim(d$data$files$url[data3_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })

    if(nrow(data1)){
        data1 = select(data1, siteID, collectDate, dissolvedOxygen,
            dissolvedOxygenSaturation, specificConductance, waterTemp,
            maxDepth)
    }
    if(nrow(data2)){
        data2 = select(data2, siteID, collectDate, pH, externalConductance,
            externalANC, starts_with('water'), starts_with('total'),
            starts_with('dissolved'), uvAbsorbance250, uvAbsorbance284,
            shipmentWarmQF, externalLabDataQF)
    }
    if(nrow(data3)){
        data3 = select(data3, siteID, collectDate, starts_with('alk'),
            starts_with('anc'))
    }

    out_sub = plyr::join_all(list(data1, data2, data3), type='full') %>%
        group_by(collectDate) %>%
        summarise_each(list(~ if(is.numeric(.)){
            mean(., na.rm = TRUE)
        } else {
            first(.)
        })) %>%
        ungroup() %>%
        mutate(datetime=as.POSIXct(collectDate, tz='UTC',
            format='%Y-%m-%dT%H:%MZ')) %>%
        select(-collectDate)

    return(out_sub)
} #chem: obsolete

process_0_DP1.20288.001_api = function(d, set_details){

    data_inds = intersect(grep("expanded", d$data$files$name),
        grep("instantaneous", d$data$files$name))

    site_with_suffixes = determine_upstream_downstream(d, data_inds,
        set_details)
    if(is_ms_err(site_with_suffixes)) return(site_with_suffixes)

    #process upstream and downstream sites independently
    for(j in 1:length(data_inds)){

        site_with_suffix = site_with_suffixes[j]

        #download data
        out_sub = read.delim(d$data$files$url[data_inds[j]], sep=",",
            stringsAsFactors=FALSE)

        out_sub = resolve_neon_naming_conflicts(out_sub,
            set_details_=set_details,
            replacements=c('specificCond'='specificConductance',
                'dissolvedOxygenSat'='dissolvedOxygenSaturation'))
        if(is_ms_err(out_sub)) return(out_sub)

        out_sub = mutate(out_sub,
            datetime=as.POSIXct(startDateTime,
                tz='UTC', format='%Y-%m-%dT%H:%M:%SZ'),
            site=site_with_suffix) %>%
            select(-startDateTime)
    }

    return(out_sub)
} #waterqual: obsolete

resolve_neon_naming_conflicts_OBSOLETE = function(out_sub_, replacements=NULL,
    from_api=FALSE, set_details_){

    #obsolete now that neonUtilities package is working

    #replacements is a named vector. name=find, value=replace;
    #failed match does nothing

    prodcode = set_details_$prodcode
    site = set_details_$site
    date = set_details_$date

    out_cols = colnames(out_sub_)

    if(! is.null(replacements)){

        #get list of variables included
        varind = grep('SciRvw', out_cols)
        rgx = str_match(out_cols[varind], '^(\\w*)(?:FinalQFSciRvw|SciRvwQF)$')
        # varlist = flagprefixlist = rgx[,2]
        varlist = rgx[,2]

        #harmonize redundant variable names
        for(i in 1:length(replacements)){
            r = replacements[i]
            varlist = replace(varlist, which(varlist == names(r)), r)
        }
    }

    if('startDate' %in% out_cols){
        colnames(out_sub_) = replace(out_cols, which(out_cols == 'startDate'),
            'startDateTime')
    } else if('endDate' %in% out_cols){
        colnames(out_sub_) = replace(out_cols, which(out_cols == 'endDate'),
            'startDateTime') #not a mistake
    } else if(! 'startDateTime' %in% out_cols){
        msg = glue('Datetime column not found for site ',
            '{site} ({prodname_ms}, {date}).', site=site, prodname_ms=prodcode, date=date)
        logwarn(msg, logger=logger_module)
        return(generate_ms_err())
    }

    #subset relevant columns (needed if NEON API was used)
    if(from_api){
        flagcols = grepl('.+?FinalQF(?!SciRvw)', out_cols,
            perl=TRUE)
        datacols = ! grepl('QF', out_cols, perl=TRUE)
        relevant_cols = flagcols | datacols
        out_sub_ = out_sub_[, relevant_cols]
    }

    return(out_sub_)
}

determine_upstream_downstream_api_OBSOLETE = function(d_, data_inds_, set_details_){
    #obsolete now that neonUtilities package is working

    prodcode = set_details_$prodcode
    site = set_details_$site
    date = set_details_$date

    #determine which dataset is upstream/downstream if necessary
    updown_suffixes = c('-up', '-down')
    if(length(data_inds_) == 2){
        position = str_split(d_$data$files$name[data_inds_[1]], '\\.')[[1]][7]
        updown_order = if(position == '101') 1:2 else 2:1
    } else if(length(data_inds_) == 1){
        updown_order = 1
    } else {
        msg = glue('Problem with upstream/downstream indicator for site ',
            '{site} ({prodname_ms}, {date}).', site=site, prodname_ms=prodcode, date=date)
        logwarn(msg, logger=logger_module)
        return(generate_ms_err())
    }

    site_with_suffixes = paste0(site, updown_suffixes[updown_order])

    return(site_with_suffixes)
}

#precipitation: STATUS=TEST (hbef)
process_2_13TEST <- function(network, domain, prodname_ms){

    #910.945x slower than shortcut method

    #load precipitation data, watershed boundaries, rain gauge locations
    precip2 <- read_combine_feathers(network, domain, prodname_ms)
    wb <- read_combine_shapefiles(network, domain, 'ws_boundary__94')
    rg <- read_combine_shapefiles(network, domain, 'precip_gauge_locations__100')

    #project based on average latlong of watershed boundaries
    bbox <- as.list(sf::st_bbox(wb))
    projstring <- choose_projection(lat = mean(bbox$ymin, bbox$ymax),
                                    long = mean(bbox$xmin, bbox$xmax))
    wb <- sf::st_transform(wb, projstring)
    rg <- sf::st_transform(rg, projstring)

    #get a DEM that encompasses all watersheds
    dem <- sm(elevatr::get_elev_raster(wb, z = 12))

    precip2 <- precip2 %>%
        mutate(datetime = lubridate::year(datetime)) %>% #temporary
        group_by(site_name, datetime) %>%
        summarize(
            precip = mean(precip, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup() %>%
        filter(site_name %in% rg$site_name)

    for(j in 1){
        # for(j in 1:nrow(wb)){

        wbj <- slice(wb, j)
        site_name <- wbj$site_name
        dem_wbj <- terra::crop(dem, wbj)
        dem_wbj <- terra::mask(dem_wbj, wbj)

        #calculate mean watershed precipitation for every timestep
        ws_mean_precip <- rep(NA, nrow(precip))
        timesteps <- unique(precip2$datetime)
        for(k in 1:length(timesteps)){

            precip_k <- precip2 %>%
                filter(datetime == timesteps[k]) %>%
                left_join(rg) %>%
                sf::st_as_sf()

            # zz <- bind_cols(precip_k, as_tibble(sf::st_coordinates(precip_k$geometry)))
            # zz2 = sf::st_read('~/Downloads/hmm/hbef_raingage/hbef_raingage.shp')
            gs <- gstat::gstat(formula = precip~1,
                               locations = precip_k)
            idw <- interpolate(dem, gs)
            idw_mask <- raster::mask(idw, wbj)
            ws_mean_precip[k] <- raster::values(idw_mask) %>%
                mean(., na.rm = TRUE)
        }

        site_precip2 <- tibble(datetime = precip_dt,
                               site_name = site_name,
                               precip = ws_mean_precip,
                               ms_status = precip_status)
    }

    #interp final precip to a desirable interval?
    return()
}

#precipitation: STATUS=OBSOLETE (hbef)
process_2_13OBS <- function(network, domain, prodname_ms){

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

#precip_chemistry: STATUS=OBSOLETE (hbef)
process_2_208OBS <- function(network, domain, prodname_ms){

    #this logic is temporary, and just gets pchem into the format that works
    #with the portal. but once interp is working, we'll perform that here
    #instead

    mfiles <- ue(list_munged_files(network = network,
                                   domain = domain,
                                   prodname_ms = prodname_ms))

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

#precip_flux_inst: STATUS=OBSOLETE (hbef; must localize precip to stream sites first)
process_2_ms002 <- function(network, domain, prodname_ms){

    chemprod <- 'precip_chemistry__208'
    qprod <- 'precipitation__13'

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

### test code####
    desired_interval = '5 days'; impute_limit=30
    # saveRDS(ws_mean_precip, '~/Desktop/ws_precip_temp.rds')
    ws_mean_precip = readRDS('~/Desktop/ws_precip_temp.rds') %>%
        mutate(datetime = as_datetime(as.character(datetime), format='%Y'))
    w2 = w3 = ws_mean_precip
    w2$datetime = as.POSIXct(1:nrow(w2), origin='2000-01-01', tz = 'UTC')
    w3$precip[c(3:9, 15)] = NA
    w4 = w3
    w4$ms_interp = 0

    ms_df = ws_mean_precip
    ms_df = w2
    ms_df = w3
    ms_df = w4

#in case we ever want to track changes in detection limits through time ####
#identify_detection_limit would become identify_detection_limit_s and 
#would have this documentation prepended:

    #this is the scalar version of identify_detection_limit (_s).
    #it was the first iteration, and has been superseded by the temporally-
    #explicit version (identify_detection_limit_t). that version relies on stored data, so automatically
    #writes to data/<network>/<domain>/detection_limits.json. This version
    #just returns its output.

#apply_detection_limit_s would have this documentation prepended

    #this is the scalar version of apply_detection_limit (_s).
    #it was the first iteration, and has been superseded by the temporally-
    #explicit version (apply_detection_limit_t).
    #that version relies on stored data, so automatically
    #reads from data/<network>/<domain>/detection_limits.json. This version
    #just accepts detection limits as an argument.

#all kernels would also have to be modified so that datetime is determined
    #before detection limit is decided. an accurate datetime column
    #is needed to calculate temporally explicit detlims

#. handle_errors
identify_detection_limit_t <- function(x, network, domain, prodname_ms){
    
    #this is the temporally explicit version of identify_detection_limit (_t).
    #it supersedes the scalar version (identify_detection_limit_s).
    #that version just returns its output. This version relies on stored data,
    #so automatically writes to data/<network>/<domain>/detection_limits.json.
    
    #x is a 2d array-like object with column names. must have a datetime column
    
    #the rolling mode detection limit (number of decimal places)
    #of each column is written to data/<network>/<domain>/detection_limits.json
    #as a nested list:
    #prodname_ms
    #    variable
    #        startdt1: limit1
    #        startdt2: limit2 ...
    #non-numeric columns are not considered variables and are ignored, with the
    #   exception of datetime, which is used to compute monthly mode detection
    #   limits.
    
    #detection limit for each site-variable is computed as the mode
    #of the number of characters following each decimal place for each month.
    #Each time the mode changes, a new startdt and limit are recorded.
    #NAs and zeros are ignored when computing detection limit. For months when
    #no data are recorded, the detection limit of the previous month is carried
    #forward. 
    
    identify_detection_limit_v <- function(x, dt){
        
        #x is a vector, or it will be coerced to one.
        #non-numeric vectors return NA vectors of the same length
        
        x <- unname(unlist(x))
        if(! is.numeric(x)) return(list(startdt = dt[1],
                                        lim = NA))
        
        #STOPPED BUILDING HERE
        
        options(scipen = 100)
        nas <- is.na(x) | x == 0
        
        x <- as.character(x)
        nsigdigs <- stringr::str_split_fixed(x, '\\.', 2)[, 2] %>%
            nchar()
        
        nsigdigs[nas] <- NA
        
        options(scipen = 0)
        
        return(nsigdigs)
    }
    
    if(! is.null(dim(x))){
        
        detlim <- lapply(X = x,
                         FUN = identify_detection_limit_v,
                         dt = x$datetime)
                         # FUN = function(y){
                         #     identify_detection_limit_v(y) %>%
                         #         Mode(na.rm = TRUE)
                         # })
        
    } else {
        stop('x must be a 2d array-like')
    }
    
    return(detlim)
}

#. handle_errors
read_detection_limit <- function(network, domain, prodname_ms){

    detlims <- glue('data/{n}/{d}/detection_limits.json',
                    n = network,
                    d = domain) %>%
        readr::read_file() %>%
        jsonlite::fromJSON()

    detlims_prod <- detlims[[prodname_ms]]

    return(detlims_prod)
}

#. handle_errors
write_detection_limit <- function(network, domain, prodname_ms){

    NULL
    # tracker = get_data_tracker(network=network, domain=domain)
    #
    # mt = tracker[[prodname_ms]][[site_name]]$munge
    #
    # mt$status = new_status
    # mt$mtime = as.character(Sys.time())
    #
    # tracker[[prodname_ms]][[site_name]]$munge = mt
    #
    # assign(tracker_name, tracker, pos=.GlobalEnv)
    #
    # trackerfile = glue('data/{n}/{d}/data_tracker.json', n=network, d=domain)
    # readr::write_file(jsonlite::toJSON(tracker), trackerfile)
    # backup_tracker(trackerfile)
    #
    #
    # detlims <- glue('data/{n}/{d}/detection_limits.json',
    #                 n = network,
    #                 d = domain) %>%
    #     readr::read_file() %>%
    #     jsonlite::fromJSON()
    #
    # detlims_prod <- detlims[[prodname_ms]]
    #
    # return(detlims_prod)
}
