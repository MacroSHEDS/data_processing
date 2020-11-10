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

### attempt to bundle all idw loggers (environment issues) ####
#. handle_errors
idw_log <- function(phase, from_env, ...){

    #phase is one of "wb" (watershed boundary), "var" (variable), or
    #   "ts" (timestep), corresponding to the phase of idw processing that
    #   should be logged.
    #from_env is the environment in which to look for variables passed as
    #   positional arguments. This will usually be the parent environment of
    #   idw_log and can be specified in the call as from_env = environment()
    #... must contain any variables needed inside the subsection of
    #   idw_log specified by phase.

    if(! verbose) return()

    #populate positional arguments in local environment
    dots = match.call(expand.dots = FALSE)$...
    names = vapply(dots, as.character, '')
    vars = mget(names, inherits = TRUE)
    thisenv = environment()
    mapply(function(n, v) assign(n, v, envir = thisenv),
           names,
           vars)

    #log the specified phase of idw processing
    if(phase == 'wb'){

        msg <- glue('site: {s}; {ii}/{w}',
                    s = site_name,
                    ii = i,
                    w = nrow(wb))

    } else if(phase == 'var'){

        msg <- glue('site: {s}; var: {vv}; {jj}/{nv}',
                    s = site_name,
                    vv = v,
                    jj = j,
                    nv = nvars)

    } else if(phase == 'ts'){

        if(k == 1 || k %% 1000 == 0){
            msg <- glue('timestep: {kk}/{nt}',
                        kk = k,
                        nt = ntimesteps)
        }

    } else {
        stop('phase must be one of "wb", "var", "ts"')
    }

    loginfo(msg,
            logger = logger_module)

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

#all kernels would also have to be modified so that datetime is determined
    #before detection limit is decided. an accurate datetime column
    #is needed to calculate temporally explicit detlims

# gee stuff ####
gee_to_timeseries <- function(gee_id, band, com_name, start, end, ws_bound,
                              scale) {
        
        col_name <- paste0(com_name, 'X')
        
        sheds<- sf::st_transform(sheds,4326) %>%
            sf::st_set_crs(4326)
        
        ws_ee <- sf_as_ee(sheds)
        
        gee_imcol <- ee$ImageCollection(gee_id)$
            #filterBounds(ws_ee)$
            filterDate(start, end)$
            select(band)$
            map(function(x){
                date <- ee$Date(x$get("system:time_start"))$format('YYYY_MM_dd')
                #name <- ee$String$cat(col_name, date)
                #x$select(band)$reproject("EPSG:4326")$set("RGEE_NAME", name)
                x$set("RGEE_NAME", date)
            })
        
        ee_ws_table <- ee_extract(
            x = gee_imcol,
            y = sheds,
            scale = scale,
            fun = ee$Reducer$median(),
            sf = FALSE
        )
        
        table_nrow <- sheds %>%
            mutate(nrow = row_number()) %>%
            as.data.frame() %>%
            #need common col name for watershed
            dplyr::select(site_name, nrow)
        
        table <- ee_ws_table %>%
            mutate(nrow = row_number()) %>%
            full_join(table_nrow) %>%
            dplyr::select(-nrow)
        
        col_names <- colnames(table)
        
        leng <- length(col_names) -1
        
        table_time <- table %>%
            pivot_longer(col_names[1:leng])
        
        for(i in 1:nrow(table_time)) {
            table_time[i,'date'] <- stringr::str_split_fixed(table_time[i,2], pattern = 'X', n = 2)[2]
        }
        
        table_fin <- table_time %>%
            dplyr::select(-name) %>%
            mutate(date = ymd(date)) %>%
            rename(!!com_name := value)
        
        return(table_fin)
    }

#### sourceflags_to_ms_status (pre-longform-cast) ####
#. handle_errors
sourceflags_to_ms_status <- function(d, flagstatus_mappings,
                                     exclude_mapvals = rep(FALSE, length(flagstatus_mappings))){

    #d is a df/tibble with flag and/or status columns
    #flagstatus_mappings is a list of flag or status column names mapped to
    #vectors of values that might be encountered in those columns.
    #see exclude_mapvals.
    #exclude_mapvals: a boolean vector of length equal to the length of
    #flagstatus_mappings. for each FALSE, values in the corresponding vector
    #are treated as OK values (mapped to ms_status 0). values
    #not in the vector are treated as flagged (mapped to ms_status 1).
    #For each TRUE, this relationship is inverted, i.e. values *in* the
    #corresponding vector are treated as flagged.

    flagcolnames = names(flagstatus_mappings)
    d = mutate(d, ms_status = 0)

    for(i in 1:length(flagstatus_mappings)){
        # d = filter(d, !! sym(flagcolnames[i]) %in% flagcols[[i]])
        # d = mutate(d,
        #     ms_status = ifelse(flagcolnames[i] %in% flagcols[[i]], 0, 1))

        if(exclude_mapvals[i]){
            ok_bool = ! d[[flagcolnames[i]]] %in% flagstatus_mappings[[i]]
        } else {
            ok_bool = d[[flagcolnames[i]]] %in% flagstatus_mappings[[i]]
        }

        d$ms_status[! ok_bool] = 1
    }

    d = select(d, -one_of(flagcolnames))

    return(d)
}


#working on automatic determination of data column regimen now, but here's some starter
#code (and documentation) for doing it manually if we ever want to go that route####
    #data_col_regimen: either a character vector of the same length as data_cols,
    #   or a character vector of length one, which will be applied to all data columns.
    #   Elements of this vector must be either "grab" for periodically sampled
    #   data or "sensor" for data collected on an automated, regular schedule
    #   by a mechanical/electrical device. This information will be stored in
    #   data/<network>/<domain>/sample_regimens.json as a nested list:
    #   prodname_ms
    #       variable
    #           startdt: datetime1, datetime2, datetimeN...
    #           regimen: regimen1,  regimen2,  regimenN...
    #   TODO: handle the case of multiple regimens for the same variable in the same file
    #   TODO: what about different regimens for different sites?


    if(! length(data_col_regimen) %in% c(1, length(data_cols))){
        stop('data_col_regimen must have length 1 or length(data_cols)')
    }

    #save sample collection regimens to file
    if(length(data_col_regimen) == 1){
        data_col_regimen <- rep(data_col_regimen, length(data_cols))
    }

    names(data_col_regimen) <- unname(data_cols)
    remaining_data_cols <- na.omit(str_match(string = colnames(d),
                                             pattern = '^(.*?)__\\|dat$')[, 2])
    data_col_regimen <- data_col_regimen[names(data_col_regimen) %in%
                                             remaining_data_cols]

    thisenv <- environment()
    cnt <- 0
    first_nonNA_inds <- d %>%
        arrange(datetime) %>%
        select(ends_with('__|dat')) %>%
        dplyr::rename_with(~ sub('__\\|dat', '', .x)) %>%
        purrr::map(function(z, reg = data_col_regimen){
            ind <- Position(function(w) ! is.na(w), z)
            assign('cnt', cnt + 1, envir = thisenv)
            list(startdt = as.character(d$datetime[ind]),
                 regimen = unname(reg[cnt]))
        })

    write_sample_regimens(regimens,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms)

# we used to link some munged and some derived products to the data portal.
#   now we only link derived products, so this function is deprecated
    create_portal_link <- function(network, domain, prodname_ms, site_name,
                               level = 'derived', dir = FALSE){

    #level is either 'munged' or 'derived', corresponding to the
    #   location, within the data_acquisition system, of the data to be linked.
    #   DEPRECATED. only derived files should be linked to the portal.
    #if dir=TRUE, treat site_name as a directory name, and link all files
    #   within (necessary for e.g. shapefiles, which often come with other files)

    if(! level %in% c('munged', 'derived')){
        stop('level must be "munged" or "derived"')
    }

    portal_prod_dir = glue('../portal/data/{d}/{p}', #portal ignores network
                           d = domain,
                           p = strsplit(prodname_ms, '__')[[1]][1])

    dir.create(portal_prod_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    if(! dir){

        portal_site_file = glue('{pd}/{s}.feather',
                                pd = portal_prod_dir,
                                s = site_name)

        #if there's already a data file for this site-time-product in
        #the portal repo, remove it
        unlink(portal_site_file)

        #create a link to the portal repo from the new site file
        #(note: really, to and from are equivalent, as they both
        #point to the same underlying structure in the filesystem)
        site_file = glue('data/{n}/{d}/{l}/{p}/{s}.feather',
                         n = network,
                         d = domain,
                         l = level,
                         p = prodname_ms,
                         s = site_name)

        invisible(sw(file.link(to = portal_site_file,
                               from = site_file)))

    } else {

        site_dir <- glue('data/{n}/{d}/{l}/{p}/{s}',
                         n = network,
                         d = domain,
                         l = level,
                         p = prodname_ms,
                         s = site_name)

        portal_prod_dir <- glue('../portal/data/{d}/{p}',
                                d = domain,
                                p = strsplit(prodname_ms, '__')[[1]][1])

        dir.create(portal_prod_dir,
                   showWarnings = FALSE,
                   recursive = TRUE)

        site_files <- list.files(site_dir)
        for(s in site_files){

            site_file <- glue(site_dir, '/', s)
            portal_site_file <- glue(portal_prod_dir, '/', s)
            unlink(portal_site_file)
            invisible(sw(file.link(to = portal_site_file,
                                   from = site_file)))
        }

    }

    return()
}

#this is now handled by munge_by_site()
#. handle_errors
munge_by_site_product <- function(network, domain, site_name, prodname_ms, tracker,
                          spatial_regex = '(location|boundary)',
                          silent = TRUE){

    #for when a data product is organized with only one site and one variable
    #in a data product (e.g. Konza has a discharge data product for each site)

    #

    #spatial_regex is a regex string that matches one or more prodname_ms
    #    values. If the prodname_ms being munged matches this string,
    #    write_ms_file will assume it's writing a spatial object, and not a
    #    standalone file

    retrieval_log <- extract_retrieval_log(tracker,
                                           prodname_ms,
                                           site_name)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out <- tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode <- prodcode_from_prodname_ms(prodname_ms)

        processing_func <- get(paste0('process_1_', prodcode))
        in_comp <- retrieval_log[k, 'component', drop=TRUE]

        out_comp <- sw(do.call(processing_func,
                               args = list(network = network,
                                           domain = domain,
                                           prodname_ms = prodname_ms,
                                           site_name = site_name,
                                           component = in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }
    }

    site <- unique(out$site_name)

    if(length(site) > 1) {
        stop('multiple sites encountered in a dataset that should contain only one')
    }

    is_spatial <- ifelse(grepl(spatial_regex,
                               prodname_ms),
                         TRUE,
                         FALSE)

    write_ms_file(d = out,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_name = site,
                  level = 'munged',
                  shapefile = is_spatial,
                  link_to_portal = FALSE)

    update_data_tracker_m(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          prodname_ms = prodname_ms,
                          site_name = site_name,
                          new_status = 'ok')

    msg <- glue('munged {p} ({n}/{d}/{s})',
                p = prodname_ms,
                n = network,
                d = domain,
                s = site_name)

    loginfo(msg,
            logger = logger_module)

    return()
}

#now we just load delin specs from acquisition_master.R
read_wb_delin_specs <- function(network, domain, site_name){

    ds <- tryCatch(sm(read_csv('data/general/watershed_delineation_specs.csv')),
                   error = function(e){
                       empty_tibble <- tibble(network = 'a',
                                              domain = 'a',
                                              site_name = 'a',
                                              buffer_radius_m = 1,
                                              snap_method = 'a',
                                              snap_distance_m = 1,
                                              dem_resolution = 1)

                       return(empty_tibble[-1, ])
                   })

    ds <- filter(ds,
                 network == !!network,
                 domain == !!domain,
                 site_name == !!site_name)

    return(ds)
}

calc_inst_flux_wrap <- function(chemprod, qprod, prodname_ms) {

    warning("derive_stream_flux is fully automated. let's use it!")

    level_chem <- ifelse(is_derived_product(chemprod),
                    'derived',
                    'munged')

    level_q <- ifelse(is_derived_product(qprod),
                         'derived',
                         'munged')

    chemfiles <- ms_list_files(network = network,
                               domain = domain,
                               level = level_chem,
                               prodname_ms = chemprod)

    qfiles <- ms_list_files(network = network,
                            domain = domain,
                            level = level_q,
                            prodname_ms = qprod)

    flux_sites <- generics::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        flux <- sw(calc_inst_flux(chemprod = chemprod,
                                  qprod = qprod,
                                  site_name = s))

        write_ms_file(d = flux,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = s,
                      level = 'derived',
                      shapefile = FALSE)
    }

    return()
}
