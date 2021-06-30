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
        group_by(site_code, datetime) %>%
        summarize(
            precip = mean(precip, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup() %>%
        filter(site_code %in% rg$site_code)

    for(j in 1){
        # for(j in 1:nrow(wb)){

        wbj <- slice(wb, j)
        site_code <- wbj$site_code
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
                               site_code = site_code,
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

    flux_sites <- base::intersect(
        ue(fname_from_fpath(qfiles, include_fext = FALSE)),
        ue(fname_from_fpath(chemfiles, include_fext = FALSE)))

    for(s in flux_sites){

        flux <- sw(ue(calc_inst_flux(chemprod = chemprod,
                                     qprod = qprod,
                                     site_code = s,
                                     dt_round_interv = 'hours')))

        ue(write_ms_file(d = flux,
                         network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         site_code = s,
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
                    s = site_code,
                    ii = i,
                    w = nrow(wb))

    } else if(phase == 'var'){

        msg <- glue('site: {s}; var: {vv}; {jj}/{nv}',
                    s = site_code,
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
            dplyr::select(site_code, nrow)

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
    create_portal_link <- function(network, domain, prodname_ms, site_code,
                               level = 'derived', dir = FALSE){

    #level is either 'munged' or 'derived', corresponding to the
    #   location, within the data_acquisition system, of the data to be linked.
    #   DEPRECATED. only derived files should be linked to the portal.
    #if dir=TRUE, treat site_code as a directory name, and link all files
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
                                s = site_code)

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
                         s = site_code)

        invisible(sw(file.link(to = portal_site_file,
                               from = site_file)))

    } else {

        site_dir <- glue('data/{n}/{d}/{l}/{p}/{s}',
                         n = network,
                         d = domain,
                         l = level,
                         p = prodname_ms,
                         s = site_code)

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
munge_by_site_product <- function(network, domain, site_code, prodname_ms, tracker,
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
                                           site_code)

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
                                           site_code = site_code,
                                           component = in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }
    }

    site <- unique(out$site_code)

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
                  site_code = site,
                  level = 'munged',
                  shapefile = is_spatial,
                  link_to_portal = FALSE)

    update_data_tracker_m(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          prodname_ms = prodname_ms,
                          site_code = site_code,
                          new_status = 'ok')

    msg <- glue('munged {p} ({n}/{d}/{s})',
                p = prodname_ms,
                n = network,
                d = domain,
                s = site_code)

    loginfo(msg,
            logger = logger_module)

    return()
}

#now we just load delin specs from acquisition_master.R
read_wb_delin_specs <- function(network, domain, site_code){

    ds <- tryCatch(sm(read_csv('data/general/watershed_delineation_specs.csv')),
                   error = function(e){
                       empty_tibble <- tibble(network = 'a',
                                              domain = 'a',
                                              site_code = 'a',
                                              buffer_radius_m = 1,
                                              snap_method = 'a',
                                              snap_distance_m = 1,
                                              dem_resolution = 1)

                       return(empty_tibble[-1, ])
                   })

    ds <- filter(ds,
                 network == !!network,
                 domain == !!domain,
                 site_code == !!site_code)

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

    flux_sites <- base::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        flux <- sw(calc_inst_flux(chemprod = chemprod,
                                  qprod = qprod,
                                  site_code = s))

        write_ms_file(d = flux,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = s,
                      level = 'derived',
                      shapefile = FALSE)
    }

    return()
}

precip_idw <- function(precip_prodname,
                                              wb_prodname,
                                                                     pgauge_prodname,
                                                                     precip_prodname_out,
                                                                                            verbose = TRUE){

        #load watershed boundaries, rain gauge locations, precip data
        wb <- read_combine_shapefiles(network = network,
                                                                        domain = domain,
                                                                                                          prodname_ms = wb_prodname)
    rg <- read_combine_shapefiles(network = network,
                                                                    domain = domain,
                                                                                                      prodname_ms = pgauge_prodname)
        precip <- read_combine_feathers(network = network,
                                                                            domain = domain,
                                                                                                                prodname_ms = precip_prodname) %>%
            filter(site_code %in% rg$site_code)
            # precip = manufacture_uncert_msdf(precip)

            precip_varname <- precip$var[1]

                #project based on average latlong of watershed boundaries
                bbox <- as.list(sf::st_bbox(wb))
                projstring <- choose_projection(lat = mean(bbox$ymin, bbox$ymax),
                                                                                    long = mean(bbox$xmin, bbox$xmax))
                    wb <- sf::st_transform(wb, projstring)
                    rg <- sf::st_transform(rg, projstring)

                        #get a DEM that encompasses all watersheds; add elev column to rain gauges
                        dem <- sm(elevatr::get_elev_raster(wb, z = 12)) #res should adjust with area
                        rg$elevation <- terra::extract(dem, rg)

                            #this avoids a lot of slow summarizing during the next step
                            status_cols <- precip %>%
                                        select(datetime, ms_status, ms_interp) %>%
                                                group_by(datetime) %>%
                                                        summarize(
                                                                              ms_status = numeric_any(ms_status),
                                                                                          ms_interp = numeric_any(ms_interp))

                            #clean precip and arrange for matrixification
                            precip <- precip %>%

                                        #this block is for testing only (makes dataset smaller)
                                        # mutate(datetime = lubridate::year(datetime)) %>% #by year
                                        # # # mutate(datetime = lubridate::as_date(datetime)) %>% #by day
                                        # group_by(site_code, datetime) %>%
                                        # summarize(
                                        #     precip = mean(precip, na.rm=TRUE),
                                        #     ms_status = numeric_any(ms_status),
                                        #     ms_interp = numeric_any(ms_status)) %>%
                                        # ungroup() %>%

                                        select(-ms_status, -ms_interp, -var) %>%
                                                tidyr::pivot_wider(names_from = site_code,
                                                                                              values_from = val) %>%
                                    left_join(status_cols,
                                                                by = 'datetime') %>%
                                            arrange(datetime)

                                                # #kept this here in case it's actually somehow faster? (never benchmarked)
                                                # group_by(datetime) %>%
                                                # summarize_all(max, na.rm = FALSE) %>%
                                                # ungroup() %>%
                                                # arrange(datetime)

                                                # # and this is the clunky way to summarize status cols (left jic)
                                                #mutate(
                                                #    ms_status = as.logical(ms_status),
                                                #    ms_interp = as.logical(ms_interp)) %>%
                                                #group_by(datetime) %>%
                                                #summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
                                                #ungroup() %>%
                                                #mutate(
                                                #    ms_status = as.numeric(ms_status),
                                                #    ms_interp = as.numeric(ms_interp)) %>%
                                                #filter_at(vars(-datetime, -ms_status, -ms_interp),
                                                #    any_vars(! is.na(.))) %>%
                                                #arrange(datetime)

                                            clst <- ms_parallelize()

                                                # .packages = idw_pkg_export,
                                                # .export = idw_var_export,
                                                # .errorhandling = 'remove',
                                                # .verbose = TRUE) %dopar% {

                                                #exports from an attempt to use socket cluster parallelization;
                                                 idw_pkg_export <- c('logging', 'errors', 'jsonlite', 'plyr',
                                                                                              'tidyverse', 'lubridate', 'feather', 'glue',
                                                                                                                       'emayili', 'tinsel', 'imputeTS')
                                                 idw_var_export <- c('logger_module', 'err_cnt', 'idw_log_wb', 'shortcut_idw',
                                                                                              'get_detlim_precursors', 'apply_detection_limit_t',
                                                                                                                       'write_ms_file', 'err_df_to_matrix', 'idw_log_timestep')

                                                      shed_names <- pull(wb, site_code)

                                                      fils <- list.files('data/lter/hjandrews/derived/ws_boundary__ms008', full.names = T)

                                                          #interpolate precipitation volume and write watershed averages
                                                          catchout <- foreach::foreach(i = 1:length(wb),
                                                                                                                        .export = idw_var_export,
                                                                                                                                                         .packages = idw_pkg_export) %dopar% {

                                                                      wbi <- slice(wb, i)

                                                                              # wbi <- sf::st_read(fils[i])
                                                                              site_code <- wbi$site_code

                                                                              idw_log_wb(verbose = verbose,
                                                                                                            site_code = site_code,
                                                                                                                               i = i,
                                                                                                                               nw = nrow(wb))

                                                                                      ws_mean_precip <- shortcut_idw(encompassing_dem = dem,
                                                                                                                                                            wshd_bnd = wbi,
                                                                                                                                                                                                   data_locations = rg,
                                                                                                                                                                                                   data_values = precip,
                                                                                                                                                                                                                                          stream_site_code = site_code,
                                                                                                                                                                                                                                          output_varname = precip_varname,
                                                                                                                                                                                                                                                                                 elev_agnostic = FALSE,
                                                                                                                                                                                                                                                                                 verbose = verbose)

                                                                                      # ws_mean_precip$precip <- apply_detection_limit_s(ws_mean_precip$precip,
                                                                                      #                                                 detlim)
                                                                                      # identify_detection_limit_s(ws_mean_precip$val)
                                                                                      precursor_prodname <- get_detlim_precursors(network = network,
                                                                                                                                                                                      domain = domain,
                                                                                                                                                                                                                                          prodname_ms = prodname_ms)

                                                                                              ws_mean_precip <- apply_detection_limit_t(ws_mean_precip,
                                                                                                                                                                                          network = network,
                                                                                                                                                                                                                                            domain = domain,
                                                                                                                                                                                                                                            prodname_ms = precursor_prodname)

                                                                                              #interp final precip to a desirable interval?
                                                                                              write_ms_file(ws_mean_precip,
                                                                                                                                  network = network,
                                                                                                                                                        domain = domain,
                                                                                                                                                        prodname_ms = precip_prodname_out,
                                                                                                                                                                              site_code = site_code,
                                                                                                                                                                              level = 'derived',
                                                                                                                                                                                                    shapefile = FALSE,
                                                                                                                                                                                                    link_to_portal = FALSE)
                                                                                                  }

                                                          ms_unparallelize(clst)

                                                              #return()
}

pchem_idw <- function(pchem_prodname,
                                            precip_prodname,
                                                                  wb_prodname,
                                                                  pgauge_prodname,
                                                                                        pchem_prodname_out,
                                                                                        verbose = TRUE){

        #load watershed boundaries, rain gauge locations, precip and pchem data
        wb <- read_combine_shapefiles(network = network,
                                                                        domain = domain,
                                                                                                          prodname_ms = wb_prodname)
    rg <- read_combine_shapefiles(network = network,
                                                                    domain = domain,
                                                                                                      prodname_ms = pgauge_prodname)
        pchem <- read_combine_feathers(network = network,
                                                                          domain = domain,
                                                                                                             prodname_ms = pchem_prodname) %>%
            filter(site_code %in% rg$site_code)
            # pchem = manufacture_uncert_msdf(pchem)

            #project based on average latlong of watershed boundaries
            bbox <- as.list(sf::st_bbox(wb))
                projstring <- choose_projection(lat = mean(bbox$ymin, bbox$ymax),
                                                                                    long = mean(bbox$xmin, bbox$xmax))
                wb <- sf::st_transform(wb, projstring)
                    rg <- sf::st_transform(rg, projstring)

                    #get a DEM that encompasses all watersheds; add elev column to rain gauges
                    dem <- sm(elevatr::get_elev_raster(wb, z = 12)) #res should adjust with area
                        rg$elevation <- terra::extract(dem, rg)

                        #this avoids a lot of slow summarizing
                        status_cols <- pchem %>%
                                    select(datetime, ms_status, ms_interp) %>%
                                            group_by(datetime) %>%
                                                    summarize(
                                                                          ms_status = numeric_any(ms_status),
                                                                                      ms_interp = numeric_any(ms_interp))

                            #clean pchem one variable at a time, matrixify it, insert it into list
                            pchem_vars <- unique(pchem$var)
                                nvars <- length(pchem_vars)
                                pchem_setlist <- as.list(rep(NA, nvars))
                                    for(i in 1:nvars){

                                                v <- pchem_vars[i]

                                        #clean data and arrange for matrixification
                                        pchem_setlist[[i]] <- pchem %>%
                                                        filter(var == v) %>%
                                                                    select(-var, -ms_status, -ms_interp) %>%
                                                                                tidyr::pivot_wider(names_from = site_code,
                                                                                                                                  values_from = val) %>%
                                                    left_join(status_cols,
                                                                                    by = 'datetime') %>%
                                                                arrange(datetime)
                                                                }

                                    clst <- ms_parallelize()
                                        # clst <- parallel::makeCluster(4, type = 'PSOCK')
                                        # doParallel::registerDoParallel(clst)

                                        #send vars into regular idw interpolator WITHOUT precip, one at a time;
                                        #combine and write outputs by site
                                        # catchout <- foreach::foreach(i = 1:nrow(wb)) %:% {
                                        # pchem_setlist = lapply(pchem_setlist, function(x) x[1:4,])
                                        # pchem_setlist = pchem_setlist[1:8]
                                        # nvars = length(pchem_setlist)
                                        # pchem_setlist = lapply(pchem_setlist, manufacture_uncert_msdf)
                                        # verbose = TRUE
                                        for(i in 1:nrow(wb)){

                                                    wbi <- slice(wb, i)
                                            site_code <- wbi$site_code

                                                    idw_log_wb(verbose = verbose,
                                                                                  site_code = site_code,
                                                                                                     i = i,
                                                                                                     nw = nrow(wb))

                                                    # for(j in 1:nvars){q
                                                    ws_mean_d <- foreach::foreach(j = 1:nvars,
                                                                                                                        .combine = idw_parallel_combine,
                                                                                                                                                              .init = 'first iter') %dopar% {
                                                                                              # .packages = idw_pkg_export,
                                                                                              # .export = idw_var_export,
                                                                                              # .errorhandling = 'remove',
                                                                                              # .verbose = TRUE) %dopar% {

                                                                    v <- pchem_vars[j]

                                                                                idw_log_var(verbose = verbose,
                                                                                                                    site_code = site_code,
                                                                                                                                            v = v,
                                                                                                                                            j = j,
                                                                                                                                                                    nvars = nvars)

                                                                                # ws_mean <- shortcut_idw(encompassing_dem = dem, (handled by foreach now)
                                                                                shortcut_idw(encompassing_dem = dem,
                                                                                                                      wshd_bnd = wbi,
                                                                                                                                               data_locations = rg,
                                                                                                                                               data_values = pchem_setlist[[j]],
                                                                                                                                                                        stream_site_code = site_code,
                                                                                                                                                                        output_varname = v,
                                                                                                                                                                                                 elev_agnostic = TRUE,
                                                                                                                                                                                                 verbose = verbose)
                                                                                         }

                                                            #     if(j == 1){
                                                            #         datetime_out <- select(ws_mean, datetime)
                                                            #         site_code_out <- select(ws_mean, site_code)
                                                            #         ms_status_out <- ws_mean$ms_status
                                                            #         ms_interp_out <- ws_mean$ms_interp
                                                            #
                                                            #         ws_mean_d <- ws_mean %>%
                                                            #             select(!!v)
                                                            #     } else {
                                                            #         ws_mean_d <- ws_mean %>%
                                                            #             select(!!v) %>%
                                                            #             bind_cols(ws_mean_d)
                                                            #     }
                                                            #
                                                            #     ms_status_out <- bitwOr(ws_mean$ms_status, ms_status_out)
                                                            #     ms_interp_out <- bitwOr(ws_mean$ms_interp, ms_interp_out)
                                                            # }

                                                            # #reassemble tibbles
                                                            # ws_mean_d <- bind_cols(datetime_out, site_code_out, ws_mean_d)
                                                            # ws_mean_d$ms_status <- ms_status_out
                                                            # ws_mean_d$ms_interp <- ms_interp_out

                                                            if(any(is.na(ws_mean_d$datetime))){
                                                                            stop('NA datetime found in ws_mean_d')
                                                                    }

                                                                    ws_mean_d <- arrange(ws_mean_d, var, datetime)

                                                                    precursor_prodname <- get_detlim_precursors(network = network,
                                                                                                                                                                    domain = domain,
                                                                                                                                                                                                                        prodname_ms = prodname_ms)
                                                                            ws_mean_d <- apply_detection_limit_t(ws_mean_d,
                                                                                                                                                              network = network,
                                                                                                                                                                                                           domain = domain,
                                                                                                                                                                                                           prodname_ms = precursor_prodname)

                                                                            write_ms_file(ws_mean_d,
                                                                                                                network = network,
                                                                                                                                      domain = domain,
                                                                                                                                      prodname_ms = pchem_prodname_out,
                                                                                                                                                            site_code = site_code,
                                                                                                                                                            level = 'derived',
                                                                                                                                                                                  shapefile = FALSE,
                                                                                                                                                                                  link_to_portal = FALSE)
                                                                                }

                                        # parallel::stopCluster(clst)
                                        ms_unparallelize(clst)

                                            #return()
}

flux_idw <- function(pchem_prodname,
                                          precip_prodname,
                                                               wb_prodname,
                                                               pgauge_prodname,
                                                                                    flux_prodname_out,
                                                                                    verbose = TRUE){

        #load watershed boundaries, rain gauge locations, precip and pchem data
        wb <- read_combine_shapefiles(network = network,
                                                                        domain = domain,
                                                                                                          prodname_ms = wb_prodname)
    rg <- read_combine_shapefiles(network = network,
                                                                    domain = domain,
                                                                                                      prodname_ms = pgauge_prodname)
        pchem <- read_combine_feathers(network = network,
                                                                          domain = domain,
                                                                                                             prodname_ms = pchem_prodname) %>%
            filter(site_code %in% rg$site_code)
            # pchem = manufacture_uncert_msdf(pchem)
            precip <- read_combine_feathers(network = network,
                                                                                domain = domain,
                                                                                                                    prodname_ms = precip_prodname) %>%
                    filter(site_code %in% rg$site_code)
                    # precip = manufacture_uncert_msdf(precip)

                    #project based on average latlong of watershed boundaries
                    bbox <- as.list(sf::st_bbox(wb))
                        projstring <- choose_projection(lat = mean(bbox$ymin, bbox$ymax),
                                                                                            long = mean(bbox$xmin, bbox$xmax))
                        wb <- sf::st_transform(wb, projstring)
                            rg <- sf::st_transform(rg, projstring)

                            #get a DEM that encompasses all watersheds; add elev column to rain gauges
                            dem <- sm(elevatr::get_elev_raster(wb, z = 12)) #res should adjust with area
                                rg$elevation <- terra::extract(dem, rg)

                                #this avoids a lot of slow summarizing
                                status_cols <- precip %>%
                                            select(datetime, ms_status, ms_interp) %>%
                                                    group_by(datetime) %>%
                                                            summarize(
                                                                                  ms_status = numeric_any(ms_status),
                                                                                              ms_interp = numeric_any(ms_interp))

                                    #clean precip and arrange for matrixification
                                    precip <- precip %>%
                                                select(-ms_status, -ms_interp, -var) %>%
                                                        tidyr::pivot_wider(names_from = site_code,
                                                                                                      values_from = val) %>%
                                            left_join(status_cols,
                                                                        by = 'datetime') %>%
                                                    arrange(datetime)

                                                    #determine which variables can be flux converted (prefix handling clunky here)
                                                    flux_vars <- ms_vars$variable_code[as.logical(ms_vars$flux_convertible)]
                                                        pchem_vars <- unique(pchem$var)
                                                        pchem_vars_fluxable0 <- base::intersect(drop_var_prefix(pchem_vars),
                                                                                                                                            flux_vars)
                                                            pchem_vars_fluxable <- pchem_vars[drop_var_prefix(pchem_vars) %in%
                                                                                                                                        pchem_vars_fluxable0]

                                                                                                                                        #this avoids a lot of slow summarizing
                                                                                                                                        status_cols <- pchem %>%
                                                                                                                                                    select(datetime, ms_status, ms_interp) %>%
                                                                                                                                                            group_by(datetime) %>%
                                                                                                                                                                    summarize(
                                                                                                                                                                                          ms_status = numeric_any(ms_status),
                                                                                                                                                                                                      ms_interp = numeric_any(ms_interp))

                                                                                                                                            #clean pchem one variable at a time, matrixify it, insert it into list
                                                                                                                                            nvars_fluxable <- length(pchem_vars_fluxable)
                                                                                                                                                pchem_setlist_fluxable <- as.list(rep(NA, nvars_fluxable))
                                                                                                                                                for(i in 1:nvars_fluxable){

                                                                                                                                                            v <- pchem_vars_fluxable[i]

                                                                                                                                                        #clean data and arrange for matrixification
                                                                                                                                                        pchem_setlist_fluxable[[i]] <- pchem %>%
                                                                                                                                                                        filter(var == v) %>%
                                                                                                                                                                                    select(-var, -ms_status, -ms_interp) %>%
                                                                                                                                                                                                tidyr::pivot_wider(names_from = site_code,
                                                                                                                                                                                                                                                  values_from = val) %>%
                                                                                                                                                                    left_join(status_cols,
                                                                                                                                                                                                    by = 'datetime') %>%
                                                                                                                                                                                arrange(datetime)
                                                                                                                                                                                }

                                                                                                                                                    clst <- ms_parallelize()

                                                                                                                                                        #send vars into flux interpolator with precip, one at a time;
                                                                                                                                                        #combine and write outputs by site
                                                                                                                                                        # catchout <- foreach::foreach(i = 1:nrow(wb)) %dopar% {

                                                                                                                                                        # #for testing
                                                                                                                                                        # precip = filter(precip, datetime < as.POSIXct('2010-02-01'), datetime > as.POSIXct('2010-01-01'))
                                                                                                                                                        # pchem_setlist_fluxable = lapply(pchem_setlist_fluxable, function(x) filter(x, datetime < as.POSIXct('2010-02-01'), datetime > as.POSIXct('2010-01-01')))
                                                                                                                                                        # drop_these = which(sapply(pchem_setlist_fluxable, function(x) is_empty(x[[1]])))
                                                                                                                                                        # pchem_setlist_fluxable = pchem_setlist_fluxable[-drop_these]
                                                                                                                                                        # pchem_vars_fluxable = pchem_vars_fluxable[-drop_these]

                                                                                                                                                        for(i in 1:nrow(wb)){

                                                                                                                                                                    wbi <- slice(wb, i)
                                                                                                                                                            site_code <- wbi$site_code

                                                                                                                                                                    idw_log_wb(verbose = verbose,
                                                                                                                                                                                                  site_code = site_code,
                                                                                                                                                                                                                     i = i,
                                                                                                                                                                                                                     nw = nrow(wb))

                                                                                                                                                                    # ws_mean_flux <- foreach::foreach(j = 1:4,
                                                                                                                                                                    ws_mean_flux <- foreach::foreach(j = 1:nvars_fluxable,
                                                                                                                                                                                                                                              .combine = idw_parallel_combine,
                                                                                                                                                                                                                                                                                       .init = 'first iter') %dopar% {
                                                                                                                                                                                                                 # .packages = idw_pkg_export,
                                                                                                                                                                                                                 # .export = idw_var_export) %dopar% {
                                                                                                                                                                                # for(j in 1:nvars_fluxable){

                                                                                                                                                                                    v <- pchem_vars_fluxable[j]

                                                                                                                                                                                                idw_log_var(verbose = verbose,
                                                                                                                                                                                                                                    site_code = site_code,
                                                                                                                                                                                                                                                            v = v,
                                                                                                                                                                                                                                                            j = j,
                                                                                                                                                                                                                                                                                    nvars = nvars_fluxable)

                                                                                                                                                                                                shortcut_idw_concflux(encompassing_dem = dem,
                                                                                                                                                                                                                                                        wshd_bnd = wbi,
                                                                                                                                                                                                                                                                                          data_locations = rg,
                                                                                                                                                                                                                                                                                          precip_values = precip,
                                                                                                                                                                                                                                                                                                                            chem_values = pchem_setlist_fluxable[[j]],
                                                                                                                                                                                                                                                                                                                            stream_site_code = site_code,
                                                                                                                                                                                                                                                                                                                                                              output_varname = v,
                                                                                                                                                                                                                                                                                                                                                              verbose = verbose)
                                                                                                                                                                                                        }

                                                                                                                                                                            if(any(is.na(ws_mean_flux$datetime))){
                                                                                                                                                                                            stop('NA datetime found in ws_mean_flux')
                                                                                                                                                                                    }

                                                                                                                                                                                    ws_mean_flux <- ws_mean_flux %>%
                                                                                                                                                                                                    select(-concentration) %>%
                                                                                                                                                                                                                rename(val = flux) %>%
                                                                                                                                                                                                                            arrange(var, datetime)

                                                                                                                                                                                                                                # ue(write_ms_file(ws_mean_conc,
                                                                                                                                                                                                                                #                  network = network,
                                                                                                                                                                                                                                #                  domain = domain,
                                                                                                                                                                                                                                #                  prodname_ms = prodname_ms,
                                                                                                                                                                                                                                #                  site_code = site_code,
                                                                                                                                                                                                                                #                  level = 'derived',
                                                                                                                                                                                                                                #                  shapefile = FALSE,
                                                                                                                                                                                                                                #                  link_to_portal = FALSE))

                                                                                                                                                                                                                                precursor_prodname <- get_detlim_precursors(network = network,
                                                                                                                                                                                                                                                                                                                                domain = domain,
                                                                                                                                                                                                                                                                                                                                                                                    prodname_ms = prodname_ms)
                                                                                                                                                                                            ws_mean_flux <- apply_detection_limit_t(ws_mean_flux,
                                                                                                                                                                                                                                                                                    network = network,
                                                                                                                                                                                                                                                                                                                                    domain = domain,
                                                                                                                                                                                                                                                                                                                                    prodname_ms = precursor_prodname)

                                                                                                                                                                                                    write_ms_file(ws_mean_flux,
                                                                                                                                                                                                                                        network = network,
                                                                                                                                                                                                                                                              domain = domain,
                                                                                                                                                                                                                                                              prodname_ms = flux_prodname_out,
                                                                                                                                                                                                                                                                                    site_code = site_code,
                                                                                                                                                                                                                                                                                    level = 'derived',
                                                                                                                                                                                                                                                                                                          shapefile = FALSE,
                                                                                                                                                                                                                                                                                                          link_to_portal = FALSE)
                                                                                                                                                                                                }

                                                                                                                                                        # parallel::stopCluster(clst)
                                                                                                                                                        ms_unparallelize(clst)

                                                                                                                                                            #return()
}

shortcut_idw_concflux <- function(encompassing_dem, wshd_bnd, data_locations,
                                                                    precip_values, chem_values, stream_site_code,
                                                                                                      output_varname, verbose = FALSE){

        #superseded by shortcut_idw_concflux_v2

        #this function is similar to shortcut_idw, but when it gets to the
        #vectorized raster stage, it multiplies precip chem by precip volume
        #to calculate flux for each cell. then it returns a list containing two
        #derived values: watershed average concentration and ws ave flux.

        #encompassing_dem must cover the area of wshd_bnd and precip_gauges
        #wshd_bnd is an sf object with columns site_code and geometry
        #it represents a single watershed boundary
        #data_locations is an sf object with columns site_code and geometry
        #it represents all sites (e.g. rain gauges) that will be used in
        #the interpolation
        #precip_values is a data.frame with one column each for datetime and ms_status,
        #and an additional named column of data values for each precip location.
        #chem_values is a data.frame with one column each for datetime and ms_status,
        #and an additional named column of data values for each
        #precip chemistry location.

        # loginfo(glue('shortcut_idw_concflux: working on {ss}', ss=stream_site_code),
        #     logger = logger_module)

        common_dts <- base::intersect(as.character(precip_values$datetime),
                                                                        as.character(chem_values$datetime))
    precip_values <- filter(precip_values,
                                                        as.character(datetime) %in% common_dts)
        chem_values <- filter(chem_values,
                                                        as.character(datetime) %in% common_dts)

        #matrixify input data so we can use matrix operations
        d_dt <- precip_values$datetime

            p_status <- precip_values$ms_status
            p_interp <- precip_values$ms_interp
                p_matrix <- select(precip_values,
                                                          -ms_status,
                                                                                 -datetime,
                                                                                 -ms_interp) %>%
                    err_df_to_matrix()

                    c_status <- chem_values$ms_status
                        c_interp <- chem_values$ms_interp
                        c_matrix <- select(chem_values,
                                                                  -ms_status,
                                                                                         -datetime,
                                                                                         -ms_interp) %>%
                                err_df_to_matrix()

                                d_status = bitwOr(p_status, c_status)
                                    d_interp = bitwOr(p_interp, c_interp)

                                    # gauges <- base::union(colnames(p_matrix),
                                    #                       colnames(c_matrix))
                                    # ngauges <- length(gauges)

                                    #clean dem and get elevation values
                                    dem_wb <- terra::crop(encompassing_dem, wshd_bnd)
                                        dem_wb <- terra::mask(dem_wb, wshd_bnd)
                                        elevs <- terra::values(dem_wb)

                                            #compute distances from all dem cells to all precip locations
                                            inv_distmat_p <- matrix(NA, nrow = length(dem_wb), ncol = ncol(p_matrix),
                                                                                                dimnames = list(NULL, colnames(p_matrix)))
                                            for(k in 1:ncol(p_matrix)){
                                                        dk <- filter(data_locations, site_code == colnames(p_matrix)[k])
                                                    inv_dist2 <- 1 / raster::distanceFromPoints(dem_wb, dk)^2 %>%
                                                                    terra::values(.)
                                                                        inv_dist2[is.na(elevs)] <- NA #mask
                                                            inv_distmat_p[, k] <- inv_dist2
                                                                }

                                                #compute distances from all dem cells to all chemistry locations
                                                inv_distmat_c <- matrix(NA, nrow = length(dem_wb), ncol = ncol(c_matrix),
                                                                                                    dimnames = list(NULL, colnames(c_matrix)))
                                                    for(k in 1:ncol(c_matrix)){
                                                                dk <- filter(data_locations, site_code == colnames(c_matrix)[k])
                                                            inv_dist2 <- 1 / raster::distanceFromPoints(dem_wb, dk)^2 %>%
                                                                            terra::values(.)
                                                                                inv_dist2[is.na(elevs)] <- NA
                                                                    inv_distmat_c[, k] <- inv_dist2
                                                                        }

                                                    #calculate watershed mean concentration and flux at every timestep
                                                    ptm <- proc.time()
                                                        if(nrow(p_matrix) != nrow(c_matrix)) stop('P and C timesteps not equal')
                                                        ntimesteps <- nrow(p_matrix)
                                                            ws_mean_conc <- ws_mean_flux <- rep(NA, ntimesteps)

                                                            if(TRUE){ #TODO: modify this to allow user to select GPU or not
                                                                        #ths block is non-gpu
                                                                        for(k in 1:ntimesteps){

                                                                                    idw_log_timestep(verbose = verbose,
                                                                                                                              site_code = stream_site_code,
                                                                                                                                                       v = output_varname,
                                                                                                                                                       k = k,
                                                                                                                                                                                ntimesteps = ntimesteps,
                                                                                                                                                                                time_elapsed = (proc.time() - ptm)[3] / 60)

                                                                    #assign cell weights as normalized inverse squared distances (p)
                                                                    pk <- t(p_matrix[k, , drop = FALSE])
                                                                            inv_distmat_p_sub <- inv_distmat_p[, ! is.na(pk), drop=FALSE]
                                                                            pk <- pk[! is.na(pk), , drop=FALSE]
                                                                                    weightmat_p <- do.call(rbind, #avoids matrix transposition
                                                                                                                                          unlist(apply(inv_distmat_p_sub, #normalize by row
                                                                                                                                                                                                   1,
                                                                                                                                                                                                                                               function(x) list(x / sum(x))),
                                                                                                                                                                                       recursive = FALSE))

                                                                                    #assign cell weights as normalized inverse squared distances (c)
                                                                                    ck <- t(c_matrix[k, , drop = FALSE])
                                                                                            inv_distmat_c_sub <- inv_distmat_c[, ! is.na(ck), drop=FALSE]
                                                                                            ck <- ck[! is.na(ck), , drop=FALSE]
                                                                                                    weightmat_c <- do.call(rbind,
                                                                                                                                                          unlist(apply(inv_distmat_c_sub,
                                                                                                                                                                                                                   1,
                                                                                                                                                                                                                                                               function(x) list(x / sum(x))),
                                                                                                                                                                                                       recursive = FALSE))

                                                                                                    #determine data-elevation relationship for interp weighting (p only)
                                                                                                    d_elev <- tibble(site_code = rownames(pk),
                                                                                                                                              precip = pk[,1]) %>%
                                                                                                                left_join(data_locations,
                                                                                                                                                by = 'site_code')
                                                                                                                        mod <- lm(precip ~ elevation, data = d_elev)
                                                                                                                        ab <- as.list(mod$coefficients)

                                                                                                                                #perform vectorized idw (p)
                                                                                                                                pk[is.na(pk)] <- 0 #allows matrix multiplication
                                                                                                                                p_idw <- weightmat_p %*% pk

                                                                                                                                        #perform vectorized idw (c)
                                                                                                                                        ck[is.na(ck)] <- 0
                                                                                                                                        c_idw <- weightmat_c %*% ck

                                                                                                                                                #reapply uncertainty dropped by `%*%`
                                                                                                                                                errors(p_idw) <- weightmat_p %*% matrix(errors(pk),
                                                                                                                                                                                                                                        nrow = nrow(pk))
                                                                                                                                                errors(c_idw) <- weightmat_c %*% matrix(errors(ck),
                                                                                                                                                                                                                                        nrow = nrow(ck))

                                                                                                                                                        #estimate raster values from elevation alone (p only)
                                                                                                                                                        p_from_elev <- ab$elevation * elevs + ab$`(Intercept)`

                                                                                                                                                        #average both approaches (p only; this should be weighted toward idw
                                                                                                                                                        #when close to any data location, and weighted half and half when far)
                                                                                                                                                        p_ensemb <- (p_idw + p_from_elev) / 2

                                                                                                                                                                #calculate flux for every cell
                                                                                                                                                                flux_interp <- c_idw * p_ensemb

                                                                                                                                                                #calculate watershed averages (work around error drop)
                                                                                                                                                                ws_mean_conc[k] <- mean(c_idw, na.rm=TRUE)
                                                                                                                                                                        ws_mean_flux[k] <- mean(flux_interp, na.rm=TRUE)
                                                                                                                                                                        errors(ws_mean_conc)[k] <- mean(errors(c_idw), na.rm=TRUE)
                                                                                                                                                                                errors(ws_mean_flux)[k] <- mean(errors(flux_interp), na.rm=TRUE)
                                                                                                                                                                                }
                                                                } else {

                                                                            p_matrix <- errors::drop_errors(p_matrix)
                                                                        c_matrix <- errors::drop_errors(c_matrix)

                                                                                for(k in 1:ntimesteps){
                                                                                            # for(k in 1000:1400){

                                                                                            idw_log_timestep(verbose = verbose,
                                                                                                                                      site_code = stream_site_code,
                                                                                                                                                               v = output_varname,
                                                                                                                                                               k = k,
                                                                                                                                                                                        ntimesteps = ntimesteps,
                                                                                                                                                                                        time_elapsed = (proc.time() - ptm)[3] / 60)

                                                                                #assign cell weights as normalized inverse squared distances (p)
                                                                                pk <- t(p_matrix[k, , drop = FALSE])
                                                                                        inv_distmat_p_sub <- inv_distmat_p[, ! is.na(pk), drop=FALSE]
                                                                                        pk <- pk[! is.na(pk), , drop=FALSE]

                                                                                                #normalize by row, GPUify, transpose
                                                                                                inv_distmat_p_sub_norm <- apply(inv_distmat_p_sub,
                                                                                                                                                                        1,
                                                                                                                                                                                                                function(x) x / sum(x))

                                                                                                if(ncol(inv_distmat_p_sub) == 1){
                                                                                                                inv_distmat_p_sub_norm <- matrix(inv_distmat_p_sub_norm,
                                                                                                                                                                                              ncol = 1)
                                                                                                        }

                                                                                                        # inv_distmat_p_sub_norm <- gpuR::gpuMatrix(inv_distmat_p_sub_norm)
                                                                                                        inv_distmat_p_sub_norm <- gpuR::vclMatrix(inv_distmat_p_sub_norm)

                                                                                                        weightmat_p <- gpuR::t(inv_distmat_p_sub_norm)
                                                                                                                # weightmat_p <- do.call(rbind, #avoids matrix transposition ###
                                                                                                                #                        unlist(apply(inv_distmat_p_sub, #normalize by row
                                                                                                                #                                     1,
                                                                                                                #                                     function(x) list(x / sum(x))),
                                                                                                                #                               recursive = FALSE))

                                                                                                                #assign cell weights as normalized inverse squared distances (c)
                                                                                                                ck <- t(c_matrix[k, , drop = FALSE])
                                                                                                                inv_distmat_c_sub <- inv_distmat_c[, ! is.na(ck), drop=FALSE]
                                                                                                                        ck <- ck[! is.na(ck), , drop=FALSE]

                                                                                                                        #normalize by row, GPUify, transpose
                                                                                                                        inv_distmat_c_sub_norm <- apply(inv_distmat_c_sub,
                                                                                                                                                                                                1,
                                                                                                                                                                                                                                        function(x) x / sum(x))

                                                                                                                                if(ncol(inv_distmat_c_sub) == 1){
                                                                                                                                                inv_distmat_c_sub_norm <- matrix(inv_distmat_c_sub_norm,
                                                                                                                                                                                                                              nrow = 1)
                                                                                                                                        }

                                                                                                                                # inv_distmat_c_sub_norm <- gpuR::gpuMatrix(inv_distmat_c_sub_norm)
                                                                                                                                inv_distmat_c_sub_norm <- gpuR::vclMatrix(inv_distmat_c_sub_norm)

                                                                                                                                        weightmat_c <- gpuR::t(inv_distmat_c_sub_norm)

                                                                                                                                        #determine data-elevation relationship for interp weighting (p only)
                                                                                                                                        d_elev <- tibble(site_code = rownames(pk),
                                                                                                                                                                                  precip = pk[,1]) %>%
                                                                                                                                                    left_join(data_locations,
                                                                                                                                                                                    by = 'site_code')
                                                                                                                                                            mod <- lm(precip ~ elevation, data = d_elev)
                                                                                                                                                            ab <- as.list(mod$coefficients)

                                                                                                                                                                    #perform vectorized idw (p)
                                                                                                                                                                    pk[is.na(pk)] <- 0 #allows matrix multiplication
                                                                                                                                                                    # pk <- gpuR::gpuMatrix(pk) ###
                                                                                                                                                                    pk <- gpuR::vclMatrix(pk) ###
                                                                                                                                                                            p_idw <- weightmat_p %*% pk

                                                                                                                                                                            #perform vectorized idw (c)
                                                                                                                                                                            ck[is.na(ck)] <- 0
                                                                                                                                                                                    # ck <- gpuR::gpuMatrix(ck) ###
                                                                                                                                                                                    ck <- gpuR::vclMatrix(ck) ###
                                                                                                                                                                                    c_idw <- weightmat_c %*% ck

                                                                                                                                                                                            #reapply uncertainty dropped by `%*%`
                                                                                                                                                                                            # errors(p_idw) <- weightmat_p %*% matrix(errors(pk), ###
                                                                                                                                                                                            #                                         nrow = nrow(pk))
                                                                                                                                                                                            # errors(c_idw) <- weightmat_c %*% matrix(errors(ck), ###
                                                                                                                                                                                            #                                         nrow = nrow(ck))

                                                                                                                                                                                            #estimate raster values from elevation alone (p only)
                                                                                                                                                                                            p_from_elev <- ab$elevation * elevs + ab$`(Intercept)`

                                                                                                                                                                                            #average both approaches (p only; this should be weighted toward idw
                                                                                                                                                                                            #when close to any data location, and weighted half and half when far)
                                                                                                                                                                                            # p_from_elev <- gpuR::gpuVector(p_from_elev) ###
                                                                                                                                                                                            p_from_elev <- gpuR::vclVector(p_from_elev) ###
                                                                                                                                                                                                    p_ensemb <- (p_idw + p_from_elev) / 2

                                                                                                                                                                                                    #calculate flux for every cell
                                                                                                                                                                                                    flux_interp <- c_idw * p_ensemb

                                                                                                                                                                                                            c_idw <- as.matrix(c_idw) ###
                                                                                                                                                                                                            flux_interp <- as.matrix(flux_interp) ###

                                                                                                                                                                                                                    #calculate watershed averages (work around error drop)
                                                                                                                                                                                                                    ws_mean_conc[k] <- mean(c_idw, na.rm=TRUE)
                                                                                                                                                                                                                    ws_mean_flux[k] <- mean(flux_interp, na.rm=TRUE)
                                                                                                                                                                                                                            # errors(ws_mean_conc)[k] <- mean(errors(c_idw), na.rm=TRUE) ###
                                                                                                                                                                                                                            # errors(ws_mean_flux)[k] <- mean(errors(flux_interp), na.rm=TRUE) ###

                                                                                                                                                                                                                            #gpuR is too fast for default garbage collection
                                                                                                                                                                                                                            rm(inv_distmat_p_sub_norm, weightmat_p, pk, p_idw, p_from_elev, p_ensemb,
                                                                                                                                                                                                                                          inv_distmat_p_sub_norm, weightmat_p, pk, p_idw, p_from_elev, p_ensemb,
                                                                                                                                                                                                                                                     flux_interp)
                                                                                                                                                                                                                            gc()
                                                                                                                                                                                                                                    }
                                                                            }

                                                                # compare_interp_methods()

                                                                ws_means <- tibble(datetime = d_dt,
                                                                                                          site_code = stream_site_code,
                                                                                                                                 var = output_varname,
                                                                                                                                 concentration = ws_mean_conc,
                                                                                                                                                        flux = ws_mean_flux,
                                                                                                                                                        ms_status = d_status,
                                                                                                                                                                               ms_interp = d_interp)

                                                                    return(ws_means)
}

derive_precip <- function(network, domain, prodname_ms){

        #only use this when you can't use derive_precip_pchem_pflux

        precip_prodname_ms <- get_derive_ingredient(network = network,
                                                                                                    domain = domain,
                                                                                                                                                    prodname = 'precipitation',
                                                                                                                                                    ignore_derprod = TRUE,
                                                                                                                                                                                                    accept_multiple = TRUE)

    wb_prodname_ms <- get_derive_ingredient(network = network,
                                                                                        domain = domain,
                                                                                                                                    prodname = 'ws_boundary',
                                                                                                                                    accept_multiple = TRUE)

        rg_prodname_ms <- get_derive_ingredient(network = network,
                                                                                            domain = domain,
                                                                                                                                        prodname = 'precip_gauge_locations',
                                                                                                                                        accept_multiple = TRUE)

        precip_idw(precip_prodname = precip_prodname_ms,
                                  wb_prodname = wb_prodname_ms,
                                                 pgauge_prodname = rg_prodname_ms,
                                                 precip_prodname_out = prodname_ms)

            return()
}

derive_precip_chem <- function(network, domain, prodname_ms){

        #only use this when you can't use derive_precip_pchem_pflux

        pchem_prodname_ms <- get_derive_ingredient(network = network,
                                                                                                  domain = domain,
                                                                                                                                                 prodname = 'precip_chemistry',
                                                                                                                                                 ignore_derprod = TRUE,
                                                                                                                                                                                                accept_multiple = TRUE)

    precip_prodname_ms <- get_derive_ingredient(network = network,
                                                                                                domain = domain,
                                                                                                                                                prodname = 'precipitation',
                                                                                                                                                ignore_derprod = TRUE,
                                                                                                                                                                                                accept_multiple = TRUE)

        wb_prodname_ms <- get_derive_ingredient(network = network,
                                                                                            domain = domain,
                                                                                                                                        prodname = 'ws_boundary',
                                                                                                                                        accept_multiple = TRUE)

        rg_prodname_ms <- get_derive_ingredient(network = network,
                                                                                            domain = domain,
                                                                                                                                        prodname = 'precip_gauge_locations',
                                                                                                                                        accept_multiple = TRUE)

            pchem_idw(pchem_prodname = pchem_prodname_ms,
                                    precip_prodname = precip_prodname_ms,
                                                  wb_prodname = wb_prodname_ms,
                                                  pgauge_prodname = rg_prodname_ms,
                                                                pchem_prodname_out = prodname_ms)

            return()
}

derive_precip_flux <- function(network, domain, prodname_ms){

        #only use this when you can't use derive_precip_pchem_pflux

        pchem_prodname_ms <- get_derive_ingredient(network = network,
                                                                                                  domain = domain,
                                                                                                                                                 prodname = 'precip_chemistry',
                                                                                                                                                 ignore_derprod = TRUE,
                                                                                                                                                                                                accept_multiple = TRUE)

    precip_prodname_ms <- get_derive_ingredient(network = network,
                                                                                                domain = domain,
                                                                                                                                                prodname = 'precipitation',
                                                                                                                                                ignore_derprod = TRUE,
                                                                                                                                                                                                accept_multiple = TRUE)

        wb_prodname_ms <- get_derive_ingredient(network = network,
                                                                                            domain = domain,
                                                                                                                                        prodname = 'ws_boundary',
                                                                                                                                        accept_multiple = TRUE)

        rg_prodname_ms <- get_derive_ingredient(network = network,
                                                                                            domain = domain,
                                                                                                                                        prodname = 'precip_gauge_locations',
                                                                                                                                        accept_multiple = TRUE)

            flux_idw(pchem_prodname = pchem_prodname_ms,
                                  precip_prodname = precip_prodname_ms,
                                               wb_prodname = wb_prodname_ms,
                                               pgauge_prodname = rg_prodname_ms,
                                                            flux_prodname_out = prodname_ms)

            return()
}


get_gee_large <- function(network, domain, gee_id, band, prodname, rez,
                          start, ws_prodname) {

    area <- sf::st_area(ws_prodname)

    sheds <- ws_prodname %>%
        as.data.frame() %>%
        sf::st_as_sf() %>%
        select(site_code) %>%
        sf::st_transform(4326) %>%
        sf::st_set_crs(4326)

    site <- unique(sheds$site_code)

    ee_shape <- sf_as_ee(sheds,
                         via = 'getInfo_to_asset',
                         assetId = 'users/spencerrhea/data_aq_sheds',
                         overwrite = TRUE,
                         quiet = TRUE)

    imgcol <- ee$ImageCollection(gee_id)$select(band)

    flat_img <- imgcol$map(function(image) {
        image$select(band)$reduceRegions(
            collection = ee_shape,
            reducer = ee$Reducer$stdDev()$combine(
                reducer2 = ee$Reducer$median(),
                sharedInputs = TRUE),
            scale = rez
        )$map(function(f) {
            f$set('imageId', image$id())
        })
    })$flatten()

    gee <- flat_img$select(propertySelectors = c('site_code', 'imageId',
                                                 'stdDev', 'median'),
                           retainGeometry = FALSE)

    ee_description <-  glue('{n}_{d}_{s}_{p}',
                            d = domain,
                            n = network,
                            s = site,
                            p = prodname)

    task <- ee$batch$Export$table$toDrive(collection = gee,
                                          description = ee_description,
                                          fileFormat = 'CSV',
                                          folder = 'GEE',
                                          fileNamePrefix = 'rgee')

    task$start()
    ee_monitoring(task)

    temp_rgee <- tempfile(fileext = '.csv')
    rgee::ee_drive_to_local(task,
                            temp_rgee,
                            overwrite = TRUE,
                            quiet = TRUE)

    sd_name <- glue('{c}_sd', c = prodname)
    median_name <- glue('{c}_median', c = prodname)

    fin_table <- read_csv(temp_rgee) %>%
        select(site_code, stdDev, median, imageId) %>%
        rename(date = imageId,
               !!sd_name := stdDev,
               !!median_name := median) %>%
        pivot_longer(cols = c(sd_name, median_name),
                     names_to = 'var',
                     values_to = 'val')

    googledrive::drive_rm('GEE/rgee.csv')
    rgee::ee_manage_delete(path_asset = 'users/spencerrhea/data_aq_sheds',
                           quiet = TRUE)

    final <- list(table = fin_table,
                  type = 'batch')
    # } else {
    #
    #     current <- Sys.Date() + years(5)
    #     current <- as.numeric(str_split_fixed(current, '-', n = Inf)[1,1])
    #     dates <- seq(start, 2025)
    #
    #     date_ranges <- dates[seq(0, 100, by = 5)]
    #     date_ranges <- date_ranges[!is.na(date_ranges)]
    #     date_ranges <- append(start, date_ranges)
    #
    #     final <- tibble()
    #     for(i in 1:(length(date_ranges)-1)) {
    #         imgcol <- get_gee_imgcol(gee_id, band, prodname,
    #                                  paste0(date_ranges[i]), paste0(date_ranges[i+1]))
    #
    #         median <- try(ee_extract(
    #             x = imgcol,
    #             y = sheds,
    #             scale = rez,
    #             fun = ee$Reducer$median(),
    #             sf = FALSE
    #         ))
    #
    #         if(length(median) <= 4 || class(median) == 'try-error') {
    #             return(NULL)
    #         }
    #
    #         median_name <- glue('{c}_median', c = prodname)
    #         median <- clean_gee_tabel(median, sheds, median_name)
    #
    #         sd <- try(ee_extract(
    #             x = imgcol,
    #             y = sheds,
    #             scale = rez,
    #             fun = ee$Reducer$stdDev(),
    #             sf = FALSE
    #         ))
    #
    #         if(length(sd) <= 4 || class(sd) == 'try-error') {
    #             sd <- tibble()
    #         } else {
    #             sd_name <- glue('{c}_sd', c = prodname)
    #             sd <- clean_gee_tabel(sd, sheds, sd_name)
    #         }

    # count <- try(ee_extract(
    #     x = imgcol,
    #     y = sheds,
    #     scale = rez,
    #     fun = ee$Reducer$count(),
    #     sf = FALSE
    # ))
    #
    # if(length(count) <= 4 || class(count) == 'try-error'){
    #     count <- tibble()
    # } else {
    #     count_name <- glue('{c}_count', c = prodname)
    #     count <- clean_gee_tabel(count, sheds, count_name)
    # }

    #     fin <- rbind(median, sd)
    #
    #     final <- rbind(final, fin)
    # }


    #
    # col <- ee$ImageCollection(gee_id)$
    #     filterDate('1957-10-04', '2040-01-01')
    #
    # colList = col$toList(count = 10000000)
    # n = colList$size()$getInfo()
    # n <- n-1
    #
    # all_times_m <- tibble()
    # for (i in 0:n) {
    #     print(i)
    #     img = ee$Image(colList$get(i));
    #     id = img$id()$getInfo();
    #
    #     export <- img$reduceRegions(collection = ee_shape,
    #                                 reducer = ee$Reducer$median(),
    #                                 scale = rez)
    #
    #     image_info <- export$getInfo()
    #
    #     all_sites_m <- tibble()
    #     for(p in 1:nrow(sheds)){
    #         site_d_val <- image_info[["features"]][[p]][["properties"]][[band]]
    #         site_d_val <- ifelse(is.null(site_d_val), NA, site_d_val)
    #         site_code_p <- image_info[["features"]][[p]][["properties"]][["site_code"]]
    #         one_y_d <- tibble(val = site_d_val,
    #                           site_code = site_code_p,
    #                           date = id)
    #
    #         all_sites_m <- rbind(all_sites_m, one_y_d)
    #     }
    #     all_times_m <- rbind(all_times_m, all_sites_m)
    # }
    #
    # median_name <- glue('{c}_median', c = prodname)
    # median <- all_times_m %>%
    #     mutate(var = !!median_name)
    #
    # if(is.na(unique(median$val)) && length(unique(median$val)) == 1){
    #     return(NULL)
    # }
    #
    # all_times_sd <- tibble()
    # for (i in 0:n) {
    #     img = ee$Image(colList$get(i));
    #     id = img$id()$getInfo();
    #
    #     export <- img$reduceRegions(collection = ee_shape,
    #                                 reducer = ee$Reducer$stdDev(),
    #                                 scale = rez)
    #
    #     image_info <- export$getInfo()
    #
    #     all_sites_sd <- tibble()
    #     for(p in 1:nrow(sheds)){
    #         site_d_val <- image_info[["features"]][[p]][["properties"]][[band]]
    #         site_d_val <- ifelse(is.null(site_d_val), NA, site_d_val)
    #         site_code_p <- image_info[["features"]][[p]][["properties"]][["site_code"]]
    #         one_y_d <- tibble(val = site_d_val,
    #                           site_code = site_code_p,
    #                           date = id)
    #
    #         all_sites_sd <- rbind(all_sites_sd, one_y_d)
    #     }
    #     all_times_sd <- rbind(all_times_sd, all_sites_sd)
    # }
    #
    # sd_name <- glue('{c}_sd', c = prodname)
    # sd <- all_times_sd %>%
    #     mutate(var = !!sd_name)
    #


    return(final)
}

chunk_bb <- function(bb){
    # divides a bounding box into 4 so a web request can be made for spatial data
    og_bb <- bb
    x_mid <- (bb[1]+bb[3])/2
    y_mid <- (bb[2]+bb[4])/2

    bb_1 <- og_bb
    bb_2 <- og_bb
    bb_3 <- og_bb
    bb_4 <- og_bb

    bb_1[2] <- y_mid; bb_1[3] <- x_mid
    bb_2[1] <- x_mid; bb_2[2] <- y_mid
    bb_3[3] <- x_mid; bb_3[4] <- y_mid
    bb_4[1] <- x_mid; bb_4[4] <- y_mid

    bb_all <- list(bb_1, bb_2, bb_3, bb_4)

    return(bb_all)
}
