lat=44.21013; long=-122.2571; crs=4326; dev_machine_status = 'n00b'; verbose = TRUE
mapview = mapview::mapview
buffer_radius = 100; dem_resolution = 10; snap_method = 'standard'; snap_dist = 150

delineate_watershed_test2 <- function(temp_dir = tmp,
                                      site_location_file = point_f,
                                      flow_accum_file = flow_f,
                                      d8_pointer_file = d8_f,
                                      snap_method,
                                      snap_dist){

    #temp_dir is created by tempdir() inside delineate_watershed_apriori
    #   or delineate_watershed_from_specs. its variable name is always "tmp"
    #site_location_file, flow_accum_file, and d8_pointer_file are
    #   created in delineate_watershed_apriori and
    #   delineate_watershed_from_specs.
    #snap_method is one of "jenson" or "standard"
    #snap_dist is the snapping distance in meters

    #returns an object that can be viewed with mapview::mapview()

    require(whitebox)

    test_file_base <- glue('{tem}/test_{typ}_dist{dst}',
                           tem = temp_dir,
                           typ = snap_method,
                           dst = snap_dist)

    test_snap_f <- paste0(test_file_base,
                           '.shp')
    test_wb_f <- paste0(test_file_base,
                        '.tif')

    if(snap_method == 'standard'){
        args <- list('pour_pts' = site_location_file,
                     'flow_accum' = flow_accum_file,
                     'output' = test_snap_f,
                     'snap_dist' = snap_dist)
        desired_func <- 'wbt_snap_pour_points'
    } else if(snap_method == 'jenson'){
        args <- list('pour_pts' = site_location_file,
                     'streams' = flow_accum_file,
                     'output' = test_snap_f,
                     'snap_dist' = snap_dist)
        desired_func <- 'wbt_jenson_snap_pour_points'
    } else {
        stop('snap_method must be "standard" or "jenson"')
    }

    do.call(desired_func, args)

    whitebox::wbt_watershed(d8_pntr = d8_pointer_file,
                            pour_pts = test_snap_f,
                            output = test_wb_f)

    test_wb_sf <- raster::raster(test_wb_f) %>%
        raster::rasterToPolygons() %>%
        sf::st_as_sf()

    map_out <- mapview::mapview(test_wb_sf)

    return(map_out)
}


delineate_watershed_test1 <- function(lat, long, crs,
                                      buffer_radius, dem_resolution,
                                      snap_method, snap_dist){

    #lat: numeric representing latitude in decimal degrees
    #   (negative indicates southern hemisphere)
    #long: numeric representing longitude in decimal degrees
    #   (negative indicates west of prime meridian)
    #crs: numeric representing the coordinate reference system (e.g. WSG84)

    #returns the location of the candidate watershed boundary file

    require(whitebox)
    require(mapview)

    tmp <- tempdir()
    inspection_dir <- glue(tmp, '/INSPECT_THESE')
    dem_f <- glue(tmp, '/dem.tif')
    point_f <- glue(tmp, '/point.shp')
    d8_f <- glue(tmp, '/d8_pntr.tif')
    flow_f <- glue(tmp, '/flow.tif')

    dir.create(path = inspection_dir,
               showWarnings = FALSE)

    proj <- choose_projection(lat = lat,
                              long = long)

    site <- tibble(x = lat,
                   y = long) %>%
        sf::st_as_sf(coords = c("y", "x"),
                     crs = crs) %>%
        sf::st_transform(proj)

    site_buf <- sf::st_buffer(x = site,
                              dist = buffer_radius)
    dem <- elevatr::get_elev_raster(locations = site_buf,
                                    z = dem_resolution)

    raster::writeRaster(x = dem,
                        filename = dem_f,
                        overwrite = TRUE)

    sf::st_write(obj = site,
                 dsn = point_f,
                 delete_layer = TRUE,
                 quiet = TRUE)

    whitebox::wbt_fill_single_cell_pits(dem = dem_f,
                                        output = dem_f)

    whitebox::wbt_breach_depressions(dem = dem_f,
                                     output = dem_f,
                                     flat_increment = 0.01)

    whitebox::wbt_d8_pointer(dem = dem_f,
                             output = d8_f)

    whitebox::wbt_d8_flow_accumulation(input = dem_f,
                                       output = flow_f,
                                       out_type = 'catchment area')

    test_file_base <- glue('{tem}/test_{typ}_dist{dst}',
                           tem = tmp,
                           typ = snap_method,
                           dst = snap_dist)

    test_snap_f <- paste0(test_file_base,
                          '.shp')
    test_wb_f <- paste0(test_file_base,
                        '.tif')

    if(snap_method == 'standard'){
         args <- list('pour_pts' = point_f,
                      'flow_accum' = flow_f,
                      'output' = test_snap_f,
                      'snap_dist' = snap_dist)
         desired_func <- 'wbt_snap_pour_points'
    } else if(snap_method == 'jenson'){
         args <- list('pour_pts' = point_f,
                      'streams' = flow_f,
                      'output' = test_snap_f,
                      'snap_dist' = snap_dist)
         desired_func <- 'wbt_jenson_snap_pour_points'
    } else {
        stop('snap_method must be "standard" or "jenson"')
    }

    do.call(desired_func, args)

    whitebox::wbt_watershed(d8_pntr = d8_f,
                            pour_pts = test_snap_f,
                            output = test_wb_f)

    test_wb_sf <- raster::raster(test_wb_f) %>%
        raster::rasterToPolygons() %>%
        sf::st_as_sf()

    map_out <- mapview(test_wb_sf) + mapview(dem)

    return(map_out)
}
delineate_watershed_test1(lat, long, crs, buffer_radius = 100,
                          dem_resolution = 10, snap_method = 'standard',
                          snap_dist = 150)

#. handle_errors
delineate_watershed_apriori <- function(lat, long, crs,
                                        dev_machine_status = 'n00b',
                                        verbose = FALSE){

    #lat: numeric representing latitude in decimal degrees
    #   (negative indicates southern hemisphere)
    #long: numeric representing longitude in decimal degrees
    #   (negative indicates west of prime meridian)
    #crs: numeric representing the coordinate reference system (e.g. WSG84)
    #dev_machine_status: either '1337', indicating that your machine has >= 16 GB
    #   RAM, or 'n00b', indicating < 16 GB RAM. DEM resolution is chosen accordingly
    #verbose: logical. determines the amount of informative messaging during run

    #returns the location of candidate watershed boundary files

    tmp <- tempdir()
    inspection_dir <- glue(tmp, '/INSPECT_THESE')
    dem_f <- glue(tmp, '/dem.tif')
    point_f <- glue(tmp, '/point.shp')
    d8_f <- glue(tmp, '/d8_pntr.tif')
    flow_f <- glue(tmp, '/flow.tif')

    dir.create(path = inspection_dir,
               showWarnings = FALSE)

    proj <- choose_projection(lat = lat,
                              long = long)

    site <- tibble(x = lat,
                   y = long) %>%
        sf::st_as_sf(coords = c("y", "x"),
                     crs = crs) %>%
        sf::st_transform(proj)
    # sf::st_transform(4326) #WGS 84 (would be nice to do this unprojected)

    #prepare for delineation loops
    buffer_radius <- 100
    dem_coverage_insufficient <- FALSE
    while_loop_begin <- TRUE

    #snap site to flowlines 3 different ways. delineate watershed boundaries (wb)
    #for each unique snap. if the delineations get cut off, get more elevation data
    #and try again
    while(while_loop_begin || dem_coverage_insufficient){

        while_loop_begin <- FALSE

        if(dev_machine_status == '1337'){
            dem_resolution <- case_when(
                buffer_radius <= 1e4 ~ 12,
                buffer_radius == 1e5 ~ 11,
                buffer_radius == 1e6 ~ 10,
                buffer_radius == 1e7 ~ 8,
                buffer_radius == 1e8 ~ 6,
                buffer_radius == 1e9 ~ 4,
                buffer_radius >= 1e10 ~ 2)
        } else if(dev_machine_status == 'n00b'){
            dem_resolution <- case_when(
                buffer_radius <= 1e4 ~ 10,
                buffer_radius == 1e5 ~ 8,
                buffer_radius == 1e6 ~ 6,
                buffer_radius == 1e7 ~ 4,
                buffer_radius == 1e8 ~ 2,
                buffer_radius >= 1e9 ~ 1)
        } else {
            stop('dev_machine_status must be either "1337" or "n00b"')
        }

        site_buf <- sf::st_buffer(x = site,
                                  dist = buffer_radius)
        dem <- elevatr::get_elev_raster(locations = site_buf,
                                        z = dem_resolution)

        raster::writeRaster(x = dem,
                            filename = dem_f,
                            overwrite = TRUE)

        sf::st_write(obj = site,
                     dsn = point_f,
                     delete_layer = TRUE,
                     quiet = TRUE)

        whitebox::wbt_fill_single_cell_pits(dem = dem_f,
                                            output = dem_f)

        whitebox::wbt_breach_depressions(dem = dem_f,
                                         output = dem_f,
                                         flat_increment = 0.01)

        whitebox::wbt_d8_pointer(dem = dem_f,
                                 output = d8_f)

        whitebox::wbt_d8_flow_accumulation(input = dem_f,
                                           output = flow_f,
                                           out_type = 'catchment area')

        snap1_f <- glue(tmp, '/snap1_jenson_dist150.shp')
        whitebox::wbt_jenson_snap_pour_points(pour_pts = point_f,
                                              streams = flow_f,
                                              output = snap1_f,
                                              snap_dist = 150)
        snap2_f <- glue(tmp, '/snap2_standard_dist50.shp')
        whitebox::wbt_snap_pour_points(pour_pts = point_f,
                                       flow_accum = flow_f,
                                       output = snap2_f,
                                       snap_dist = 50)
        snap3_f <- glue(tmp, '/snap3_standard_dist150.shp')
        whitebox::wbt_snap_pour_points(pour_pts = point_f,
                                       flow_accum = flow_f,
                                       output = snap3_f,
                                       snap_dist = 150)

        #the site has been snapped 3 different ways. identify unique snap locations.
        snap1 <- sf::st_read(snap1_f, quiet = TRUE)
        snap2 <- sf::st_read(snap2_f, quiet = TRUE)
        snap3 <- sf::st_read(snap3_f, quiet = TRUE)
        unique_snaps_f <- snap1_f
        if(! identical(snap1, snap2)) unique_snaps_f <- c(unique_snaps_f, snap2_f)
        if(! identical(snap1, snap3)) unique_snaps_f <- c(unique_snaps_f, snap3_f)

        #good for experimenting with snap specs:
        # delineate_watershed_test2(tmp, point_f, flow_f,
        #                           d8_f, 'standard', 1000)

        #delineate each unique location
        for(i in 1:length(unique_snaps_f)){

            rgx <- str_match(unique_snaps_f[i],
                             '.*?_(standard|jenson)_dist([0-9]+)\\.shp$')
            snap_method <- rgx[, 2]
            snap_distance <- rgx[, 3]

            wb_f <- glue('{path}/wb{n}_buffer{b}_{typ}_dist{dst}.tif',
                         path = tmp,
                         n = i,
                         b = buffer_radius,
                         typ = snap_method,
                         dst = snap_distance)

            whitebox::wbt_watershed(d8_pntr = d8_f,
                                    pour_pts = unique_snaps_f[i],
                                    output = wb_f)

            wb <- raster::raster(wb_f)

            #check how many wb cells coincide with the edge of the DEM.
            #If > 0.1% or > 5, broader DEM needed
            smry <- raster_intersection_summary(wb = wb,
                                                dem = dem)

            if(verbose){
                print(glue('buffer radius: {br}; snap: {sn}/{tot}; ',
                           'n intersecting cells: {ni}; pct intersect: {pct}',
                           br = buffer_radius,
                           sn = i,
                           tot = length(unique_snaps_f),
                           ni = round(smry$n_intersections, 2),
                           pct = round(smry$pct_wb_cells_intersect, 2)))
            }

            if(smry$pct_wb_cells_intersect > 0.1 || smry$n_intersections > 5){
                buffer_radius_new <- buffer_radius * 10
                dem_coverage_insufficient <- TRUE
            } else {
                buffer_radius_new <- buffer_radius

                #write and record temp files for the technician to visually inspect
                wb_sf <- wb %>%
                    raster::rasterToPolygons() %>%
                    sf::st_as_sf() %>%
                    sf::st_buffer(dist = 0.1) %>%
                    sf::st_union() %>%
                    sf::st_as_sf()#again? ugh.

                wb_sf <-  sf::st_transform(wb_sf, 4326) #EPSG for WGS84

                wb_sf_f <- glue('{path}/wb{n}_buffer{b}_{typ}_dist{dst}.shp',
                                path = inspection_dir,
                                n = i,
                                b = buffer_radius,
                                typ = snap_method,
                                dst = snap_distance)

                sf::st_write(obj = wb_sf,
                             dsn = wb_sf_f,
                             delete_dsn = TRUE,
                             quiet = TRUE)
            }
        }

        buffer_radius <- buffer_radius_new
    } #end while loop

    if(verbose){
        message(glue('Candidate delineations are in: ', inspection_dir))
    }

    return(inspection_dir)
}
# delineate_watershed_apriori(lat=lat, long=long, crs=crs, 'n00b', TRUE)

ms_delineate <- function(network, domain,
                         dev_machine_status,
                         verbose = FALSE){

    #dev_machine_status: either '1337', indicating that your machine has >= 16 GB
    #   RAM, or 'n00b', indicating < 16 GB RAM. DEM resolution is chosen
    #   accordingly. passed to delineate_watershed_apriori
    #verbose: logical. determines the amount of informative messaging during run

    site_locations <- sm(read_csv('data/general/site_data.csv')) %>%
        filter(
            as.logical(in_workflow),
            network == !!network,
            domain == !!domain,
            # ! is.na(latitude),
            # ! is.na(longitude),
            site_type == 'stream_gauge') %>%
        select(site_name, latitude, longitude, CRS, ws_area_ha)

    #checks
    if(any(is.na(site_locations$latitude) | is.na(site_locations$longitude))){

        missing_loc <- is.na(site_locations$latitude) |
            is.na(site_locations$longitude)

        missing_site_names <- site_locations$site_name[missing_loc]

        stop(glue('Missing/incomplete site location for:\nnetwork: {n}\ndomain: {d}\n',
                  'site(s): {ss}\n(data/general/site_data.csv)',
                  n = network,
                  d = domain,
                  ss = paste(missing_site_names,
                             collapse = ', ')))
    }

    if(any(is.na(site_locations$CRS))){

        missing_site_names <- site_locations$site_name[is.na(site_locations$CRS)]

        stop(glue('Missing CRS for:\nnetwork: {n}\ndomain: {d}\n',
                  'site(s): {ss}\n(data/general/site_data.csv)',
                  n = network,
                  d = domain,
                  ss = paste(missing_site_names,
                             collapse = ', ')))
    }

    #locate or create the directory that contains watershed boundaries
    munged_dirs <- list.dirs(glue('data/{n}/{d}/munged',
                                  n = network,
                                  d = domain),
                             recursive = FALSE,
                             full.names = FALSE)

    ws_boundary_dir <- grep(pattern = '^ws_boundary.*',
                            x = munged_dirs,
                            value = TRUE)

    if(! length(ws_boundary_dir)){
        ws_boundary_dir <- 'ws_boundary__ms000'
        dir.create(glue('data/{n}/{d}/munged/{w}',
                        n = network,
                        d = domain,
                        w = ws_boundary_dir))
    }

    #for each stream gauge site, check for existing wb file. if none, delineate
    for(i in 1:nrow(site_locations)){

        site <- site_locations$site_name[i]

        if(verbose){
            print(glue('delineating {n}-{d}-{s} (site {sti} of {sl})',
                       n = network,
                       d = domain,
                       s = site,
                       sti = i,
                       sl = nrow(site_locations)))
        }

        site_dir <- glue('data/{n}/{d}/munged/{w}/{s}',
                         n = network,
                         d = domain,
                         w = ws_boundary_dir,
                         s = site)

        if(dir.exists(site_dir) && length(dir(site_dir))){
            message(glue('Watershed boundaries already available in ', site_dir))
            return()
        }

        dir.create(site_dir,
                   showWarnings = FALSE)

        specs <- read_wb_delin_specs(network = network,
                                     domain = domain,
                                     site_name = site) %>%
            filter(
                network == !!network,
                domain == !!domain,
                site_name == !!site)

        if(nrow(specs) == 1){

            catch <- delineate_watershed_by_specification(
                lat = site_locations$latitude[i],
                long = site_locations$longitude[i],
                crs = site_locations$CRS[i],
                write_dir = site_dir)

            return()
            #everything that follows pertains to interactive selection of an
            #appropriate delineation

        } else if(nrow(specs) == 0){

            inspection_dir <- delineate_watershed_apriori(
                lat = site_locations$latitude[i],
                long = site_locations$longitude[i],
                crs = site_locations$CRS[i],
                dev_machine_status = dev_machine_status,
                verbose = verbose)

        } else {
            stop('Multiple entries for same network/domain/site in site_data.csv')
        }

        files_to_inspect <- list.files(path = inspection_dir,
                                       pattern = '.shp')

        #if only one delineation, write it into macrosheds storage
        if(length(files_to_inspect) == 1){

            selection <- files_to_inspect[1]

            move_shapefiles(shp_files = selection,
                            from_dir = inspection_dir,
                            to_dir = site_dir)

            message(glue('Delineation successful. Shapefile written to ',
                         site_dir))

            #otherwise, technician must inspect all delineations and choose one
        } else {

            nshapes <- length(files_to_inspect)

            wb_selections <- paste(paste0('[',
                                          1:nshapes,
                                          ']'),
                                   files_to_inspect,
                                   sep = ': ',
                                   collapse = '\n')

            helper_code <- glue('mapview::mapview(sf::st_read("{wd}/{f}"))',
                                wd = inspection_dir,
                                f = files_to_inspect) %>%
                paste(collapse = '\n\n')

            msg <- glue('Visually inspect the watershed boundary candidate shapefiles ',
                        'in {td}, then enter the number corresponding to the ',
                        'one that looks most legit. Here\'s some ',
                        'helper code you can paste into an R instance running ',
                        'in a shell (terminal):\n\n{hc}\n\nIf you aren\'t ',
                        'sure which is correct, get a site manager to verify:\n',
                        'request_site_manager_verification(type=\'wb delin\', ',
                        'network, domain)\n\nChoices:\n{sel}\n\nEnter number here > ',
                        hc = helper_code,
                        sel = wb_selections,
                        td = inspection_dir)

            resp <- get_response_1char(msg = msg,
                                       possible_chars = 1:nshapes)

            selection <- files_to_inspect[as.numeric(resp)]

            move_shapefiles(shp_files = selection,
                            from_dir = inspection_dir,
                            to_dir = site_dir,
                            new_name_vec = site)

            message(glue('Selection:\n\t{sel}\nwas written to\n\t{sdr}',
                         sel = selection,
                         sdr = site_dir))
        }

        #write the specifications of the correctly delineated watershed
        rgx <- str_match(selection,
                         '^wb[0-9]+_buffer([0-9]+)_(standard|jenson)_dist([0-9]+)\\.shp$')

        write_wb_delin_specs(network = network,
                             domain = domain,
                             site_name = site,
                             buffer_radius = as.numeric(rgx[, 2]),
                             snap_method = rgx[, 3],
                             snap_distance = as.numeric(rgx[, 4]))

        #calculate watershed area and write it to site_data.csv
        catch <- ms_calc_watershed_area(network = network,
                                        domain = domain,
                                        site_name = site,
                                        update_site_file = TRUE)
    }

    message(glue('Delineation specifications were written to:\n\t',
                 'data/general/watershed_delineation_specs.csv\n',
                 'watershed areas were written to:\n\t',
                 'data/general/site_data.csv'))

    return()
}
ms_delineate(network, domain, 'n00b', TRUE)

delineate_watershed_by_specification <- function(lat, long, crs, write_dir){

    #lat: numeric representing latitude in decimal degrees
    #   (negative indicates southern hemisphere)
    #long: numeric representing longitude in decimal degrees
    #   (negative indicates west of prime meridian)
    #crs: numeric representing the coordinate reference system (e.g. WSG84)
    #write_dir: character. the directory to write shapefile watershed boundary to

    #returns the location of candidate watershed boundary files

    require(whitebox) #can't do e.g. whitebox::func in do.call

    specs <- read_wb_delin_specs(network = network,
                                 domain = domain,
                                 site_name = site_name)

    tmp <- tempdir()
    inspection_dir <- glue(tmp, '/INSPECT_THESE')
    dem_f <- glue(tmp, '/dem.tif')
    point_f <- glue(tmp, '/point.shp')
    d8_f <- glue(tmp, '/d8_pntr.tif')
    flow_f <- glue(tmp, '/flow.tif')
    snap_f <- glue(tmp, '/snap.shp')
    wb_f <- glue(tmp, '/wb.tif')

    dir.create(path = inspection_dir,
               showWarnings = FALSE)

    proj <- choose_projection(lat = lat,
                              long = long)

    site <- tibble(x = lat,
                   y = long) %>%
        sf::st_as_sf(coords = c("y", "x"),
                     crs = crs) %>%
        sf::st_transform(proj)
    # sf::st_transform(4326) #WGS 84 (would be nice to do this unprojected)

    site_buf <- sf::st_buffer(x = site,
                              dist = specs$buffer_radius_m)
    dem <- elevatr::get_elev_raster(locations = site_buf,
                                    z = dem_resolution)

    raster::writeRaster(x = dem,
                        filename = dem_f,
                        overwrite = TRUE)

    sf::st_write(obj = site,
                 dsn = point_f,
                 delete_layer = TRUE,
                 quiet = TRUE)

    whitebox::wbt_fill_single_cell_pits(dem = dem_f,
                                        output = dem_f)

    whitebox::wbt_breach_depressions(dem = dem_f,
                                     output = dem_f,
                                     flat_increment = 0.01)

    whitebox::wbt_d8_pointer(dem = dem_f,
                             output = d8_f)

    whitebox::wbt_d8_flow_accumulation(input = dem_f,
                                       output = flow_f,
                                       out_type = 'catchment area')

    #call the appropriate snapping function from whitebox
    args <- list(pour_pts = point_f,
                 output = snap_f,
                 snap_dist = specs$snap_dist)

    if(snap_method == 'standard'){
        args$flow_accum <- flow_f,
        desired_func <- 'wbt_snap_pour_points'
    } else if(snap_method == 'jenson'){
        args$streams <- flow_f,
        desired_func <- 'wbt_jenson_snap_pour_points'
    } else {
        stop('snap_method must be "standard" or "jenson"')
    }

    do.call(desired_func, args)

    #delineate
    whitebox::wbt_watershed(d8_pntr = d8_f,
                            pour_pts = snap_f,
                            output = wb_f)

    wb_sf <- raster::raster(wb_f) %>%
        raster::rasterToPolygons() %>%
        sf::st_as_sf() %>%
        sf::st_buffer(dist = 0.1) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>% #again? ugh.
        sf::st_transform(4326) #EPSG for WGS84

    sf::st_write(obj = wb_sf,
                 dsn = write_dir,
                 delete_dsn = TRUE,
                 quiet = TRUE)

    #HERE
    message(glue('Candidate delineations are in: ', inspection_dir))

    return()
}

#ABANDONED for ms_delineate, but see if the mapview part can be done
delineate_whole_domain_apriori <- function(network, domain,
                                           dev_machine_status,
                                           verbose = FALSE){

    #dev_machine_status: either '1337', indicating that your machine has >= 16 GB
    #   RAM, or 'n00b', indicating < 16 GB RAM. DEM resolution is chosen
    #   accordingly. passed to delineate_watershed_apriori
    #verbose: logical. determines the amount of informative messaging during run

    site_locations <- sm(read_csv('data/general/site_data.csv')) %>%
        filter(
            as.logical(in_workflow),
            network == !!network,
            domain == !!domain) %>%
        select(latitude, longitude, CRS)

    for(i in 1:nrow(site_locations)){

        if(verbose){
            print(glue('delineating site {st} of {sl}',
                       st = i,
                       sl = nrow(site_locations)))
        }

        inspection_dir <- delineate_watershed_apriori(
            lat = site_locations$latitude[i],
            long = site_locations$longitude[i],
            crs = site_locations$CRS[i],
            dev_machine_status = dev_machine_status,
            verbose = verbose)

        #direct user to files
        files_to_inspect <- list.files(path = inspection_dir,
                                       pattern = '.shp')

        #if only one delineation, write it into macrosheds storage
        if(length(files_to_inspect) == 1){

            # sf::st_read(files_to_inspect)
            #
            # sf::st_write(obj = wb_sf,
            #              dsn = wb_sf_f,
            #              delete_dsn = TRUE,
            #              quiet = TRUE)

            file.rename(from = files_to_inspect[1],
                        to = glue('data/lter/hjandrews/munged/'))

        }
    }

    #would be nice to watch the progression, but mapview only
    #works in interactive mode (not insude functions)
    # if(pause_after_delins){
    #
    #     snapped_point <- sf::st_read(unique_snaps_f[i],
    #                                  quiet = TRUE)
    #
    #     mapview::mapview(snapped_point,
    #                      layer.name = 'site snap') +
    #         mapview::mapview(wb_sf,
    #                          layer.name = 'wb') +
    #         mapview::mapview(dem,
    #                          layer.name = 'dem')
    #
    #     catch <- readline('Check the viewer panel. ENTER to continue')
    # }

    #collect response

    #write selection

    #return specs

    #notify dev about email tool

    return()
}

#should be a check for whether ms_delineate is running on server
    #if so, return error instead of running apriori_delin
#should check for wbs by site, not by ws_boundary__
#add an informative log message at the end of delineate_watershed_by_specification
