lat=44.21013; long=-122.2571; crs=4326; dev_machine_status = 'n00b'
verbose = TRUE
# mapview = mapview::mapview

#. handle_errors
delineate_watershed_apriori <- function(lat, long, crs,
                                        dev_machine_status = '1337',
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
    snap1_f <- glue(tmp, '/snap1.shp')
    snap2_f <- glue(tmp, '/snap2.shp')
    snap3_f <- glue(tmp, '/snap3.shp')
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

        whitebox::wbt_jenson_snap_pour_points(pour_pts = point_f,
                                              streams = flow_f,
                                              output = snap1_f,
                                              snap_dist = 150)
        whitebox::wbt_snap_pour_points(pour_pts = point_f,
                                       flow_accum = flow_f,
                                       output = snap2_f,
                                       snap_dist = 50)
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

        #delineate each unique location
        for(i in 1:length(unique_snaps_f)){

            wb_f <- glue('{path}/wb{n}_buffer{b}.tif',
                         path = tmp,
                         n = i,
                         b = buffer_radius)

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

                wb_sf_f <- glue('{path}/wb_sf{n}_buffer{b}.shp',
                                 path = inspection_dir,
                                 n = i,
                                 b = buffer_radius)

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
                       sl = length(site_locations)))
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
    if(any(is.na(site_locations$latitude) | site_locations$longitude)){

        missing_loc <- is.na(site_locations$latitude) |
            is.na(site_locations$longitude)

        missing_site_names <- site_locations$site_name[missing_loc]

        stop(glue('Missing site location for:\nnetwork: {n}\ndomain: {d}\n',
                  'site(s): {ss}\n(data/general/site_data.csv)',
                  n = network,
                  d = domain,
                  ss = paste(missing_crs,
                             collapse = ', ')))
    }

    if(any(is.na(site_locations$CRS))){

        missing_crs <- site_locations$site_name[is.na(site_locations$CRS)]

        stop(glue('Missing CRS for:\nnetwork: {n}\ndomain: {d}\n',
                  'site(s): {ss}\n(data/general/site_data.csv)',
                  n = network,
                  d = domain,
                  ss = paste(missing_crs,
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
        dir.create('data/{n}/{d}/munged/{w}',
                   n = network,
                   d = domain,
                   w = ws_boundary_dir)
    }

    #for each stream gauge site, check for existing wb file. if none, delineate
    for(i in 1:nrow(site_locations)){

        if(verbose){
            print(glue('delineating site {st} of {sl}',
                       st = i,
                       sl = length(site_locations)))
        }

        site <- site_locations$site_name[i]

        site_dir <- glue('data/{n}/{d}/munged/{w}/{s}',
                         n = network,
                         d = domain,
                         w = ws_boundary_dir,
                         s = site)

        if(! dir.exists(site_dir) || ! length(dir(site_dir))){

            files_to_inspect <- delineate_watershed_apriori(
                lat = site_locations$latitude[i],
                long = site_locations$longitude[i],
                crs = site_locations$CRS[i],
                dev_machine_status = dev_machine_status,
                verbose = verbose))

            files_to_inspect <- list.files(path = inspection_dir,
                                           pattern = '.shp')

            #if only one delineation, write it into macrosheds storage
            if(length(files_to_inspect) == 1){
                file.rename(from = files_to_inspect[1],
                            to = glue('data/lter/hjandrews/munged/')) #HERE
            }

            #collect response

            #write selection (GOT A FUNC FOR THIS ALREADY)

            #return specs

            #notify dev about email tool

            #IF WS_AREA_HA NOT YET CALCED, CALC IT
        }

    }

    return()
}

write_wb_delin_specs <- function(){

}

read_wb_delin_specs <- function(){

}

delineate_watershed_by_specification <- function(){

}


