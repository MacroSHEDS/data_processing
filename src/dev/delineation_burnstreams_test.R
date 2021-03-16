require(tidyverse)
require(glue)
require(sf)
require(elevatr)
require(raster)
require(whitebox)
library(osmdata)

mv = mapview::mapview
sm <- suppressMessages
sw <- suppressWarnings

rebuild = FALSE

# setup ####

lat = 35.9795
long = -79.0018
WGS84 = 'epsg:4326'

buffer_radius <- 100
dem_resolution = 10

# if(rebuild){
# tmp <- tempdir()
#     write_file(tmp, '/tmp/tmp_save.txt')
# } else {
#     tmp <- read_file('/tmp/tmp_save.txt')
# }
tmp <- '/tmp/rtmp2'
inspection_dir <- glue(tmp, '/INSPECT_THESE')
dem_f <- glue(tmp, '/dem.tif')
point_f <- glue(tmp, '/point.shp')
d8_f <- glue(tmp, '/d8_pntr.tif')
flow_f <- glue(tmp, '/flow.tif')

dir.create(path = inspection_dir,
           showWarnings = FALSE)

choose_projection <- function(lat = NULL,
                              long = NULL,
                              unprojected = FALSE){

    #TODO: CHOOSE PROJECTIONS MORE CAREFULLY

    if(unprojected){
        PROJ4 <- glue('+proj=longlat +datum=WGS84 +no_defs ',
                      '+ellps=WGS84 +towgs84=0,0,0')
        return(PROJ4)
    }

    if(is.null(lat) || is.null(long)){
        stop('If projecting, lat and long are required.')
    }

    abslat <- abs(lat)

    # THIS WORKS (PROJECTS STUFF), BUT CAN'T BE READ AUTOMATICALLY BY st_read
    if(abslat < 23){ #tropical
        PROJ4 = glue('+proj=laea +lon_0=', long)
        # ' +datum=WGS84 +units=m +no_defs')
    } else { #temperate or polar
        PROJ4 = glue('+proj=laea +lat_0=', lat, ' +lon_0=', long)
    }

    return(PROJ4)
}

# proj ####

proj <- 'ESRI:102003'
# proj <- choose_projection(lat = lat,
#                           long = long)
#                           # unprojected = TRUE)

site_wgs84 <- tibble(x = lat,
               y = long) %>%
    sf::st_as_sf(coords = c("y", "x"),
                 crs = WGS84)
site_proj <- sf::st_transform(site_wgs84, crs = proj)
# sf::st_transform(WGS84) #WGS 84 (would be nice to do this unprojected)

site_buf_proj <- sf::st_buffer(x = site_proj,
                          dist = buffer_radius * 1000)
    # sf::st_transform(crs = WGS84)
# site <- sf::st_transform(site, crs = WGS84) #proj back!

dem_proj <- elevatr::get_elev_raster(locations = site_buf_proj,
                                     z = dem_resolution,
                                     verbose = FALSE)

dem_wgs84 <- terra::project(terra::rast(dem_proj),
                            y = WGS84)
# raster::projectRaster(dem,
#     crs = WGS84,
#     res = raster::res(dem))

# osm ####

streams_f <- glue(tmp, '/streams.shp')
roads_f <- glue(tmp, '/roads.shp')

if(rebuild){

    dem_bounds <- terra::ext(dem_wgs84)[c(1, 3, 2, 4)]
    # dem_bounds <- sf::st_bbox(dem)

    # highway_types <- c('motorway', 'trunk', 'primary')
    highway_types <- c('motorway', 'trunk', 'primary', 'secondary', 'tertiary')
    highway_types <- c(highway_types,
                       paste(highway_types, 'link', sep = '_'))
    roads_query <- opq(dem_bounds) %>%
        add_osm_feature(key = 'highway',
                        value = highway_types)
    roads <- osmdata_sf(roads_query)
    roads <- roads$osm_lines$geometry
    # plot(roads$osm_lines, max.plot = 1)

    streams_query <- opq(dem_bounds) %>%
        add_osm_feature(key = 'waterway',
                        value = c('river', 'stream'))
    streams <- osmdata_sf(streams_query)
    streams <- streams$osm_lines$geometry
    # streams$osm_lines$geometry
    # plot(streams$osm_lines)

    streams_proj <- streams %>%
        sf::st_transform(crs = proj) %>%
        sf::st_union() %>%
        # sf::st_transform(crs = WGS84) %>%
        sf::st_as_sf() %>%
        rename(geometry = x) %>%
        mutate(FID = 0:(n() - 1)) %>%
        dplyr::select(FID, geometry)
    streams_wgs84 <- sf::st_transform(streams_proj, crs=WGS84)
    sf::st_write(streams_proj,
                 dsn = tmp,
                 layer = 'streams',
                 driver = 'ESRI Shapefile',
                 delete_layer = TRUE,
                 silent = TRUE)

    roads_proj <- roads %>%
        sf::st_transform(crs = proj) %>%
        sf::st_union() %>%
        # sf::st_transform(crs = WGS84) %>%
        sf::st_as_sf() %>%
        rename(geometry = x) %>%
        mutate(FID = 0:(n() - 1)) %>%
        dplyr::select(FID, geometry)
    roads_wgs84 <- sf::st_transform(roads_proj, crs=WGS84)
    sf::st_write(roads_proj,
                 dsn = tmp,
                 layer = 'roads',
                 driver = 'ESRI Shapefile',
                 delete_layer = TRUE,
                 silent = TRUE)
} else {
    streams_proj <- sf::st_read(dsn = streams_f,
                           crs = proj)
                           # crs = WGS84)
    roads_proj <- sf::st_read(dsn = roads_f,
                         crs = proj)
                         # crs = WGS84)
}

# mv(dem) + mv(roads, color='gray') + mv(streams)

# watershed (whitebox) ####

# raster::writeRaster(x = dem,
terra::writeRaster(x = dem_proj,
                   filename = dem_f,
                   overwrite = TRUE)
sf::st_write(obj = site_proj,
             dsn = point_f,
             delete_layer = TRUE,
             quiet = TRUE,
             driver = 'ESRI Shapefile')
# mv(dem_proj) + mv(streams_proj, color='blue') + mv(roads_proj, color='gray') + mv(site_proj, col.regions='green')

whitebox::wbt_fill_single_cell_pits(dem = dem_f,
                                    output = dem_f)

#might be better in rare cases (open pit mine)
# whitebox::wbt_fill_depressions(dem = dem_f,
#                                output = dem_f)
                               # # flat_increment = 0.01)
# whitebox::wbt_breach_depressions(dem = dem_f,
#                                  output = dem_f,
#                                  flat_increment = 0.01)
#claims to be the best method, but leaves holes in the delineated output
whitebox::wbt_breach_depressions_least_cost(dem = dem_f,
                                            output = dem_f,
                                            dist = 10000, #maximum trench length
                                            fill = TRUE)
                                            # flat_increment = 0.01)

# dem_conditioned <- raster::raster(dem_f)
# mv(dem_conditioned) + mv(streams_proj, color='blue') + mv(roads_proj, color='gray') + mv(site_proj, col.regions='black')

whitebox::wbt_burn_streams_at_roads(dem = dem_f,
                                    streams = streams_f,
                                    roads = roads_f,
                                    output = dem_f,
                                    width = 50)
whitebox::wbt_fill_burn(dem = dem_f,
                        streams = streams_f,
                        output = dem_f)
# dem_conditioned <- raster::raster(dem_f)
# mv(dem_conditioned) + mv(streams_proj, color='blue') + mv(roads_proj, color='gray') + mv(site_proj, col.regions='black')

whitebox::wbt_d8_pointer(dem = dem_f,
                         output = d8_f)

whitebox::wbt_d8_flow_accumulation(input = dem_f,
                                   output = flow_f,
                                   out_type = 'catchment area')

snap1_f <- glue(tmp, '/snap1_jenson_dist150.shp')
# whitebox::wbt_jenson_snap_pour_points(pour_pts = point_f,
#                                       streams = flow_f,
#                                       output = snap1_f,
#                                       snap_dist = 150)
whitebox::wbt_snap_pour_points(pour_pts = point_f,
                               flow_accum = flow_f,
                               output = snap1_f,
                               snap_dist = 150)
# flow_dem <- raster::raster(flow_f)
# snap1 <- sf::st_read(snap1_f, quiet = F)
# mv(flow_dem) + mv(streams_proj, color='blue') + mv(roads_proj, color='gray') +
#     mv(site_proj, col.regions='gray') + mv(snap1, col.region='black')

wb_f <- glue('{path}/wb{n}_buffer{b}_{typ}_dist{dst}.tif',
             path = tmp,
             n = 1,
             b = buffer_radius,
             typ = 'jenson',
             dst = 150)

# terra::rast(d8_f) %>%
#     terra::project(y = WGS84) %>%
#     # raster::raster()
# # plot(d8_dem); par(new=T); plot(gg2)
#     terra::writeRaster(filename = d8_f,
#                        overwrite = TRUE)
# sf::st_read(snap1_f, quiet = TRUE) %>%
#     sf::st_transform(crs = WGS84) %>%
#     sf::st_write(dsn = snap1_f,
#                  delete_layer = TRUE,
#                  quiet = TRUE,
#                  driver = 'ESRI Shapefile')

whitebox::wbt_watershed(d8_pntr = d8_f,
                        pour_pts = snap1_f,
                        output = wb_f)
# d8_dem <- raster::raster(d8_f)
# mv(d8_dem) + mv(streams_proj, color='blue') + mv(roads_proj, color='gray') +
#     mv(site_proj, col.regions='gray') + mv(snap1, col.region='black')

# wb <- raster::raster(wb_f)
wb <- terra::rast(wb_f)

#write and record temp files for the technician to visually inspect
wb_sf <- wb %>%
    raster::raster() %>%
    raster::rasterToPolygons() %>%
    sf::st_as_sf()
    # sf::st_buffer(dist = 0.1) %>%
    # sf::st_union() %>%
    # sf::st_as_sf()#again? ugh.

# wb_sf <- sf::st_transform(wb_sf, 4326) #EPSG for WGS84

wb_sf_f <- glue('{path}/wb{n}_BUF{b}{typ}DIST{dst}RES{res}.shp',
                path = inspection_dir,
                n = 1,
                b = buffer_radius,
                typ = 'jenson',
                dst = '150',
                res = dem_resolution)

wb %>%
    terra::as.polygons() %>%
    terra::writeVector(filename = wb_sf_f,
                       overwrite = TRUE)

# sf::st_write(obj = wb_sf,
#              dsn = wb_sf_f,
#              delete_dsn = TRUE,
#              quiet = TRUE)

# zz = sf::st_read(wb_sf_f, crs=WGS84)
dem_conditioned <- raster::raster(dem_f)
d8_dem <- raster::raster(d8_f)
snap1 <- sf::st_read(snap1_f, quiet = F)
mv(dem_conditioned) + mv(d8_dem) + mv(roads_proj, color='gray') + mv(streams_proj, color='blue') +
    mv(wb_sf) + mv(snap1, col.region='black') + mv(site_proj, col.region='gray')


# raster intersection summary ####

wb_rast <- raster::raster(wb) %>%
    raster::setValues(values = 1:ncell(.)) %>%
    raster::projectRaster()
plot(dem_proj, xlim=c(1500000, 1520000), ylim=c(-50000, -28000))
par(new=T)
plot(raster::raster(wb), col='red', xlim=c(1500000, 1520000), ylim=c(-50000, -28000))

# wb_rast <- raster::raster(wb_f)

raster_intersection_summary <- function(wb, dem){

    #wb is a delineated watershed boundary as a rasterLayer
    #dem is a DEM rasterLayer

    summary_out <- list()

    #convert wb to sf object
    wb <- wb %>%
        raster::rasterToPolygons() %>%
        sf::st_as_sf()

    #get edge of DEM as sf object
    dem_edge <- raster::boundaries(dem) %>%
        raster::reclassify(matrix(c(0, NA),
                                  ncol = 2)) %>%
        raster::rasterToPolygons() %>%
        sf::st_as_sf()

    #tally raster cells
    summary_out$n_wb_cells <- length(wb$geometry)
    summary_out$n_dem_cells <- length(dem_edge$geometry)

    #tally intersections; calc percent of wb cells that overlap
    intersections <- sf::st_intersects(wb, dem_edge) %>%
        as.matrix() %>%
        apply(MARGIN = 2,
              FUN = sum) %>%
        table()

    true_intersections <- sum(intersections[names(intersections) > 0])

    summary_out$n_intersections <- true_intersections
    summary_out$pct_wb_cells_intersect <- true_intersections /
        summary_out$n_wb_cells * 100

    return(summary_out)
}

smry <- raster_intersection_summary(wb = wb_rast,
                                    dem = dem_proj)

# watershed (RSAGA) ####
