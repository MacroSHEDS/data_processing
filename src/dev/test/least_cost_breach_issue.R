library(tidyverse)
library(raster)
library(whitebox)
library(sf)
library(elevatr)

lat <- 42.56257
long <- -71.10933
proj4 <- '+proj=cea +lon_0=-71.1093305 +lat_ts=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs'

tmp <- tempdir()
dem_f <- paste0(tmp, '/dem.tif')
point_f <- paste0(tmp, '/point.shp')

site <- tibble(x = lat, y = long) %>%
    sf::st_as_sf(coords = c("y", "x"), crs = 4326) %>%
    sf::st_transform(proj4)

site_buf <- sf::st_buffer(x = site, dist = 1000)
dem <- elevatr::get_elev_raster(locations = site_buf, z = 10, verbose = FALSE)

test <- function(){

    raster::writeRaster(x = dem, filename = dem_f, overwrite = TRUE)
    sf::st_write(obj = site, dsn = point_f, delete_layer = TRUE, quiet = TRUE)

    whitebox::wbt_fill_single_cell_pits(dem = dem_f, output = dem_f)

    whitebox::wbt_breach_depressions_least_cost(
        dem = dem_f,
        output = dem_f,
        dist = 10000,
        fill = TRUE)

    return(raster::values(raster::raster(dem_f)))
}

vals_1 <- test()
vals_2 <- test()

any(na.omit(vals_1 != vals_2))
