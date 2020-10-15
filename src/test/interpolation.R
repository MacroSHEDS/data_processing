library(tidyverse)
library(stars)
library(sf)
library(lubridate)
library(readr)
library(raster)
library(sp)
library(mapview)
library(gstat)


## Read in data
rain_location <- st_read("data_in/hbef_raingage")

watersheds <- st_read("data_in/hbef_wsheds")

rain_raw <- read_csv("data_in/hbef_precip.csv") %>%
    rename(date = 1, ID = 2, precip = 3)

## Summarise data
rain_annual <- rain_raw %>%
    mutate(date = ymd(date)) %>%
    filter(year(date) == 2004) %>%
    group_by(ID) %>%
    summarise(annual = sum(precip, na.rm = T))

rain_join <- left_join(rain_location, rain_annual) %>%
    st_transform(102008)

## Interpolation
v = variogram(annual~1, rain_join)
m = fit.variogram(v, vgm(1, "Sph"))

ws_grid <- dem

#May not work if you have an updated stars but not an updated sf
ws_stars <- st_as_stars(ws_grid)

interp = krige(formula = annual~1, rain_join, ws_stars, model = m)

plot(interp)

test <- st_as_sf(interp)

mapview(test, zcol = "var1.pred", lwd = 0)

mapview(rain_join, zcol = "annual")


# IDW
test <- dem
gs <- gstat(formula=annual~1, locations=rain_join)
idw <- interpolate(test, gs)

idw_mask <- mask(idw, ws8)

idw_trm <- trim(idw_mask)





