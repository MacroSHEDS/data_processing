#ms
lat0=44.43538
lon0=-72.038495
zz = sf::st_as_sf(tibble(lon=lon0, lat=lat0),
             coords = c('lon', 'lat'),
             crs = 4326)

#usgs
lat=44.4353353
lon=-72.03842899
z2 = sf::st_as_sf(tibble(lon=lon, lat=lat),
             coords = c('lon', 'lat'),
             crs = 4326) %>%
    sf::st_transform(4326)

mapview::mapview(zz) + mapview::mapview(z2)

