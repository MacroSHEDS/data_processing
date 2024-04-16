#gis data from here:
#https://www.hydroshare.org/resource/b134a93a92ba482abc5176e029a6687a

allz = sf::st_read('~/Downloads/foo/all_features.dbf') %>%
    sf::st_as_sf(coords = c(y = 'long', x = 'lat'), crs = 4326)
q9 = filter(allz, grepl('[CO]9', Object_ID))
q11 = filter(allz, grepl('11', Object_ID))
def_gauges = filter(allz, type == 'Rain')
baro = filter(allz, type == 'barometric')

othz = sf::st_read('~/Downloads/foo/other_features.dbf') %>%
    sf::st_as_sf(coords = c(y = 'long', x = 'lat'), crs = 4326)

ss2 = sf::st_read('~/Downloads/foo/CalhounWSBoundaries/CalhounWS2Boundary.shp')
ss3 = sf::st_read('~/Downloads/foo/CalhounWSBoundaries/CalhounWS3Boundary.shp')
ss4 = sf::st_read('~/Downloads/foo/CalhounWSBoundaries/CalhounWS4Boundary.shp')
rg1 = sf::st_read('data/czo/calhoun/derived/precip_gauge_locations__ms001/reasearch_area_1/reasearch_area_1.shp')
mapview::mapview->mv
mv(allz) +
    mv(ss2, col.regions = 'green') + mv(ss3, col.regions = 'green') +
    mv(ss4, col.regions = 'green') + mv(rg1, col.regions = 'green') +
    mv(q9, col.regions = 'red') + mv(q11, col.regions = 'purple') +
    mv(def_gauges, col.regions = 'cyan') +
    mv(baro, col.regions = 'orange')

all_sites %>% pivot_wider(names_from = site, values_from = rain)->xx

xx
xxx = xts::xts(select(xx, -date),
               order.by = xx$date)
dygraphs::dygraph(xx)
