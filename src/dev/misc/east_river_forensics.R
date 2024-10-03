
f1 = list.files('/tmp/RtmpvGYnYw/isotope_data_2014-2023/', full.names = T)
f2 = list.files('/tmp/RtmpvGYnYw/cation_data_2014-2023//', full.names = T)
f3 = list.files('/tmp/RtmpvGYnYw/anion_data_2014-2023//', full.names = T)
f4 = list.files('/tmp/RtmpvGYnYw/dic_npoc_data_2014-2023//', full.names = T)
f5 = list.files('/tmp/RtmpvGYnYw/tdn_ammonia_data_2015-2023//', full.names = T)

rgx = '([^_]+_[^_]+)'
# rgx = '([a-zA-Z0-9_]+?)_[a-zA-Z0-9]+\\.csv$'
ff1 = str_extract(basename(f1), rgx, group = 1)# %>% unique() %>% sort()
ff1 <- grep('splains|evans', ff1, value = TRUE, invert = TRUE)
ff2 = str_extract(basename(f2), rgx, group = 1)# %>% unique() %>% sort()
ff2 <- grep('splains|evans|blank', ff2, value = TRUE, invert = TRUE)
ff3 = str_extract(basename(f3), rgx, group = 1)# %>% unique() %>% sort()
ff3 <- grep('splains|evans|blank', ff3, value = TRUE, invert = TRUE)
ff4 = str_extract(basename(f4), rgx, group = 1)# %>% unique() %>% sort()
ff4 <- grep('splains|evans|blank', ff4, value = TRUE, invert = TRUE)
ff5 = str_extract(basename(f5), rgx, group = 1)# %>% unique() %>% sort()
ff5 <- grep('splains|evans|blank', ff5, value = TRUE, invert = TRUE)

#check consistency of site codes
table(c(ff1, ff2, ff3, ff4, ff5))

#look for sites we had in gsheet that weren't accounted for (these have been dropped)
srch = 'ph'
grep(srch, f1, value=T)
grep(srch, f2, value=T)
grep(srch, f3, value=T)
grep(srch, f4, value=T)
grep(srch, f5, value=T)

#check site locations
qqqq = filter(site_data, domain == 'east_river')
sf::st_as_sf(qqqq, coords = c(y = 'longitude', x = 'latitude'), crs = 4326) %>%
    mapview::mapview()

#what about depth?
ttt = grep('depth', f2, value=T)
unique(basename(ttt))
#which sites have it? any of ours?
str_extract(basename(ttt), rgx, group = 1) %>% unique() %>% sort()
#depth can be dropped for cations
ttt = grep('depth', f3, value=T)
str_extract(basename(ttt), rgx, group = 1) %>% unique() %>% sort()
ttt = grep('depth', f4, value=T)
str_extract(basename(ttt), rgx, group = 1) %>% unique() %>% sort()
ttt = grep('depth', f5, value=T)
str_extract(basename(ttt), rgx, group = 1) %>% unique() %>% sort()
#can always be dropped

#discharge site locations
kmzf = "/tmp/RtmpvGYnYw/onesite/locations_updated.kmz"
kmzf = "/tmp/RtmpvGYnYw/onesite/doc.kml"
kmz_data <- raster::shapefile("/tmp/RtmpvGYnYw/onesite/locations_updated.kmz")
sf_data <- st_read(kmzf)
mapview::mapview(sf_data)

cnrfc_basins <- readOGR(kmzf, "cnrfc_09122018_basins_thin")
