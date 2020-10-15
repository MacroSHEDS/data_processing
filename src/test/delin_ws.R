lat=44.21013; long=-122.2571; crs=4326; #dev_machine_status = 'n00b'; verbose = TRUE
mapview = mapview::mapview
# buffer_radius = 100; dem_resolution = 10; snap_method = 'standard'; snap_dist = 150

delineate_watershed_test1(lat, long, crs, buffer_radius = 100,
                          dem_resolution = 10, snap_method = 'jenson',
                          snap_dist = 150)

delineate_watershed_apriori(lat=lat, long=long, crs=crs,
                            dev_machine_status = 'n00b', verbose = TRUE)

ms_delineate(network, domain, dev_machine_status = 'n00b', verbose = TRUE)
