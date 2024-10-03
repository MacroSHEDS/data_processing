zz = system("find data/ -path '*/derived/ws_boundary*/*.shp'", intern = TRUE)
dmns = str_extract(zz, 'data/[^/]+/([^/]+)/', group = 1)
for(dd in unique(dmns)){
    print(dd)
    shps = grep(paste0('/', dd, '/'), zz, value = TRUE)
    ms_code = unique(str_extract(shps, 'ws_boundary__ms([0-9]+)', group = 1))
    if(length(ms_code) != 1) stop('oi')
    if(ms_code == '000'){
        print(paste('heads up. we delineated this domain.', length(shps), 'sites'))
    } else {
        print('these delins were provided. all good')
        next
    }
    qj = map_dfr(shps, ~sf::st_read(., quiet = TRUE))
    print(mapview::mapview(qj))
    message('press any key for next')
    readLines(n=1)
}

#check some of the ones that have issues (santa_barbara: FK00, TE03, AT07; niwot: green5, martinelli, saddle
#   catalina: bigelow, marshallgulch; calhoun: weir_4)

z2 = system("find misc_backups/old_derive_dirs_rerun_20240605/calhoun_derived -path '*/ws_boundary*/*.shp'", intern = TRUE)
qj2 = map_dfr(z2, ~sf::st_read(., quiet = TRUE))
print(mapview::mapview(qj2))

z2 = system("find misc_backups/old_derive_dirs_rerun_20240605/niwot_derived -path '*/ws_boundary*/*.shp'", intern = TRUE)
qj2 = map_dfr(z2, ~sf::st_read(., quiet = TRUE))
print(mapview::mapview(qj2))

z2 = system("find misc_backups/old_derive_dirs_rerun_20240605/santa_barbara_derived -path '*/ws_boundary*/*.shp'", intern = TRUE)
qj2 = map_dfr(z2, ~sf::st_read(., quiet = TRUE))
print(mapview::mapview(qj2))
