zz = system('find data/ -type f -name "*.feather" -path "*/precipitation*/*"',
       intern = TRUE)

dd = tibble()
for(f in zz){
    domain = str_extract(f, 'data/[a-z_]+/([a-z_]+)', group = 1)
    site = str_extract(f, '/([\\w\\.\\-_]+?)\\.feather$', group = 1)
    d = read_feather(f)
    interps = tryCatch(sum(d$ms_interp == 1),
                       warning = function(w){
                           print(domain)
                           print(site)
                       })
    # dd <- bind_rows(dd, tibble(domain = domain,
    #                            site = site,
    #                            interps = interps))
    # readLines(n=1)
}
dd->dd_

good_dmns = filter(dd, interps == 0) %>%
    print(n=500) %>%
    pull(domain)
bad_dmns = filter(dd, interps > 1) %>%
    print(n=500) %>%
    pull(domain)

intersect(good_dmns, bad_dmns)
setdiff(good_dmns, bad_dmns)
setdiff(bad_dmns, good_dmns)
