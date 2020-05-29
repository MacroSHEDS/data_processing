Q = feather::read_feather('~/git/macrosheds/portal/data/hjandrews/discharge.feather')

q = filter(Q, site_name %in% c('GSWSMA', 'GSWSMF', 'GSWSMC'),
    datetime < as.POSIXct('1997-01-01')) %>%
    spread(site_name, Q) %>%
    filter_at(vars(-datetime, -GSWSMA), any_vars(! is.na(.))) %>%
    select(-datetime)

sumq = colSums(q)
GSWSMC_area = 580

(estimated_ws_area = sumq[c(1, 3)] * GSWSMC_area / sumq[2])
