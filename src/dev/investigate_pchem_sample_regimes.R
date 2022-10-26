library(ggplot2)

#pchem
d = load_product('precip_chemistry', 'macrosheds_dataset')
doms = distinct(d, domain) %>% pull()

for(dom in doms){

    dd = filter(d, domain == !!dom)

    dd = dd %>%
        # filter(ms_interp == 0) %>%
        mutate(val = drop_errors(val)) %>%
        select(-ms_status, -network) %>%
        arrange(domain, site_code, datetime)

    gg = dd %>%
        mutate(val = ifelse(ms_interp == 0, val, -10)) %>%
        filter(datetime > max(dd$datetime) - 60 * 60 * 24 * 100) %>%
        ggplot() +
        geom_line(aes(x = datetime, y = val, color = var)) +
        labs(title = dom) +
        facet_wrap(vars(site_code))
    print(gg)
    readLines(n=1)

    gg = dd %>%
        filter(datetime > max(dd$datetime) - 60 * 60 * 24 * 100) %>%
        ggplot() +
        geom_line(aes(x = datetime, y = val, color = var)) +
        labs(title = dom) +
        facet_wrap(vars(site_code))
    print(gg)
    readLines(n=1)

    gg = dd %>%
        mutate(val = ifelse(ms_interp == 0, val, -10)) %>%
        filter(datetime > max(dd$datetime) - 2 * 60 * 60 * 24 * 365) %>%
        ggplot() +
        geom_line(aes(x = datetime, y = val, color = var)) +
        labs(title = dom) +
        facet_wrap(vars(site_code))
    print(gg)
    # filter(dd, site_code == 'WS78', datetime > as.Date('2015-07-01'), datetime < as.Date('2015-10-01'), var == 'GN_TP') %>%
    #     ggplot()+geom_point(aes(datetime, val, color = var))
    readLines(n=1)

    gg = dd %>%
        filter(datetime > max(dd$datetime) - 2 * 60 * 60 * 24 * 365) %>%
        ggplot() +
        geom_line(aes(x = datetime, y = val, color = var)) +
        labs(title = dom) +
        facet_wrap(vars(site_code))
    print(gg)
    readLines(n=1)

    # gg = dd %>%
    #     # mutate(val = ifelse(ms_interp == 1, NA_real_, val)) %>%
    #     ggplot() +
    #     geom_line(aes(x = datetime, y = val, color = var)) +
    #     labs(title = dom) +
    #     facet_wrap(vars(site_code))
    # print(gg)
    # readLines(n=1)
}

#precip
d = load_product('precipitation', 'macrosheds_dataset')
doms = distinct(d, domain) %>% pull()

for(dom in doms){

    dd = filter(d, domain == !!dom)

    dd = dd %>%
        # filter(ms_interp == 0) %>%
        mutate(val = drop_errors(val)) %>%
        select(-ms_status, -network) %>%
        arrange(domain, site_code, datetime)

    # gg = dd %>%
    #     mutate(val = ifelse(ms_interp == 0, val, -10)) %>%
    #     filter(datetime > max(dd$datetime) - 60 * 60 * 24 * 100) %>%
    #     ggplot() +
    #     geom_line(aes(x = datetime, y = val)) +
    #     labs(title = dom) +
    #     facet_wrap(vars(site_code))
    # print(gg)
    # readLines(n=1)
    #
    # gg = dd %>%
    #     filter(datetime > max(dd$datetime) - 60 * 60 * 24 * 100) %>%
    #     ggplot() +
    #     geom_line(aes(x = datetime, y = val)) +
    #     labs(title = dom) +
    #     facet_wrap(vars(site_code))
    # print(gg)
    # readLines(n=1)
    #
    # gg = dd %>%
    #     mutate(val = ifelse(ms_interp == 0, val, -10)) %>%
    #     filter(datetime > max(dd$datetime) - 2 * 60 * 60 * 24 * 365) %>%
    #     ggplot() +
    #     geom_line(aes(x = datetime, y = val)) +
    #     labs(title = dom) +
    #     facet_wrap(vars(site_code))
    # print(gg)
    # readLines(n=1)

    print(dom)
    qqq = filter(dd, datetime > max(dd$datetime) - 2 * 60 * 60 * 24 * 365)
    print(paste(sum(is.na(qqq$val)), nrow(qqq)))

    gg = dd %>%
        filter(datetime > max(dd$datetime) - 2 * 60 * 60 * 24 * 365) %>%
        ggplot() +
        geom_line(aes(x = datetime, y = val)) +
        labs(title = dom) +
        facet_wrap(vars(site_code))
    print(gg)
    # readLines(n=1)
    #
    # ggsave(glue('~/git/macrosheds/data_acquisition/plots/precip_coverage/{dom}.png'))
}


#investigate konza
bb = read_csv('data/lter/konza/raw/precipitation__4/sitename_NA/APT011.csv') %>%
    mutate(ppt = as.numeric(ppt))
bb %>%
    mutate(RecDate = as.Date(RecDate, format = '%d/%m/%Y')) %>%
    group_by(watershed) %>%
    summarize(n_recs = n(), mindate = min(RecDate, na.rm=T), maxdate = max(RecDate, na.rm=T))

bb %>%
    filter(! is.na(RecDate),
           duplicated(RecDate)) %>%
    mutate(RecDate = as.Date(RecDate, format = '%d/%m/%Y')) %>%
    arrange(watershed, RecDate) %>%
    ggplot() +
    geom_line(aes(x=RecDate, y=ppt)) +
    facet_wrap(vars(watershed))

bb %>%
    filter(! is.na(RecDate),
           duplicated(RecDate)) %>%
    mutate(RecDate = as.Date(RecDate, format = '%d/%m/%Y')) %>%
    filter(RecDate > as.Date('2020-01-01')) %>%
    arrange(watershed, RecDate) %>%
    ggplot() +
    geom_point(aes(x=RecDate, y=ppt)) +
    facet_wrap(vars(watershed))


dd2 = dd %>%
    filter(site_code == 'N01B') %>%
    mutate(date = as.Date(datetime)) %>%
    select(date, val)
bb2 = bb %>%
    filter(! is.na(RecDate),
           duplicated(RecDate)) %>%
    mutate(RecDate = as.Date(RecDate, format = '%d/%m/%Y')) %>%
    filter(RecDate > as.Date('2020-01-01')) %>%
    arrange(watershed, RecDate) %>%
    filter(watershed == 'K01B') %>%
    select(RecDate, ppt, Comments) %>%
    full_join(dd2, by = c('RecDate' = 'date')) %>%
    filter(if_any(c(ppt, val), ~!is.na(.))) %>%
    filter(RecDate > as.Date('2020-01-01')) %>%
    arrange(RecDate) %>%
    mutate(Comments = ifelse(! is.na(Comments), 'blue', 'black'))
plot(bb2$RecDate, bb2$ppt, col=bb2$Comments)
lines(bb2$RecDate, bb2$val, col = 'red')
bb3 = filter(bb2, RecDate > as.Date('2021-01-01'))

png('~/Desktop/konza_precip.png')
plot(bb3$RecDate, bb3$ppt, col=bb3$Comments)
lines(bb3$RecDate, bb3$val, col = 'red')
dev.off()
