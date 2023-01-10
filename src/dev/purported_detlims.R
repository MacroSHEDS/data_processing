
purported_detlims = list()
for(i in 1:nrow(network_domain)){
    ntw = network_domain$network[i]
    dmn = network_domain$domain[i]

    prodname_ms <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = ntw,
                              d = dmn))) %>%
        mutate(prodname_ms = paste(prodname, prodcode, sep = '__')) %>%
        pull(prodname_ms)

    pdl__ = c()
    for(j in seq_along(prodname_ms)){

        detlim = try(read_detection_limit(ntw, dmn, prodname_ms[j]),
                     silent = TRUE)

        if(inherits(detlim, 'try-error')) next

        pdl_ = c()
        for(k in length(detlim)){
            dl = sapply(detlim[[k]], function(x) x$lim)
            if(length(dl)){
                pdl_ = c(pdl_, dl)
            }
        }

        pdl__ <- c(pdl__, pdl_)
        # min(abs(na.omit(d$dat[d$dat != 0])))
    }
    purported_detlims[[i]] <- pdl__
}

zz = Reduce(function(x, y) c(unlist(x), unlist(y)), purported_detlims)

plot(density(na.omit(zz)), xlab='decimal place')
