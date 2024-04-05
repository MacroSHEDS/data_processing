loginfo('Beginning munge',
        logger = logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'munge',
                              get_statuses = 'ready') %>%
    filter(! grepl('^VERSIONLESS', prodcode))

if(! is.null(prodname_filter)){
    prod_info <- filter(prod_info, prodname %in% prodname_filter)
}

if(nrow(prod_info) == 0) return()

for(i in seq_len(nrow(prod_info))){

    #it's possible that munge errors can cause raw neon data to be lost,
    #due to the fact that raw zips have to be copied to tempdir for preservation while
    #neonUtilities compiles them.
    #this will determine if data loss has happened. will not work on windows though.
    empty_rawdata_dirs <- system('find data/neon/neon/raw/ -type d -empty',
                                 intern = TRUE)
    if(length(empty_rawdata_dirs)){
        stop('empty dirs detected in data/neon/neon/raw:\n\t',
             paste(empty_rawdata_dirs, collapse = '\n\t'))
    }

    prodname_ms <<- paste0(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data <<- get_data_tracker(network = network,
                                   domain = domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        logwarn(glue('Product {p} is not yet tracked. Retrieve ',
                     'it before munging it.',
                     p = prodname_ms),
                logger = logger_module)
        next
    }

    sites <- names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){

        site_code <- sites[j]

        munge_status <- get_munge_status(tracker = held_data,
                                         prodname_ms = prodname_ms,
                                         site_code = site_code)

        if(munge_status == 'ok'){

            loginfo(glue('Nothing to do for {s} {p}',
                         s = site_code,
                         p = prodname_ms),
                    logger = logger_module)
            next

        } else {
            loginfo(glue('Munging {s} {p}',
                         s = site_code,
                         p = prodname_ms),
                    logger = logger_module)
        }

        munge_rtn <- munge_by_site(network = network,
                                   domain = domain,
                                   site_code = sites[j],
                                   prodname_ms = prodname_ms,
                                   tracker = held_data,
                                   keep_status = c('ok', 'pending'),
                                   interpolate_pchem = FALSE)

        if(is_ms_err(munge_rtn)){
            update_data_tracker_m(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  prodname_ms = prodname_ms,
                                  site_code = sites[j],
                                  new_status = 'error')

        } else {
            invalidate_derived_products(successor_string = prod_info$precursor_of[i])
        }
    }

    write_metadata_m(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     tracker = held_data)

    gc()
}

loginfo('Munging complete for all sites and products',
        logger = logger_module)

