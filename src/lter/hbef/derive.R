loginfo('Beginning derive', logger=logger_module)
site_name <- 'sitename_NA' #sites handled idiosyncratically within kernels

prod_info <- get_product_info(network = network,
                             domain = domain,
                             status_level = 'derive',
                             get_statuses = 'ready')

if(! is.null(prodname_filter)){
    prod_info <- filter(prod_info, prodname %in% prodname_filter)
}

# i=1
for(i in seq_len(nrow(prod_info))){

    prodname_ms <<- glue(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data <<- get_data_tracker(network = network,
                                 domain = domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        if(is_ms_prodcode(prodcode_from_prodname_ms(prodname_ms))){
            held_data <<- track_new_product(tracker = held_data,
                                           prodname_ms = prodname_ms)
            held_data <<- insert_site_skeleton(tracker = held_data,
                                             prodname_ms = prodname_ms,
                                             site_name = site_name,
                                             site_components = 'NA')
            update_data_tracker_d(network = network,
                                  domain = domain,
                                  tracker = held_data)
        } else {
            logwarn(glue('Product {p} is not yet tracked. Retrieve and munge ',
                'it before deriving from it.', p=prodname_ms), logger=logger_module)
            next
        }
    }

    derive_status <- get_derive_status(tracker = held_data,
                                       prodname_ms = prodname_ms,
                                       site_name = site_name)

    if(derive_status == 'ok'){
        loginfo(glue('Nothing to do for {p}',
                     p = prodname_ms),
                logger = logger_module)
        next
    } else {
        loginfo(glue('Deriving {p}',
                     p = prodname_ms),
                logger = logger_module)
    }

    prodcode <- prodcode_from_prodname_ms(prodname_ms)

    processing_func <- get(paste0('process_2_', prodcode))

    derive_msg <- sw(do.call(processing_func,
                             args = list(network = network,
                                         domain = domain,
                                         prodname_ms = prodname_ms)))

    stts <- ifelse(is_ms_err(derive_msg), 'error', 'ok')
    update_data_tracker_d(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          prodname_ms = prodname_ms,
                          site_name = site_name,
                          new_status = stts)

    if(stts == 'ok'){
        msg <- glue('Derived {p} ({n}/{d}/{s})',
                    p = prodname_ms,
                    n = network,
                    d = domain,
                    s = site_name)
        loginfo(msg, logger=logger_module)
    }

    write_metadata_d(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

    gc()
}

loginfo('Derive complete for all products',
        logger=logger_module)
