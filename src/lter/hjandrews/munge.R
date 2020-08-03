loginfo('Beginning munge', logger=logger_module)

prod_info = get_product_info(network=network, domain=domain,
    status_level='munge', get_statuses='ready')

# i=8
for(i in 1:nrow(prod_info)){
    # for(i in 2){

    prodname_ms = paste0(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        logwarn(glue('Product {p} is not yet tracked. Retrieve ',
            'it before munging it.', p=prodname_ms), logger=logger_module)
        next
    }

    sites = names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){

        site_name <- sites[j]

        munge_status <- get_munge_status(tracker = held_data,
                                         prodname_ms = prodname_ms,
                                         site_name = site_name)
        if(munge_status == 'ok'){
            loginfo(glue('Nothing to do for {s} {p}',
                         s=site_name, p=prodname_ms), logger=logger_module)
            next
        } else {
            loginfo(glue('Munging {s} {p}',
                         s=site_name, p=prodname_ms), logger=logger_module)
        }

        if(grepl('(precip|flux|chemistry|boundary|locations)',
                 prodname_ms)){
            munge_msg = munge_lter_combined_split(domain = domain,
                                                  site_name = site_name,
                                                  prodname_ms = prodname_ms,
                                                  tracker = held_data)
        } else if(grepl('discharge', prodname_ms)){
            #gotta figure out how to get hjandrews discharge product efficiently
            stop('havent figured out how to munge this product yet')
            # munge_msg = munge_lter_combined(domain = domain,
            #                                 site_name = site_name,
            #                                 prodname_ms = prodname_ms,
            #                                 tracker = held_data)
        } else { #probably won't ever use this munge engine for hjandrews
            munge_msg = munge_lter_site(domain, site_name, prodname_ms, held_data)
        }

        if(is_ms_err(munge_msg)){
            update_data_tracker_m(network=network, domain=domain,
                tracker_name='held_data', prodname_ms=prodname_ms,
                site_name=site_name, new_status='error')
        } else {

            if(! is.na(prod_info$derive_status[i])){
                update_data_tracker_d(network = network,
                                      domain = domain,
                                      tracker_name = 'held_data',
                                      prodname_ms = prodname_ms,
                                      site_name = site_name,
                                      new_status = 'pending')
            }

            invalidate_derived_products(successor_string = prod_info$precursor_of)
        }
    }

    gc()
}

loginfo('Munging complete for all sites and products',
    logger=logger_module)
