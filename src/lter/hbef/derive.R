loginfo('Beginning derive', logger=logger_module)

prod_info = get_product_info(network=network, domain=domain,
    status_level='derive', get_statuses='pending')
    # status_level='derive', get_statuses='ready')

# i=2
for(i in 1:nrow(prod_info)){
# for(i in 2){

    prodname_ms = paste0(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        if(grepl('ms[0-9]{3}$', prodname_ms)){
            held_data = track_new_product(held_data, prodname_ms)
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
        loginfo(glue('Nothing to do for {s} {p}',
            s=site_name, p=prodname_ms), logger=logger_module)
        next
    }

    sites = names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){

        #UPDATE THIS AFTER WRITING DERIVE KERNELS
        if(grepl('(precip|stream_chemistry)', prodname_ms)){
            munge_msg = munge_hbef_combined(domain, sites[j], prodname_ms,
                held_data)
        } else {
            munge_msg = munge_hbef_site(domain, sites[j], prodname_ms, held_data)
        }

        if(is_ms_err(derive_msg)){
            update_data_tracker_d(network=network, domain=domain,
                tracker_name='held_data', prodname_ms=prodname_ms,
                site_name=sites[j], new_status='error')
        }
    }

    gc()
}

loginfo('Derive complete for all sites and products',
    logger=logger_module)
