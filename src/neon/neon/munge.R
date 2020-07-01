prod_info = get_product_info(network=network,
    status_level='munge', get_statuses='ready')

# i=1; j=1; k=2
for(i in 1:nrow(prod_info)){
# for(i in 2){

    prodname_ms = paste0(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        logwarn(glue('Product {p} is not yet tracked. Retrieve ',
            'it before munging it.', p=prodname_ms), logger=logger_module)
        next
    }

    munge_status <- get_munge_status(tracker = held_data,
                                     prodname_ms = prodname_ms,
                                     site_name = site_name)

    if(munge_status == 'ok'){
        loginfo(glue('Nothing to do for {s} {p}',
            s=site_name, p=prodname_ms), logger=logger_module)
        next
    }

    if(! is.na(prod_info$derive_status[i])){
        update_data_tracker_d(network = network,
                              domain = domain,
                              tracker_name = 'held_data',
                              prodname_ms = prodname_ms,
                              site_name = site_name,
                              new_status = 'pending')
    }

    sites = names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){
    # for(j in 2){

        munge_msg = munge_neon_site(domain, sites[j], prodname_ms, held_data)

        if(is_ms_err(munge_msg)){
            update_data_tracker_m(network=network, domain=domain,
                tracker_name='held_data', prodname_ms=prodname_ms, site_name=sites[j],
                new_status='error')
        }
    }

    gc()
}

loginfo('Munging complete for all sites and products',
    logger=logger_module)

