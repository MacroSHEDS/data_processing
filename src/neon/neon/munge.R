prod_info = get_product_info(network=network, domain=domain,
    status_level='munge', get_statuses='ready')

# i=1; j=1; k=1
for(i in 1:nrow(prod_info)){
# for(i in 2){

    prodname_ms = paste0(prod_info$prodname[i], '_', prod_info$prodcode[i])

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        logging::logwarn(glue('Product {p} is not yet tracked. Retrieve ',
            'it before munging it.', p=prodname_ms), logger=logger_module)
        next
    }

    sites = names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){

        munge_msg = munge_neon_site(domain, sites[j], prodname_ms, held_data)

        if(is_ms_err(munge_msg)){
            update_data_tracker_m(network=network, domain=domain,
                tracker_name='held_data', prod=prodname_ms, site=sites[j],
                new_status='error')
        }
    }

    gc()
}
