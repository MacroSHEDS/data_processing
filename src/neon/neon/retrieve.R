prod_info = get_product_info(network=network,
    status_level='retrieve', get_statuses='ready')

# i=5; j=1
for(i in 1:nrow(prod_info)){
# for(i in 4){

    prodname_ms = paste0(prod_info$prodname[i], '_', prod_info$prodcode[i])
    prod_specs = get_neon_product_specs(prod_info$prodcode[i])
    if(is_ms_err(prod_specs)) next

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        held_data = track_new_product(held_data, prodname_ms)
    }

    avail_sets = sm(get_avail_neon_product_sets(prod_specs$prodcode_full))
    if(is_ms_err(avail_sets)) next

    #retrieve data by site; log acquisitions and revisions
    avail_sites = unique(avail_sets$site_name)
    for(j in 1){
    # for(j in 1:length(avail_sites)){

        curr_site = avail_sites[j]
        avail_site_sets = avail_sets[avail_sets$site_name == curr_site, ,
            drop=FALSE]

        if(! site_is_tracked(held_data, prodname_ms, curr_site)){
            held_data = insert_site_skeleton(held_data, prodname_ms, curr_site,
                site_components=avail_site_sets$component)
        }

        held_data = track_new_site_components(held_data, prodname_ms, curr_site,
            avail_site_sets)
        if(is_ms_err(held_data)) next

        retrieval_details = populate_set_details(held_data, prodname_ms,
            curr_site, avail_site_sets)
        if(is_ms_err(retrieval_details)) next

        new_sets = filter_unneeded_sets(retrieval_details)

        if(nrow(new_sets) == 0){
            loginfo(glue('Nothing to do for {s} {n}',
                    s=curr_site, n=prodname_ms), logger=logger_module)
            next
        }

        update_data_tracker_r(network=network, domain=domain, tracker=held_data)

        get_neon_data(domain=domain, new_sets, held_data)
    }

    gc()
    loginfo('Retrieval complete for all sites and products',
        logger=logger_module)
}
