#notice, in LTER parlance, our domains are "sites", our prodcodes
#are "identifiers", our components are "elements", and each element has an "element id"

prod_info = get_product_info(network=network, domain=domain,
    # status_level='retrieve', get_statuses='ready')
    status_level='retrieve', get_statuses='pending')

# i=1; j=2
# prod_info = prod_info[c(1, 1, 1, 1, 1),]
# for(i in 1:nrow(prod_info)){
for(i in 1){

    prodname_ms = paste0(prod_info$prodname[i], '_', prod_info$prodcode[i])

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        held_data = track_new_product(held_data, prodname_ms)
    }

    latest_vsn = get_latest_product_version(prodname=prodname_ms,
        domain=domain, data_tracker=held_data)
    if(is_ms_err(latest_vsn)) next

    avail_sets = get_avail_lter_product_sets(prodname=prodname_ms,
        version=latest_vsn, domain=domain, data_tracker=held_data)
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
            curr_site, avail_site_sets, latest_vsn)
        if(is_ms_err(retrieval_details)) next

        new_sets = filter_unneeded_sets(retrieval_details)

        if(nrow(new_sets) == 0){
            loginfo(glue('Nothing to do for {s} {n}',
                    s=curr_site, n=prodname_ms), logger=logger_module)
            next
        }

        update_data_tracker_r(network=network, domain=domain, tracker=held_data)

        get_lter_data(domain=domain, new_sets, held_data)
    }

    gc()
}
