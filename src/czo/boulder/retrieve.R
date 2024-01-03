loginfo('Beginning retrieve', logger=logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve',
                              get_statuses = 'ready')

if(! is.null(prodname_filter)){
    prod_info <- filter(prod_info, prodname %in% prodname_filter)
}

# i=3
for(i in seq_len(nrow(prod_info))){

    prodname_ms <<- glue(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data <<- get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        held_data <<- track_new_product(held_data, prodname_ms)
    }

    hydroshare_code <- prod_info$hydroshare_id[i]

    latest_vsn <<- get_czo_product_version(prodname_ms = prodname_ms,
                                           domain = domain,
                                           hydroshare_code = hydroshare_code,
                                           data_tracker = held_data)

    if(is_ms_err(latest_vsn)) next

    component <- prod_info$component[i]

    component <- get_czo_components(search_string = component,
                                    hydroshare_code = hydroshare_code)

    avail_sets <- construct_czo_product_sets(hydroshare_code = hydroshare_code,
                                             component = component,
                                             data_tracker = held_data,
                                             latest_vsn = latest_vsn)

    if(is_ms_err(avail_sets)) next

    avail_sites <- unique(avail_sets$site_code)

    #j=1
    for(j in 1:length(avail_sites)){

        site_code <- avail_sites[j]
        avail_site_sets <- avail_sets[avail_sets$site_code == site_code,]

        if(! site_is_tracked(held_data, prodname_ms, site_code)){
            held_data <<- insert_site_skeleton(held_data, prodname_ms, site_code,
                                               site_components=avail_site_sets$component)
        }

        held_data <<- track_new_site_components(held_data, prodname_ms, site_code,
                                                avail_site_sets)

        if(is_ms_err(held_data)) next

        retrieval_details <- populate_set_details(held_data, prodname_ms,
                                                  site_code, avail_site_sets)

        if(is_ms_err(retrieval_details)) next

        new_sets <- filter_unneeded_sets(retrieval_details)

        if(nrow(new_sets) == 0){
            loginfo(glue('Nothing to do for {s} {p}',
                         s=site_code, p=prodname_ms), logger=logger_module)
            next
        } else {
            loginfo(glue('Retrieving {s} {p}',
                         s=site_code, p=prodname_ms), logger=logger_module)
        }

        update_data_tracker_r(network=network, domain=domain, tracker=held_data)

        get_czo_data(domain=domain, new_sets, held_data)

        if(! is.na(prod_info$munge_status[i])){
            update_data_tracker_m(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  prodname_ms = prodname_ms,
                                  site_code = site_code,
                                  new_status = 'pending')

        }
    }

    write_metadata_r(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

    gc()
}

loginfo('Retrieval complete for all sites and products',
        logger=logger_module)
