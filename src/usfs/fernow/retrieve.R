loginfo('Beginning retrieve', logger=logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve',
                              get_statuses = 'ready')

# i=1
for(i in 1:nrow(prod_info)){

    prodname_ms <<- glue(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data <<- get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        held_data <<- track_new_product(held_data, prodname_ms)
    }

    usfs_url <- prod_info$usfs_link[i]

    latest_vsn <- 'TEST'

    avail_sets <- construct_usfs_product_sets(prod_url = usfs_url,
                                              prodname = prodname_ms)

    if(is_ms_err(avail_sets)) next

    avail_sites <- unique(avail_sets$site_name)

    # j=1
    for(j in 1:length(avail_sites)){

        site_name <- avail_sites[j]
        avail_site_sets <- avail_sets[avail_sets$site_name == site_name,]

        if(! site_is_tracked(held_data, prodname_ms, site_name)){
            held_data <<- insert_site_skeleton(held_data, prodname_ms, site_name,
                                               site_components=avail_site_sets$component)
        }

        held_data <<- track_new_site_components(held_data, prodname_ms, site_name,
                                                avail_site_sets)

        if(is_ms_err(held_data)) next

        retrieval_details <- populate_set_details(held_data, prodname_ms,
                                                  site_name, avail_site_sets,
                                                  latest_vsn)

        if(is_ms_err(retrieval_details)) next

        new_sets <- filter_unneeded_sets(retrieval_details)

        if(nrow(new_sets) == 0){
            loginfo(glue('Nothing to do for {s} {p}',
                         s=site_name, p=prodname_ms), logger=logger_module)
            next
        } else {
            loginfo(glue('Retrieving {s} {p}',
                         s=site_name, p=prodname_ms), logger=logger_module)
        }

        update_data_tracker_r(network=network, domain=domain, tracker=held_data)

        get_usfs_data(domain, new_sets, held_data)

        if(! is.na(prod_info$munge_status[i])){
            update_data_tracker_m(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  prodname_ms = prodname_ms,
                                  site_name = site_name,
                                  new_status = 'pending')

        }
    }

    url_split <- str_replace(str_split_fixed(usfs_url, '/', n = Inf)[1,], 'products', 'Catalog')

    metadata_url <- paste(url_split[1:length(url_split)-1], collapse = '/')

    write_metadata_r(murl = metadata_url,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

    gc()
}

loginfo('Retrieval complete for all sites and products',
        logger=logger_module)
