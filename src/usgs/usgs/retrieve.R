loginfo('Beginning retrieve', logger=logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve',
                              get_statuses = 'ready')

if(! is.null(prodname_filter)){
    prod_info <- filter(prod_info, prodname %in% prodname_filter)
}

# In the future when we want to get all small usgs sites, this could be changed
usgs_sites <- c('black_earth_creek' = '05406457')

# i=1
for(i in seq_len(nrow(prod_info))){

    prodname_ms <<- glue(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data <<- get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        held_data <<- track_new_product(held_data, prodname_ms)
    }

    usgs_code <- get_usgs_codes(prodname_ms = prodname_ms)

    # j=1
    for(j in 1:length(usgs_sites)){

        site_name <- names(usgs_sites[j])

        latest_vsn <- get_usgs_verstion(prodname_ms = prodname_ms,
                                        domain = domain,
                                        usgs_code = usgs_code,
                                        usgs_site = usgs_sites[j])

        avail_site_sets <- latest_vsn %>%
            select(url, site_name, component)

        if(! site_is_tracked(held_data, prodname_ms, site_name)){
            held_data <<- insert_site_skeleton(tracker = held_data,
                                               prodname_ms = prodname_ms,
                                               site_name = site_name,
                                               site_components = avail_site_sets$component)
        }

        held_data <<- track_new_site_components(held_data,
                                                prodname_ms,
                                                site_name,
                                                avail_site_sets)

        if(is_ms_err(held_data)) next

        retrieval_details <<- populate_set_details(tracker = held_data,
                                                   prodname_ms = prodname_ms,
                                                   latest_vsn = latest_vsn,
                                                   site_name = site_name)

        if(is_ms_err(retrieval_details)) next

        new_sets <- filter_unneeded_sets(retrieval_details)

        if(nrow(new_sets) == 0){
            loginfo(glue('Nothing to do for {s} {p}',
                         s=site_name, p=prodname_ms), logger=logger_module)
            next
        }

        update_data_tracker_r(network=network, domain=domain, tracker=held_data)

        get_usgs_data(sets = new_sets)

        if(! is.na(prod_info$munge_status[i])){
            update_data_tracker_m(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  prodname_ms = prodname_ms,
                                  site_name = site_name,
                                  new_status = 'pending')

        }
    }

    metadata_url <- 'https://waterdata.usgs.gov/nwis'

    write_metadata_r(murl = metadata_url,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

    gc()
}

loginfo('Retrieval complete for all sites and products',
        logger=logger_module)
