loginfo('Beginning retrieve', logger=logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve',
                              get_statuses = 'ready')

# i=2
for(i in 1:nrow(prod_info)){
# for(i in 1){

    prodname_ms <<- glue(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data <<- get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        held_data <<- track_new_product(held_data, prodname_ms)
    }

    latest_vsn <- get_latest_product_version(prodname_ms = prodname_ms,
                                             domain = domain,
                                             data_tracker = held_data)

    if(is_ms_err(latest_vsn)) next

    avail_sets = get_avail_lter_product_sets(prodname_ms = prodname_ms,
                                             version = latest_vsn,
                                             domain = domain,
                                             data_tracker = held_data)

    if(is_ms_err(avail_sets)) next

    if(grepl('(precip|stream_chemistry)', prodname_ms)){
        avail_sets$site_name <- 'sitename_NA'
    }

    avail_sites <- unique(avail_sets$site_name)

    # j=1
    for(j in 1:length(avail_sites)){

        site_name <- avail_sites[j]
        avail_site_sets <- avail_sets[avail_sets$site_name == site_name, ,
            drop=FALSE]

        if(! site_is_tracked(held_data, prodname_ms, site_name)){

            held_data <<- insert_site_skeleton(held_data, prodname_ms, site_name,
                site_components=avail_site_sets$component)
        }

        held_data <<- track_new_site_components(held_data, prodname_ms, site_name,
            avail_site_sets)

        if(is_ms_err(held_data)) next

        retrieval_details <- populate_set_details(held_data, prodname_ms,
            site_name, avail_site_sets, latest_vsn)

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

        get_lter_data(domain=domain, new_sets, held_data)

        if(! is.na(prod_info$munge_status[i])){
            update_data_tracker_m(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  prodname_ms = prodname_ms,
                                  site_name = site_name,
                                  new_status = 'pending')
        }
    }

    metadata_url <- glue('https://portal.lternet.edu/nis/mapbrowse?',
                         'packageid=knb-lter-hbr.{p}.{v}',
                         p = prodcode_from_prodname_ms(prodname_ms),
                         v = latest_vsn)

    write_metadata_r(murl = metadata_url,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)
    gc()
}

loginfo('Retrieval complete for all sites and products',
    logger=logger_module)
