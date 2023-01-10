loginfo('Beginning retrieve', logger=logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve', 
                              get_statuses = 'ready')

if(! is.null(prodname_filter)){
    prod_info <- filter(prod_info, prodname %in% prodname_filter)
}

# i=9
for(i in seq_len(nrow(prod_info))){

    prodname_ms <<- glue(prod_info$prodname[i], '__', prod_info$prodcode[i])

    held_data <<- get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        held_data <<- track_new_product(held_data, prodname_ms)
    }

    latest_vsn <- get_latest_product_version(prodname_ms=prodname_ms,
        domain=domain, data_tracker=held_data)
    if(is_ms_err(latest_vsn)) next

    avail_sets <- get_avail_lter_product_sets(prodname_ms=prodname_ms,
        version=latest_vsn, domain=domain, data_tracker=held_data)
    if(is_ms_err(avail_sets)) next

    if(grepl('(discharge|precip|flux|chemistry|boundary|locations)',
             prodname_ms)){
        avail_sets$site_code <- 'sitename_NA'
    }
    avail_sites <- unique(avail_sets$site_code)

    # j=1
    for(j in 1:length(avail_sites)){

        site_code <- avail_sites[j]
        avail_site_sets <- avail_sets[avail_sets$site_code == site_code, ,
            drop=FALSE]

        if(! site_is_tracked(held_data, prodname_ms, site_code)){
            held_data <<- insert_site_skeleton(held_data, prodname_ms, site_code,
                site_components=avail_site_sets$component)
        }

        held_data <<- track_new_site_components(held_data, prodname_ms, site_code,
            avail_site_sets)
        if(is_ms_err(held_data)) next

        retrieval_details <- populate_set_details(held_data, prodname_ms,
            site_code, avail_site_sets, latest_vsn)
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

        get_lter_data(domain=domain, new_sets, held_data)

        if(! is.na(prod_info$munge_status[i])){
            update_data_tracker_m(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  prodname_ms = prodname_ms,
                                  site_code = site_code,
                                  new_status = 'pending')
        }
    }

    metadata_url <- glue('https://portal.lternet.edu/nis/mapbrowse?',
                         'packageid=knb-lter-and.{p}.{v}',
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
