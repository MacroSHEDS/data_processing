loginfo('Beginning retrieve',
        logger = logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve',
                              get_statuses = 'ready') %>%
    filter(! grepl(pattern = '^VERSIONLESS',
                   x = prodcode))

if(! is.null(prodname_filter)){
    prod_info <- filter(prod_info, prodname %in% prodname_filter)
}

neon_streams <- site_data %>%
    filter(domain == 'neon',
           site_type == 'stream_gauge',
           in_workflow == 1) %>%
    pull(site_code)

for(i in seq_len(nrow(prod_info))){

    prodcode <- prod_info$prodcode[i]

    prodname_ms <<- paste0(prod_info$prodname[i],
                           '__',
                           prodcode)

    prod_specs <- get_neon_product_specs(prodcode)
    if(is_ms_err(prod_specs)) next

    held_data <<- get_data_tracker(network = network,
                                   domain = domain)

    if(! product_is_tracked(tracker = held_data,
                            prodname_ms = prodname_ms)){

        held_data <<- track_new_product(tracker = held_data,
                                        prodname_ms = prodname_ms)
    }

    # avail_sets <- sm(get_avail_neon_product_sets(prod_specs$prodcode_full))
    # if(is_ms_err(avail_sets)) next
    #
    # #retrieve data by site; log acquisitions and revisions
    # avail_sites <- unique(avail_sets$site_code)
    #
    # #filter for only neon stream sites
    # avail_sites <- avail_sites[avail_sites %in% neon_streams]

    for(j in 1:length(avail_sites)){

        site_code <- avail_sites[j]
        # avail_site_sets <- avail_sets[avail_sets$site_code == site_code, ,
        #     drop = FALSE]

        #2024 update: no longer retrieving by site-month
        #assuming no munging issues on server side
        #should reduce total data stored on disk (no need to duplicate metadata files)
        #will also simplify QC assimilation.
        #neon data change without version increment, so we need to retrieve the whole set each year
        # avail_site_sets <- avail_site_sets %>%
        #     mutate(component = lubridate::ym(component)) %>%
        #     group_by(site_code) %>%
        #     summarize(start_date = min(component),
        #               end_date = max(component)) %>%
        #     ungroup() %>%
        #     mutate(across(-site_code, ~substr(., 1, 7))) %>%
        #     mutate(component = paste(start_date, end_date, sep = '_')) %>%
        #     select(-ends_with('date')) %>%
        #     mutate(prodcode_full = prod_specs$prodcode_full) %>%
        #     relocate(prodcode_full, .before = 'site_code')

        if(! site_is_tracked(tracker = held_data,
                             prodname_ms = prodname_ms,
                             site_code = site_code)){

            held_data <<- insert_site_skeleton(
                tracker = held_data,
                prodname_ms = prodname_ms,
                site_code = site_code,
                site_components = 'placeholder'
                # site_components = avail_site_sets$component
            )
        }

        # held_data <<- track_new_site_components(tracker = held_data,
        #                                         prodname_ms = prodname_ms,
        #                                         site_code = site_code,
        #                                         avail = avail_site_sets)
        # if(is_ms_err(held_data)) next

        #this was first designed for site-month retrieval.
        #now retrieving by site, and always retrieving (because neon products are updated without version increment)

        # retrieval_details <- populate_set_details(tracker = held_data,
        #                                           prodname_ms = prodname_ms,
        #                                           site_code = site_code,
        #                                           avail = avail_site_sets)
        # if(is_ms_err(retrieval_details)) next
        #
        # new_sets <- filter_unneeded_sets(tracker_with_details = retrieval_details)
        #
        # if(nrow(new_sets) == 0){
        if(nrow(avail_site_sets) == 0){
            loginfo(glue('Nothing to do for {s} {p}',
                         s = site_code,
                         p = prodname_ms),
                    logger = logger_module)
            next
        } else {
            loginfo(glue('Retrieving {s} {p}',
                         s = site_code,
                         p = prodname_ms),
                    logger = logger_module)
        }

        update_data_tracker_r(network = network,
                              domain = domain,
                              tracker = held_data)

        warning('still gotta pass prodcode_full into get_neon_data, and then on to the kernel')
        browser()
        get_neon_data(domain = domain,
                      # sets = new_sets,
                      # sets = avail_site_sets,
                      site_code = site_code,
                      tracker = held_data)

        if(! is.na(prod_info$munge_status[i])){
            update_data_tracker_m(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  prodname_ms = prodname_ms,
                                  site_code = site_code,
                                  new_status = 'pending')
        }
    }

    write_metadata_r(murl = 'data.neonscience.org (downloaded via neonUtilities)',
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

    gc()
}

loginfo('Retrieval complete for all sites and products',
        logger = logger_module)
