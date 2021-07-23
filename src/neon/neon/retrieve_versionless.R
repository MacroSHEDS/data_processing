#this is for unorganized versioness data (e.g. a single zip file for all
#sites). we could turn this into a function, and make a separate function for
#versionless data that's separated into several files.

loginfo('Beginning retrieve (versionless products)',
        logger = logger_module)

prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve',
                              get_statuses = 'ready') %>%
    filter(grepl(pattern = '^VERSIONLESS',
                 x = prodcode))

if(! is.null(prodname_filter)){
    prod_info <- filter(prod_info, prodname %in% prodname_filter)
}

if(nrow(prod_info) == 0) return()

site_code <- 'sitename_NA'

for(i in seq_len(nrow(prod_info))){

    prodcode <- prod_info$prodcode[i]

    prodname_ms <<- paste0(prod_info$prodname[i],
                           '__',
                           prodcode)

    # prod_specs <- get_neon_product_specs(prodcode)
    # if(is_ms_err(prod_specs)) next

    held_data <<- get_data_tracker(network = network,
                                   domain = domain)

    if(! product_is_tracked(tracker = held_data,
                            prodname_ms = prodname_ms)){

        held_data <<- track_new_product(tracker = held_data,
                                        prodname_ms = prodname_ms)
    }

    # avail_sets <- sm(get_avail_neon_product_sets(prod_specs$prodcode_full))
    # if(is_ms_err(avail_sets)) next

    #retrieve data; log acquisitions and revision datetimes
    # avail_sites <- unique(avail_sets$site_code)
    # for(j in 1:length(avail_sites)){

    # site_code <- avail_sites[j]
    # avail_site_sets <- avail_sets[avail_sets$site_code == site_code, ,
    #     drop=FALSE]

    if(! site_is_tracked(tracker = held_data,
                         prodname_ms = prodname_ms,
                         site_code = site_code)){

        held_data <<- insert_site_skeleton(
            tracker = held_data,
            prodname_ms = prodname_ms,
            site_code = site_code,
            site_components = 'component001',
            versionless = TRUE
        )
    }

    # held_data <<- track_new_site_components(tracker = held_data,
    #                                         prodname_ms = prodname_ms,
    #                                         site_code = site_code,
    #                                         avail = avail_site_sets)
    # if(is_ms_err(held_data)) next

    # retrieval_details <- populate_set_details(tracker = held_data,
    #                                           prodname_ms = prodname_ms,
    #                                           site_code = site_code,
    #                                           avail = avail_site_sets)
    # if(is_ms_err(retrieval_details)) next

    # new_sets <- filter_unneeded_sets(tracker_with_details = retrieval_details)

    # if(nrow(new_sets) == 0){
    #     loginfo(glue('Nothing to do for {s} {p}',
    #                  s = site_code,
    #                  p = prodname_ms),
    #             logger = logger_module)
    #     next
    # } else {
    #     loginfo(glue('Retrieving {s} {p}',
    #                  s = site_code,
    #                  p = prodname_ms),
    #             logger = logger_module)
    # }

    update_data_tracker_r(network = network,
                          domain = domain,
                          tracker = held_data)

    dest_dir <- glue('data/{n}/{d}/raw/{p}/{s}',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code)

    dir.create(path = dest_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    retrieve_versionless_product(network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms,
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

    gc()
}

loginfo('Retrieval complete for all versionless products',
        logger = logger_module)
