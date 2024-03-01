#this is for unorganized versioness data (e.g. a single zip file for all
#sites). we could turn this into a function, and make a separate function for
#versionless data that's separated into several files.

loginfo('Beginning retrieve (versionless products)',
        logger = logger_module)

library(dataRetrieval)

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

    held_data <<- get_data_tracker(network = network,
                                   domain = domain)

    if(! product_is_tracked(tracker = held_data,
                            prodname_ms = prodname_ms)){

        held_data <<- track_new_product(tracker = held_data,
                                        prodname_ms = prodname_ms)
    }

    if(! site_is_tracked(tracker = held_data,
                         prodname_ms = prodname_ms,
                         site_code = site_code)){

        held_data <<- insert_site_skeleton(
            tracker = held_data,
            prodname_ms = prodname_ms,
            site_code = site_code,
            site_components = prod_info$components[i],
            versionless = TRUE
        )
    }

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

    retrieval_s <- held_data[[prodname_ms]][['sitename_NA']][['retrieve']][['status']]

    if(retrieval_s == 'ok'){
        loginfo(glue('Nothing to do for {s} {p}',
                     s=site_code, p=prodname_ms), logger=logger_module)
        next
    } else {
        loginfo(glue('Retrieving {s} {p}',
                     s=site_code, p=prodname_ms), logger=logger_module)
    }

    retrieve_versionless_product(network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms,
                                 site_code = site_code,
                                 tracker = held_data,
                                 resource_url = prod_info$url[i])

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
