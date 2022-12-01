
construct_usfs_product_sets <- function(prod_url, prodname){

    #returns: tibble with url, site_code, component (aka element_name)

    avail_sets <- tibble(
        url = prod_url,
        site_code = 'sitename_NA',
        component = prodname,
        )

    return(avail_sets)
}

populate_set_details <- function(tracker, prodname_ms, site_code, avail, latest_vsn){
    # tracker=held_data;avail=avail_site_sets

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker_old = tracker[[prodname_ms]][[site_code]]$retrieve

    prodcode = prodcode_from_prodname_ms(prodname_ms)

    retrieval_tracker = avail %>%
        mutate(
            avail_version = latest_vsn,
            prodcode_full = NA, #no such thing for lter. could simply omit
            prodcode_id = prodcode,
            prodname_ms = prodname_ms) %>%
        full_join(retrieval_tracker_old, by='component') %>%
        mutate(needed = !avail_version == held_version)

    if(any(is.na(retrieval_tracker$needed))){
        msg = paste0('Must run `track_new_site_components` before ',
            'running `populate_set_details`')
        logerror(msg, logger=logger_module)
        stop(msg)
    }

    return(retrieval_tracker)
}

get_usfs_data <- function(domain, sets, tracker, silent=TRUE){
    # sets <- new_sets; tracker <- held_data

    if(nrow(sets) == 0) return()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Retrieving {st}, {p}, {c}',
            st=s$site_code, p=s$prodname_ms, c=s$component)
        loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        result = do.call(processing_func,
            args=list(set_details=s, network=network, domain=domain))
        # process_0_1(set_details=s, network=network, domain=domain)

        new_status <- evaluate_result_status(result)
        update_data_tracker_r(network=network, domain=domain,
            tracker_name='held_data', set_details=s, new_status=new_status)
    }
}

download_raw_file <- function(network, domain, set_details, file_type = '.csv') {
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(is.null(file_type)) {

        download.file(url = set_details$url,
                      destfile = glue(raw_data_dest,
                                      '/',
                                      set_details$component,
                                      '.zip'),
                      cacheOK = FALSE,
                      method = 'libcurl')
    } else {

        download.file(url = set_details$url,
                      destfile = glue(raw_data_dest,
                                      '/',
                                      set_details$component,
                                      file_type),
                      cacheOK = FALSE,
                      method = 'libcurl')
    }
}
