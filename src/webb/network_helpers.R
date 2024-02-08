# lines below to set up environment for WEBB pkernels
webb_setup <- function(network = 'webb', domain = 'loch_vale') {
    get_all_local_helpers(network = network,
                          domain = domain)
    # this should only run when you have your producs.csv
    # and processing kernels prod information matching
    update_product_statuses(network = network,
                           domain = domain)
}

webb_pkernel_setup <- function(network = 'webb', domain = 'loch_vale', prodcode = "VERSIONLESS001") {
 #  network= network
  # domain = domain

    logger_module <- set_up_logger(network = network, domain = domain)

    loginfo(logger = logger_module,
            msg = glue('Processing network: {n}, domain: {d}',
                       n = network,
                       d = domain))

    # ms_retrieve really just executes your retrieve.R scripts
    # so we go directly to code from retrieve.r/retrieve_versionless.R

    # now we are using lines from retrieve scripts
    loginfo('Beginning retrieve (versionless products)',
        logger = logger_module)
    # NOTE: this could be end of webb_setup function

    # loading in your products csv (and making sure matches pkernels)
    prod_info <- get_product_info(network = network,
                              domain = domain,
                              status_level = 'retrieve',
                              get_statuses = 'ready') %>%
    filter(grepl(pattern = '^VERSIONLESS',
                 x = prodcode))

    if(prodcode %in% prod_info$prodcode){
      prod_info <- filter(prod_info, prodcode == !!prodcode)
    }

    if(length(prodcode) > 1) {
      stop("this helper function is made to load a single domain product at a time, pass only one prodcode to this function")
    }


    ## old filter used for product *name*, we need to use code, as above
    ## if(! is.null(prodname_filter)){
    ##   prod_info <- filter(prod_info, prodname %in% prodname_filter)
    ## }

    # if there are 0 or more than 1 products, function exits
    if(nrow(prod_info) != 1) stop("you have 0 or more than 1 products, run again with only one prodcode and make sure this prodcode exists in products.csv and is registered in processing kernel comments above proccess kernel functions")

    ## NOTE: this could be end of single get_webb_product function
    ## return(prod_info)

    # assume site code NA is default, it usually does not matter
    site_code <- 'sitename_NA'

    # code runs for only single product at a time
    prodname_ms <<- paste0(prod_info$prodname,
                           '__',
                           prodcode)

    # *** tracking stuff ***
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
            site_components = prod_info$components,
            versionless = TRUE
        )
    }

    update_data_tracker_r(network = network,
                          domain = domain,
                          tracker = held_data)

    # directory raw data for this product will be saved
    # of form: data/network/domain/prodname_ms/site_code
    dest_dir <- glue('data/{n}/{d}/raw/{p}/{s}',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code)

    # creating this directory
    dir.create(path = dest_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    # retrieve!
    tracker = held_data
    url = prod_info$url[1]

    # creating a string which matches the names of processing kernels
    processing_func <- paste0('process_0_',
                                  # these names or based off of prod names in products.csv
                                  prodcode_from_prodname_ms(prodname_ms))

    # tracking the "version" of the product
    rt <- tracker[[prodname_ms]][[site_code]]$retrieve

    held_dt <- as.POSIXct(rt$held_version,
                          tz = 'UTC')

    # "deets" is a list of all the information originally from a row in products.csv
    deets <- list(prodname_ms = prodname_ms,
                  site_code = site_code,
                  component = rt$component,
                  last_mod_dt = held_dt,
                  url = url)

    writeLines(glue('\n\nyou have loaded the arguments needed to run processing kernel: \n  {pk}\n\n', pk = processing_func))

    return(deets)
}

retrieve_webb_product <- function(network,
                                  domain,
                                  prodname_ms,
                                  site_code,
                                  tracker,
                                  url){

    ##
    ##not needed. switching back to retrieve_versionless_product
    ##

    # creating a string which matches the names of processing kernels
    processing_func <- get(paste0('process_0_',
                                  # these names or based off of prod names in products.csv
                                  prodcode_from_prodname_ms(prodname_ms)))

    # tracking the "version" of the product
    rt <- tracker[[prodname_ms]][[site_code]]$retrieve

    held_dt <- as.POSIXct(rt$held_version,
                          tz = 'UTC')

    # "deets" is a list of all the information originally from a row in products.csv
    deets <- list(prodname_ms = prodname_ms,
                  site_code = site_code,
                  component = rt$component,
                  last_mod_dt = held_dt,
                  url = url)

    # these "deets" are fed as arguments to wwhatever processing kernel is currently being called
    # remember, this "retrieve_product" function is being called, in the retrieve.R script,
    # in a loop over the product names from products.csv -- this is why the products.csv prod names
    # must match the end of the procesing kernels which are written to retrieve that product

    # if you're working on pkernels and not actually running this func, uncomment and run these lines:
    ## set_details = deets
    ## network = network
    ## domain = domain

    result <- do.call(processing_func,
                      args = list(set_details = deets,
                                  network = network,
                                  domain = domain))


    new_status <- evaluate_result_status(result)

    if('access_time' %in% names(result) && any(! is.na(result$access_time))){
        deets$last_mod_dt <- result$access_time[! is.na(result$access_time)][1]
    }

    update_data_tracker_r(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          set_details = deets,
                          new_status = new_status)

    source_urls <- get_source_urls(result_obj = result,
                                   processing_func = processing_func)

    write_metadata_r(murl = source_urls,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

}

retrieve_usgs_webb_daily_q <- function(set_details) {
    if(grepl("w5", set_details$component) == TRUE) {
        q <- dataRetrieval::readNWISdv(siteNumbers = "01135300",
                                       parameterCd = "00060")

    } else if(grepl("w3", set_details$component) == TRUE) {
        q <- dataRetrieval::readNWISdv(siteNumbers = "01135150",
                                       parameterCd = "00060")
    }

    return(q)
}
