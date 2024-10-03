retrieve_doe_product <- function(network,
                                 domain,
                                 prodname_ms,
                                 site_code,
                                 tracker,
                                 url,
                                 orcid_login,
                                 orcid_pass){

    processing_func <- get(paste0('process_0_',
                                  prodcode_from_prodname_ms(prodname_ms)))

    rt <- tracker[[prodname_ms]][[site_code]]$retrieve

    held_dt <- as.POSIXct(rt$held_version,
                          tz = 'UTC')

    deets <- list(prodname_ms = prodname_ms,
                  site_code = site_code,
                  component = rt$component,
                  last_mod_dt = held_dt,
                  url = url,
                  orcid_login = orcid_login,
                  orcid_pass = orcid_pass)

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

    if(prodname_ms == 'stream_chemistry__VERSIONLESS009'){
        result <- list()
        result$url <- read_file('data/doe/east_river/raw/documentation/documentation_discharge__VERSIONLESS009.txt')
    }
    source_urls <- get_source_urls(result_obj = result,
                                   processing_func = processing_func)

    write_metadata_r(murl = source_urls,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)
}
