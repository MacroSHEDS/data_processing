retrieve_doe_product <- function(network,
                                 domain,
                                 prodname_ms,
                                 site_name,
                                 tracker,
                                 url,
                                 orcid_login,
                                 orcid_pass){

    processing_func <- get(paste0('process_0_',
                                  prodcode_from_prodname_ms(prodname_ms)))

    rt <- tracker[[prodname_ms]][[site_name]]$retrieve


    held_dt <- as.POSIXct(rt$held_version,
                          tz = 'UTC')

    deets <- list(prodname_ms = prodname_ms,
                  site_name = site_name,
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

    if(is.POSIXct(result)){
        deets$last_mod_dt <- as.character(result)
    }

    update_data_tracker_r(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          set_details = deets,
                          new_status = new_status)
}
