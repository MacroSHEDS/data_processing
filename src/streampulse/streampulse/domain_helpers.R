## run these before working inside retrieve_product func
## network = network
## domain = domain
## prodname_ms = prodname_ms
## site_code = site_code
## tracker = held_data
## url = prod_info$url[i]

retrieve_sleepers_product <- function(network,
                                 domain,
                                 prodname_ms,
                                 site_code,
                                 tracker,
                                 url){
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

retrieve_usgs_sleepers_daily_q <- function(set_details) {
  if(grepl("w5", set_details$component) == TRUE) {
    q <- dataRetrieval::readNWISdv(siteNumbers = "01135300",
                                   parameterCd = "00060")
  } else if(grepl("w3", set_details$component) == TRUE) {
    q <- dataRetrieval::readNWISdv(siteNumbers = "01135150",
                                   parameterCd = "00060")
  }

  return(q)
}
scrape_data <- function(sites) {
  
  site_dataframes <- list()
  
  for (site in sites) {
    
    ava_data <- tryCatch({
      query_available_data(region = "NC", site = site)
    }, error = function(e) {
      print(paste("Error in querying data for site", site, ":", e))
      return(NULL)
    })
    
    if (!is.null(ava_data)) {
      
      start_year <- as.integer(strsplit(as.character(ava_data$datebounds$firstRecord), "-")[[1]][1])
      end_year <- as.integer(strsplit(as.character(ava_data$datebounds$lastRecord), "-")[[1]][1])
      
      
      d_site <- data.frame()
      for (year in start_year:end_year) {
        
        res_year <- tryCatch({
          request_results(sitecode = paste("NC", site, sep = "_"), year = as.character(year))
        }, error = function(e) {
          print(paste("Error in requesting results for site", site, "year", year, ":", e))
          return(NULL)
        })
        
        if (!is.null(res_year)) {
          d_site <- dplyr::bind_rows(d_site, res_year$model_results$fit$daily)
        }
      }
      
      site_dataframes[[site]] <- d_site
    }
  }
  return(site_dataframes)
}
