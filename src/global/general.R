source('src/global/general_kernels.R')
loginfo('Beginning derive', logger=logger_module)
site_name <- 'sitename_NA'

unprod <- sm(read_csv('data/general/universal_products.csv')) %>%
  filter(status == 'ready')

files <- list.files(glue('data/{n}/{d}/munged/',
                         n = network,
                         d = domain))

ws_prodname <- grep('ws_boundary', files, value = TRUE)

sheds <- try(read_combine_shapefiles(network=network, domain=domain, 
                                     prodname_ms=ws_prodname))

if(class(sheds)[1] == 'ms_err' | is.null(sheds[1])) {
  stop('Watershed boundaries are required for general products')
}

# i <- 7
for(i in 1:nrow(unprod)){
  
  prodname_ms = glue(unprod$prodname[i], '__', unprod$prodcode[i])
  
  held_data = get_data_tracker(network=network, domain=domain)
  
  if(! product_is_tracked(held_data, prodname_ms)){
    
    held_data <- track_new_product(tracker = held_data,
                                   prodname_ms = prodname_ms)
    held_data <- insert_site_skeleton(tracker = held_data,
                                      prodname_ms = prodname_ms,
                                      site_name = site_name,
                                      site_components = 'NA')
    
    held_data[[prodname_ms]][[site_name]][['general']][['status']] <- 'pending'
    
    held_data[[prodname_ms]][[site_name]][['general']][['mtime']] <- '1500-01-01'
    
    update_data_tracker_d(network = network,
                          domain = domain,
                          tracker = held_data)
  }
  
  general_status <- get_general_status(tracker = held_data,
                                     prodname_ms = prodname_ms,
                                     site_name = site_name)
  
  if(general_status == 'ok'){
    loginfo(glue('Nothing to do for {p}',
                 p=prodname_ms),
            logger=logger_module)
    next
  } else {
    loginfo(glue('Getting general products {p}',
                 p=prodname_ms),
            logger=logger_module)
  }
  
  prodcode = prodcode_from_prodname_ms(prodname_ms)
  
  processing_func = get(paste0('process_3_', prodcode))
  
  gerneral_msg <- sw(do.call(processing_func,
                           args=list(network = network,
                                     domain = domain,
                                     prodname_ms = prodname_ms,
                                     ws_boundry = sheds)))
  
  stts <- ifelse(is_ms_err(gerneral_msg), 'error', 'ok')
  update_data_tracker_g(network=network, domain=domain,
                        tracker_name='held_data', prodname_ms=prodname_ms,
                        site_name=site_name, new_status=stts)
  
  if(stts == 'ok'){
    msg = glue('Got general {p} ({n}/{d}/{s})',
               p = prodname_ms,
               n = network,
               d = domain,
               s = site_name)
    loginfo(msg, logger=logger_module)
  }
  
  gc()

}

loginfo('General computation complete for all products',
        logger=logger_module)
