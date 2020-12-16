loginfo('Beginning general', logger=logger_module)
source('src/global/general_kernels.R')
library(rgee)

unprod <- univ_products %>%
  filter(status == 'ready')

files <- list.files(glue('data/{n}/{d}/derived/',
                         n = network,
                         d = domain))

ws_prodname <- grep('ws_boundary', files, value = TRUE)

sheds <- try(read_combine_shapefiles(network=network, domain=domain,
                                     prodname_ms=ws_prodname))

if(class(sheds)[1] == 'ms_err' | is.null(sheds[1])) {
  stop('Watershed boundaries are required for general products')
}



site_names <- unique(sheds$site_name)

for(i in 1:nrow(unprod)){

  prodname_ms = glue(unprod$prodname[i], '__', unprod$prodcode[i])

  held_data = get_data_tracker(network=network, domain=domain)

  if(! product_is_tracked(held_data, prodname_ms)){

    held_data <- track_new_product(tracker = held_data,
                                   prodname_ms = prodname_ms)
  }

  loginfo(glue('Acquiring {p}',
               p = prodname_ms),
          logger=logger_module)

  for(k in 1:length(site_names)) {

    site_name <- site_names[k]

    if(! site_is_tracked(held_data, prodname_ms, site_name)) {
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
      loginfo(glue('Nothing to do for {s}',
                   s = site_name),
              logger = logger_module)
      next
    } else {
      loginfo(glue('Acquiring {s}',
                   p = prodname_ms, s = site_name),
              logger=logger_module)
    }

    prodcode = prodcode_from_prodname_ms(prodname_ms)

    processing_func = get(paste0('process_3_', prodcode))

    gerneral_msg <- sw(do.call(processing_func,
                               args=list(network = network,
                                         domain = domain,
                                         prodname_ms = prodname_ms,
                                         site = site_name,
                                         boundaries = sheds)))

    stts_e <- ifelse(is_ms_err(gerneral_msg), 'error', 'ok')
    stts_w <- ifelse(is_ms_exception(gerneral_msg), 'exception', 'ok')
    if(stts_e == 'ok' && stts_w == 'ok'){

      update_data_tracker_g(network=network, domain=domain,
                            tracker_name='held_data', prodname_ms=prodname_ms,
                            site_name=site_name, new_status='ok')

      msg = glue('Acquired general {p} ({n}/{d}/{s})',
                 p = prodname_ms,
                 n = network,
                 d = domain,
                 s = site_name)
      loginfo(msg, logger=logger_module)
    }
    gc()
  }
}

loginfo('General acquisition complete for all products',
        logger=logger_module)
