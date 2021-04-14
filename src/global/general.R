loginfo('Beginning general',
        logger = logger_module)

source('src/global/general_kernels.R',
       local = TRUE)

unprod <- univ_products %>%
  filter(status == 'ready')

# Load spatial files from Drive if not already held on local machine (takes a long time)
# load_spatial_data()

files <- list.files(glue('data/{n}/{d}/derived/',
                         n = network,
                         d = domain))

ws_prodname <- grep('ws_boundary', files, value = TRUE)

boundaries <- try(read_combine_shapefiles(network = network,
                                     domain = domain,
                                     prodname_ms = ws_prodname))

if(class(boundaries)[1] == 'ms_err' | is.null(boundaries[1])){
    stop('Watershed boundaries are required for general products')
}

site_names <- unique(boundaries$site_name)

for(i in 1:nrow(unprod)){

    prodname_ms <- glue(unprod$prodname[i], '__', unprod$prodcode[i])

    held_data <- get_data_tracker(network = network,
                                  domain = domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        held_data <<- track_new_product(tracker = held_data,
                                        prodname_ms = prodname_ms)
    }

    loginfo(glue('Acquiring product: {p}',
                 p = prodname_ms),
            logger = logger_module)

    for(k in 1:length(site_names)){
    #for(k in c(26, 27)){

        site_name <- site_names[k]

        if(! site_is_tracked(held_data, prodname_ms, site_name)){

            held_data[[prodname_ms]][[site_name]]$general$status <- 'pending'
            held_data[[prodname_ms]][[site_name]]$general$mtime <- '1500-01-01'
            held_data <<- held_data
        }

        general_status <- get_general_status(tracker = held_data,
                                             prodname_ms = prodname_ms,
                                             site_name = site_name)

        general_status <- 'pending'

        if(general_status %in% c('ok', 'no_data_avail')){

            loginfo(glue('Nothing to do for product: {p}, site: {s}',
                         p = prodname_ms,
                         s = site_name),
                    logger = logger_module)

            next

        } else {

            loginfo(glue('Working on product: {p}, site: {s}',
                         p = prodname_ms,
                         s = site_name),
                     logger = logger_module)
        }

        prodcode <- prodcode_from_prodname_ms(prodname_ms)
        processing_func <- get(paste0('process_3_', prodcode))

        general_msg <- sw(do.call(processing_func,
                                  args = list(network = network,
                                              domain = domain,
                                              prodname_ms = prodname_ms,
                                              site_name = site_name,
                                              boundaries = boundaries)))

        if(is_ms_exception(general_msg)){

            #This indicates that GEE returned an empty table which likely means
            #The gee product is not available at this location i.e PRISM is only available
            #in the continental US
            msg_string <- 'No data available for product: {p}, site: {s}'
            new_status <- 'no_data_avail'

        } else if(is_ms_err(general_msg)){

            msg_string <- 'Error in product: {p}, site: {s}'
            new_status <- 'error'

        } else {

            msg_string <- 'Acquired product {p} for site {s}'
            new_status <- 'ok'
        }

        msg <- glue(msg_string,
                    p = prodname_ms,
                    s = site_name)

        update_data_tracker_g(network = network,
                              domain = domain,
                              tracker = held_data,
                              prodname_ms = prodname_ms,
                              site_name = site_name,
                              new_status = new_status)

        loginfo(msg = msg,
                logger = logger_module)

        gc()
    }
}

#this isn't needed, right?
# create_portal_links(network = network,
#                     domain = domain)

loginfo(msg = 'General acquisition complete for all products',
        logger = logger_module)
