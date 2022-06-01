loginfo('Beginning general',
        logger = logger_module)

source('src/global/general_kernels.R',
       local = TRUE)

if(ms_instance$use_ms_error_handling){
    source_decoratees('src/global/general_kernels.R')
}

unprod <- univ_products  %>%
    filter(status == 'ready')

# unprod = filter(unprod, grepl('season', prodname))


# Load spatial files from Drive if not already held on local machine
# (takes a long time)
# load_spatial_data()

# Load in watershed Boundaries
files <- list.files(glue('data/{n}/{d}/derived/',
                         n = network,
                         d = domain))

ws_prodname <- grep('ws_boundary', files, value = TRUE)

boundaries <- try(read_combine_shapefiles(network = network,
                                          domain = domain,
                                          prodname_ms = ws_prodname))

if(any(!sf::st_is_valid(boundaries))){
    log_with_indent(generate_ms_err('All watershed boundaries must be s2 valid'),
                    logger_module)
}

if(class(boundaries)[1] == 'ms_err' | is.null(boundaries[1])){
    stop('Watershed boundaries are required for general products')
}

# Upload watersheds to GEE
user_info <- rgee::ee_user_info(quiet = TRUE)
asset_folder <- glue('{a}/macrosheds_ws_boundaries/{d}',
                     a = user_info$asset_home,
                     d = domain)

gee_file_exist <- try(rgee::ee_manage_assetlist(asset_folder), silent = TRUE)

if(inherits(gee_file_exist, 'try-error') || nrow(gee_file_exist) == 0){

    loginfo('Uploading ws_boundaries to GEE',
            logger = logger_module)

    sm(rgee::ee_manage_create(asset_folder))

    asset_path <- paste0(asset_folder, '/', 'all_ws_boundaries')

    ee_shape <- try(sf_as_ee(boundaries,
                             via = 'getInfo_to_asset',
                             assetId = asset_path,
                             overwrite = TRUE,
                             quiet = TRUE),
                    silent = TRUE)

    if('try-error' %in% class(ee_shape)){

        for(i in 1:nrow(boundaries)){
            one_boundary <- boundaries[i,]
            asset_path <- paste0(asset_folder, '/', one_boundary$site_code)
            sf_as_ee(one_boundary,
                     via = 'getInfo_to_asset',
                     assetId = asset_path,
                     overwrite = TRUE,
                     quiet = TRUE)
        }
    }

} else {
    loginfo('ws_boundaries already uploaded to GEE',
            logger = logger_module)
}

# i = 25
for(i in 1:nrow(unprod)){
# for(i in 26:26){

    sf::sf_use_s2(TRUE)
    
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

    site_code <- 'all_sites'

    if(! site_is_tracked(held_data, prodname_ms, site_code)){

        held_data[[prodname_ms]][[site_code]]$general$status <- 'pending'
        held_data[[prodname_ms]][[site_code]]$general$mtime <- '1500-01-01'
        held_data <<- held_data
    }

    # general_status <- get_general_status(tracker = held_data,
    #                                      prodname_ms = prodname_ms,
    #                                      site_code = site_code)

    if(get_missing_only){

        trait_dir <- glue('data/{network}/{domain}/ws_traits/{p}',
                          p = prodname_from_prodname_ms(prodname_ms))
        already_have <- dir.exists(trait_dir) && length(list.files(trait_dir))
        general_status <- ifelse(already_have, 'ok', 'pending')

    } else {
        general_status <- 'pending'
    }

    if(general_status %in% c('ok', 'no_data_avail')){

        loginfo(glue('Nothing to do for product: {p}, site: {s}',
                     p = prodname_ms,
                     s = site_code),
                logger = logger_module)

        next

    } else {

        loginfo(glue('Working on product: {p}, site: {s}',
                     p = prodname_ms,
                     s = site_code),
                 logger = logger_module)
    }

    prodcode <- prodcode_from_prodname_ms(prodname_ms)
    processing_func <- get(paste0('process_3_', prodcode))

    general_msg <- sw(do.call(processing_func,
                              args = list(network = network,
                                          domain = domain,
                                          prodname_ms = prodname_ms,
                                          site_code = site_code,
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
                s = site_code)

    update_data_tracker_g(network = network,
                          domain = domain,
                          tracker = held_data,
                          prodname_ms = prodname_ms,
                          site_code = site_code,
                          new_status = new_status)

    loginfo(msg = msg,
            logger = logger_module)

    gc()
}
