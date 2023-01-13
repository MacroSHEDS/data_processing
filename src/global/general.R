loginfo('Beginning general',
        logger = logger_module)

source('src/global/general_kernels.R',
       local = TRUE)

if(ms_instance$use_ms_error_handling){
    source_decoratees('src/global/general_kernels.R')
}

unprod <- univ_products  %>%
    filter(status == 'ready')

if(exists('general_prod_filter_')){
    unprod <- filter(unprod, prodname %in% general_prod_filter_)

    loginfo(paste('general_prod_filter_ is set. only working on',
                  paste(general_prod_filter_, collapse = ', ')),
            logger = logger_module)
}

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

#tiny watersheds can't be summarized by gee for some products.
#if < 15 ha, replace with 15 ha circle on centroid
ws_areas <- site_data %>%
    filter(in_workflow == 1,
           site_type != 'rain_gauge',
           domain == !!domain) %>%
    right_join(select(boundaries, site_code)) %>%
    select(site_code, ws_area_ha)

boundaries <- boundaries %>%
    select(-any_of('area')) %>%
    left_join(ws_areas) %>%
    rename(area = ws_area_ha)

too_small_wb <- boundaries$area < 15
# reupload <- FALSE
# if(any(too_small_wb)) reupload <- TRUE
boundaries[too_small_wb, ] <- boundaries[too_small_wb, ] %>%
    mutate(geometry = st_buffer(st_centroid(geometry),
                                dist = sqrt(10000 * 15 / pi)))

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

# if(inherits(gee_file_exist, 'try-error') || nrow(gee_file_exist) == 0 || reupload){

loginfo('(Re)uploading ws_boundaries to GEE',
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

# } else {
#     loginfo('ws_boundaries already uploaded to GEE',
#             logger = logger_module)
# }

# i = 27
for(i in 1:nrow(unprod)){
# for(i in 28:28){

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
        trait_dir <- sub('prism_', 'cc_', trait_dir)
        trait_dir <- sub('_temp_mean', '_temp', trait_dir)
        trait_dir <- sub('season_length', 'length_season', trait_dir)
        trait_dir <- sub('idbp', 'igbp', trait_dir)

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
