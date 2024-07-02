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

if(FALSE %in% is.na(too_small_wb)){

    boundaries[too_small_wb, ] <- boundaries[too_small_wb, ] %>%
        mutate(geometry = st_buffer(st_centroid(geometry),
                                    dist = sqrt(10000 * 15 / pi)))
}

if(any(! sf::st_is_valid(boundaries))){
    log_with_indent(generate_ms_err('All watershed boundaries must be s2 valid'),
                    logger_module)
}

if(inherits(boundaries, 'ms_err') || is.null(boundaries[1])){
    stop('Some or all watershed boundaries not yet delineated/retrieved/linked for ', domain)
}

# Upload watersheds to GEE
user_info <- rgee::ee_user_info(quiet = TRUE)
asset_folder <- glue('{a}/macrosheds_ws_boundaries/{d}',
                     a = user_info$asset_home,
                     d = domain)

gee_file_exist <- try(rgee::ee_manage_assetlist(asset_folder), silent = TRUE)

loginfo('(Re)uploading ws_boundaries to GEE',
        logger = logger_module)

sm(rgee::ee_manage_create(asset_folder))

asset_path <- file.path(asset_folder, 'all_ws_boundaries')

# ee_shape <- try(sf_as_ee(boundaries,
#                          via = 'getInfo_to_asset',
#                          assetId = asset_path,
#                          overwrite = TRUE,
#                          quiet = TRUE),
#                 silent = TRUE)
#
# if(inherits(ee_shape, 'try-error')){
#
#     for(i in 1:nrow(boundaries)){
#         one_boundary <- boundaries[i,]
#         asset_path <- paste0(asset_folder, '/', one_boundary$site_code)
#         sf_as_ee(one_boundary,
#                  via = 'getInfo_to_asset',
#                  assetId = asset_path,
#                  overwrite = TRUE,
#                  quiet = TRUE)
#     }
# }

for(i in 1:nrow(unprod)){

    sf::sf_use_s2(TRUE)

    prodname_ms <<- glue(unprod$prodname[i], '__', unprod$prodcode[i])

    held_data <- get_data_tracker(network = network,
                                  domain = domain)

    if(! product_is_tracked(held_data, prodname_ms)){

        held_data <<- track_new_product(tracker = held_data,
                                        prodname_ms = prodname_ms)
    }

    loginfo(glue('Acquiring product: {p}',
                 p = prodname_ms),
            logger = logger_module)

    if(bulk_mode){
        dmn_sites <- 'all_sites'
    } else {
        dmn_sites <- site_data %>%
            filter(network == !!network,
                   domain == !!domain,
                   site_type != 'rain_gauge',
                   in_workflow == 1) %>%
            pull(site_code)
    }

    for(site_code in dmn_sites){

        if(! site_is_tracked(held_data, prodname_ms, site_code)){

            held_data[[prodname_ms]][[site_code]]$general$status <- 'pending'
            held_data[[prodname_ms]][[site_code]]$general$mtime <- '1500-01-01'
            held_data <<- held_data
        }

        if(get_missing_only){

            if(! bulk_mode) stop('get_missing_only currently only works in bulk_mode, but it would not take much to enable it for bulk_mode = FALSE')

            trait_dir <- glue('data/{network}/{domain}/ws_traits/{p}',
                              p = prodname_from_prodname_ms(prodname_ms))
            trait_dir <- sub('prism_', 'cc_', trait_dir)
            trait_dir <- sub('_temp_mean', '_temp', trait_dir)
            trait_dir <- sub('season_length', 'length_season', trait_dir)
            trait_dir <- sub('idbp', 'igbp', trait_dir)

            already_have_dir <- dir.exists(trait_dir) && length(list.files(trait_dir))

            if(already_have_dir){
                fs <- list.files(trait_dir, full.names = TRUE)
                last_retrieve <- min(file.mtime(fs))
                if(is.na(last_retrieve)) stop('oi')
                already_have_dates <- difftime(Sys.time(), last_retrieve,  units = 'days') < 120
                general_status <- ifelse(already_have_dir && already_have_dates, 'ok', 'pending')
            } else {
                general_status <- 'pending'
            }

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

        if(exists('last_retrieve')){
            #so crude, but would be a huge pain to do proper passing at this point
            last_retrieve_buffer <- as.Date(last_retrieve) - days(365 + 32)
            gee_bounding_dates <<- as.character(c(last_retrieve_buffer,
                                                  Sys.Date()))
            trait_dir <<- trait_dir
        }

        if(length(dmn_sites) > 1 || dmn_sites != 'all_sites'){
            bnd <- filter(boundaries, site_code == !!site_code)
        } else {
            bnd <- boundaries
        }

        general_msg <- sw(do.call(processing_func,
                                  args = list(network = network,
                                              domain = domain,
                                              prodname_ms = prodname_ms,
                                              site_code = site_code,
                                              boundaries = bnd)))

        sw(rm('gee_bounding_dates', 'trait_dir', envir = .GlobalEnv))

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
}
