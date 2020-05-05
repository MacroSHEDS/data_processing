# library(logging)
# library(tidyverse)
#
# glue = glue::glue

extract_from_config = function(key){
    ind = which(lapply(conf, function(x) grepl(key, x)) == TRUE)
    val = stringr::str_match(conf[ind], '.*\\"(.*)\\"')[2]
    return(val)
}

flagmap = list(
    clean=c(NA, 'example flag 0'),
    sensor_concern=c('example flag 1'),
    unit_unknown=c('example flag 2'),
    unit_concern=c('example flag 3'),
    method_unknown=c('example flag 4', 'example flag 5'),
    method_concern=c('example flag 6'),
    general_concern=c('example flag 7')
)

get_flag_types = function(mapping, flags) {

    #MUST ENHANCE THIS TO ACCOUNT FOR MULTIPLE FLAGS APPLIED TO THE SAME
    #datapoint. see DB I/O card in projects todo list

    flags = paste(flags) #convert special types to char
    lengths = sapply(mapping, length)
    keyvec = rep(names(mapping), times=lengths)
    valvec = paste(unlist(unname(mapping)))

    if(any(! flags %in% valvec)){
        unaccounted_for = flags[which(! flags %in% valvec)]
        stop(paste('Missing mapping for flags:',
            paste(unaccounted_for, collapse=', ')))
    }

    flag_types = keyvec[match(flags, valvec)]

    return(flag_types)
}

resolve_commas = function(vec, comma_standin){
    vec = gsub(',', '\\,', vec, fixed=TRUE)
    vec = gsub(comma_standin, ',', vec)
    return(vec)
}

postgres_arrayify = function(vec){
    vec = paste0('{', vec, '}')
    vec = gsub('\\{\\}', '{""}', vec)
    return(vec)
}

clear_from_mem = function(...){
    dots = match.call(expand.dots = FALSE)$...
    names = vapply(dots, as.character, '')
    rm(list=names, envir=globalenv())
    gc()
}

generate_ms_err = function(text=1){
    errobj = text
    class(errobj) = 'ms_err'
    return(errobj)
}

generate_ms_exception = function(text=1){
    excobj = text
    class(excobj) = 'ms_exception'
    return(excobj)
}

is_ms_err = function(x){
    return('ms_err' %in% class(x))
}

is_ms_exception = function(x){
    return('ms_exception' %in% class(x))
}

# msg='neon data acquisition error. check the logs.'
# addr='mjv22@duke.edu'; pw=conf$gmail_pw
email_err = function(msg, addr, pw){

    mailout = tryCatch({
        email = envelope() %>%
            from('grdouser@gmail.com') %>%
            to(addr) %>%
            subject('MacroSheds error') %>%
            text(msg)
        smtp = server(host='smtp.gmail.com',
            port=587, #or 465 for SMTPS
            username='grdouser@gmail.com',
            password=pw)
        smtp(email, verbose=FALSE)
    }, error=function(e){
        errout = 'err'
        class(errout) = 'err'
        return(errout)
    })

    if('err' %in% class(mailout)){
        msg = 'Something bogus happened in email_err'
        logging::logerr(msg, logger='neon.module')
        return('email fail')
    } else {
        return('email success')
    }

}

get_data_tracker = function(domain){

    #domain is a macrosheds domain string

    thisenv = environment()

    tryCatch({

        tracker_data = glue('data_acquisition/data/{d}/data_tracker.json',
                d=domain) %>%
            readr::read_file() %>%
            jsonlite::fromJSON()

        # tracker_data = lapply(tracker_data, function(x){
        #     lapply(x, function(y){
        #         y$retrieve = as_tibble(y$retrieve)
        #     })
        # })

    }, error=function(e){
        assign('tracker_data', list(), pos=thisenv)
    })

    return(tracker_data)
}

get_data_tracker_OBSOLETE = function(domain, category, level){

    #OBSOLETE

    #domain is a macrosheds domain string
    #category is one of 'held', 'problem', 'blacklist'
    #level is processing level (0 = retrieval, 1 = munge, 2 = derive)

    level = as.character(level)
    processing_level = switch(level, '0'='0_retrieval_trackers',
        '1'='1_munge_trackers', '2'='2_derive_trackers')

    tracker_data = try(jsonlite::fromJSON(readr::read_file(
        glue('data_acquisition/data/{d}/data_trackers/{l}/{c}_data.json',
            d=domain, l=processing_level, c=category)
    )), silent=TRUE)

    if('try-error' %in% class(tracker_data)) tracker_data = list()

    return(tracker_data)
}

update_data_tracker_dates_OBSOLETE = function(new_dates, set_details_, domain){

    #new_dates is a vector of year-months, e.g. '2019-12'
    #domain is a macrosheds domain string
    #category is one of 'held', 'problem', 'blacklist'
    #level is processing level (0 = retrieval, 1 = munge, 2 = derive)

    level = as.character(level)

    prodcode = set_details_$prodcode
    site = set_details_$site
    processing_level = switch(level, '0'='0_retrieval_trackers',
        '1'='1_munge_trackers', '2'='2_derive_trackers')

    held_dates = held_data[[prodcode]][[site]]$months
    held_data_ = held_data
    held_data_[[prodcode]][[site]]$months = append(held_dates, new_dates)
    assign('held_data', held_data_, pos=.GlobalEnv)

    readr::write_file(jsonlite::toJSON(held_data),
        glue('data_acquisition/data/{d}/data_trackers/{l}/{c}_data.json',
            d=domain, l=processing_level, c=category))
}

update_data_tracker_dates_OBSOLETE = function(new_dates, set_details_, domain, category,
    level){

    #OBSOLETE

    #new_dates is a vector of year-months, e.g. '2019-12'
    #domain is a macrosheds domain string
    #category is one of 'held', 'problem', 'blacklist'
    #level is processing level (0 = retrieval, 1 = munge, 2 = derive)

    level = as.character(level)

    prodcode = set_details_$prodcode
    site = set_details_$site
    processing_level = switch(level, '0'='0_retrieval_trackers',
        '1'='1_munge_trackers', '2'='2_derive_trackers')

    held_dates = held_data[[prodcode]][[site]]$months
    held_data_ = held_data
    held_data_[[prodcode]][[site]]$months = append(held_dates, new_dates)
    assign('held_data', held_data_, pos=.GlobalEnv)

    readr::write_file(jsonlite::toJSON(held_data),
        glue('data_acquisition/data/{d}/data_trackers/{l}/{c}_data.json',
            d=domain, l=processing_level, c=category))
}

make_tracker_skeleton = function(retrieval_chunks){

    #retrieval_chunks is a vector of identifiers for subsets (chunks) of
    #the overall dataset to be retrieved, e.g. sitemonths for NEON

    munge_derive_skeleton = list(status='pending', mtime='1900-01-01')

    tracker_skeleton = list(
        retrieve=tibble::tibble(
            component=retrieval_chunks, mtime='1900-01-01',
            held_version='-1', status='pending'),
        munge=munge_derive_skeleton,
        derive=munge_derive_skeleton)

    return(tracker_skeleton)
}

insert_site_skeleton = function(tracker, prod, site, site_components){

    tracker[[prod]][[site]] =
        make_tracker_skeleton(retrieval_chunks=site_components)

    return(tracker)
}

product_is_tracked = function(tracker, prod){
    bool = prod %in% names(tracker)
    return(bool)
}

site_is_tracked = function(tracker, prod, site){
    bool = site %in% names(tracker[[prod]])
    return(bool)
}

track_new_product = function(tracker, prod){

    if(prod %in% names(tracker)){
        msg = 'This product is already being tracked.'
        logging::logerror(msg, logger='neon.module')
        stop(msg)
    }

    tracker[[prod]] = list()
    return(tracker)
}

track_new_site_components = function(tracker, prod, site, avail){

    retrieval_tracker = tracker[[prod]][[site]]$retrieve

    retrieval_tracker = avail %>%
        filter(! component %in% retrieval_tracker$component) %>%
        select(component) %>%
        mutate(mtime='1900-01-01', held_version='-1', status='pending') %>%
        bind_rows(retrieval_tracker) %>%
        arrange(component)

    tracker[[prod]][[site]]$retrieve = retrieval_tracker

    return(tracker)
}

# tracker=held_data; prod=prodname_ms; site=curr_site; avail=avail_site_sets
# specs=prod_specs
# rm(tracker, prod, site, avail, specs)
# filter_unneeded_sets = function(tracker, prod, site, avail, specs){
populate_set_details = function(tracker, prod, site, avail, specs){

    retrieval_tracker = tracker[[prod]][[site]]$retrieve

    rgx = '/(DP[0-9]\\.([0-9]+)\\.([0-9]+))/[A-Z]{4}/[0-9]{4}\\-[0-9]{2}$'
    rgx_capt = str_match(avail$url, rgx)[, -1]

    retrieval_tracker = avail %>%
        mutate(
            avail_version = as.numeric(rgx_capt[, 3]),
            prodcode_full = rgx_capt[, 1],
            prodcode_id = rgx_capt[, 2],
            prodname_ms = prod) %>%
        full_join(retrieval_tracker, by='component') %>%
        # filter(status != 'blacklist' | is.na(status)) %>%
        mutate(
            held_version = as.numeric(held_version),
            needed = avail_version - held_version > 0)
        # filter(needed == TRUE | is.na(needed))

    if(any(is.na(retrieval_tracker$needed))){
        msg = paste0('Must run `track_new_site_components` before ',
            'running `populate_set_details`')
        logging::logerror(msg, logger='neon.module')
        stop(msg)
    }

    return(retrieval_tracker)
}

filter_unneeded_sets = function(tracker_with_details){


    new_sets = tracker_with_details %>%
        filter(status != 'blacklist' | is.na(status)) %>%
        filter(needed == TRUE | is.na(needed))

    if(any(is.na(new_sets$needed))){
        msg = paste0('Must run `track_new_site_components` and ',
            '`populate_set_details` before running `populate_set_details`')
        logging::logerror(msg, logger='neon.module')
        stop(msg)
    }

    return(new_sets)
}

update_data_tracker_r = function(domain, tracker=NULL, tracker_name=NULL,
    set_details=NULL, new_status=NULL){

    #this updates the retrieve section of a data tracker in memory and on disk.
    #see update_data_tracker_m for the munge section and update_data_tracker_d
    #for the derive section

    #if tracker is supplied, it will be used to write/overwrite the one on disk.
    #if it is omitted or set to NULL, the appropriate tracker will be loaded
    #from disk.

    if(is.null(tracker) && (
            is.null(tracker_name) || is.null(set_details) || is.null(new_status)
    )){
        msg = paste0('If tracker is not supplied, these args must be:',
            'tracker_name, set_details, new_status.')
        logging::logerror(msg, logger='neon.module')
        stop(msg)
    }

    if(is.null(tracker)){

        tracker = get_data_tracker(domain)

        rt = tracker[[set_details$prodname_ms]][[set_details$site_name]]$retrieve

        set_ind = which(rt$component == set_details$component)

        if(new_status %in% c('pending', 'ok')){
            rt$held_version[set_ind] = as.character(set_details$avail_version)
        }
        rt$status[set_ind] = new_status
        rt$mtime[set_ind] = as.character(Sys.time())

        tracker[[set_details$prodname_ms]][[set_details$site_name]]$retrieve = rt

        assign(tracker_name, tracker, pos=.GlobalEnv)
    }

    readr::write_file(jsonlite::toJSON(tracker),
        glue('data_acquisition/data/{d}/data_tracker.json', d=domain))

}
