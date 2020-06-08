# library(logging)
# library(tidyverse)

source('src/function_aliases.R')

get_all_helpers = function(network=NULL, domain){

    if(is.null(network)) network = domain

    sw(try(source(glue('src/{n}/network_helpers.R', n=network)), silent=TRUE))
    sw(try(source(glue('src/{n}/{d}/domain_helpers.R', n=network, d=domain)),
        silent=TRUE))

    sw(try(source(glue('src/{n}/processing_kernels.R', d=domain)), silent=TRUE))
    sw(try(source(glue('src/{n}/{d}/processing_kernels.R', n=network, d=domain)),
        silent=TRUE))
}

set_up_logger = function(network, domain){

    logger_name = glue(network, '_', domain)
    logger_module = glue(logger_name, '.module')

    logging::basicConfig()
    logging::addHandler(logging::writeToFile, logger=logger_name,
        file=glue('logs/{n}_{d}.log', n=network, d=domain))

    return(logger_module)
}

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

# addr='mjv22@duke.edu'; pw=conf$gmail_pw
email_err = function(msgs, addrs, pw){

    if(is.list(msgs)){
        msgs = Reduce(function(x, y) paste(x, y, sep='\n---\n'), msgs)
    }

    text_body = glue('Error list:\n\n', msgs, '\n\nEnd of errors')

    mailout = tryCatch({

        for(a in addrs){

            email = envelope() %>%
                from('grdouser@gmail.com') %>%
                to(a) %>%
                subject('MacroSheds error') %>%
                text(text_body)

            smtp = server(host='smtp.gmail.com',
                port=587, #or 465 for SMTPS
                username='grdouser@gmail.com',
                password=pw)

            smtp(email, verbose=FALSE)
        }

    }, error=function(e){
        errout = 'err'
        class(errout) = 'err'
        return(errout)
    })

    if('err' %in% class(mailout)){
        msg = 'Something bogus happened in email_err'
        logging::logerr(msg, logger=logger_module)
        return('email fail')
    } else {
        return('email success')
    }

}

get_data_tracker = function(network=NULL, domain){

    #network is an optional macrosheds network name string. If omitted, it's
        #assumed to be identical to the domain string.
    #domain is a macrosheds domain string

    if(is.null(network)) network = domain

    thisenv = environment()

    tryCatch({

        tracker_data = glue('data/{n}/{d}/data_tracker.json',
                n=network, d=domain) %>%
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
        logging::logerror(msg, logger=logger_module)
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

filter_unneeded_sets = function(tracker_with_details){


    new_sets = tracker_with_details %>%
        filter(status != 'blacklist' | is.na(status)) %>%
        filter(needed == TRUE | is.na(needed))

    if(any(is.na(new_sets$needed))){
        msg = paste0('Must run `track_new_site_components` and ',
            '`populate_set_details` before running `populate_set_details`')
        logging::logerror(msg, logger=logger_module)
        stop(msg)
    }

    return(new_sets)
}

# tracker_name='held_data'; set_details=s; new_status='ok'
update_data_tracker_r = function(network=NULL, domain, tracker=NULL,
    tracker_name=NULL, set_details=NULL, new_status=NULL){

    #this updates the retrieve section of a data tracker in memory and on disk.
    #see update_data_tracker_m for the munge section and update_data_tracker_d
    #for the derive section

    #if tracker is supplied, it will be used to write/overwrite the one on disk.
    #if it is omitted or set to NULL, the appropriate tracker will be loaded
    #from disk, updated, and then written back to disk.

    if(is.null(network)) network = domain

    if(is.null(tracker) && (
            is.null(tracker_name) || is.null(set_details) || is.null(new_status)
    )){
        msg = paste0('If tracker is not supplied, these args must be:',
            'tracker_name, set_details, new_status.')
        logging::logerror(msg, logger=logger_module)
        stop(msg)
    }

    if(is.null(tracker)){

        tracker = get_data_tracker(network=network, domain=domain)

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
        glue('data/{n}/{d}/data_tracker.json', n=network, d=domain))

}

# tracker_name='held_data'; prod=prodname_ms; new_status='ok'
update_data_tracker_m = function(network=NULL, domain, tracker_name, prod,
    site, new_status){

    #this updates the munge section of a data tracker in memory and on disk.
    #see update_data_tracker_d for the derive section

    if(missing(network)) network = domain

    tracker = get_data_tracker(network=network, domain=domain)

    mt = tracker[[prod]][[site]]$munge

    mt$status = new_status
    mt$mtime = as.character(Sys.time())

    tracker[[prod]][[site]]$munge = mt

    assign(tracker_name, tracker, pos=.GlobalEnv)

    readr::write_file(jsonlite::toJSON(tracker),
        glue('data/{n}/{d}/data_tracker.json', n=network, d=domain))
}

#build this when the time comes
populate_missing_shiny_files = function(domain){

    #this is not yet working. first, shiny needs to be reconfigured to
    #pull site files as requested. atm all sites are bound into one feather
    #by domain and dataset (i.e. neon-precip, neon-Q, etc)

    list.files('data/hbef/')

    qq = read_feather('data/hbef/discharge.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/discharge.feather')

    qq = read_feather('data/hbef/flux.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/flux.feather')

    qq = read_feather('data/hbef/pchem.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/pchem.feather')

    qq = read_feather('data/hbef/precip.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/precip.feather')

}

extract_retrieval_log = function(tracker, prod, site, keep_status='ok'){

    retrieved_data = tracker[[prod]][[site]]$retrieve %>%
        tibble::as_tibble() %>%
        filter(status == keep_status)

    return(retrieved_data)
}

get_product_info = function(network, domain=NULL, status_level, get_statuses){

    #unlike other functions with network and domain arguments, this one accepts
    #either network alone, or network and domain. if just network is given,
    #it will look for products.csv at the network level

    if(is.null(domain)){
        prods = read_csv(glue('src/{n}/products.csv', n=network))
    } else {
        prods = read_csv(glue('src/{n}/{d}/products.csv', n=network, d=domain))
    }

    status_column = glue(status_level, '_status')
    prods = prods[prods[[status_column]] %in% get_statuses, ]

    return(prods)
}

prodcode_from_ms_prodname = function(ms_prodname){

    prodcode = strsplit(ms_prodname, '_')[[1]][2]

    return(prodcode)
}

pprint_callstack = function(){

    funcnamevec = unlist(lapply(as.list(sys.calls()), deparse))
    funcnamevec = funcnamevec[-length(funcnamevec)] #exclude pprint_callstack
    callchain = paste(funcnamevec, collapse=' --> ')

    return(callchain)
}

handle_error = function(err, note){

    #combines error message, callstack, and any notes as string.
    #logs this collection, adds it to global list of errors to email,
    #and returns it as custom error class

    if(missing(note)) note = 'No note'

    pretty_callstack = pprint_callstack()

    full_message = glue('Error: {e}\nCallstack: {c}\nMS note: {n}',
        e=err, c=pretty_callstack, n=note)

    logging::logerror(full_message, logger=logger_module)

    email_err_msg = append(email_err_msg, full_message)
    assign('email_err_msg', email_err_msg, pos=.GlobalEnv)

    ms_err = generate_ms_err(full_message)

    return(ms_err)
}
