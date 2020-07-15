#functions without the "#. handle_errors" decorator have special error handling

source('src/function_aliases.R')

assign('email_err_msgs', list(), envir=.GlobalEnv)
assign('err_cnt', 0, envir=.GlobalEnv)
assign('unique_errors', c(), envir=.GlobalEnv)
assign('unique_exceptions', c(), envir=.GlobalEnv)

# flag systems (future use?; increasing user value and difficulty to manage):

# system1: binary status (0=chill, 1=unchill)
# system2: 0=chill, 1=unit_unknown, 2=unit_concern, 4=other_concern
# system3: 0=chill, 1=unit_unknown, 2=unit_concern,
    #4=method_unknown, 8=method_concern, 16=other_concern
# system4: 0=chill, 1=unit_unknown, 2=unit_concern,
    #4=method_unknown, 8=method_concern, 16=sensor_concern, 32=general_concern
flagmap = list(
    clean=c(), #0
    method_unknown=c(), #1; method not given
    method_concern=c(), #2; method known to be lame
    #--- flagsum <= 3: ok
    #--- flagsum > 3: dirty
    sensor_concern=c(), #4; keywords: sensor*, expos*, detect*
    unit_unknown=c(), #8; unit not given
    unit_concern=c(), #16; unit unclear or inconvertible
    general_concern=c() #32; keywords:
)

handle_errors = function(f){

    #decorator function. takes any function as argument and executes it.
    #if error occurs within passed function, combines error message and
    #pretty callstack as single message, logs and emails this message.
    #returns custom macrosheds (ms) error class.

    #TODO: log unabridged callstack, email pretty one

    wrapper = function(...){

        thisenv = environment()

        tryCatch({
            return_val = f(...)
        }, error=function(e){

            err_cnt_new = err_cnt + 1
            assign('err_cnt', err_cnt_new, pos=.GlobalEnv)

            err_msg = as.character(e)
            if(! err_msg %in% unique_errors){
                unique_errors_new = append(unique_errors, err_msg)
                assign('unique_errors', unique_errors_new, pos=.GlobalEnv)
            }

            pretty_callstack = pprint_callstack()

            if(exists('s')) site_name = s
            if(! exists('site_name')) site_name = 'NO SITE'
            if(! exists('prodname_ms')) prodname_ms = 'NO PRODUCT'

            full_message = glue('{ec}\n\n',
                'NETWORK: {n}\nDOMAIN: {d}\nSITE: {s}\n',
                'PRODUCT: {p}\nERROR_MSG: {e}\nMS_CALLSTACK: {c}\n\n_',
                ec=err_cnt, n=network, d=domain, s=site_name, p=prodname_ms,
                e=err_msg, c=pretty_callstack)

            logerror(full_message, logger=logger_module)

            email_err_msgs = append(email_err_msgs, full_message)
            assign('email_err_msgs', email_err_msgs, pos=.GlobalEnv)

            assign('return_val', generate_ms_err(full_message), pos=thisenv)

        })

        if(is_ms_exception(return_val)){

            exception_msg = as.character(return_val)

            if(! exception_msg %in% unique_exceptions){
                unique_exceptions_new = append(unique_exceptions, exception_msg)
                assign('unique_exceptions', unique_exceptions_new,
                    pos=.GlobalEnv)
            }

            message(glue('MS Exception: ', exception_msg))
        }

        if(! is.null(return_val)) return(return_val)
    }

    return(wrapper)
}

pprint_callstack = function(){

    #make callstack more informative. if we end up sourcing files rather than
    #packaging, print which files were sourced along with callstack.

    # zz = as.list(sys.calls())
    # zx[[zxc]] <<- zz
    # zxc <<- zxc + 1
    # call_list = zz
    # # call_list = zx[[1]]

    call_list = as.list(sys.calls())
    call_names = sapply(call_list, all.names, max.names=2)
    ms_entities = grep('pprint', ls(envir=.GlobalEnv), value=TRUE, invert=TRUE)
    ms_calls_bool = sapply(call_names, function(x) any(x %in% ms_entities))
    # call_vec = call_vec[call_vec != 'pprint_callstack']

    # l = length(call_list)
    # call_list_cleaned = call_list[-((l - 4):l)]
    call_list_cleaned = call_list[ms_calls_bool]
    call_vec = unlist(lapply(call_list_cleaned,
        function(x){
            paste(deparse(x), collapse='\n')
        }))

    call_string_pretty = paste(call_vec, collapse=' -->\n\t')

    return(call_string_pretty)
}

#. handle_errors
sourceflags_to_ms_status <- function(d, flagstatus_mappings,
    exclude_mapvals = rep(FALSE, length(flagstatus_mappings))){

    #d is a df/tibble with flag and/or status columns
    #flagstatus_mappings is a list of flag or status column names mapped to
        #vectors of values that might be encountered in those columns.
        #see exclude_mapvals.
    #exclude_mapvals: a boolean vector of length equal to the length of
        #flagstatus_mappings. for each TRUE, values in the corresponding vector
        #are treated as OK values (mapped to ms_status 0). values
        #not in the vector are treated as flagged (mapped to ms_status 1).
        #For each FALSE, this relationship is inverted, i.e. values *in* the
        #corresponding vector are treated as flagged.

    flagcolnames = names(flagstatus_mappings)
    d = mutate(d, ms_status = 0)

    for(i in 1:length(flagstatus_mappings)){
        # d = filter(d, !! sym(flagcolnames[i]) %in% flagcols[[i]])
        # d = mutate(d,
        #     ms_status = ifelse(flagcolnames[i] %in% flagcols[[i]], 0, 1))

        if(exclude_mapvals[i]){
            ok_bool = ! d[[flagcolnames[i]]] %in% flagstatus_mappings[[i]]
        } else {
            ok_bool = d[[flagcolnames[i]]] %in% flagstatus_mappings[[i]]
        }

        d$ms_status[! ok_bool] = 1
    }

    d = select(d, -one_of(flagcolnames))

    return(d)
}

#. handle_errors
numeric_any <- function(num_vec){
    return(as.numeric(any(as.logical(num_vec))))
}

#. handle_errors
export_to_global <- function(from_env, exclude=NULL){

    #exclude is a character vector of names not to export.
    #unmatched names will be ignored.

    #vars could also be passed individually and handled by ...
    # vars = list(...)
    # varnames = all.vars(match.call())

    varnames = ls(name=from_env)
    varnames = varnames[! varnames %in% exclude]
    vars = mget(varnames, envir=from_env)

    for(i in 1:length(varnames)){
        assign(varnames[i], vars[[i]], .GlobalEnv)
    }

    return()
}

#. handle_errors
get_all_local_helpers <- function(network=domain, domain){

    #source_decoratees reads in decorator functions (tinsel package).
    #because it can only read them into the current environment, all files
    #sourced by this function are exported locally, then exported globally

    location1 = glue('src/{n}/network_helpers.R', n=network)
    if(file.exists(location1)){
        sw(source(location1, local=TRUE))
        sw(source_decoratees(location1))
    }

    location2 = glue('src/{n}/{d}/domain_helpers.R', n=network, d=domain)
    if(file.exists(location2)){
        sw(source(location2, local=TRUE))
        sw(source_decoratees(location2))
    }

    location3 = glue('src/{n}/processing_kernels.R', n=network)
    if(file.exists(location3)){
        sw(source(location3, local=TRUE))
        sw(source_decoratees(location3))
    }

    location4 = glue('src/{n}/{d}/processing_kernels.R', n=network, d=domain)
    if(file.exists(location4)){
        sw(source(location4, local=TRUE))
        sw(source_decoratees(location4))
    }

    rm(location1, location2, location3, location4)

    export_to_global(from_env=environment(),
        exclude=c('network', 'domain', 'thisenv'))

    return()
}

#. handle_errors
set_up_logger <- function(network=domain, domain){

    #the logging package establishes logger hierarchy based on name.
    #our root logger is named "ms", and our network-domain loggers are named
    #ms.network.domain, e.g. "ms.lter.hbef". When messages are logged, loggers
    #are referred to as name.module. A message logged to
    #logger="ms.lter.hbef.module" would be handled by loggers named
    #"ms.lter.hbef", "ms.lter", and "ms", some of which may not have established
    #handlers

    logger_name = glue('ms.{n}.{d}', n=network, d=domain)
    logger_module = glue(logger_name, '.module')

    logging::addHandler(logging::writeToFile, logger=logger_name,
        file=glue('logs/{n}_{d}.log', n=network, d=domain))

    return(logger_module)
}

#. handle_errors
extract_from_config <- function(key){
    ind = which(lapply(conf, function(x) grepl(key, x)) == TRUE)
    val = stringr::str_match(conf[ind], '.*\\"(.*)\\"')[2]
    return(val)
}

#. handle_errors
clear_from_mem <- function(..., clearlist){

    if(missing(clearlist)){
        dots = match.call(expand.dots = FALSE)$...
        clearlist = vapply(dots, as.character, '')
    }

    rm(list=clearlist, envir=.GlobalEnv)
    gc()

    return()
}

#. handle_errors
retain_ms_globals <- function(retain_vars){

    all_globals = ls(envir=.GlobalEnv, all.names=TRUE)
    clutter = all_globals[! all_globals %in% retain_vars]

    clear_from_mem(clearlist=clutter)

    return()
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

generate_blacklist_indicator = function(text=1){
    indobj = text
    class(indobj) = 'blacklist_indicator'
    return(indobj)
}

#. handle_errors
is_ms_err <- function(x){
    return('ms_err' %in% class(x))
}

#. handle_errors
is_ms_exception <- function(x){
    return('ms_exception' %in% class(x))
}

#. handle_errors
is_blacklist_indicator <- function(x){
    return('blacklist_indicator' %in% class(x))
}

#. handle_errors
evaluate_result_status <- function(r){

    if(is_ms_err(r) || is_ms_exception(r)){
        status <- 'error'
    } else if(is_blacklist_indicator(r)){
        status <- 'blacklist'
    } else {
        status <- 'ok'
    }

    return(status)
}

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

        #not sure if class "error" is always returned by tryCatch,
        #so creating custom class
        errout = 'err'
        class(errout) = 'err'
        return(errout)
    })

    if('err' %in% class(mailout)){
        msg = 'Something bogus happened in email_err'
        logerr(msg, logger=logger_module)
        return('email fail')
    } else {
        return('email success')
    }

}

get_data_tracker = function(network=domain, domain){

    #network is an optional macrosheds network name string. If omitted, it's
        #assumed to be identical to the domain string.
    #domain is a macrosheds domain string

    thisenv = environment()

    tryCatch({

        tracker_data = glue('data/{n}/{d}/data_tracker.json',
                n=network, d=domain) %>%
            readr::read_file() %>%
            jsonlite::fromJSON()

    }, error=function(e){
        assign('tracker_data', list(), pos=thisenv)
    })

    return(tracker_data)
}

#. handle_errors
make_tracker_skeleton <- function(retrieval_chunks){

    #retrieval_chunks is a vector of identifiers for subsets (chunks) of
    #the overall dataset to be retrieved, e.g. sitemonths for NEON

    munge_derive_skeleton = list(status='pending', mtime='1500-01-01')

    tracker_skeleton = list(
        retrieve=tibble::tibble(
            component=retrieval_chunks, mtime='1500-01-01',
            held_version='-1', status='pending'),
        munge=munge_derive_skeleton,
        derive=munge_derive_skeleton)

    return(tracker_skeleton)
}

#. handle_errors
insert_site_skeleton <- function(tracker, prodname_ms, site_name,
    site_components){

    tracker[[prodname_ms]][[site_name]] =
        make_tracker_skeleton(retrieval_chunks=site_components)

    return(tracker)
}

#. handle_errors
product_is_tracked <- function(tracker, prodname_ms){
    bool = prodname_ms %in% names(tracker)
    return(bool)
}

#. handle_errors
site_is_tracked <- function(tracker, prodname_ms, site_name){
    bool = site_name %in% names(tracker[[prodname_ms]])
    return(bool)
}

#. handle_errors
track_new_product <- function(tracker, prodname_ms){

    if(prodname_ms %in% names(tracker)){
        logwarn('This product is already being tracked.', logger=logger_module)
        return(tracker)
    }

    tracker[[prodname_ms]] = list()
    return(tracker)
}

#. handle_errors
track_new_site_components <- function(tracker, prodname_ms, site_name, avail){

    retrieval_tracker = tracker[[prodname_ms]][[site_name]]$retrieve

    retrieval_tracker = avail %>%
        filter(! component %in% retrieval_tracker$component) %>%
        select(component) %>%
        mutate(mtime='1900-01-01', held_version='-1', status='pending') %>%
        bind_rows(retrieval_tracker) %>%
        arrange(component)

    tracker[[prodname_ms]][[site_name]]$retrieve = retrieval_tracker

    return(tracker)
}

#. handle_errors
filter_unneeded_sets <- function(tracker_with_details){

    new_sets = tracker_with_details %>%
        filter(status != 'blacklist' | is.na(status)) %>%
        filter(needed == TRUE | is.na(needed))

    if(any(is.na(new_sets$needed))){
        msg = paste0('Must run `track_new_site_components` and ',
            '`populate_set_details` before running `populate_set_details`')
        logerror(msg, logger=logger_module)
        stop(msg)
    }

    return(new_sets)
}

#. handle_errors
update_data_tracker_r <- function(network=domain, domain, tracker=NULL,
    tracker_name=NULL, set_details=NULL, new_status=NULL){

    #this updates the retrieve section of a data tracker in memory and on disk.
    #see update_data_tracker_m for the munge section and update_data_tracker_d
    #for the derive section

    #if tracker is supplied, it will be used to write/overwrite the one on disk.
    #if it is omitted or set to NULL, the appropriate tracker will be loaded
    #from disk, updated, and then written back to disk.

    if(is.null(tracker) && (
            is.null(tracker_name) || is.null(set_details) || is.null(new_status)
    )){
        msg = paste0('If tracker is not supplied, these args must be:',
            'tracker_name, set_details, new_status.')
        logerror(msg, logger=logger_module)
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

    trackerfile = glue('data/{n}/{d}/data_tracker.json', n=network, d=domain)
    readr::write_file(jsonlite::toJSON(tracker), trackerfile)
    backup_tracker(trackerfile)

    return()
}

#. handle_errors
update_data_tracker_m <- function(network=domain, domain, tracker_name,
    prodname_ms, site_name, new_status){

    #this updates the munge section of a data tracker in memory and on disk.
    #see update_data_tracker_r for the retrieval section and
    #update_data_tracker_d for the derive section

    tracker = get_data_tracker(network=network, domain=domain)

    mt = tracker[[prodname_ms]][[site_name]]$munge

    mt$status = new_status
    mt$mtime = as.character(Sys.time())

    tracker[[prodname_ms]][[site_name]]$munge = mt

    assign(tracker_name, tracker, pos=.GlobalEnv)

    trackerfile = glue('data/{n}/{d}/data_tracker.json', n=network, d=domain)
    readr::write_file(jsonlite::toJSON(tracker), trackerfile)
    backup_tracker(trackerfile)

    return()
}

#. handle_errors
update_data_tracker_d <- function(network=domain, domain, tracker=NULL,
    tracker_name=NULL, prodname_ms=NULL, site_name=NULL, new_status=NULL){

    #this updates the derive section of a data tracker in memory and on disk.
    #see update_data_tracker_r for the retrieval section and
    #update_data_tracker_m for the munge section

    #if tracker is supplied, it will be used to write/overwrite the one on disk.
    #if it is omitted or set to NULL, the appropriate tracker will be loaded
    #from disk, updated, and then written back to disk.

    if(is.null(tracker) && (
        is.null(tracker_name) || is.null(prodname_ms) ||
        is.null(new_status) || is.null(site_name)
    )){
        msg = paste0('If tracker is not supplied, these args must be:',
                     'tracker_name, prodname_ms, new_status, new_status.')
        logerror(msg, logger=logger_module)
        stop(msg)
    }

    if(is.null(tracker)){

        tracker = get_data_tracker(network=network, domain=domain)

        dt = tracker[[prodname_ms]][[site_name]]$derive

        if(is.null(dt)){
           return(generate_ms_exception('Product not yet tracked; no action taken.'))
        }

        dt$status = new_status
        dt$mtime = as.character(Sys.time())

        tracker[[prodname_ms]][[site_name]]$derive = dt

        assign(tracker_name, tracker, pos=.GlobalEnv)
    }

    trackerfile = glue('data/{n}/{d}/data_tracker.json', n=network, d=domain)
    readr::write_file(jsonlite::toJSON(tracker), trackerfile)
    backup_tracker(trackerfile)

    return()
}

#. handle_errors
backup_tracker <- function(path){

    mch = stringr::str_match(path,
        '(data/.+?/.+?)/(data_tracker.json)')[, 2:3]

    if(any(is.na(mch))){
        stop('Invalid tracker path or name')
    }

    dir.create(glue(mch[1], '/tracker_backups'),
        recursive=TRUE, showWarnings=FALSE)

    tstamp = Sys.time() %>%
        with_tz(tzone='UTC') %>%
        format('%Y%m%dT%HZ') #tstamp format: YYYYMMDDTHHZ

    newpath = glue('{p}/tracker_backups/{f}_{t}', p=mch[1], f=mch[2], t=tstamp)
    file.copy(path, newpath, overwrite=FALSE) #write only one tracker per hour

    #remove tracker backups older than 7 days
    system2('find', c(glue(mch[1], '/tracker_backups/*'),
        '-mtime', '+7', '-exec', 'rm', '{}', '\\;'))

    return()
}

#. handle_errors
extract_retrieval_log <- function(tracker, prodname_ms, site_name,
    keep_status='ok'){

    retrieved_data = tracker[[prodname_ms]][[site_name]]$retrieve %>%
        tibble::as_tibble() %>%
        filter(status == keep_status)

    return(retrieved_data)
}

#. handle_errors
get_munge_status <- function(tracker, prodname_ms, site_name){
    munge_status = tracker[[prodname_ms]][[site_name]]$munge$status
    return(munge_status)
}

#. handle_errors
get_derive_status <- function(tracker, prodname_ms, site_name){
    derive_status = tracker[[prodname_ms]][[site_name]]$derive$status
    return(derive_status)
}

#. handle_errors
get_product_info <- function(network, domain=NULL, status_level, get_statuses){

    #unlike other functions with network and domain arguments, this one accepts
    #either network alone, or network and domain. if just network is given,
    #it will look for products.csv at the network level

    if(is.null(domain)){
        prods = sm(read_csv(glue('src/{n}/products.csv', n=network)))
    } else {
        prods = sm(read_csv(glue('src/{n}/{d}/products.csv', n=network, d=domain)))
    }

    status_column = glue(status_level, '_status')
    prods = prods[prods[[status_column]] %in% get_statuses, ]

    return(prods)
}

#. handle_errors
prodcode_from_prodname_ms <- function(prodname_ms){

    #prodname_ms consists of the macrosheds official name for a data
    #category, e.g. discharge, and the source-specific code for that
    #data product, e.g. DP1.20093. These two values are concatenated,
    #separated by a double underscore. So long as we never use a double
    #underscore in a macrosheds official data category name, this function
    #will be able to split a prodname_ms into its two constituent parts.

    namesplit <- strsplit(prodname_ms, '__')[[1]]
    name_length <- length(namesplit)
    prodcode <- namesplit[2:name_length]
    prodcode <- paste(prodcode, collapse = '__')

    return(prodcode)
}

#. handle_errors
prodname_from_prodname_ms <- function(prodname_ms){

    #prodname_ms consists of the macrosheds official name for a data
    #category, e.g. discharge, and the source-specific code for that
    #data product, e.g. DP1.20093. These two values are concatenated,
    #separated by a double underscore. So long as we never use a double
    #underscore in a macrosheds official data category name, this function
    #will be able to split a prodname_ms into its two constituent parts.

    prodname <- strsplit(prodname_ms, '__')[[1]][1]
    return(prodname)
}

#. handle_errors
ms_retrieve <- function(network=domain, domain){
    source(glue('src/{n}/{d}/retrieve.R', n=network, d=domain))
    return()
}

#. handle_errors
ms_munge <- function(network=domain, domain){
    source(glue('src/{n}/{d}/munge.R', n=network, d=domain))
    return()
}

#. handle_errors
ms_derive <- function(network=domain, domain){
    source(glue('src/{n}/{d}/derive.R', n=network, d=domain))
    return()
}

#. handle_errors
serialize_list_to_dir <- function(l, dest){

    #l must be a named list
    #dest is the path to a directory that will be created if it doesn't exist

    #list element classes currently handled: data.frame, character

    elemclasses = lapply(l, class)

    handled = lapply(elemclasses,
        function(x) any(c('character', 'data.frame') %in% x))

    if(! all(unlist(handled))){
        stop('Unhandled class encountered')
    }

    dir.create(dest, showWarnings=FALSE, recursive=TRUE)

    for(i in 1:length(l)){

        if('data.frame' %in% elemclasses[[i]]){

            fpath = paste0(dest, '/', names(l)[i], '.feather')
            write_feather(l[[i]], fpath)

        } else if('character' %in% x){

            fpath = paste0(dest, '/', names(l)[i], '.txt')
            readr::write_file(l[[i]], fpath)
        }
    }

    return()
}

#. handle_errors
parse_molecular_formulae <- function(formulae){

    #`formulae` is a vector

    # formulae = c('C', 'C4', 'Cl', 'Cl2', 'CCl', 'C2Cl', 'C2Cl2', 'C2Cl2B2')
    # formulae = 'BCH10He10PLi2'
    # formulae='Mn'

    conc_vars = str_match(formulae, '^(?:OM|TM|DO|TD|UT|UTK|TK)?([A-Za-z0-9]+)_?')[,2]
    two_let_symb_num = str_extract_all(conc_vars, '([A-Z][a-z][0-9]+)')
    conc_vars = str_remove_all(conc_vars, '([A-Z][a-z][0-9]+)')
    one_let_symb_num = str_extract_all(conc_vars, '([A-Z][0-9]+)')
    conc_vars = str_remove_all(conc_vars, '([A-Z][0-9]+)')
    two_let_symb = str_extract_all(conc_vars, '([A-Z][a-z])')
    conc_vars = str_remove_all(conc_vars, '([A-Z][a-z])')
    one_let_symb = str_extract_all(conc_vars, '([A-Z])')

    constituents = mapply(c, SIMPLIFY=FALSE,
        two_let_symb_num, one_let_symb_num, two_let_symb, one_let_symb)

    return(constituents) # a list of vectors
}

#. handle_errors
combine_atomic_masses <- function(molecular_constituents){

    #`molecular_constituents` is a vector

    xmat = str_match(molecular_constituents,
        '([A-Z][a-z]?)([0-9]+)?')[, -1, drop=FALSE]
    elems = xmat[,1]
    mults = as.numeric(xmat[,2])
    mults[is.na(mults)] = 1
    molecular_mass = sum(PeriodicTable::mass(elems) * mults)

    return(molecular_mass) #a scalar
}

#. handle_errors
calculate_molar_mass <- function(molecular_formula){

    if(length(molecular_formula) > 1){
        stop('molecular_formula must be a string of length 1')
    }

    parsed_formula = parse_molecular_formulae(molecular_formula)[[1]]
    molar_mass = combine_atomic_masses(parsed_formula)

    return(molar_mass)
}

#. handle_errors
convert_molecule <- function(x, from, to){

    #e.g. convert_molecule(1.54, 'NH4', 'N')

    from_mass <- calculate_molar_mass(from)
    to_mass <- calculate_molar_mass(to)
    converted_mass <- x * to_mass / from_mass

    return(converted_mass)
}

#. handle_errors
update_product_file <- function(network, domain, level, prodcode, status,
                                prodname){

    if(network == domain){
        prods = sm(read_csv(glue('src/{n}/products.csv', n=network)))
    } else {
        prods = sm(read_csv(glue('src/{n}/{d}/products.csv', n=network, d=domain)))
    }

    prodname_list <- strsplit(prodname, '; ')
    names_matched <- sapply(prodname_list, function(x) all(x %in% prods$prodname))
    if(! all(names_matched)){
        stop(glue('All prodnames in processing_kernels.R must match ',
                  'prodnames in products.csv'))
    }

    for(i in 1:length(prodcode)){
        col_name = as.character(glue(level[i], '_status'))
        row_num <- which(prods$prodcode == prodcode[i] &
                             prods$prodname %in% prodname_list[[i]])
        prods[row_num, col_name] = status[i]
    }

    if(network == domain){
        write_csv(prods, glue('src/{n}/products.csv', n=network))
    } else {
        write_csv(prods, glue('src/{n}/{d}/products.csv', n=network, d=domain))
    }

    return()
}

#. handle_errors
update_product_statuses <- function(network, domain){

    #status_codes should maybe be defined globally, or in a file
    status_codes = c('READY', 'PENDING', 'PAUSED')
    kf = glue('src/{n}/{d}/processing_kernels.R', n=network, d=domain)
    kernel_lines = read_lines(kf)

    status_line_inds = grep('STATUS=([A-Z]+)', kernel_lines)
    mch = stringr::str_match(kernel_lines[status_line_inds],
        '#(.+?): STATUS=([A-Z]+)')[, 2:3]
    prodnames = mch[, 1, drop=TRUE]
    statuses = mch[, 2, drop=TRUE]

    if(any(! statuses %in% status_codes)){
        stop(glue('Illegal status in ', kf))
    }

    funcname_lines = kernel_lines[status_line_inds + 2]

    if(any(! grep('process_[0-2]_.+?', funcname_lines))){
        stop(glue('function definition must begin exactly two lines after STATUS',
            ' indicator. function must be named "process_<level>_<prodcode>"'))
    }

    func_codes = stringr::str_match(funcname_lines,
        'process_([0-2])_(.+)? <-')[, 2:3, drop=FALSE]

    func_lvls = func_codes[, 1, drop=TRUE]
    prodcodes = func_codes[, 2, drop=TRUE]

    level_names = case_when(func_lvls == 0 ~ "retrieve",
       func_lvls == 1 ~ "munge",
       func_lvls == 2 ~ "derive")

    status_names = tolower(statuses)

    update_product_file(network=network, domain=domain, level=level_names,
                        prodcode=prodcodes, status=status_names,
                        prodname=prodnames)

    return()
}

#. handle_errors
convert_unit <- function(val, input_unit, output_unit){

    units <- tibble(prefix = c('n', "u", "m", "c", "d", "h", "k", "M"),
        convert_factor = c(0.000000001, 0.000001, 0.001, 0.01, 0.1, 100,
            1000, 1000000))

    old_fraction <- as.vector(str_split_fixed(input_unit, "/", n = Inf))

    old_top <- as.vector(str_split_fixed(old_fraction[1], "", n = Inf))

    if(length(old_top) == 2) {
        old_top_unit <- str_split_fixed(old_top, "", 2)[1]

        old_top_conver <- as.numeric(filter(units, prefix == old_top_unit)[,2])

    }else {
        old_top_conver <- 1
    }

    if(length(old_fraction) == 2) {
        old_bottom <- as.vector(str_split_fixed(old_fraction[2], "", n = Inf))

        old_bottom_conver <- ifelse(length(old_bottom) == 1, 1,
            as.numeric(filter(units, prefix == old_bottom[1])[,2]))
    } else{old_bottom_conver <- 1}

    new_fraction <- as.vector(str_split_fixed(output_unit, "/", n = Inf))

    new_top <- as.vector(str_split_fixed(new_fraction[1], "", n = Inf))

    if(length(new_top) == 2) {
        new_top_unit <- str_split_fixed(new_top, "", 2)[1]

        new_top_conver <- as.numeric(filter(units, prefix == new_top_unit)[,2])

    }else {
        new_top_conver <- 1
    }

    if(length(new_fraction) == 2) {
        new_bottom <- as.vector(str_split_fixed(new_fraction[2], "", n = Inf))

        new_bottom_conver <- ifelse(length(new_bottom) == 1, 1,
            as.numeric(filter(units, prefix == new_bottom[1])[,2]))
    } else{new_bottom_conver <- 1}

        new_val <- val*old_top_conver*new_top_conver

        new_val <- new_val/(old_bottom_conver*new_bottom_conver)

    return(new_val) }

#. handle_errors
write_munged_file <- function(d, network, domain, prodname_ms, site_name,
                              shapefile=FALSE){

    if(shapefile){

        site_dir = glue('{wd}/data/{n}/{d}/munged/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = prodname_ms,
                         s = site_name)

        dir.create(site_dir,
                   showWarnings = FALSE,
                   recursive = TRUE)

        sw(sf::st_write(obj = d,
                        dsn = glue(site_dir, '/', site_name, '.shp'),
                        delete_dsn = TRUE,
                        quiet = TRUE))
    } else {

        prod_dir = glue('data/{n}/{d}/munged/{p}', n=network, d=domain,
            p=prodname_ms)
        dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)

        site_file = glue('{pd}/{s}.feather', pd=prod_dir, s=site_name)
        write_feather(d, site_file)
    }

    return()
}

#. handle_errors
create_portal_link <- function(network, domain, prodname_ms, site_name,
                               dir=FALSE){

    #if dir=TRUE, treat site_name as a directory name, and link all files
        #within (necessary for e.g. shapefiles, which often come with other files)

    portal_prod_dir = glue('../portal/data/{d}/{p}', #ignore network
        d=domain, p=strsplit(prodname_ms, '__')[[1]][1])
    dir.create(portal_prod_dir, showWarnings=FALSE, recursive=TRUE)

    if(! dir){

        portal_site_file = glue('{pd}/{s}.feather',
            pd=portal_prod_dir, s=site_name)

        #if there's already a data file for this site-time-product in
        #the portal repo, remove it
        unlink(portal_site_file)

        #create a link to the portal repo from the new site file
        #(note: really, to and from are equivalent, as they both
        #point to the same underlying structure in the filesystem)
        site_file = glue('data/{n}/{d}/munged/{p}/{s}.feather',
            n=network, d=domain, p=prodname_ms, s=site_name)
        invisible(sw(file.link(to=portal_site_file, from=site_file)))

    } else {

        site_dir <- glue('data/{n}/{d}/munged/{p}/{s}',
                         n = network,
                         d = domain,
                         p = prodname_ms,
                         s = site_name)

        portal_prod_dir <- glue('../portal/data/{d}/{p}', #ignore network
                                d = domain,
                                p = strsplit(prodname_ms, '__')[[1]][1])

        dir.create(portal_prod_dir,
                   showWarnings = FALSE,
                   recursive = TRUE)

        site_files <- list.files(site_dir)
        for(s in site_files){

            site_file <- glue(site_dir, '/', s)
            portal_site_file <- glue(portal_prod_dir, '/', s)
            unlink(portal_site_file)
            invisible(sw(file.link(to = portal_site_file,
                                   from = site_file)))
        }

    }

    return()
}

#. handle_errors
is_ms_prodcode <- function(prodcode){

    #always specify macrosheds "pseudo product codes" as "msXXX" where
    #X is zero-padded integer. these codes are used for derived products
    #that don't exist within the data source.

    return(grepl('ms[0-9]{3}', prodcode))
}

#. handle_errors
list_munged_files <- function(network, domain, prodname_ms){

    mfiles <- glue('data/{n}/{d}/munged/{p}',
                   n = network,
                   d = domain,
                   p = prodname_ms) %>%
        list.files(full.names = TRUE)

    return(mfiles)
}

#. handle_errors
fname_from_fpath <- function(paths, include_fext = TRUE){

    #paths is a vector of filepaths of/this/form. final slash is used to
    #delineate file name.

    #if include_fext == FALSE, file extension will not be included

    fnames <- vapply(strsplit(paths, '/'),
                     function(x) x[length(x)],
                     FUN.VALUE = '')

    if(! include_fext){
        fnames <- str_match(fnames,'(.*?)\\..*')[, 2]
    }

    return(fnames)
}

#still in progress
delineate_watershed <- function(lat, long) {

    site <- tibble(x = lat,
                     y = long) %>%
        sf::st_as_sf(coords = c("y", "x"), crs = 4269) %>%
        sf::st_transform(102008)

    start_comid <- nhdplusTools::discover_nhdplus_id(sf::st_sfc(
        sf::st_point(c(long, lat)), crs = 4269))

    flowline <- nhdplusTools::navigate_nldi(list(featureSource = "comid",
                                                 featureID = start_comid),
                                            mode = "upstreamTributaries",
                                            data_source = "")

    subset_file <- tempfile(fileext = ".gpkg")

    subset <- nhdplusTools::subset_nhdplus(comids = flowline$nhdplus_comid,
                                           output_file = subset_file,
                                           nhdplus_data = "download",
                                           return_data = TRUE)

    flowlines <- subset$NHDFlowline_Network %>%
        sf::st_transform(102008)

    catchments <- subset$CatchmentSP %>%
        sf::st_transform(102008)

    upstream <- nhdplusTools::get_UT(flowlines, start_comid)

    watershed <- catchments %>%
        filter(featureid %in% upstream) %>%
        sf::st_buffer(0.01) %>%
        sf::st_union() %>%
        sf::st_as_sf()

    if(as.numeric(sf::st_area(watershed)) >= 60000000) {
        return(watershed)
        }
    else {

        outline = sf::st_as_sfc(sf::st_bbox(flowlines))

        outline_buff <- outline %>%
            sf::st_buffer(5000)

        dem <- elevatr::get_elev_raster(as(outline_buff, 'Spatial'), z=12)

        temp_raster <- tempfile(fileext = ".tif")

        raster::writeRaster(dem, temp_raster, overwrite = T)

        temp_point <- tempfile(fileext = ".shp")

        sf::st_write(sf::st_zm(site), temp_point, delete_layer=TRUE)

        temp_breash2 <- tempfile(fileext = ".tif")
        whitebox::wbt_fill_single_cell_pits(temp_raster, temp_breash2)

        temp_breached <- tempfile(fileext = ".tif")
        whitebox::wbt_breach_depressions(temp_breash2,temp_breached,flat_increment=.01)

        temp_d8_pntr <- tempfile(fileext = ".tif")
        whitebox::wbt_d8_pointer(temp_breached,temp_d8_pntr)

        temp_shed <- tempfile(fileext = ".tif")
        whitebox::wbt_unnest_basins(temp_d8_pntr, temp_point, temp_shed)

        # No idea why but wbt_unnest_basins() aves whatever file path with a _1 after the name so must add
        file_new <- str_split_fixed(temp_shed, "[.]", n = 2)

        file_shed <- paste0(file_new[1], "_1.", file_new[2])

        check <- raster::raster(file_shed)
        values <- raster::getValues(check)
        values[is.na(values)] <- 0


    if(sum(values, na.rm = TRUE) < 100) {

        flow <- tempfile(fileext = ".tif")
        whitebox::wbt_d8_flow_accumulation(temp_breached,flow,out_type='catchment area')

        snap <- tempfile(fileext = ".shp")
        whitebox::wbt_snap_pour_points(temp_point, flow, snap, 50)

        temp_shed <- tempfile(fileext = ".tif")
        whitebox::wbt_unnest_basins(temp_d8_pntr, snap, temp_shed)

        file_new <- str_split_fixed(temp_shed, "[.]", n = 2)

        file_shed <- paste0(file_new[1], "_1.", file_new[2])

        check <- raster::raster(file_shed)
        values <- raster::getValues(check)
        values[is.na(values)] <- 0
    }
    watershed_raster <- raster::rasterToPolygons(raster::raster(file_shed))

    #Convert shapefile to sf
    watershed_df <- sf::st_as_sf(watershed_raster)

    #buffer to join all pixles into one shape
    watershed <- sf::st_buffer(watershed_df, 0.1) %>%
        sf::st_union() %>%
        sf::st_as_sf()

    if(sum(values, na.rm = T) < 100) {
        watershed <- watershed %>%
            mutate(flag = "check")
    }

    #sf::st_write(watershed_union, dsn =
     #                glue("data/{n}/{d}/geospatial/ws_boundaries/{s}.shp",
      #                    n = sites$network, d = sites$domain, s = sites$site_name))
    return(watershed)

    }
}

#. handle_errors
calc_inst_flux <- function(chemprod, qprod, site_name, dt_round_interv){

    #chemprod is the prodname_ms for stream or precip chemistry
    #qprod is the prodname_ms for stream discharge or precip volume over time
    #dt_round_interv is a rounding interval passed to lubridate::round_date

    qvar <- prodname_from_prodname_ms(qprod)
    if(! qvar %in% c('precipitation', 'discharge')){
        stop('Could not determine stream/precip')
    }

    flux_vars <- ms_vars %>% #ms_vars is global
        filter(flux_convertible == 1) %>%
        pull(variable_code)

    chem <- read_feather(glue('data/{n}/{d}/munged/{cp}/{s}.feather',
                              n = network,
                              d = domain,
                              cp = chemprod,
                              s = site_name)) %>%
        select(one_of(flux_vars), 'datetime', 'ms_status') %>%
        mutate(datetime = lubridate::round_date(datetime, dt_round_interv)) %>%
        group_by(datetime) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup()

    daterange <- range(chem$datetime)
    fulldt <- tibble(datetime = seq(daterange[1], daterange[2],
                                    by=dt_round_interv))

    discharge <- read_feather(glue('data/{n}/{d}/munged/{qp}/{s}.feather',
                                   n = network,
                                   d = domain,
                                   qp = qprod,
                                   s = site_name)) %>%
        select(-site_name) %>%
        filter(datetime >= daterange[1], datetime <= daterange[2]) %>%
        mutate(datetime = lubridate::round_date(datetime, dt_round_interv)) %>%
        group_by(datetime) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup()

    flux <- chem %>%
        full_join(discharge,
                  by = 'datetime') %>%
        mutate(ms_status = numeric_any(c(ms_status.x, ms_status.y))) %>%
        select(-ms_status.x, -ms_status.y) %>%
        full_join(fulldt,
                  by='datetime') %>%
        arrange(datetime) %>%
        select_if(~(! all(is.na(.)))) %>%
        mutate_at(vars(-datetime, -ms_status),
                  imputeTS::na_interpolation,
                  maxgap = 30) %>%
        mutate_at(vars(-datetime, -!!sym(qvar), -ms_status),
                  ~(. * !!sym(qvar))) %>%
        mutate(site_name = !!(site_name)) %>%
        select(-!!sym(qvar)) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                   any_vars(! is.na(.))) %>%
        select(datetime, site_name, everything())

    return(flux)
}

#. handle_errors
read_combine_shapefiles <- function(network, domain, prodname_ms){

    prodpaths <- list.files(glue('data/{n}/{d}/munged/{p}',
                                 n = network,
                                 d = domain,
                                 p = prodname_ms),
                            recursive = TRUE,
                            full.names = TRUE,
                            pattern = '*.shp')

    shapes <- lapply(prodpaths,
                     function(x){
                         sf::st_read(x,
                                     stringsAsFactors = FALSE,
                                     quiet = TRUE)
                     })

    # wb <- sw(Reduce(sf::st_union, wbs)) %>%
    combined <- sw(Reduce(rbind, shapes))
        # sf::st_transform(projstring)

    return(combined)
}

#. handle_errors
read_combine_feathers <- function(network, domain, prodname_ms){

    prodpaths <- list_munged_files(network = network,
                                   domain = domain,
                                   prodname_ms = prodname_ms)

    combined <- tibble()
    for(i in 1:length(prodpaths)){
        combined <- read_feather(prodpaths[i]) %>%
            bind_rows(combined)
    }

    return(combined)
}
