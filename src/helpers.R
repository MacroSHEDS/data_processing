library(logging)
library(tidyverse)

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

get_data_tracker_UNNEEDED = function(domain){

    #domain is a macrosheds domain string

    tracker_data = try(jsonlite::fromJSON(readr::read_file(
        glue::glue('data_acquisition/data/{d}/data_tracker.json', d=domain)
    )), silent=TRUE)

    if('try-error' %in% class(tracker_data)) tracker_data = list()

    return(tracker_data)
}

get_data_tracker_LIST = function(domain, category, level){

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

update_data_tracker_dates = function(new_dates, loginfo_, domain){

    #new_dates is a vector of year-months, e.g. '2019-12'
    #domain is a macrosheds domain string
    #category is one of 'held', 'problem', 'blacklist'
    #level is processing level (0 = retrieval, 1 = munge, 2 = derive)

    level = as.character(level)

    prodcode = loginfo_$prodcode
    site = loginfo_$site
    processing_level = switch(level, '0'='0_retrieval_trackers',
        '1'='1_munge_trackers', '2'='2_derive_trackers')

    held_dates = held_data[[prodcode]][[site]]$months
    held_data_ = held_data
    held_data_[[prodcode]][[site]]$months = append(held_dates, new_dates)
    assign('held_data', held_data_, pos=.GlobalEnv)

    readr::write_file(jsonlite::toJSON(held_data),
        glue::glue('data_acquisition/data/{d}/data_trackers/{l}/{c}_data.json',
            d=domain, l=processing_level, c=category))
}

update_data_tracker_dates_LIST = function(new_dates, loginfo_, domain, category,
    level){

    #OBSOLETE

    #new_dates is a vector of year-months, e.g. '2019-12'
    #domain is a macrosheds domain string
    #category is one of 'held', 'problem', 'blacklist'
    #level is processing level (0 = retrieval, 1 = munge, 2 = derive)

    level = as.character(level)

    prodcode = loginfo_$prodcode
    site = loginfo_$site
    processing_level = switch(level, '0'='0_retrieval_trackers',
        '1'='1_munge_trackers', '2'='2_derive_trackers')

    held_dates = held_data[[prodcode]][[site]]$months
    held_data_ = held_data
    held_data_[[prodcode]][[site]]$months = append(held_dates, new_dates)
    assign('held_data', held_data_, pos=.GlobalEnv)

    readr::write_file(jsonlite::toJSON(held_data),
        glue::glue('data_acquisition/data/{d}/data_trackers/{l}/{c}_data.json',
            d=domain, l=processing_level, c=category))
}
