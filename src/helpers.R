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

generate_ms_err = function(){
    errobj = 1
    class(errobj) = 'ms_err'
    return(errobj)
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
        return('fail')
    } else {
        return('success')
    }

}

is_ms_err = function(x){
    return('ms_err' %in% class(x))
}

