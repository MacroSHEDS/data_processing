
resolve_neon_naming_conflicts = function(out_sub_, replacements=NULL,
    from_api=FALSE, set_details_){

    #obsolete now that neonUtilities package is working

    #replacements is a named vector. name=find, value=replace;
    #failed match does nothing

    prodcode = set_details_$prodcode
    site = set_details_$site
    date = set_details_$date

    out_cols = colnames(out_sub_)

    if(! is.null(replacements)){

        #get list of variables included
        varind = grep('SciRvw', out_cols)
        rgx = str_match(out_cols[varind], '^(\\w*)(?:FinalQFSciRvw|SciRvwQF)$')
        # varlist = flagprefixlist = rgx[,2]
        varlist = rgx[,2]

        #harmonize redundant variable names
        for(i in 1:length(replacements)){
            r = replacements[i]
            varlist = replace(varlist, which(varlist == names(r)), r)
        }
    }

    if('startDate' %in% out_cols){
        colnames(out_sub_) = replace(out_cols, which(out_cols == 'startDate'),
            'startDateTime')
    } else if('endDate' %in% out_cols){
        colnames(out_sub_) = replace(out_cols, which(out_cols == 'endDate'),
            'startDateTime') #not a mistake
    } else if(! 'startDateTime' %in% out_cols){
        msg = glue('Datetime column not found for site ',
            '{site} ({prod}, {date}).', site=site, prod=prodcode, date=date)
        logging::logwarn(msg, logger='neon.module')
        return(generate_ms_err())
    }

    #subset relevant columns (needed if NEON API was used)
    if(from_api){
        flagcols = grepl('.+?FinalQF(?!SciRvw)', out_cols,
            perl=TRUE)
        datacols = ! grepl('QF', out_cols, perl=TRUE)
        relevant_cols = flagcols | datacols
        out_sub_ = out_sub_[, relevant_cols]
    }

    return(out_sub_)
}

download_sitemonth_details = function(geturl){

    thisenv = environment()

    tryCatch({
        d = httr::GET(geturl)
        d = jsonlite::fromJSON(httr::content(d, as="text"))
    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('d', generate_ms_err(), pos=thisenv)
    })

    return(d)
}

determine_upstream_downstream_api = function(d_, data_inds_, set_details_){
    #obsolete now that neonUtilities package is working

    prodcode = set_details_$prodcode
    site = set_details_$site
    date = set_details_$date

    #determine which dataset is upstream/downstream if necessary
    updown_suffixes = c('-up', '-down')
    if(length(data_inds_) == 2){
        position = str_split(d_$data$files$name[data_inds_[1]], '\\.')[[1]][7]
        updown_order = if(position == '101') 1:2 else 2:1
    } else if(length(data_inds_) == 1){
        updown_order = 1
    } else {
        msg = glue('Problem with upstream/downstream indicator for site ',
            '{site} ({prod}, {date}).', site=site, prod=prodcode, date=date)
        logging::logwarn(msg, logger='neon.module')
        return(generate_ms_err())
    }

    site_with_suffixes = paste0(site, updown_suffixes[updown_order])

    return(site_with_suffixes)
}

# d_ = data_pile$waq_instantaneous
determine_upstream_downstream = function(d_){

    updown = substr(d_$horizontalPosition, 3, 3)
    updown[updown == '1'] = '-up'

    #2 means downstream. 0 means only one sensor? 3 means ???
    updown[updown %in% c('0', '2', '3')] = ''

    if(any(! updown %in% c('-up', ''))){
        # return(generate_ms_err())
        stop('upstream/downstream indicator error')
    }

    return(updown)
}

get_avail_neon_products = function(){

    req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/"))
    txt = httr::content(req, as="text")
    data_pile = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
    prodlist = data_pile$data$productCode

    return(prodlist)
}

get_neon_product_specs = function(code){

    prodlist = try(get_avail_neon_products())
    if('try-error' %in% class(prodlist)){
        logging::logerror(glue("Can't retrieve NEON product list for {c}",
            c=code), logger='neon.module')
        stop()
    }

    prod_variant_inds = grep(code, prodlist)

    if(length(prod_variant_inds) > 1){
        return(generate_ms_err('More than one product variant for this prodcode.'))
    }

    newest_variant_ind = prodlist[prod_variant_inds] %>%
        substr(11, 13) %>%
        as.numeric() %>%
        which.max()

    prodcode_full = prodlist[prod_variant_inds[newest_variant_ind]]
    prod_version = strsplit(prodcode_full, '\\.')[[1]][3]

    return(list(prodcode_full=prodcode_full, prod_version=prod_version))
}

get_avail_neon_product_sets = function(prodcode_full){

    thisenv = environment()

    tryCatch({
        req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
            prodcode_full))
        txt = httr::content(req, as="text")
        neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        # email_err_msg <<- outer_loop_err <<- TRUE
        # assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('avail_sets', generate_ms_err(), pos=thisenv)
    })

    urls = unlist(neondata$data$siteCodes$availableDataUrls)

    avail_sets = stringr::str_match(urls,
        '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})') %>%
        as_tibble(.name_repair='unique') %>%
        rename(url=`...1`, site_name=`...2`, component=`...3`)

    return(avail_sets)

}
