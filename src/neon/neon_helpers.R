update_held_data = function(new_dates, loginfo_){

    prodcode = loginfo_$prodcode
    site = loginfo_$site

    held_dates = held_data[[prodcode]][[site]]
    held_data_ = held_data
    held_data_[[prodcode]][[site]] = append(held_dates, new_dates)
    assign('held_data', held_data_, pos=.GlobalEnv)

    readr::write_file(jsonlite::toJSON(held_data),
        'data_acquisition/data/neon/held_data.json')
}

resolve_neon_naming_conflicts = function(out_sub_, replacements=NULL,
    from_api=FALSE, loginfo_){

    #replacements is a named vector. name=find, value=replace;
    #failed match does nothing

    prodcode = loginfo_$prodcode
    site = loginfo_$site
    date = loginfo_$date

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

#obsolete now that neonUtilities package is working
determine_upstream_downstream_api = function(d_, data_inds_, loginfo_){

    prodcode = loginfo_$prodcode
    site = loginfo_$site
    date = loginfo_$date

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

determine_upstream_downstream = function(d_){

    updown = substr(d_$horizontalPosition, 3, 3)
    updown[updown == '1'] = 'u'
    updown[updown == '2'] = 'd'
    updown[updown == '0'] = 'b'

    if(any(! updown %in% c('u', 'd', 'b'))){
        # return(generate_ms_err())
        stop('upstream/downstream indicator error')
    }

    return(updown)
}
