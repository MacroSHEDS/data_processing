#junk functions, possibly useful for parts

# obsolete kernels ####

process_0_DP1.20093.001_api = function(d, set_details){

    data1_ind = intersect(grep("expanded", d$data$files$name),
        grep("fieldSuperParent", d$data$files$name))
    data2_ind = intersect(grep("expanded", d$data$files$name),
        grep("externalLabData", d$data$files$name))
    data3_ind = intersect(grep("expanded", d$data$files$name),
        grep("domainLabData", d$data$files$name))

    data1 = tryCatch({
        read.delim(d$data$files$url[data1_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data2 = tryCatch({
        read.delim(d$data$files$url[data2_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data3 = tryCatch({
        read.delim(d$data$files$url[data3_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })

    if(nrow(data1)){
        data1 = select(data1, siteID, collectDate, dissolvedOxygen,
            dissolvedOxygenSaturation, specificConductance, waterTemp,
            maxDepth)
    }
    if(nrow(data2)){
        data2 = select(data2, siteID, collectDate, pH, externalConductance,
            externalANC, starts_with('water'), starts_with('total'),
            starts_with('dissolved'), uvAbsorbance250, uvAbsorbance284,
            shipmentWarmQF, externalLabDataQF)
    }
    if(nrow(data3)){
        data3 = select(data3, siteID, collectDate, starts_with('alk'),
            starts_with('anc'))
    }

    out_sub = plyr::join_all(list(data1, data2, data3), type='full') %>%
        group_by(collectDate) %>%
        summarise_each(list(~ if(is.numeric(.)){
            mean(., na.rm = TRUE)
        } else {
            first(.)
        })) %>%
        ungroup() %>%
        mutate(datetime=as.POSIXct(collectDate, tz='UTC',
            format='%Y-%m-%dT%H:%MZ')) %>%
        select(-collectDate)

    return(out_sub)
} #chem: obsolete

process_0_DP1.20288.001_api = function(d, set_details){

    data_inds = intersect(grep("expanded", d$data$files$name),
        grep("instantaneous", d$data$files$name))

    site_with_suffixes = determine_upstream_downstream(d, data_inds,
        set_details)
    if(is_ms_err(site_with_suffixes)) return(site_with_suffixes)

    #process upstream and downstream sites independently
    for(j in 1:length(data_inds)){

        site_with_suffix = site_with_suffixes[j]

        #download data
        out_sub = read.delim(d$data$files$url[data_inds[j]], sep=",",
            stringsAsFactors=FALSE)

        out_sub = resolve_neon_naming_conflicts(out_sub,
            set_details_=set_details,
            replacements=c('specificCond'='specificConductance',
                'dissolvedOxygenSat'='dissolvedOxygenSaturation'))
        if(is_ms_err(out_sub)) return(out_sub)

        out_sub = mutate(out_sub,
            datetime=as.POSIXct(startDateTime,
                tz='UTC', format='%Y-%m-%dT%H:%M:%SZ'),
            site=site_with_suffix) %>%
            select(-startDateTime)
    }

    return(out_sub)
} #waterqual: obsolete

resolve_neon_naming_conflicts_OBSOLETE = function(out_sub_, replacements=NULL,
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
        logwarn(msg, logger=logger_module)
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

determine_upstream_downstream_api_OBSOLETE = function(d_, data_inds_, set_details_){
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
        logwarn(msg, logger=logger_module)
        return(generate_ms_err())
    }

    site_with_suffixes = paste0(site, updown_suffixes[updown_order])

    return(site_with_suffixes)
}

