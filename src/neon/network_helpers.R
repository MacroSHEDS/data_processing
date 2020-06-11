
#. handle_errors
get_neon_data = function(domain, sets, tracker, silent=TRUE){
    # sets=new_sets; i=1; tracker=held_data

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {site}, {prod}, {month}',
            site=s$site_name, prod=s$prodname_ms, month=s$component)
        loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        out_sitemonth = do.call(processing_func, args=list(set_details=s))

        if(is_ms_err(out_sitemonth) || is_ms_exception(out_sitemonth)){
            update_data_tracker_r(network=domain, domain=domain,
                tracker_name='held_data', set_details=s, new_status='error')
            next
        }

        site_dir = glue('data/{n}/{d}/raw/{p}/{s}',
            n=network, d=domain, p=s$prodname_ms, s=s$site_name)
        dir.create(site_dir, showWarnings=FALSE, recursive=TRUE)

        sitemonth_file = glue('{sd}/{t}.feather',
            sd=site_dir, t=s$component)
        write_feather(out_sitemonth, sitemonth_file)

        update_data_tracker_r(network=domain, domain=domain,
            tracker_name='held_data', set_details=s, new_status='ok')
    }
}

#. handle_errors
munge_neon_site = function(domain, site, prod, tracker, silent=TRUE){
    # domain='neon'; site='ARIK'; prod=prodname_ms; tracker=held_data

    retrieval_log = extract_retrieval_log(held_data, prod, site)

    if(nrow(retrieval_log) == 0){
        return()
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){

        sitemonth = retrieval_log[k, 'component']
        comp = feather::read_feather(glue('data/{n}/{d}/raw/',
            '{p}/{s}/{sm}.feather', n=network, d=domain, p=prod, s=site,
            sm=sitemonth))

        prodcode = strsplit(prod, '_')[[1]][2]
        processing_func = get(paste0('process_1_', prodcode))

        out_comp = sw(do.call(processing_func,
            args=list(set=comp, site_name=site)))
        out = bind_rows(out, out_comp)
    }

    prod_dir = glue('data/{n}/{d}/munged/{p}', n=network, d=domain, p=prod)
    dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)

    site_file = glue('{pd}/{s}.feather', pd=prod_dir, s=site)
    write_feather(out, site_file)

    #create a link to the new file from the portal repo
    #(from and to seem logically reversed in file.link)
    sw(file.link(to=glue('../portal/data/{d}/{p}/{s}.feather',
        d=domain, p=strsplit(prod, '_')[[1]][1], s=site), from=site_file))

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prod=prodname_ms, site=site, new_status='ok')

    return('sitemunge complete')
}

#. handle_errors
download_sitemonth_details <- function(geturl){

    d = httr::GET(geturl)
    d = jsonlite::fromJSON(httr::content(d, as="text"))

    return(d)
}

#. handle_errors
determine_upstream_downstream <- function(d_){

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

#. handle_errors
get_avail_neon_products <- function(){

    req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/"))
    txt = httr::content(req, as="text")
    data_pile = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
    prodlist = data_pile$data$productCode

    return(prodlist)
}

#. handle_errors
get_neon_product_specs <- function(code){

    prodlist = get_avail_neon_products()

    prod_variant_inds = grep(code, prodlist)

    if(length(prod_variant_inds) > 1){
        stop(glue('More than one product variant for this prodcode. Did neon ',
            'make a v.002 data product?')
    }

    newest_variant_ind = prodlist[prod_variant_inds] %>%
        substr(11, 13) %>%
        as.numeric() %>%
        which.max()

    prodcode_full = prodlist[prod_variant_inds[newest_variant_ind]]
    prod_version = strsplit(prodcode_full, '\\.')[[1]][3]

    return(list(prodcode_full=prodcode_full, prod_version=prod_version))
}

#. handle_errors
get_avail_neon_product_sets <- function(prodcode_full){

    #returns: tibble with url, site_name, component columns

    avail_sets = tibble()

    req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
        prodcode_full))
    txt = httr::content(req, as="text")
    neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)

    urls = unlist(neondata$data$siteCodes$availableDataUrls)

    avail_sets = stringr::str_match(urls,
        '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})') %>%
        as_tibble(.name_repair='unique') %>%
        rename(url=`...1`, site_name=`...2`, component=`...3`)

    return(avail_sets)
}

#. handle_errors
populate_set_details <- function(tracker, prod, site, avail){

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

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
        stop(glue('Must run `track_new_site_components` before ',
            'running `populate_set_details`'))
    }

    return(retrieval_tracker)
}


# resolve_neon_naming_conflicts_OBSOLETE = function(out_sub_, replacements=NULL,
#     from_api=FALSE, set_details_){
#
#     #obsolete now that neonUtilities package is working
#
#     #replacements is a named vector. name=find, value=replace;
#     #failed match does nothing
#
#     prodcode = set_details_$prodcode
#     site = set_details_$site
#     date = set_details_$date
#
#     out_cols = colnames(out_sub_)
#
#     if(! is.null(replacements)){
#
#         #get list of variables included
#         varind = grep('SciRvw', out_cols)
#         rgx = str_match(out_cols[varind], '^(\\w*)(?:FinalQFSciRvw|SciRvwQF)$')
#         # varlist = flagprefixlist = rgx[,2]
#         varlist = rgx[,2]
#
#         #harmonize redundant variable names
#         for(i in 1:length(replacements)){
#             r = replacements[i]
#             varlist = replace(varlist, which(varlist == names(r)), r)
#         }
#     }
#
#     if('startDate' %in% out_cols){
#         colnames(out_sub_) = replace(out_cols, which(out_cols == 'startDate'),
#             'startDateTime')
#     } else if('endDate' %in% out_cols){
#         colnames(out_sub_) = replace(out_cols, which(out_cols == 'endDate'),
#             'startDateTime') #not a mistake
#     } else if(! 'startDateTime' %in% out_cols){
#         msg = glue('Datetime column not found for site ',
#             '{site} ({prod}, {date}).', site=site, prod=prodcode, date=date)
#         logwarn(msg, logger=logger_module)
#         return(generate_ms_err())
#     }
#
#     #subset relevant columns (needed if NEON API was used)
#     if(from_api){
#         flagcols = grepl('.+?FinalQF(?!SciRvw)', out_cols,
#             perl=TRUE)
#         datacols = ! grepl('QF', out_cols, perl=TRUE)
#         relevant_cols = flagcols | datacols
#         out_sub_ = out_sub_[, relevant_cols]
#     }
#
#     return(out_sub_)
# }

# determine_upstream_downstream_api_OBSOLETE = function(d_, data_inds_, set_details_){
#     #obsolete now that neonUtilities package is working
#
#     prodcode = set_details_$prodcode
#     site = set_details_$site
#     date = set_details_$date
#
#     #determine which dataset is upstream/downstream if necessary
#     updown_suffixes = c('-up', '-down')
#     if(length(data_inds_) == 2){
#         position = str_split(d_$data$files$name[data_inds_[1]], '\\.')[[1]][7]
#         updown_order = if(position == '101') 1:2 else 2:1
#     } else if(length(data_inds_) == 1){
#         updown_order = 1
#     } else {
#         msg = glue('Problem with upstream/downstream indicator for site ',
#             '{site} ({prod}, {date}).', site=site, prod=prodcode, date=date)
#         logwarn(msg, logger=logger_module)
#         return(generate_ms_err())
#     }
#
#     site_with_suffixes = paste0(site, updown_suffixes[updown_order])
#
#     return(site_with_suffixes)
# }
