
#. handle_errors
get_neon_data = function(domain, sets, tracker, silent=TRUE){
    # sets=new_sets; i=20; tracker=held_data

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {st}, {p}, {c}',
            st=s$site_name, p=s$prodname_ms, c=s$component)
        loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        result = do.call(processing_func,
            args=list(set_details=s, network=network, domain=domain))

        new_status <- evaluate_result_status(result)
        update_data_tracker_r(network=network, domain=domain,
            tracker_name='held_data', set_details=s, new_status=new_status)

    }
}

#. handle_errors
munge_neon_site <- function(domain, site_name, prodname_ms, tracker, silent=TRUE){
    # site_name=sites[j]; tracker=held_data

    retrieval_log = extract_retrieval_log(held_data, prodname_ms, site_name)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){

        # sitemonth = retrieval_log[k, 'component']
        # comp = read_feather(glue('data/{n}/{d}/raw/',
        #     '{p}/{s}/{sm}.feather', n=network, d=domain, p=prodname_ms, s=site_name,
        #     sm=sitemonth))

        prodcode = prodcode_from_prodname_ms(prodname_ms)

        processing_func = get(paste0('process_1_', prodcode))
        in_comp = retrieval_log[k, 'component']

        out_comp = sw(do.call(processing_func,
            # args=list(set=comp, network=network, domain=domain,site_name=site_name)))
            args=list(network=network, domain=domain, prodname_ms=prodname_ms,
                site_name=site_name, component=in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }

    }

    write_ms_file(d = out,
        network = network,
        domain = domain,
        prodname_ms = prodname_ms,
        site_name = site_name,
        level = 'munged',
        shapefile = FALSE,
        link_to_portal = TRUE)

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prodname_ms=prodname_ms, site_name=site_name,
        new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
            p=prodname_ms, n=network, d=domain, s=site_name)
    loginfo(msg, logger=logger_module)

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
    updown[updown == '1'] = '-up' #1 means upstream sensor

    #2 means downstream sensor. 0 means only one sensor? 3 means ???
    updown[updown %in% c('0', '2', '3')] = ''

    if(any(! updown %in% c('-up', ''))){
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

        if(length(keep) != 1) {
            stop(glue('More than one product variant for this prodcode. Did neon ',
                      'make a v.002 data product?'))
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
populate_set_details <- function(tracker, prodname_ms, site_name, avail){

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker = tracker[[prodname_ms]][[site_name]]$retrieve

    rgx = '/((DP[0-9]\\.[0-9]+)\\.([0-9]+))/[A-Z]{4}/[0-9]{4}\\-[0-9]{2}$'
    rgx_capt = str_match(avail$url, rgx)[, -1]

    retrieval_tracker = avail %>%
        mutate(
            avail_version = as.numeric(rgx_capt[, 3]),
            prodcode_full = rgx_capt[, 1],
            prodcode_id = rgx_capt[, 2],
            prodname_ms = prodname_ms) %>%
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

write_neon_readme = function(raw_neonfile_dir, dest){

    readme_name = grep('readme', list.files(raw_neonfile_dir), value=TRUE)
    readme = read_feather(glue(raw_neonfile_dir, '/', readme_name))
    readr::write_lines(readme$X1, dest)
}

write_neon_variablekey = function(raw_neonfile_dir, dest){

    varkey_name = grep('variables', list.files(raw_neonfile_dir), value=TRUE)
    varkey = read_feather(glue(raw_neonfile_dir, '/', varkey_name))
    write_csv(varkey, dest)

    return(varkey)
}
