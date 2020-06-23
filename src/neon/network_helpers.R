
#. handle_errors
get_neon_data = function(domain, sets, tracker, silent=TRUE){
    # sets=new_sets; i=20; tracker=held_data

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {site}, {prodname_ms}, {month}',
            site=s$site_name, prodname_ms=s$prodname_ms, month=s$component)
        loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        result = do.call(processing_func,
            args=list(set_details=s, network=network, domain=domain))

        if(is_ms_err(result) || is_ms_exception(result)){
            update_data_tracker_r(network=network, domain=domain,
                tracker_name='held_data', set_details=s, new_status='error')
            next
        } else {
            update_data_tracker_r(network=network, domain=domain,
                tracker_name='held_data', set_details=s, new_status='ok')
        }

        # if(is_ms_err(out_sitemonth) || is_ms_exception(out_sitemonth)){
        #     update_data_tracker_r(network=domain, domain=domain,
        #         tracker_name='held_data', set_details=s, new_status='error')
        #     next
        # }
        #
        # site_dir = glue('data/{n}/{d}/raw/{p}/{s}',
        #     n=network, d=domain, p=s$prodname_ms, s=s$site_name)
        # dir.create(site_dir, showWarnings=FALSE, recursive=TRUE)
        #
        # sitemonth_file = glue('{sd}/{t}.feather',
        #     sd=site_dir, t=s$component)
        # write_feather(out_sitemonth, sitemonth_file)
        #
        # update_data_tracker_r(network=domain, domain=domain,
        #     tracker_name='held_data', set_details=s, new_status='ok')
    }
}

#. handle_errors
munge_neon_site <- function(domain, site, prodname_ms, tracker, silent=TRUE){
    site=sites[j]; tracker=held_data

    retrieval_log = extract_retrieval_log(held_data, prodname_ms, site)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){

        # sitemonth = retrieval_log[k, 'component']
        # comp = read_feather(glue('data/{n}/{d}/raw/',
        #     '{p}/{s}/{sm}.feather', n=network, d=domain, p=prodname_ms, s=site,
        #     sm=sitemonth))

        prodcode = prodcode_from_prodname_ms(prodname_ms)

        processing_func = get(paste0('process_1_', prodcode))
        in_comp = retrieval_log[k, 'component']

        out_comp = sw(do.call(processing_func,
            # args=list(set=comp, network=network, domain=domain,site_name=site)))
            args=list(network=network, domain=domain, prodname_ms=prodname_ms,
                site_name=site, component=in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }

    }

    prod_dir = glue('data/{n}/{d}/munged/{p}', n=network, d=domain, p=prodname_ms)
    dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)
    site_file = glue('{pd}/{s}.feather', pd=prod_dir, s=site)
    write_feather(out, site_file)

    portal_prod_dir = glue('../portal/data/{d}/{p}',
        d=domain, p=strsplit(prodname_ms, '_')[[1]][1])
    dir.create(portal_prod_dir, showWarnings=FALSE, recursive=TRUE)
    portal_site_file = glue('{pd}/{s}.feather', pd=portal_prod_dir, s=site)

    #if there's already a data file for this site-time-product in the portal
    #repo, remove it
    unlink(portal_site_file)

    #create a link to the new file from the portal repo
    #(from and to seem logically reversed in file.link)
    sw(file.link(to=portal_site_file, from=site_file))

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prodname_ms=prodname_ms, site=site,
        new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
            p=prodname_ms, n=network, d=domain, s=site)
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

    if(length(prod_variant_inds) > 1){
        #works for this code but maybe not others? not sure the best way to handle this situation

        prod_info<- sm(read_csv("src/neon/products.csv")) %>%
            filter(prodcode == code)

        split <- unlist(strsplit(as.character(prod_info[1,"notes"]), split = " "))

        keep <- as.character(grep(code, split, value=T))

        if(length(keep) != 1) {
            stop(glue('More than one product variant for this prodcode. Did neon ',
                      'make a v.002 data product?'))
        }
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
populate_set_details <- function(tracker, prodname_ms, site, avail){

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker = tracker[[prodname_ms]][[site]]$retrieve

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
