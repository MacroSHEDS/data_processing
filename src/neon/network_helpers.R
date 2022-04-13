
get_neon_data <- function(domain,
                          sets,
                          tracker,
                          silent = TRUE){

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s <- sets[i, ]

        msg <- glue('Retrieving {st}, {p}, {c}',
                    st = s$site_code,
                    p = s$prodname_ms,
                    c = s$component)

        loginfo(msg,
                logger = logger_module)

        processing_func <- get(paste0('process_0_',
                                      s$prodcode_id))

        result <- do.call(processing_func,
                          args = list(set_details = s,
                                      network = network,
                                      domain = domain))

        new_status <- evaluate_result_status(result)

        if(new_status == 'error'){
            logging::logwarn(result,
                         logger = logger_module)
        }

        update_data_tracker_r(network = network,
                              domain = domain,
                              tracker_name = 'held_data',
                              set_details = s,
                              new_status = new_status)
    }
}

munge_neon_site <- function(domain, site_code, prodname_ms, tracker, silent=TRUE){
    # site_code=sites[j]; tracker=held_data

    retrieval_log <- extract_retrieval_log(held_data, prodname_ms, site_code)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){
   # for(k in 1:7){
        prodcode <- prodcode_from_prodname_ms(prodname_ms)

        processing_func <- get(paste0('process_1_', prodcode))
        in_comp <- retrieval_log[k, 'component']

        out_comp <- sw(do.call(processing_func,
                              args=list(network = network,
                                        domain = domain,
                                        prodname_ms = prodname_ms,
                                        site_code = site_code,
                                        component = in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out <- bind_rows(out, out_comp)
        }

    }

    if(nrow(out) == 0) {
        return(generate_ms_err(paste0('All data failed QA or no data is avalible at ', site_code)))
    }

        sensor <- case_when(prodname_ms == 'stream_chemistry__DP1.20093' ~ FALSE,
                            prodname_ms == 'stream_nitrate__DP1.20033' ~ TRUE,
                            prodname_ms == 'stream_temperature__DP1.20053' ~ TRUE,
                            prodname_ms == 'stream_PAR__DP1.20042' ~ TRUE,
                            prodname_ms == 'stream_gases__DP1.20097' ~ FALSE,
                            prodname_ms == 'stream_quality__DP1.20288' ~ TRUE,
                            prodname_ms == 'precip_chemistry__DP1.00013' ~ FALSE,
                            prodname_ms == 'precipitation__DP1.00006' ~ TRUE,
                            prodname_ms == 'discharge__DP4.00130' ~ TRUE,
                            prodname_ms == 'surface_elevation__DP1.20016' ~ TRUE)

        site_codes <- unique(out$site_code)
        for(y in 1:length(site_codes)) {

            d <- out %>%
                filter(site_code == !!site_codes[y])

            if(sensor){
                sampling_type <- 'I'
            } else{
                sampling_type <- 'G'
            }

            d <- identify_sampling_bypass(df = d,
                                          is_sensor =  sensor,
                                          domain = domain,
                                          network = network,
                                          prodname_ms = prodname_ms,
                                          sampling_type = sampling_type)

            d <- d %>%
                filter(!is.na(val))

            d <- remove_all_na_sites(d)

            if(nrow(d) == 0) return(NULL)

            d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

            if(nrow(d) == 0) return(NULL)

            d <- synchronize_timestep(d)

            write_ms_file(d = d,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_code = site_codes[y],
                          level = 'munged',
                          shapefile = FALSE,
                          link_to_portal = FALSE)
    }

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prodname_ms=prodname_ms, site_code=site_code,
        new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
            p=prodname_ms, n=network, d=domain, s=site_code)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}

download_sitemonth_details <- function(geturl){

    d = httr::GET(geturl)
    d = jsonlite::fromJSON(httr::content(d, as="text"))

    return(d)
}

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

get_avail_neon_products <- function(){

    req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/"))
    txt = httr::content(req, as="text")
    data_pile = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
    prodlist = data_pile$data$productCode

    return(prodlist)
}

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

get_avail_neon_product_sets <- function(prodcode_full){

    #returns: tibble with url, site_code, component columns

    avail_sets = tibble()

    req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
        prodcode_full))
    txt = httr::content(req, as="text")
    neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)

    urls = unlist(neondata$data$siteCodes$availableDataUrls)

    avail_sets = stringr::str_match(urls,
        '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})') %>%
        as_tibble(.name_repair='unique') %>%
        rename(url=`...1`, site_code=`...2`, component=`...3`)

    return(avail_sets)
}

populate_set_details <- function(tracker, prodname_ms, site_code, avail){

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker = tracker[[prodname_ms]][[site_code]]$retrieve

    rgx = '/((DP[0-9]\\.[0-9]+)\\.([0-9]+))/[A-Z]{4}/[0-9]{4}\\-[0-9]{2}$'
    rgx_capt = str_match(avail$url, rgx)[, -1, drop = FALSE]

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
