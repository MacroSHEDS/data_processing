ms_pasta_domain_refmap = list(
    hbef = 'knb-lter-hbr',
    hjandrews = 'knb-lter-and'
)

#. handle_errors
get_latest_product_version <- function(prodname_ms, domain, data_tracker){

    vsn_endpoint = 'https://pasta.lternet.edu/package/eml/'

    domain_ref = ms_pasta_domain_refmap[[domain]]
    prodcode = prodcode_from_prodname_ms(prodname_ms=prodname_ms)

    vsn_request = glue(vsn_endpoint, domain_ref, '/', prodcode)
    newest_vsn = RCurl::getURLContent(vsn_request, timeout=10)
    newest_vsn = as.numeric(stringr::str_match(newest_vsn,
        '[0-9]+$')[1])

    return(newest_vsn)
}

#. handle_errors
get_avail_lter_product_sets <- function(prodname_ms, version, domain,
    data_tracker){

    #returns: tibble with url, site_name, component (aka element_name)

    name_endpoint = 'https://pasta.lternet.edu/package/name/eml/'
    dl_endpoint = 'https://pasta.lternet.edu/package/data/eml/'

    domain_ref = ms_pasta_domain_refmap[[domain]]
    prodcode = prodcode_from_prodname_ms(prodname_ms)

    name_request = glue(name_endpoint, domain_ref, '/', prodcode, '/',
        version)
    reqdata = RCurl::getURLContent(name_request)
    reqdata = strsplit(reqdata, '\n')[[1]]
    reqdata <- grep('Constants', reqdata, invert = TRUE, value = TRUE) #junk filter for hbef. might need flex
    reqdata = str_match(reqdata, '([0-9a-zA-Z]+),(.+)')

    element_ids = reqdata[,2]
    dl_urls = paste0(dl_endpoint, domain_ref, '/', prodcode, '/', version,
        '/', element_ids)

    names <- str_match(reqdata[,3], '(.+?)_.*')[,2]
    names[names %in% c(domain, network)] = 'sitename_NA'

    avail_sets = tibble(url=dl_urls,
        site_name=names,
        component=reqdata[,3])

    return(avail_sets)
}

#. handle_errors
populate_set_details <- function(tracker, prodname_ms, site_name, avail,
    latest_vsn){

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker = tracker[[prodname_ms]][[site_name]]$retrieve
    prodcode = prodcode_from_prodname_ms(prodname_ms)

    retrieval_tracker = avail %>%
        mutate(
            avail_version = latest_vsn,
            prodcode_full = NA, #no such thing for lter. could simply omit
            prodcode_id = prodcode,
            prodname_ms = prodname_ms) %>%
        full_join(retrieval_tracker, by='component') %>%
        # filter(status != 'blacklist' | is.na(status)) %>%
        mutate(
            held_version = as.numeric(held_version),
            needed = avail_version - held_version > 0)

    if(any(is.na(retrieval_tracker$needed))){
        msg = paste0('Must run `track_new_site_components` before ',
            'running `populate_set_details`')
        logerror(msg, logger=logger_module)
        stop(msg)
    }

    return(retrieval_tracker)
}

#. handle_errors
get_lter_data <- function(domain, sets, tracker, silent=TRUE){

    if(nrow(sets) == 0) return()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {st}, {p}, {c}',
            st=s$site_name, p=s$prodname_ms, c=s$component)
        loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        result = do.call(processing_func,
            args=list(set_details=s, network=network, domain=domain))
        # process_0_1(set_details=s, network=network, domain=domain)

        new_status <- evaluate_result_status(result)
        update_data_tracker_r(network=network, domain=domain,
            tracker_name='held_data', set_details=s, new_status=new_status)
    }
}

# munge engines (consider moving to global helpers) ####

#. handle_errors
munge_lter_site <- function(domain, site_name, prodname_ms, tracker,
                            silent=TRUE){
    #for when a data product is neatly organized with one site per file,
    #and all components will be munged
    #(probably rare, but neon has this arrangement).

    retrieval_log = extract_retrieval_log(tracker, prodname_ms, site_name)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode = prodcode_from_prodname_ms(prodname_ms)

        processing_func = get(paste0('process_1_', prodcode))
        in_comp = retrieval_log[k, 'component', drop=TRUE]

        out_comp = sw(do.call(processing_func,
                              args=list(network=network, domain=domain, prodname_ms=prodname_ms,
                                        site_name=site_name, component=in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }
    }

    if(sum(dim(out)) > 0){

        ready_to_link <- ifelse(grepl('(precip_f|precip_c|precipi)',
                                      prodname_ms),
                                FALSE,
                                TRUE)

        is_spatial <- ifelse(grepl('(location|boundary)',
                                   prodname_ms),
                             TRUE,
                             FALSE)

        write_ms_file(d = out,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = site_name,
                      level = 'munged',
                      shapefile = is_spatial,
                      link_to_portal = ready_to_link)
    }

    update_data_tracker_m(network=network, domain=domain,
                          tracker_name='held_data', prodname_ms=prodname_ms, site_name=site_name,
                          new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
               p=prodname_ms, n=network, d=domain, s=site_name)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}

#. handle_errors
munge_lter_combined <- function(domain, site_name, prodname_ms, tracker,
                                silent=TRUE){
    #for when a data product has multiple sites in each component, and
    #all components will be munged

    retrieval_log = extract_retrieval_log(tracker, prodname_ms, site_name) %>%
        filter(component != "Analytical Methods")

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode = prodcode_from_prodname_ms(prodname_ms)

        processing_func = get(paste0('process_1_', prodcode))
        in_comp = pull(retrieval_log[k, 'component'])

        out_comp = sw(do.call(processing_func,
                              args=list(network=network, domain=domain, prodname_ms=prodname_ms,
                                        site_name=site_name, component=in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }

        sites <- unique(out_comp$site_name)

        for(i in 1:length(sites)){

            filt_site <- sites[i]
            out_comp_filt <- filter(out_comp, site_name == filt_site)

            #make a portal link for precip gauge locations and pflux, but not for any
            #other precip product, because the others need to be localized to
            #watersheds
            ready_to_link <- ifelse(grepl('(precip_c|precipi)',
                                          prodname_ms),
                                    FALSE,
                                    TRUE)

            is_spatial <- ifelse(grepl('(location|boundary)',
                                       prodname_ms),
                                 TRUE,
                                 FALSE)

            write_ms_file(d = out_comp_filt,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_name = filt_site,
                          level = 'munged',
                          shapefile = is_spatial,
                          link_to_portal = ready_to_link)
        }
    }

    update_data_tracker_m(network=network, domain=domain,
                          tracker_name='held_data', prodname_ms=prodname_ms, site_name=site_name,
                          new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
               p=prodname_ms, n=network, d=domain, s=site_name)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}

#. handle_errors
munge_lter_combined_split <- function(domain, site_name, prodname_ms, tracker,
                                      silent=TRUE){
    #for when a data product has multiple sites in each component, and
    #logic governing the use of components will be handled within the kernel

    # tracker=held_data; k=1

    retrieval_log = extract_retrieval_log(tracker, prodname_ms, site_name) %>%
        filter(component != "Analytical Methods")

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    prodcode = prodcode_from_prodname_ms(prodname_ms)

    processing_func = get(paste0('process_1_', prodcode))
    components = pull(retrieval_log, component)

    out_comp = sw(do.call(processing_func,
                          args=list(network=network, domain=domain, prodname_ms=prodname_ms,
                                    site_name=site_name, components=components)))

    if(is_ms_err(out_comp)){
        return(out_comp)
    }

    sites <- unique(out_comp$site_name)

    for(i in 1:length(sites)){

        filt_site <- sites[i]
        out_comp_filt <- filter(out_comp, site_name == filt_site)

        #make a portal link for precip gauge locations and pflux, but not for any
        #other precip product, because the others need to be localized to
        #watersheds
        ready_to_link <- ifelse(grepl('(precip_f|precip_c|precipi)',
                                      prodname_ms),
                                FALSE,
                                TRUE)

        is_spatial <- ifelse(grepl('(location|boundary)',
                                   prodname_ms),
                             TRUE,
                             FALSE)

        write_ms_file(d = out_comp_filt,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = filt_site,
                      level = 'munged',
                      shapefile = is_spatial,
                      link_to_portal = ready_to_link)
    }

    update_data_tracker_m(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          prodname_ms = prodname_ms,
                          site_name = site_name,
                          new_status = 'ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
               p=prodname_ms, n=network, d=domain, s=site_name)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}
