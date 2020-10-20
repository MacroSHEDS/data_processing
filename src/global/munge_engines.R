#. handle_errors
munge_by_site <- function(network, domain, site_name, prodname_ms, tracker,
                          nolink_regex = '(precip_f|precip_c|precipi)',
                          spatial_regex = '(location|boundary)',
                          silent = TRUE){

    #for when a data product is neatly organized with one site per file,
    #and all components will be munged
    #(probably rare, but neon has this arrangement).

    #nolink_regex is a regex string that matches one or more prodname_ms
    #    values. If the prodname_ms being munged matches this string, a hardlink
    #    to the data portal will not be created when the munged file is written.
    #    use this for e.g. precip data, which is localized to a rain gauge
    #    when we retrieve it, but will be localized to a watershed after we
    #    derive it. We don't want precipGauge6.feather on the portal. We want
    #    watershed6.feather.
    #spatial_regex is a regex string that matches one or more prodname_ms
    #    values. If the prodname_ms being munged matches this string,
    #    write_ms_file will assume it's writing a spatial object, and not a
    #    standalone file

    retrieval_log <- extract_retrieval_log(tracker,
                                           prodname_ms,
                                           site_name)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out <- tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode <- prodcode_from_prodname_ms(prodname_ms)

        processing_func <- get(paste0('process_1_', prodcode))
        in_comp <- retrieval_log[k, 'component', drop=TRUE]

        out_comp <- sw(do.call(processing_func,
                               args = list(network = network,
                                           domain = domain,
                                           prodname_ms = prodname_ms,
                                           site_name = site_name,
                                           component = in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }
    }

    if(sum(dim(out)) > 0){

        ready_to_link <- ifelse(grepl(nolink_regex,
                                      prodname_ms),
                                FALSE,
                                TRUE)

        is_spatial <- ifelse(grepl(spatial_regex,
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

    update_data_tracker_m(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          prodname_ms = prodname_ms,
                          site_name = site_name,
                          new_status = 'ok')

    msg <- glue('munged {p} ({n}/{d}/{s})',
                p = prodname_ms,
                n = network,
                d = domain,
                s = site_name)

    loginfo(msg,
            logger = logger_module)

    return()
}

#. handle_errors
munge_combined <- function(network, domain, site_name, prodname_ms, tracker,
                           nolink_regex = '(precip_f|precip_c|precipi)',
                           spatial_regex = '(location|boundary)',
                           silent = TRUE){

    #for when a data product has multiple sites in each component, and
    #all components will be munged

    #nolink_regex is a regex string that matches one or more prodname_ms
    #    values. If the prodname_ms being munged matches this string, a hardlink
    #    to the data portal will not be created when the munged file is written
    #    use this for e.g. precip data, which is localized to a rain gauge
    #    when we retrieve it, but will be localized to a watershed after we
    #    derive it. We don't want precipGauge6.feather on the portal. We want
    #    watershed6.feather.
    #spatial_regex is a regex string that matches one or more prodname_ms
    #    values. If the prodname_ms being munged matches this string,
    #    write_ms_file will assume it's writing a spatial object, and not a
    #    standalone file

    retrieval_log <- extract_retrieval_log(tracker,
                                           prodname_ms,
                                           site_name)
        # filter(component != "Analytical Methods")

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out <- tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode <- prodcode_from_prodname_ms(prodname_ms)

        processing_func <- get(paste0('process_1_', prodcode))
        in_comp <- pull(retrieval_log[k, 'component'])

        out_comp <- sw(do.call(processing_func,
                               args = list(network = network,
                                           domain = domain,
                                           prodname_ms = prodname_ms,
                                           site_name = site_name,
                                           component = in_comp)))

        if(is.null(out_comp)) next

        if(is_blacklist_indicator(out_comp)){
            update_data_tracker_r(network = network,
                                  domain = domain,
                                  tracker_name = 'held_data',
                                  set_details = list(prodname_ms = prodname_ms,
                                                     site_name = site_name,
                                                     component = in_comp),
                                  new_status = 'blacklist')
        }

        #BUILD THIS REGION INTO A HANDLE-ALL FUNC
        #IS IT POSSIBLE TO return(next)?

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out <- bind_rows(out, out_comp)
        }

        sites <- unique(out_comp$site_name)

        for(i in 1:length(sites)){

            filt_site <- sites[i]
            out_comp_filt <- filter(out_comp, site_name == filt_site)

            #make a portal link for precip gauge locations and pflux, but not for any
            #other precip product, because the others need to be localized to
            #watersheds
            ready_to_link <- ifelse(grepl(nolink_regex,
                                          prodname_ms),
                                    FALSE,
                                    TRUE)

            is_spatial <- ifelse(grepl(spatial_regex,
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

    update_data_tracker_m(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          prodname_ms = prodname_ms,
                          site_name = site_name,
                          new_status = 'ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
               p = prodname_ms,
               n = network,
               d = domain,
               s = site_name)

    loginfo(msg,
            logger = logger_module)

    return()
}

#. handle_errors
munge_combined_split <- function(network, domain, site_name, prodname_ms, tracker,
                                 nolink_regex = '(precip_f|precip_c|precipi)',
                                 spatial_regex = '(location|boundary)',
                                 silent = TRUE){

    #for when a data product has multiple sites in each component, and
    #logic governing the use of components will be handled within the kernel

    #nolink_regex is a regex string that matches one or more prodname_ms
    #   values. If the prodname_ms being munged matches this string, a hardlink
    #   to the data portal will not be created when the munged file is written
    #   use this for e.g. precip data, which is localized to a rain gauge
    #   when we retrieve it, but will be localized to a watershed after we
    #   derive it. We don't want precipGauge6.feather on the portal. We want
    #   watershed6.feather.
    #spatial_regex is a regex string that matches one or more prodname_ms
    #   values. If the prodname_ms being munged matches this string,
    #   write_ms_file will assume it's writing a spatial object, and not a
    #   standalone file

    # tracker=held_data; k=1

    retrieval_log <- extract_retrieval_log(tracker,
                                           prodname_ms,
                                           site_name)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    prodcode <- prodcode_from_prodname_ms(prodname_ms)

    processing_func <- get(paste0('process_1_', prodcode))
    components <- pull(retrieval_log, component)

    out_comp <- sw(do.call(processing_func,
                           args = list(network = network,
                                       domain = domain,
                                       prodname_ms = prodname_ms,
                                       site_name = site_name,
                                       components = components)))

    if(is_ms_err(out_comp)){
        return(out_comp)
    }

    sites <- unique(out_comp$site_name)

    for(i in 1:length(sites)){

        filt_site <- sites[i]
        out_comp_filt <- filter(out_comp, site_name == filt_site)

        #make a portal link for precip gauge locations, but not for any
        #other precip product, because the others need to be localized to
        #watersheds
        ready_to_link <- ifelse(grepl(nolink_regex,
                                      prodname_ms),
                                FALSE,
                                TRUE)

        is_spatial <- ifelse(grepl(spatial_regex,
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
               p = prodname_ms,
               n = network,
               d = domain,
               s = site_name)

    loginfo(msg,
            logger = logger_module)

    return()
}

#. handle_errors
munge_by_site_product <- function(domain, site_name, prodname_ms, tracker,
                          nolink_regex = '(precip_f|precip_c|precipi)',
                          spatial_regex = '(location|boundary)',
                          silent = TRUE){

    #for when a data product is organized with only one site and one variable
    #in a data product (e.g. Konza has a discharge data product for each site)

    #nolink_regex is a regex string that matches one or more prodname_ms
    #    values. If the prodname_ms being munged matches this string, a hardlink
    #    to the data portal will not be created when the munged file is written.
    #    use this for e.g. precip data, which is localized to a rain gauge
    #    when we retrieve it, but will be localized to a watershed after we
    #    derive it. We don't want precipGauge6.feather on the portal. We want
    #    watershed6.feather.
    #spatial_regex is a regex string that matches one or more prodname_ms
    #    values. If the prodname_ms being munged matches this string,
    #    write_ms_file will assume it's writing a spatial object, and not a
    #    standalone file

    retrieval_log <- extract_retrieval_log(tracker,
                                           prodname_ms,
                                           site_name)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out <- tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode <- prodcode_from_prodname_ms(prodname_ms)

        processing_func <- get(paste0('process_1_', prodcode))
        in_comp <- retrieval_log[k, 'component', drop=TRUE]

        out_comp <- sw(do.call(processing_func,
                               args = list(network = network,
                                           domain = domain,
                                           prodname_ms = prodname_ms,
                                           site_name = site_name,
                                           component = in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }
    }

    sites <- sm(read_csv('data/general/site_data.csv'))

    site_prod <- str_split_fixed(prodname_ms, '_', n = Inf)[1,]

    site <- site_prod[length(site_prod)-2]

    site <- unique(grep(paste0('\\', site, '\\b'), sites$site_name, value = T))

    if(! length(site) == 1) {
        stop('Site name must be supplied imidetly before the __prodcode for munge_by_site_product')
    } else {
        ready_to_link <- ifelse(grepl(nolink_regex,
                                      prodname_ms),
                                FALSE,
                                TRUE)

        is_spatial <- ifelse(grepl(spatial_regex,
                                   prodname_ms),
                             TRUE,
                             FALSE)

        write_ms_file(d = out,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_name = site,
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

    msg <- glue('munged {p} ({n}/{d}/{s})',
                p = prodname_ms,
                n = network,
                d = domain,
                s = site_name)

    loginfo(msg,
            logger = logger_module)

    return()
}

