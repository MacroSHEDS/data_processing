
#. handle_errors
munge_hbef_site <- function(domain, site, prodname_ms, tracker, silent=TRUE){
    # site=sites[j]; tracker=held_data; k=1

    retrieval_log = extract_retrieval_log(tracker, prodname_ms, site)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode = prodcode_from_prodname_ms(prodname_ms)

        processing_func = get(paste0('process_1_', prodcode))
        in_comp = retrieval_log[k, 'component']

        out_comp = sw(do.call(processing_func,
            args=list(network=network, domain=domain, prodname_ms=prodname_ms,
                site_name=site, component=in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }
    }

    write_munged_file(d = out,
        network = network,
        domain = domain,
        prodname_ms = prodname_ms,
        site = site)

    create_portal_link(network = network,
        domain = domain,
        prodname_ms = prodname_ms,
        site = site)

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prodname_ms=prodname_ms, site=site,
        new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
        p=prodname_ms, n=network, d=domain, s=site)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}

#. handle_errors
munge_hbef_combined <- function(domain, site, prodname_ms, tracker, silent=TRUE){
    # site=sites[j]; tracker=held_data; k=1


    retrieval_log = extract_retrieval_log(tracker, prodname_ms, site) %>%
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
                site_name=site, component=in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out = bind_rows(out, out_comp)
        }

        sites <- unique(out_comp$site_name)

        for(i in 1:length(sites)){

            filt_site <- sites[i]
            out_comp_filt <- filter(out_comp, site_name == filt_site)

            write_munged_file(d = out_comp_filt,
                network = network,
                domain = domain,
                prodname_ms = prodname_ms,
                site = filt_site)

            create_portal_link(network = network,
                domain = domain,
                prodname_ms = prodname_ms,
                site = filt_site)
        }
    }

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prodname_ms=prodname_ms, site=site,
        new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
        p=prodname_ms, n=network, d=domain, s=site)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}
