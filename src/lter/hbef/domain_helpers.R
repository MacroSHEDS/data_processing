
#. handle_errors
munge_hbef_site <- function(domain, site, prodname_ms, tracker, silent=TRUE){
    # site=sites[j]; tracker=held_data; k=1

    retrieval_log = extract_retrieval_log(held_data, prodname_ms, site)

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

    prod_dir = glue('data/{n}/{d}/munged/{p}', n=network, d=domain,
        p=prodname_ms)
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
    invisible(sw(file.link(to=portal_site_file, from=site_file)))

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prodname_ms=prodname_ms, site=site,
        new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
        p=prodname_ms, n=network, d=domain, s=site)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}

#. handle_errors
munge_hbef_combined <- function(domain, site, prodname_ms, tracker, prodcode,
    silent=TRUE){
    #site=sites[1]; tracker=held_data; k=1

    retrieval_log = extract_retrieval_log(held_data, prodname_ms, site) %>%
        filter(component != "Analytical_Methods.csv")

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

    gauges <- unique(out_comp$site_name)

    for(f in 1:length(gauges)) {
        new <- out_comp %>%
            filter(site_name == gauges[f])

        prod_dir = glue('data/{n}/{d}/munged/{p}', n=network, d=domain,
            p=prodname_ms)
        dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)
        site_file = glue('{pd}/{s}.feather', pd=prod_dir, s=gauges[f])
        write_feather(new, site_file)

        portal_prod_dir = glue('../portal/data/{d}/{p}',
            d=domain, p=strsplit(prodname_ms, '_')[[1]][1])
        dir.create(portal_prod_dir, showWarnings=FALSE, recursive=TRUE)
        portal_site_file = glue('{pd}/{s}.feather', pd=portal_prod_dir, s=gauges[f])

        #if there's already a data file for this site-time-product in the portal
        #repo, remove it
        unlink(portal_site_file)

        #create a link to the new file from the portal repo
        #(from and to seem logically reversed in file.link)
        invisible(sw(file.link(to=portal_site_file, from=site_file)))
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


