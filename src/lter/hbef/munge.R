library(tidyverse)
library(lubridate)
library(feather)
library(glue)
library(logging)
library(emayili)

network = 'neon'
domain = 'neon'

setwd('/home/mike/git/macrosheds/data_acquisition')

source('src/global_helpers.R')
get_all_helpers(network=network, domain=domain)

logger_module = set_up_logger(network=network, domain=domain)

conf = jsonlite::fromJSON('config.json')

prod_info = get_product_info(network=network, domain=domain,
    status_level='munge', get_statuses='ready')

# domain='neon'; site='ARIK'; prod=prodname_ms; tracker=held_data
munge_neon_site = function(domain, site, prod, tracker, silent=TRUE){

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

    gc()
}

# i=1; j=1; k=1
email_err_msg = FALSE
for(i in 1:nrow(prod_info)){
for(i in 2){

    prodname_ms = paste0(prod_info$prodname[i], '_', prod_info$prodcode[i])

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        logging::logwarn(glue('Product {p} is not yet tracked. Retrieve ',
            'it before munging it.', p=prodname_ms), logger=logger_module)
        next
    }

    sites = names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){

        tryCatch({
            munge_neon_site(domain, sites[j], prodname_ms, held_data)
        }, error=function(e){
            logging::logerror(e, logger=logger_module)
            email_err_msg <<- TRUE
            update_data_tracker_m(network=network, domain=domain,
                tracker_name='held_data', prod=prodname_ms, site=sites[j],
                new_status='error')
        })

    }
}

if(email_err_msg){
    email_err('neon munge error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}