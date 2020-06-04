library(httr)
library(jsonlite)
library(tidyr)
library(plyr)
library(data.table)
library(dtplyr)
library(tidyverse)
# library(lubridate)
library(feather)
library(glue)
library(logging)
library(emayili)
library(neonUtilities)

network = 'neon'
domain = 'neon'

setwd('/home/mike/git/macrosheds/data_acquisition')

source('src/global_helpers.R')
get_all_helpers(network=network, domain=domain)

logger_module = set_up_logger(network=network, domain=domain)

conf = jsonlite::fromJSON('config.json')

prod_info = get_product_info(network=network, domain=NULL,
    status_level='retrieve', get_statuses='ready')

# sets=new_sets; i=1; tracker=held_data
get_neon_data = function(domain, sets, tracker, silent=TRUE){

    if(nrow(sets) == 0) return()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {site}, {prod}, {month}',
            site=s$site_name, prod=s$prodname_ms, month=s$component)
        logging::loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        out_sitemonth = do.call(processing_func, args=list(set_details=s))

        if(is_ms_exception(out_sitemonth)){
            update_data_tracker_r(network=domain, domain=domain,
                tracker_name='held_data', set_details=s, new_status='error')
            next
        } else if(is_ms_err(out_sitemonth)){
            update_data_tracker_r(network=domain, domain=domain,
                tracker_name='held_data', set_details=s, new_status='error')
            assign('email_err_msg', TRUE, pos=.GlobalEnv)
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

# i=1; j=1
email_err_msg = FALSE
for(i in 1:nrow(prod_info)){
# for(i in 4){

    prodname_ms = paste0(prod_info$prodname[i], '_', prod_info$prodcode[i])
    prod_specs = get_neon_product_specs(prod_info$prodcode[i])
    if(is_ms_err(prod_specs)){
        msg = 'NEON may have created a v.002 product. investigate!'
        email_err(msg, 'mjv22@duke.edu', conf$gmail_pw)
        stop(msg)
    }

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        held_data = track_new_product(held_data, prodname_ms)
    }

    avail_sets = sm(get_avail_neon_product_sets(prod_specs$prodcode_full))
    if(is_ms_err(avail_sets)){
        email_err_msg = TRUE
        next
    }

    #retrieve data by site; log acquisitions and revisions
    avail_sites = unique(avail_sets$site_name)
    for(j in 1:length(avail_sites)){

        curr_site = avail_sites[j]
        avail_site_sets = avail_sets[avail_sets$site_name == curr_site, ,
            drop=FALSE]

        if(! site_is_tracked(held_data, prodname_ms, curr_site)){
            held_data = insert_site_skeleton(held_data, prodname_ms, curr_site,
                site_components=avail_site_sets$component)
        }

        held_data = track_new_site_components(held_data, prodname_ms, curr_site,
            avail_site_sets)

        retrieval_details = populate_set_details(held_data, prodname_ms,
            curr_site, avail_site_sets, prod_specs)

        new_sets = filter_unneeded_sets(retrieval_details)

        if(nrow(new_sets) == 0){
            logging::loginfo(glue('Nothing to do for {s} {n}',
                    s=curr_site, n=prodname_ms), logger=logger_module)
            next
        }

        update_data_tracker_r(network=network, domain=domain, tracker=held_data)

        tryCatch({
            get_neon_data(domain=domain, new_sets, held_data)
        }, error=function(e){
            logging::logerror(e, logger=logger_module)
            email_err_msg <<- TRUE
        })

    }

    gc()
}

if(email_err_msg){
    email_err('neon data acquisition error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}
