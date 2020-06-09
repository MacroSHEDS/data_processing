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
library(tinsel)

#notice, in LTER parlance, our domains are "sites", our prodcodes
#are "identifiers", our components are "elements", and each element has an "element id"

network = 'lter'
domain = 'hbef'

setwd('/home/mike/git/macrosheds/data_acquisition')

source('src/global_helpers.R')
get_all_helpers(network=network, domain=domain)
# rm(process_0_1)
source_decoratees('src/lter/hbef/processing_kernels.R')
# source_decoratees('~/Desktop/test_dec.R')

logger_module = set_up_logger(network=network, domain=domain)

conf = jsonlite::fromJSON('config.json')

prod_info = get_product_info(network=network, domain=domain,
    # status_level='retrieve', get_statuses='ready')
    status_level='retrieve', get_statuses='pending')

# sets=new_sets; i=1; tracker=held_data
get_lter_data = function(domain, sets, tracker, silent=TRUE){

    if(nrow(sets) == 0) return()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {site}, {prod}, {month}',
            site=s$site_name, prod=s$prodname_ms, month=s$component)
        logging::loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        result = do.call(processing_func,
            args=list(set_details=s, network=network, domain=domain))
        # process_0_1(set_details=s, network=network, domain=domain)

        if(is_ms_err(result) || is_ms_exception(result)){
            update_data_tracker_r(network=network, domain=domain,
                tracker_name='held_data', set_details=s, new_status='error')
            next
        } else {
            update_data_tracker_r(network=network, domain=domain,
                tracker_name='held_data', set_details=s, new_status='ok')
        }
    }
}

# i=1; j=2
email_err_msg = list()
for(i in 1:nrow(prod_info)){

    prodname_ms = paste0(prod_info$prodname[i], '_', prod_info$prodcode[i])

    held_data = get_data_tracker(network=network, domain=domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        held_data = track_new_product(held_data, prodname_ms)
    }

    latest_vsn = get_latest_product_version(prodname=prodname_ms,
        domain=domain, data_tracker=held_data)

    if(is_ms_err(latest_vsn)){
        email_err_msg = append(email_err_msg, latest_vsn)
        next
    }

    avail_sets = get_avail_lter_product_sets(prodname=prodname_ms,
        version=latest_vsn, domain=domain, data_tracker=held_data)

    if(is_ms_err(avail_sets)){
        email_err_msg = append(email_err_msg, avail_sets)
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
            curr_site, avail_site_sets, latest_vsn)

        new_sets = filter_unneeded_sets(retrieval_details)

        if(nrow(new_sets) == 0){
            logging::loginfo(glue('Nothing to do for {s} {n}',
                    s=curr_site, n=prodname_ms), logger=logger_module)
            next
        }

        update_data_tracker_r(network=network, domain=domain, tracker=held_data)

        tryCatch({
            get_lter_data(domain=domain, new_sets, held_data)
        }, error=function(e){
            logging::logerror(e, logger=logger_module)
            email_err_msg <<- TRUE #must change this to list append
        })

    }

    gc()
}

if(length(email_err_msg)){
    email_err(email_err_msg, conf$report_emails, conf$gmail_pw)
}
