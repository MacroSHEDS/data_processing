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

#note: see neon_notes.txt

#todo: build data blacklist (to minimize email error notifications)

setwd('/home/mike/git/macrosheds/')
source('data_acquisition/src/helpers.R')
source('data_acquisition/src/neon/neon_helpers.R')
source('data_acquisition/src/neon/neon_processing_kernels.R')

domain = 'neon'

logging::basicConfig()
logging::addHandler(logging::writeToFile, logger=domain,
    file=glue('data_acquisition/logs/{d}.log', d=domain))
# logReset()

conf = jsonlite::fromJSON('data_acquisition/config.json')

neonprods = readr::read_csv('data_acquisition/data/neon/neon_products.csv') %>%
    mutate(prodcode = sprintf('%05d', prodcode)) %>%
    filter(status == 'ready')

# sets=new_sets; i=1; tracker=held_data
get_neon_data = function(domain, sets, tracker, silent=TRUE){

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {site}, {prod}, {month}',
            site=s$site_name, prod=s$prodname_ms, month=s$component)
        logging::loginfo(msg, logger='neon.module')

        processing_func = get(paste0('process_0_', s$prodcode_id))
        out_sitemonth = do.call(processing_func, args=list(set_details=s))

        if(is_ms_exception(out_sitemonth)){
            update_data_tracker_r(domain, tracker_name='held_data', set_details=s,
                new_status='error')
            next
        } else if(is_ms_err(out_sitemonth)){
            update_data_tracker_r(domain, tracker_name='held_data', set_details=s,
                new_status='error')
            assign('email_err_msg', TRUE, pos=.GlobalEnv)
            next
        }

        site_dir = glue('data_acquisition/data/{d}/raw/{p}/{s}',
            d=domain, p=s$prodname_ms, s=s$site_name)
        dir.create(site_dir, showWarnings=FALSE, recursive=TRUE)

        sitemonth_file = glue('{sd}/{t}.feather',
            sd=site_dir, t=s$component)
        write_feather(out_sitemonth, sitemonth_file)

        update_data_tracker_r(domain, tracker_name='held_data', set_details=s,
            new_status='ok')
    }
}

# i=1; j=1
email_err_msg = FALSE
for(i in 1:nrow(neonprods)){
# for(i in 3){

    prodname_ms = paste0(neonprods$prodname[i], '_', neonprods$prodcode[i])
    prod_specs = get_neon_product_specs(neonprods$prodcode[i])
    if(is_ms_err(prod_specs)){
        msg = 'NEON may have created a v.002 product. investigate!'
        email_err(msg, 'mjv22@duke.edu', conf$gmail_pw)
        stop(msg)
    }

    held_data = get_data_tracker(domain)

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
                    s=curr_site, n=prodname_ms), logger='neon.module')
            next
        }

        update_data_tracker_r(domain, tracker=held_data)

        tryCatch({
            get_neon_data(domain=domain, new_sets, held_data)
        }, error=function(e){
            logging::logerror(e, logger='neon.module')
            email_err_msg <<- TRUE
        })

    }

    gc()
}

if(email_err_msg){
    email_err('neon data acquisition error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}
