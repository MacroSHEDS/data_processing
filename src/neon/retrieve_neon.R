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
library(data.tree)

sm = suppressMessages

#note: see neon_notes.txt

#todo: build data blacklist (to minimize email error notifications)

setwd('/home/mike/git/macrosheds/')
source('data_acquisition/src/helpers.R')
source('data_acquisition/src/neon/neon_helpers.R')
source('data_acquisition/src/neon/neon_processing_kernels.R')

domain = 'neon'

logging::basicConfig()
logging::addHandler(logging::writeToFile, logger=domain,
    file=glue::glue('data_acquisition/logs/{d}.log', d=domain))
# logReset()

conf = jsonlite::fromJSON('data_acquisition/config.json')

neonprods = readr::read_csv('data_acquisition/data/neon/neon_products.csv')
neonprods = filter(neonprods, status == 'ready')

# tracker = Node$new('neon')
# tracker$AddChild(prodname_ms)
# # tracker[[prodname_ms]]$AddChild(curr_site)
# retrieve_insert = data.frame(month=character(), status=character())
# retrieve_insert = data.frame(month='2012-04', mtime=as.POSIXct('1900-01-01'),
#     held_version='1', status='ok')
# munge_derive_insert = list(mtime=as.POSIXct('1900-01-01'), status='pending')
# tracker[[prodname_ms]]$AddChild(curr_site, retrieve=retrieve_insert,
#     munge=munge_derive_insert, derive=munge_derive_insert)
# zz = jsonlite::toJSON(as.list(tracker))
# qq = jsonlite::fromJSON(zz)
# class(qq$chemistry_20093$ARIK$retrieve)
# qq2 = as.Node(qq)
# qq2 = qq2$Do(function(n) n=as.data.frame(as.list(n)),
#     filterFun=function(x) x$name == 'retrieve')
# as.data.frame(as.list(qq2$chemistry_20093$ARIK$retrieve))

held_data = get_data_tracker(domain)
# held_data = get_data_tracker(domain=domain, category='held', level=0)
# problem_data = get_data_tracker(domain=domain, category='problem', level=0)
# blacklist_data = get_data_tracker(domain=domain, category='blacklist', level=0)

# sets = site_sets; i=1
get_neon_data = function(sets, prodcode, silent=TRUE){

    processing_func = get(paste0('process_0_', prodcode))

    out = tibble()
    # successes = failures = blacklist = c()
    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        url = sets[i, 1]
        site = sets[i, 2]
        date = sets[i, 3]

        loginfo = list(site=site, date=date, prodcode=prodcode, url=url)

        msg = glue('Processing site ',
            '{site} ({prod}, {date}).', site=site, prod=prodcode, date=date)
        logging::loginfo(msg, logger='neon.module')

        # deets = download_sitemonth_details(url) #obsolete since neonUtilities
        # if(is_ms_err(deets) || 'error' %in% names(deets)){
        #     assign('email_err_msg', TRUE, pos=.GlobalEnv)
        #     next
        # }

        out_sitemonth = do.call(processing_func, args=list(loginfo=loginfo))

        if(is_ms_exception(out_sub)){
            # update_blacklist_data(new_dates=date, loginfo)
            update_data_tracker_dates(date, loginfo, domain, category='problem',
                level=0)
            next
        } else if(is_ms_err(out_sub)){
            update_data_tracker_dates(date, loginfo, domain, category='problem',
                level=0)
            assign('email_err_msg', TRUE, pos=.GlobalEnv)
            next
        }

        site_dir = glue::glue('data_acquisition/data/{d}/raw/{p}_{id}/{s}',
            d=domain, p=neonprods$prod[i], id=prodID, s=site)
        dir.create(site_dir, showWarnings=FALSE, recursive=TRUE)

        sitemonth_file = glue::glue('{sd}/{t}.feather', sd=site_dir, t=date)
        write_feather(out_sitemonth, sitemonth_file)

        update_data_tracker_dates(date, loginfo, domain, category='held', level=0)
    }

    return(out)
}

# i=1; j=1
email_err_msg = FALSE
for(i in 1:nrow(neonprods)){
# for(i in 3){

    outer_loop_err = FALSE #should be able to rework the end of this loop so this line isnt needed
    prodcode = neonprods$prodID[i]
    prodname_ms = paste0(neonprods$prod[i], '_', prodcode)
    prod_specs = get_neon_product_specs(prodcode)
    if(is_ms_err(prod_specs)){
        email_err('NEON may have created a v.002 product. investigate!',
            'mjv22@duke.edu', conf$gmail_pw)
    }

    # if(! prodname_ms %in% names(held_data)) held_data[[prodname_ms]] = list()
    if(! product_is_tracked(held_data, prodname_ms)){
        held_data = track_new_product(held_data, prodname_ms)
    }

    avail_sets = sm(get_avail_neon_product_sets(prod_specs$prodcode_full))
    if(is_ms_err(avail_sets)){
        email_err_msg = TRUE
        next
    }

    #retrieve data by site; log acquisitions (and revisions, once neon actually has them)
    #REVISIT THIS COMMENT
    avail_sites = unique(avail_sets$site_name)
    for(j in 1:length(avail_sites)){

        curr_site = avail_sites[j]
        avail_site_sets = avail_sets[avail_sets$site_name == curr_site, ,
            drop=FALSE]

        if(! curr_site %in% names(held_data[[prodname_ms]])){
            held_data = insert_site_skeleton(held_data, prodname_ms, curr_site,
                site_components=avail_site_sets$component)
        }

        # #get last modification times of held dataset chunks
        # held_files = list.files(glue::glue('data_acquisition/data/{d}/raw/{p}/{s}',
        #     d=domain, p=prodname_ms, s=curr_site), full.names=TRUE)
        # held_chunks = data.frame(held_chunk=str_match(held_files,
        #         '([0-9]{4}-[0-9]{2}).feather$')[, 2],
        #     mtime=file.info(held_files)$mtime)

        held_data = add_new_site_components(held_data, prodname_ms, curr_site,
            avail_site_sets)

        #filter already held or ignored sitemonths from avail_site_sets
        # new_sets = avail_site_sets$component[! avail_site_sets$component %in%
        #         retrieval_tracking$component]
        skip_sets = retrieval_tracking %>%
            filter(status != 'blacklist') %>%
            pull()

        prodfiles = list.files(glue::glue('data_acquisition/data/{d}/raw/{p}',
            d=domain, p=prodname_ms), full.names=TRUE)
        for(j in 1:length(prodfiles)){
            str_match(prodfiles[j], '[A-Z]{4}[down???]')
            held_data0 = get_data_tracker(domain=domain, category='held', level=0)
            held_data0[[neonprods$prodID[i]]]
            qqq = file.info(prodfiles[j])$mtime
        }


        avail_site_sets = avail_site_sets[! avail_site_sets[, 3] %in% skip_sets, , drop=FALSE]

        if(nrow(avail_site_sets) == 0){
            logging::loginfo(glue('Nothing to do for {s} {n}',
                    s=curr_site, n=prodname_ms),
                logger='neon.module')
            next
        }
        # avail_site_sets = avail_site_sets[1:1, , drop=FALSE]

        tryCatch({
            site_dset = get_neon_data(avail_site_sets, prod_specs$prodcode_full)
        }, error=function(e){
            logging::logerror(e, logger='neon.module')
            email_err_msg <<- outer_loop_err <<- TRUE
        })
        if(outer_loop_err) next

        dir.create(glue('data_acquisition/data/{d}/raw/{p}',
            d=domain, p=neonprods$prod[i]), showWarnings=FALSE)

        site_file = glue('data_acquisition/data/{d}/raw/{p}/',
            '{p}_{n}_{site}.feather',
            d=domain, p=neonprods$prod[i], n=prodname_ms, site=curr_site)

        write_feather(site_dset, site_file)

        #update modification time for product-site feather files
        #(note that held_data in global env may contain failed dates.
        #not sure why that is, but loading it from file here gets around
        #that issue)
        held_data = get_data_tracker(domain=domain, category='held', level=0)
        held_data[[prodname_ms]][[curr_site]]$mtime = file.info(site_file)$mtime

        readr::write_file(jsonlite::toJSON(held_data),
            glue::glue('data_acquisition/data/{d}/data_trackers/{l}/{c}_data.json',
                d=domain, l='0_retrieval_trackers', c='held'))
    }

    gc()
}

if(email_err_msg){
    email_err('neon data acquisition error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}

# zip('data/neon.zip', list.files('data/neon', recursive=TRUE, full.names=TRUE))
# out = drive_upload("data/neon.zip",
#     as_id('https://drive.google.com/drive/folders/0ABfF-JkuRvL5Uk9PVA'))
