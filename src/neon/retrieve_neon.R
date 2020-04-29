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

#neon data product codes are of this form: DPx.yyyyy.zzz,
#where x is data level (1=qaqc'd), y is product id, z is revision

#the final digit of horizontalPosition indicates upstream (1) or downstream (2)

setwd('/home/mike/git/macrosheds/')
source('data_acquisition/src/helpers.R')
source('data_acquisition/src/neon/neon_helpers.R')
source('data_acquisition/src/neon/neon_processing_kernels.R')

neonprods = readr::read_csv('data_acquisition/data/neon/neon_products.csv')
neonprods = filter(neonprods, status == 'pending')

conf = jsonlite::fromJSON('data_acquisition/config.json')
held_data = try(jsonlite::fromJSON(
    readr::read_file('data_acquisition/data/neon/held_data.json')), silent=TRUE)
if('try-error' %in% class(held_data)) held_data = list()

logging::basicConfig()
logging::addHandler(logging::writeToFile, logger='neon',
    file='data_acquisition/logs/neon.log')
# logReset()

# sets = site_sets; held=held_data; i=1
get_neon_data = function(sets, prodcode, silent=TRUE){

    processing_func = get(paste0('process_0_', prodcode))

    out = tibble()
    successes = c()
    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        url = sets[i, 1]
        site = sets[i, 2]
        date = sets[i, 3]

        loginfo = list(site=site, date=date, prodcode=prodcode, url=url)

        msg = glue('Processing site ',
            '{site} ({prod}, {date}).', site=site, prod=prodcode, date=date)
        logging::loginfo(msg, logger='neon.module')

        deets = download_sitemonth_details(url) #no longer needed?
        if(is_ms_err(deets) || 'error' %in% names(deets)){
            assign('email_err_msg', TRUE, pos=.GlobalEnv)
            next
        }

        out_sub = do.call(processing_func, args=list(loginfo=loginfo))
        if(is_ms_err(out_sub)){
            assign('email_err_msg', TRUE, pos=.GlobalEnv)
            next
        }

        out = bind_rows(out, out_sub)
        successes = append(successes, date)
        update_held_data(new_dates=successes, loginfo)
    }

    return(out)
}

# i=2; j=1; sets=site_sets
email_err_msg = FALSE
# for(i in 1:nrow(neonprods)){
for(i in 3){

    outer_loop_err = FALSE

    #get available datasets for this data product
    prodcode = neonprods$prodID[i]
    prodID = strsplit(prodcode, '\\.')[[1]][2]
    if(! prodcode %in% names(held_data)) held_data[[prodcode]] = list()

    tryCatch({
        req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
            prodcode))
        txt = httr::content(req, as="text")
        neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        email_err_msg <<- outer_loop_err <<- TRUE
    })
    if(outer_loop_err) next

    #get available urls, sites, and dates
    urls = unlist(neondata$data$siteCodes$availableDataUrls)
    avail_sets = stringr::str_match(urls, '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})')

    #retrieve data by site; log acquisitions (and revisions, once neon actually has them)
    avail_sites = unique(avail_sets[, 2])
    for(j in 1:length(avail_sites)){

        curr_site = avail_sites[j]
        site_sets = avail_sets[avail_sets[, 2] == curr_site, ]

        if(! curr_site %in% names(held_data[[prodcode]])){
            held_data[[prodcode]][[curr_site]] = vector(mode='character')
        }

        #filter already held sitemonths from site_sets ####
        # site_sets = site_sets[1:1, , drop=FALSE]

        tryCatch({
            site_dset = get_neon_data(site_sets, prodcode)
        }, error=function(e){
            logging::logerror(e, logger='neon.module')
            email_err_msg <<- outer_loop_err <<- TRUE
        })
        if(outer_loop_err) next

        dir.create(glue('data_acquisition/data/neon/raw/{p}',
            p=neonprods$prod[i]), showWarnings=FALSE)

        write_feather(site_dset,
            glue('data_acquisition/data/neon/raw/{p}/{p}_{id}_{site}.feather',
            p=neonprods$prod[i], id=prodID, site=curr_site))

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
