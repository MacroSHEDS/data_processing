library(tidyverse)
library(lubridate)
library(feather)
library(glue)
library(logging)
library(emayili)

#note: see neon_notes.txt

#todo:

setwd('/home/mike/git/macrosheds/')
source('data_acquisition/src/helpers.R')
source('data_acquisition/src/neon/neon_helpers.R')
source('data_acquisition/src/neon/neon_processing_kernels.R')

domain = 'neon'

logging::basicConfig()
logging::addHandler(logging::writeToFile, logger=domain,
    file=glue('data_acquisition/logs/{d}.log', d=domain))

conf = jsonlite::fromJSON('data_acquisition/config.json')

neonprods = readr::read_csv('data_acquisition/data/neon/neon_products.csv') %>%
    mutate(prodcode = sprintf('%05d', prodcode)) %>%
    filter(status == 'ready')

# domain='neon'; site='ARIK'; prod=prodname_ms; tracker=held_data
munge_neon_site = function(domain, site, prod, tracker, silent=TRUE){

    retrieval_log = extract_retrieval_log(held_data, prod, site)

    if(nrow(retrieval_log) == 0){
        return()
    }

    out = tibble()
    for(k in 1:nrow(retrieval_log)){

        sitemonth = retrieval_log[k, 'component']
        comp = feather::read_feather(glue('data_acquisition/data/{d}/raw/',
            '{p}/{s}/{sm}.feather', d=domain, p=prod, s=site, sm=sitemonth))

        prodcode = strsplit(prod, '_')[[1]][2]
        processing_func = get(paste0('process_1_', prodcode))

        out_comp = suppressWarnings(do.call(processing_func,
            args=list(set=comp, site_name=site)))
        out = bind_rows(out, out_comp)
    }

    prod_dir = glue('data_acquisition/data/{d}/munged/{p}', d=domain, p=prod)
    dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)

    site_file = glue('{pd}/{s}.feather', pd=prod_dir, s=site)
    write_feather(out, site_file)

    update_data_tracker_m(domain, tracker_name='held_data', prod=prodname_ms,
        site=site, new_status='ok')

    gc()
}

# i=1; j=1; k=1
email_err_msg = FALSE
for(i in 1:nrow(neonprods)){

    prodname_ms = paste0(neonprods$prodname[i], '_', neonprods$prodcode[i])

    held_data = get_data_tracker(domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        logging::logwarn(glue('Product {p} is not yet tracked. Retrieve ',
            'it before munging it.', p=prodname_ms), logger='neon.module')
        next
    }

    sites = names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){

        tryCatch({
            munge_neon_site(domain, sites[j], prodname_ms, held_data)
        }, error=function(e){
            logging::logerror(e, logger='neon.module')
            email_err_msg <<- TRUE
            update_data_tracker_m(domain, tracker_name='held_data',
                prod=prodname_ms, site=site, new_status='error')
        })

    }
}

# #temp; only needed to get neon vizzed by kick-off meeting time
# fs = list.files('data_acquisition/data/neon/munged/chemistry_20093/',
#     full.names=TRUE)
# all_sites = tibble::tibble()
# for(f in fs){
#     x = read_feather(f)
#     all_sites = bind_rows(all_sites, x)
# }
# write_feather(all_sites, 'portal/data/neon/grab.feather')

if(email_err_msg){
    email_err('neon munge error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}
