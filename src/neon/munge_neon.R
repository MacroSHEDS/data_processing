# library(httr)
# library(jsonlite)
# library(tidyr)
# library(plyr)
# library(data.table)
# library(dtplyr)
library(tidyverse)
# library(lubridate)
library(feather)
library(glue)
library(logging)
library(emayili)
# library(neonUtilities)

sm = suppressMessages
glue = glue::glue

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

# i=1; j=1; k=1
email_err_msg = FALSE
for(i in 1:nrow(neonprods)){
    # for(i in 3){

    prodcode = neonprods$prodcode[i]
    prodname_ms = paste0(neonprods$prodname[i], '_', prodcode)
    # prod_specs = get_neon_product_specs(neonprods$prodcode[i])
    # if(is_ms_err(prod_specs)){
    #     email_err('NEON may have created a v.002 product. investigate!',
    #         'mjv22@duke.edu', conf$gmail_pw)
    # }

    held_data = get_data_tracker(domain)

    if(! product_is_tracked(held_data, prodname_ms)){
        stop(glue('Product {p} is not yet tracked. Retrieve ',
            'it before munging it.', p=prodname_ms))
    }

    sites = names(held_data[[prodname_ms]])

    for(j in 1:length(sites)){

        site = sites[j]

        retrieval_log = held_data[[prodname_ms]][[site]]$retrieve %>%
            tibble::as_tibble() %>%
            filter(status == 'ok')

        #wrap this in the merge analog of get_neon_data
        out = tibble()
        for(k in 1:nrow(retrieval_log)){

            file_deets = retrieval_log[k, ]

            comp = feather::read_feather(glue('data_acquisition/data/{d}/raw/',
                '{p}/{s}/{f}.feather', d=domain, p=prodname_ms, s=site,
                f=file_deets$component))

            processing_func = get(paste0('process_1_', prodcode))

            out = do.call(processing_func, args=list(set=comp, set_details=1))
            # out = bind_rows(out, comp)
        }

        # prod_dir = glue('data_acquisition/data/{d}/munged/{p}',
        #     d=domain, p=prodname_ms)
        # dir.create(prod_dir, showWarnings=FALSE, recursive=TRUE)
        #
        # site_file = glue('{sd}/{t}.feather', pd=prod_dir, c=out)
        # write_feather(out, site_file)

        gc()
    }
}

if(email_err_msg){
    email_err('neon data acquisition error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}
