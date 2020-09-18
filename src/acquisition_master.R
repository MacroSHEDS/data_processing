suppressPackageStartupMessages({

    #we should be able to avoid librarying most or all of these,
    #since we're referring to their functions directly with ::

    # #spatial packages
    # library(gstat) #must load before raster package
    # # library(terra)  #must load before gstat package
    # library(raster) #raster has been replaced by terra (way faster)
    # library(stars)
    # library(sf)
    # library(sp)
    # library(mapview)
    # library(elevatr)

    #everything else
    library(httr)
    library(rgee)
    library(jsonlite)
    library(tidyr)
    library(plyr)
    library(data.table)
    # library(dtplyr)
    library(tidyverse)
    library(lubridate)
    library(feather)
    library(glue)
    library(logging)
    library(emayili)
    # library(neonUtilities)
    library(tinsel)
    library(PeriodicTable)
    library(imputeTS)
    library(errors)
    library(foreach)
    library(doParallel)

})

try(setwd('~/git/macrosheds/data_acquisition'), silent=TRUE) #mike
try(setwd('~/desktop/macrosheds/data_acquisition'), silent=TRUE) #spencer
try(setwd('C:/Users/mrvr/Desktop/mike/data_acquisition/'), silent=TRUE) #matt
try(setwd('/home/macrosheds/data_acquisition'), silent=TRUE) #server

#connect rgee to earth engine and python 
try(ee_Initialize(email = 'spencerrhea41@gmail.com', drive = TRUE))

conf = jsonlite::fromJSON('config.json')

#set up global logger. network-domain loggers are set up later
logging::basicConfig()
logging::addHandler(logging::writeToFile, logger='ms',
    file='logs/0_ms_master.log')

source('src/dev_helpers.R') #comment before pushing live
source('src/global/global_helpers.R')
source_decoratees('src/global/global_helpers.R') #parse decorators

ms_vars <- sm(read_csv('data/general/variables.csv'))
network_domain <- sm(read_csv('data/general/site_data.csv')) %>%
    filter(as.logical(in_workflow)) %>%
    select(network, domain) %>%
    distinct() %>%
    arrange(network, domain)

ms_globals = c(ls(all.names=TRUE), 'ms_globals')


dir.create('logs', showWarnings = FALSE)

# dmnrow=2
for(dmnrow in 1:nrow(network_domain)){

    network = network_domain$network[dmnrow]
    domain = network_domain$domain[dmnrow]

    logger_module = set_up_logger(network=network, domain=domain)
    loginfo(logger=logger_module,
        msg=glue('Processing network: {n}, domain: {d}', n=network, d=domain))

    update_product_statuses(network=network, domain=domain)
    get_all_local_helpers(network=network, domain=domain)

    ms_retrieve(network=network, domain=domain)
    ms_munge(network=network, domain=domain)
    ms_derive(network=network, domain=domain)
    ms_general(network=network, domain=domain)

    retain_ms_globals(ms_globals)
}


if(length(email_err_msgs)){
    email_err(email_err_msgs, conf$report_emails, conf$gmail_pw)
}

loginfo('Run complete', logger='ms.module')
