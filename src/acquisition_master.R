library(httr)
library(jsonlite)
library(tidyr)
library(plyr)
library(data.table)
library(dtplyr)
library(tidyverse)
library(lubridate)
library(feather)
library(glue)
library(logging)
library(emayili)
library(neonUtilities)
library(tinsel)

setwd('/home/mike/git/macrosheds/data_acquisition')
conf = jsonlite::fromJSON('config.json')

#set up global logger. network-domain loggers are set up later
logging::basicConfig()
logging::addHandler(logging::writeToFile, logger='ms',
    file='logs/0_ms_master.log')

source('src/global_helpers.R')
source_decoratees('src/global_helpers.R') #parse decorators

network_domain = read_csv('data/general/site_data.csv') %>%
    filter(as.logical(in_workflow)) %>%
    select(network, domain) %>%
    distinct() %>%
    arrange(network, domain)

ms_globals = c(ls(all.names=TRUE), 'email_err_msg')

for(dmnrow in 1:nrow(network_domain)){

    network = network_domain$network[dmnrow]
    domain = network_domain$domain[dmnrow]

    logger_module = set_up_logger(network=network, domain=domain)
    logging::loginfo(logger=logger_module,
        msg=glue('Processing network: {n}, domain: {d}', n=network, d=domain))

    get_all_local_helpers(network=network, domain=domain)

    source(glue('src/{n}/{d}/retrieve.R', n=network, d=domain))
    source(glue('src/{n}/{d}/munge.R', n=network, d=domain))
    # source(glue('src/{n}/{d}/derive.R', n=network, d=domain)

    retain_ms_globals(ms_globals)
}

if(length(email_err_msg)){
    email_err(email_err_msg, conf$report_emails, conf$gmail_pw)
}

logging::loginfo('Run complete', logger='ms.module')
