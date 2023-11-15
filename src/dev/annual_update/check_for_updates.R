library(rvest)
library(stringr)
library(googlesheets4)
library(tidyverse)

setwd('/home/mike/git/macrosheds/data_acquisition')

conf <- jsonlite::fromJSON('config.json',
                           simplifyDataFrame = FALSE)

source('src/dev/annual_update/helpers.R')

site_doi_license <- googlesheets4::read_sheet(
    conf$site_doi_license_gsheet,
    skip = 4,
    na = c('', 'NA'),
    col_types = 'c',
    col_names = TRUE
) %>%
    mutate(new_url = NA_character_)

for(i in seq_along(site_doi_license$link)){

    print(paste(i, site_doi_license$domain[i], site_doi_license$macrosheds_prodcode[i]))

    oldlink <- site_doi_license$link[i]
    last_dl <- site_doi_license$link_download_datetime[i]

    if(is.na(last_dl)){
        print('never downloaded before')
        site_doi_license$new_url[i] <- 'new'
        next
    }

    if(grepl('hydroshare', oldlink)){
        site_doi_license$new_url[i] <- check_for_updates_hydroshare(oldlink, last_dl)
    } else if(grepl('edirepository', oldlink)){
        site_doi_license$new_url[i] <- check_for_updates_edi(oldlink, last_dl)
    } else {
        print('not on hydroshare or EDI. skipping')
    }
}

#get last download dates for the rest. manually follow links in
#conf$site_doi_license_gsheet and cross-reference
site_doi_license %>%
    filter(is.na(new_url)) %>%
    select(network, domain, link_download_datetime) %>%
    print(n=999)
