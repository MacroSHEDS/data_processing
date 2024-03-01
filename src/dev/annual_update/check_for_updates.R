library(rvest)
library(stringr)
library(googlesheets4)
library(tidyverse)
library(glue)

#A lot of this same code runs in the pipeline now. provenance updating is
#automated for EDI and hydroshare domains. This script is still useful
#for getting an idea of all that will be updated during a run. essential for
#record keeping and documenting as you go (makes a nice TODO itinerary)

#remember to copy the data folder over to an external drive, to
#keep a record of raw and munged products if people ever want them

#don't run this again after starting into the main acquisition loop.
#will overwrite files pertaining to this year's provenance
forthcoming_dataset_version <- 2

setwd('/home/mike/git/macrosheds/data_acquisition')

outdir <- glue('logs/v{vv}_prerun_housekeeping', vv = forthcoming_dataset_version)
dir.create(outdir, showWarnings = FALSE)

system(glue('tree data > {outdir}/data_structure_beginning_v{vv}.txt',
            vv = forthcoming_dataset_version),
       wait = TRUE)
tree_text <- system2('tree', 'data', stdout = TRUE)

# strip ANSI escape codes
ansi_escape_regex <- '\033\\[[0-9;]*m'
tree_text <- gsub(ansi_escape_regex, '', tree_text, perl = TRUE)

writeLines(tree_text, glue('{outdir}/data_structure_beginning_v{vv}.txt',
                           vv = forthcoming_dataset_version))

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

write_csv(site_doi_license, file.path(outdir, 'old_doi_license.csv'))

out <- matrix(NA, nrow = nrow(site_doi_license), ncol = 6,
              dimnames = list(NULL, c('domain', 'prodcode', 'status',
                                      'oldlink', 'newlink', 'last_dl')))

for(i in seq_along(site_doi_license$link)){

    dmn <- site_doi_license$domain[i]
    prodcode <- site_doi_license$macrosheds_prodcode[i]

    print(paste(i, dmn, prodcode))
    out[i, 'domain'] <- dmn
    out[i, 'prodcode'] <- prodcode

    oldlink <- site_doi_license$link[i]
    last_dl <- site_doi_license$link_download_datetime[i]

    out[i, 'oldlink'] <- oldlink
    out[i, 'last_dl'] <- last_dl

    if(is.na(last_dl)){
        print('never downloaded before')
        if(! is.na(oldlink)){
            site_doi_license$new_url[i] <- oldlink
            out[i, 'status'] <- oldlink
        } else {
            site_doi_license$new_url[i] <- 'new'
            out[i, 'status'] <- 'new'
        }
        next
    }

    if(grepl('hydroshare', oldlink)){
        site_doi_license$new_url[i] <- check_for_updates_hydroshare(oldlink, last_dl)
    } else if(grepl('edirepository', oldlink)){
        site_doi_license$new_url[i] <- check_for_updates_edi(oldlink, last_dl)
    } else {
        print('not on hydroshare or EDI. skipping')
    }

    if(grepl('^htt', site_doi_license$new_url[i])){
        out[i, 'newlink'] <- site_doi_license$new_url[i]
        out[i, 'status'] <- 'link changed'
    } else {
        out[i, 'status'] <- site_doi_license$new_url[i]
    }
}

write_csv(as_tibble(out), glue('{outdir}/product_update_statuses.csv'))

#get last download dates for the rest. manually follow links in
#conf$site_doi_license_gsheet and cross-reference
site_doi_license %>%
    filter(is.na(new_url)) %>%
    select(network, domain, link_download_datetime) %>%
    print(n=999)
