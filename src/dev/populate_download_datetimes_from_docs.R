
site_doi_license <- googlesheets4::read_sheet(
    conf$site_doi_license_gsheet,
    skip = 4,
    na = c('', 'NA'),
    col_types = 'c'
)

for(i in seq_len(nrow(site_doi_license))){

    ntw <- site_doi_license$network[i]
    dmn <- site_doi_license$domain[i]
    prodcode <- site_doi_license$macrosheds_prodcode[i]

    docfiles <- list.files(glue('data/{ntw}/{dmn}/raw/documentation/'))

    if(! any(grepl(paste0('__', prodcode, '\\.txt'), docfiles))){
        site_doi_license$link_download_datetime[i] <- ''
        next
    }


    prodname <- docfiles %>%
        str_subset(paste0('__', prodcode, '\\.txt')) %>%
        str_extract('documentation_([a-zA-Z_]+?)__', group = 1)

    dt <- read_metadata_r(ntw, dmn, paste(prodname[1], prodcode, sep = '__')) %>%
        str_extract('retrieved ([0-9\\- :]+)', group = 1) %>%
        paste(., 'UTC')

    if(dt == 'NA UTC'){
        #different format for CUSTOM products
        dt <- read_metadata_r(ntw, dmn, paste(prodname[1], prodcode, sep = '__')) %>%
            str_extract('\\(([0-9\\- :]+? UTC)\\)', group = 1)
    }

    if(dt == 'NA UTC'){
        stop('oi')
    }

    site_doi_license$link_download_datetime[i] <- dt
}

#missing download datetimes
filter(site_doi_license, link_download_datetime == '') %>%
    select(network, domain, macrosheds_prodcode)

# ms_write_confdata(site_doi_license, 'site_doi_license', 'remote', TRUE)
