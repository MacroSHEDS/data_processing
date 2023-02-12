scrape_data_download_urls <- function(){

    # connecting to gsheet
    citation_gsheet <- googlesheets4::read_sheet(
        conf$site_doi_license_gsheet,
        skip = 5,
        na = c('', 'NA'),
        col_types = 'c')

    # raw_fp <- './vault/raw_documentation_files/'
    raw_fp <- 'data'

    networks <- list.files(raw_fp)
    networks <- grep('general|spatial', networks,
                     invert = TRUE,
                     value = TRUE)

    for(network in networks){

        domains <- list.files(file.path(raw_fp, network))
        for(domain in domains){

            writeLines(paste('reading documentation for data source:', domain))

            # list all files in this domain of this network
            product_names <- list.files(file.path(raw_fp, network, domain, "raw", "documentation"))
            product_docs <- list.files(file.path(raw_fp, network, domain, "raw", "documentation"), full.names = TRUE)

            # filter gsheet to domain, get all prodcodes
            dmn_prodcodes <- citation_gsheet %>%
                filter(network == !!network,
                       domain == !!domain) %>%
                pull(macrosheds_prodcode) %>%
                paste(collapse = '|')

            # filter product docs to only those with prodcode text matching gsheet
            cited_products <- product_docs[grepl(dmn_prodcodes, product_docs)]

            for(file in cited_products){

                cited_prodcode <- stringr::str_match(basename(file), "__(.+)?.txt$")[, 2]

                data_source_doc <- readLines(file)
                data_source_link <- trimws(stringr::str_split(data_source_doc, "[^)][0-9]{4}\\-", simplify =TRUE)[1])
                data_source_dt <- trimws(stringr::str_extract_all(data_source_doc, "[0-9]{4}\\-.*[^)]", simplify =TRUE))[1]

                print(paste(network, domain, data_source_link, data_source_dt))

                if(grepl("https?://", data_source_link)) {
                    data_source_link <- stringr::str_split(data_source_link, " ", simplify = TRUE)[1]
                }

                # NOTE: documentation files are not mecha-standardized, this function scrapes the best standard
                # this *should* capture all prodcodes in citation gsheet, giving NA for versionlesss

                # now, for this prodcode in the citation_gsheet df, we put "docs" in the "link" column
                citation_gsheet <- citation_gsheet %>%
                    mutate(link = ifelse(grepl(cited_prodcode, macrosheds_prodcode) &
                                             domain == !!domain,
                                         data_source_link, link),
                           link_download_datetime = ifelse(grepl(cited_prodcode, macrosheds_prodcode) &
                                                               domain == !!domain,
                                                           data_source_dt, link_download_datetime)
                    )
            }
        }
    }

    # then, we write the edited df to the actual google sheet
    #TODO reattach pre-header first
    # googlesheets4::sheet_write(citation_gsheet, ss = conf$citation_gsheet, sheet = "timeseries")
}
