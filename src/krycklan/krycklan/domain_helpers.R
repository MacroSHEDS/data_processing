get_actual_dl_url <- function(url, web_browser = 'firefox', port = 4567L, ...){

    if(! require('RSelenium')){
        stop('RSelenium is required to use this function')
    }

    #try to connect to an existing server or start a new one
    if(is_selenium_running(port)){

        remote_driver <- remoteDriver(remoteServerAddr = "localhost",
                                      port = port,
                                      browser = web_browser)
        remote_driver$open()

    } else {

        driver <- rsDriver(browser = web_browser, port = port, ...)
        remote_driver <- driver$client
    }

    on.exit({
        remote_driver$close()
        if(exists('driver')){
            driver$server$stop()  #ensure server is stopped when function exits
        }
    }, add = TRUE)

    #visit page and wait for it to load
    remote_driver$navigate(url)
    Sys.sleep(7)

    #agree to the terms to activate the download button
    remote_driver$executeScript("document.querySelector('#agreementChkBx').click();")
    Sys.sleep(0.1)

    #get the link from the download button
    dl_btn <- remote_driver$findElement(using = 'css selector',
                                        value = '#downloadBtn')

    href <- dl_btn$getElementAttribute('href')[[1]]

    return(href)
}

retrieve_krycklan <- function(set_details, network, domain){

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    prodname <- prodname_from_prodname_ms(set_details$prodname_ms)

    unlink(file.path(raw_data_dest, '*'), recursive = TRUE)

    d <- read_csv(glue('src/krycklan/krycklan/file_object_collections/{p}.csv',
                       p = prodname))

    prov <- tibble()
    for(i in seq_len(nrow(d))){

        landing_page <- read_html(d$dobj[i])

        ## collect provenance

        bibtex <- landing_page %>%
            html_element(xpath = "//pre[@class='user-select-all w-100 m-0']") %>%
            html_text() %>%
            str_split(pattern = '\n') %>%
            {.[[1]]} %>%
            str_trim()

        title <- gsub("title=\\{(.*)\\},", "\\1", bibtex[grep("title=", bibtex)])
        yr <- gsub("year=\\{(.*)\\},", "\\1", bibtex[grep("year=", bibtex)])
        url <- gsub("url=\\{(.*)\\},", "\\1", bibtex[grep("url=", bibtex)])
        publisher <- gsub("publisher=\\{(.*)\\},", "\\1", bibtex[grep("publisher=", bibtex)])

        apa_citation <- sprintf("%s. (%s). %s. %s. %s",
                                publisher, yr, title, publisher, url)

        prov <- bind_rows(
            prov,
            tibble(
                network = network,
                domain = domain,
                macrosheds_prodcode = prodcode_from_prodname_ms(set_details$prodname_ms),
                doi = url,
                data_status = '',
                license = 'https://creativecommons.org/licenses/by/4.0/',
                license_type = 'Attribution',
                license_sharealike = '',
                IR_acknowledgement_text = 'This dataset has been made possible by data provided by the Swedish Infrastructure for Ecosystem Science (SITES).',
                IR_acknowledge_domain = '',
                IR_acknowledge_funding_sources = '',
                IR_acknowledge_grant_numbers = '',
                IR_notify_of_intentions = '',
                IR_notify_on_distribution = 's',
                IR_provide_online_access = '',
                IR_provide_two_reprints = '',
                IR_collaboration_consultation = 's',
                IR_questions = '',
                IR_needs_clarification = '',
                contact = 'kim.lindgren@slu.se',
                contact_name1 = 'Kim Lindgren',
                creator_name1 = 'Svartberget Research Station',
                funding = 'Supported by Swedish Infrastructure for Ecosystem Science, the ForWater Project, Environmental Climate Data Sweden, and INTERACT',
                citation = apa_citation,
                link = url,
                link_download_datetime = Sys.time() %>%
                    with_tz('UTC') %>%
                    format('%Y-%m-%d %H:%M:%S UTC')
            )
        )

        ## download the dataset

        btn_selector <- "//a[contains(@class, 'btn') and contains(@class, 'btn-primary') and text()='Download']"
        download_link <- html_elements(landing_page, xpath = btn_selector) %>%
            html_attr('href')

        download_link <- get_actual_dl_url(download_link)
        download.file(download_link,
                      destfile = file.path(raw_data_dest, d$fileName[i]))
    }

    provdir <- 'src/krycklan/krycklan/provenance_collections'
    dir.create(provdir,
               showWarnings = FALSE,
               recursive = TRUE)

    write_csv(prov, file.path(provdir, paste0(prodname, '.csv')))

    logwarn('new provenance details written to src/krycklan/krycklan/provenance_collections. updating gsheet should be automated in 2025',
            logger = logger_module)

    deets_out <- list(url = 'https://data.fieldsites.se/portal/',
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = as.POSIXct('2999-12-31 00:00:01', tz = 'UTC'))

    return(deets_out)
}
