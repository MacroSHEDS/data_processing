check_for_updates_hydroshare <- function(oldlink, last_download_dt){

    page <- read_html(oldlink)
    nodes <- html_nodes(page, '.col-xs-12')

    newlink <- NA
    for(node in nodes){
        text <- html_text(node)
        if(str_detect(text, 'A newer version of this resource')){
            link_node <- html_node(node, 'a')
            newlink <- html_attr(link_node, 'href')
            break
        }
    }

    #if there's a link to a new version, look no further
    if(! is.na(newlink)){
        print(paste('New link:', newlink))
        return(newlink)
    }

    #otherwise check the last modified date and see if we already have it
    lastmod_ <- page %>%
        html_nodes(xpath = '//th[contains(text(), "Last updated:")]/following-sibling::td[1]') %>%
        html_text() %>%
        str_trim() %>%
        str_extract('\\w+ \\d{1,2}, \\d{4} at \\d{1,2}:?\\d* [ap].m.')

    lastmod <- suppressWarnings(try(mdy_hm(lastmod_), silent = TRUE))
    if(inherits(lastmod, 'try-error') || is.na(lastmod)){
        lastmod <- mdy_h(lastmod_)
    }

    if(! length(lastmod)){
        warning('"Last updated" date was not provided or could not be scraped for ', i)
        next
    }

    if(lastmod > as_datetime(last_download_dt)){
        print(paste('Update:', oldlink))
        return('old resource updated')
    } else {
        return('up to date')
    }
}

check_for_updates_edi <- function(oldlink, last_download_dt){

    page <- read_html(oldlink)
    node <- html_node(page, 'div h2 font[color="darkorange"] a')

    newlink <- if(!is.null(node)) html_attr(node, 'href') else NA

    #if there's a link to a new version, look no further
    if(! is.na(newlink)){
        print(paste('New link:', newlink))
        return(newlink)
    }

    #otherwise check the last modified date and see if we already have it
    lastmod <- page %>%
        html_node('div.table-cell > ul > li > em') %>%
        html_text() %>%
        str_trim() %>%
        str_extract('\\d{4}-\\d{2}-\\d{2}') %>%
        ymd()

    if(! length(lastmod) | is.na(lastmod)){
        warning('"Updated" date was not provided or could not be scraped for ', i)
        next
    }

    if(lastmod > as_date(last_download_dt)){
        print(paste('Update:', oldlink))
        return('old resource updated')
    } else if(lastmod == as_date(last_download_dt)){
        print(paste('Maybe update:', oldlink))
        return(paste('old resource updated? updated and last retrieved on', lastmod))
    } else {
        return('up to date')
    }
}
