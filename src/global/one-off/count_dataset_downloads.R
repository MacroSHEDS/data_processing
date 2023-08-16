library(httr)
# library(jsonlite)
# library(tidyverse)

# options(timeout = 300)

# setwd('~/git/macrosheds/data_acquisition')

# conf <- jsonlite::fromJSON('config.json',
#                            simplifyDataFrame = FALSE)

# collection_id <- 5621740
token <- Sys.getenv('RFIGSHARE_PAT')
auth_header <- c(Authorization = sprintf('token %s', token))
# tld <- paste0('macrosheds_figshare_v', dataset_version)

articles <- GET('https://api.figshare.com/v2/account/articles?page_size=1000',
         add_headers(auth_header)) %>%
    content()

macrosheds_inds <- sapply(articles, function(x) x$title) %>%
    str_which('^Network|spatial_|watershed_|timeseries|data_|variable_|README|site_|macrosheds_|Daymet')

articles <- articles[macrosheds_inds]
counts <- tibble(
    id_figshare = sapply(articles, function(x) x$id),
    id_edi = NA_character_,
    title_figshare = sapply(articles, function(x) x$title),
    title_edi = NA_character_,
    downloads_figshare = NA_integer_,
    downloads_edi = NA_integer_
) %>%
    arrange(title_figshare)

for(i in 1:nrow(counts)){
    counts[i, 'downloads_figshare'] <-
        GET(paste0('https://stats.figshare.com/total/downloads/article/',
                   counts$id[i])) %>%
        content()
}


