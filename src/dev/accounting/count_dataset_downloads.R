library(httr)
# library(jsonlite)
library(tidyverse)

# options(timeout = 300)

# FIGSHARE ####

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
                   counts$id_figshare[i])) %>%
        content()
}

# EDI ####

## entity IDs are going to change every time EDI dataset is updated
## to get a proper audit on a per-item basis, can either visually cross reference
## downloads from the webpage with the figshare dataframe above, or could web scrape
## entity titles. or could just bag individual reads, which is what i've done previously.
## see below for that

library(xml2)
edi_report <- GET('https://pasta.lternet.edu/audit/reads/edi/1262') %>%
    content()
# edi_report <- GET('https://pasta.lternet.edu/audit/csv?resourceId=edi/1262/1')
# xx = as_list(read_xml(edi_report))
xx = as_list(edi_report)
# ls.str(xx)
# ls.str(xx[[1]])
# xx$resourceReads[[49]]
tot_edi_reads <- sapply(xx$resourceReads, function(z) z$totalReads[[1]],
       USE.NAMES = FALSE) %>%
    as.numeric() %>%
    sum()

# totals ####

cat(paste('Figshare indiv file downloads (why so many of "variable_metadata"?):', sum(counts$downloads_figshare)))
cat(paste('EDI KNOWN indiv file downloads (full dataset downlodas not counted still?):', tot_edi_reads))

