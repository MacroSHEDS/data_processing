#for updating prefixes (or arbitrary strings) across all ws_traits directories
#in portal/data, data_acquisition/data, and in any ws trait summary files:
#(watershed_summaries.csv, watershed_raw_spatial_timeseries.fst,
#watershed_summaries_metadata.csv). Set the values in update_map
#below, and then source this script

#NOTE: you may also have to manually update the "category" column in
#watershed_summaries_metadata.csv and the data_class_code and data_source_code
#columns in universal_products

library(tidyverse)
library(glue)
library(feather)
library(fst)

setwd('~/git/macrosheds/portal')

#names in this list correspond to ws_traits directories. List values are vectors
#that indicate which prefixes should be changed (names; may contain regex "^"),
#and what they should be changed to (values).
update_map <- list('et_ref' = c('^ci_' = 'ck_'),
                   'tcw' = c('^vh_' = 'vj_'),
                   'nlcd' = c('^vg_' = 'lg_'))


#1. update raw data files ####
all_dirs <- list.dirs('data')
all_dirs <- c(all_dirs,
              list.dirs('../data_acquisition/data'))

for(i in seq_along(update_map)){

    trait <- names(update_map)[i]
    pattern <- names(update_map[[i]])
    replacement <- unname(update_map[[i]])

    trait_dirs <- grep(paste0('ws_traits/', trait),
                       all_dirs,
                       value = TRUE)

    if(length(trait_dirs)){

        for(j in seq_along(trait_dirs)){

            trait_files <- list.files(trait_dirs[j],
                                      full.names = TRUE)

            for(f in trait_files){

                read_feather(f) %>%
                    mutate(var = str_replace_all(var, pattern, replacement)) %>%
                    write_feather(f)

                print(glue('updated {f} ({p} -> {r})',
                           f = f,
                           p = pattern,
                           r = replacement))
            }
        }
    }
}

#2. update summary files
colname_pattern <- sapply(update_map, function(x) names(x))
colname_pattern <- paste0(colname_pattern, names(colname_pattern))
colname_replacement <- sapply(update_map, function(x) unname(x))
colname_replacement <- paste0(colname_replacement, names(colname_replacement))

#watershed_summaries.csv
x <- read.csv('data/general/spatial_downloadables/watershed_summaries.csv',
              stringsAsFactors = FALSE)

for(i in seq_along(update_map)){
    colnames(x) <- sub(colname_pattern[i], colname_replacement[i], colnames(x))
}

write.csv(x, 'data/general/spatial_downloadables/watershed_summaries.csv',
          row.names = FALSE)

#watershed_summaries_metadata.csv
x <- read.csv('data/general/spatial_downloadables/watershed_summaries_metadata.csv',
              stringsAsFactors = FALSE)

for(i in seq_along(update_map)){
    x$column <- sub(colname_pattern[i], colname_replacement[i], x$column)
}

write.csv(x, 'data/general/spatial_downloadables/watershed_summaries_metadata.csv',
          row.names = FALSE)

#watershed_raw_spatial_timeseries.fst
x <- read_fst('data/general/spatial_downloadables/watershed_raw_spatial_timeseries.fst')

for(i in seq_along(update_map)){
    x$var <- sub(colname_pattern[i], colname_replacement[i], x$var)
}

write_fst(x, 'data/general/spatial_downloadables/watershed_raw_spatial_timeseries.fst')
