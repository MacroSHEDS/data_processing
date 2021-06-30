#for finding and replacing header names throughout all macrosheds feather
#and shape files, not including those in data_acquisition/data/*/raw/ directories

library(tidyverse)
library(plyr)
# library(glue)
library(feather)
library(fst)
library(sf)

pattern = 'site_code'
replacement = 'site_code'
setwd('~/git/macrosheds')

#1. update feather files (data_acquisition) ####

all_feathers = list.files('data_acquisition',
                          pattern = '*.feather$',
                          recursive = TRUE,
                          full.names = TRUE)

all_feathers = grep('/data/.*?/raw/',
                    all_feathers,
                    value = TRUE,
                    invert = TRUE)

unchanged_feathers = c()
for(i in seq_along(all_feathers)){

    f = all_feathers[i]
    d = read_feather(f)

    if(! pattern %in% colnames(d)){
        unchanged_feathers = append(unchanged_feathers, i)
        print(paste('unchanged (data_acquisition):', f))
        next
    }
    # mutate(var = str_replace_all(var, pattern, replacement)) %>%

    colnames(d) = sub(pattern, replacement, colnames(d))
    write_feather(d, f)
}

#2. update shape files (data_acquisition) ####

all_shp = list.files('data_acquisition',
                     pattern = '*.shp$',
                     recursive = TRUE,
                     full.names = TRUE)

all_shp = grep('/data/.*?/raw/',
               all_shp,
               value = TRUE,
               invert = TRUE)

unchanged_shapes = c()
for(i in seq_along(all_shp)){

    f = all_shp[i]
    d = sf::st_read(f, quiet = TRUE)

    if(! pattern %in% colnames(d)){
        unchanged_shapes = append(unchanged_shapes, i)
        print(paste('unchanged (data_acquisition):', f))
        next
    }

    colnames(d) = sub(pattern, replacement, colnames(d))
    sf::st_write(d, f)
}

#3. update feather files (portal) ####

all_feathers = list.files('portal',
                          pattern = '*.feather$',
                          recursive = TRUE,
                          full.names = TRUE)

unchanged_feathers2 = c()
for(i in seq_along(all_feathers)){

    f = all_feathers[i]
    d = read_feather(f)

    if(! pattern %in% colnames(d)){
        unchanged_feathers2 = append(unchanged_feathers2, i)
        print(paste('unchanged (portal):', f))
        next
    }

    colnames(d) = sub(pattern, replacement, colnames(d))
    write_feather(d, f)
}

#4. update shape files (portal) ####

all_shp = list.files('portal',
                     pattern = '*.shp$',
                     recursive = TRUE,
                     full.names = TRUE)

unchanged_shapes2 = c()
for(i in seq_along(all_shp)){

    f = all_shp[i]
    d = sf::st_read(f)

    if(! pattern %in% colnames(d)){
        unchanged_shapes2 = append(unchanged_shapes2, i)
        print(paste('unchanged (portal):', f))
        next
    }

    colnames(d) = sub(pattern, replacement, colnames(d))
    sf::st_write(d, f)
}

#5. update any other files piecemeal ####

p = 'portal/data/general/spatial_downloadables/watershed_raw_spatial_timeseries.fst'
read_fst(p) %>%
    rename(!!replacement := !!pattern) %>%
    write_fst(p)
