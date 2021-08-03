library(feather)
library(tidyverse)

#remove wonky varnames in year.feather
setwd('~/git/macrosheds/portal/')
zz = read_feather('data/general/biplot/year.feather')
unique(grep('__', zz$var, value=T))
zz = filter(zz, ! var %in% c('vb_', 'va__median', 'vb_n', 'pd_geo__mean'))
write_feather(zz, '/data/general/biplot/year.feather')

#don't recall why this was needed or if it might be again
setwd('data/general/catalog_files/indiv_variables/')
var_files = dir()
d_list = list()
for(i in seq_along(var_files)){

    f = var_files[i]
    var_name = str_match(f, '^(.+)?\\.csv$')[, 2]

    d_list[[i]] = read_csv(f) %>%
        mutate(Variable = !!var_name) %>%
        select(Variable, everything())
}

#check out all arctic sheds (commented ones need work)
setwd('~/git/macrosheds/data_acquisition/')
mv=mapview::mapview
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_-0.1/Kuparuk_River_-0.1.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_-0.177/Kuparuk_River_-0.177.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_-0.3/Kuparuk_River_-0.3.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_-0.47/Kuparuk_River_-0.47.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_-0.7/Kuparuk_River_-0.7.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_0/Kuparuk_River_0.shp'))
# mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_0.3/Kuparuk_River_0.3.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_0.56/Kuparuk_River_0.56.shp'))
# mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_0.74/Kuparuk_River_0.74.shp'))
# mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_1/Kuparuk_River_1.shp'))
# mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_1.39/Kuparuk_River_1.39.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_1.5/Kuparuk_River_1.5.shp'))
# mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_1.8/Kuparuk_River_1.8.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_2/Kuparuk_River_2.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_2.5/Kuparuk_River_2.5.shp'))
# mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_3/Kuparuk_River_3.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_4/Kuparuk_River_4.shp'))
mv(sf::st_read('data/lter/arctic/derived/ws_boundary__ms000/Kuparuk_River_4.1/Kuparuk_River_4.1.shp'))

