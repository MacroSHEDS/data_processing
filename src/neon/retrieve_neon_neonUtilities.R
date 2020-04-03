library(httr)
# library(jsonlite)
# library(tidyr)
# library(data.table)
# library(dtplyr)
library(tidyverse)
library(feather)
library(neonUtilities)
# library(geoNEON)

setwd('/home/mike/git/macrosheds/data_acquisition/data/neon')

#DP1.20093.001 #Chemical properties of surface water
#DP1.20267.001 #gage height
#DP4.00133.001 #Z-Q rating curve (only HOPB and GUIL)
#DP4.00130.001 #continuous Q (only HOPB)

grab_data_products = c('DP1.20093.001', 'DP1.20267.001')
sensor_data_products = c('DP1.20288.001', 'DP4.00130.001')

# neonUtilities::getPackage()

# req = GET(paste0("http://data.neonscience.org/api/v0/products/",
#     sensor_data_products[1]))
# txt = content(req, as="text")
# neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)

veg_str <- neonUtilities::loadByProduct(dpID="DP1.10098.001", site="WREF",
    package="expanded", check.size=T)
chili <- neonUtilities::loadByProduct(dpID='DP1.20288.001', site="ARIK",
    startdate='2016-12',
    enddate='2017-05',
    package="expanded", check.size=T)

sp = sensor_data_products[1]
for(sp in sensor_data_products){
    prodinf = neonUtilities::getProductInfo(sp)
    neonUtilities::getAvg(sp)
    prodid = strsplit(sp, '\\.')[[1]][2]
    # zz = neonUtilities::loadByProduct(sp, site='ARIK', startdate='2017-01',
    #     enddate='2017-05', package='expanded', check.size=FALSE)
    neonUtilities::zipsByProduct(sp, site='ARIK', startdate='2017-01',
        enddate='2017-05', package='expanded', check.size=FALSE, savepath='raw')
    # dirlist = dir('raw')
    # str_match(dirlist, '^filesToStack(.*$)')[,2]
    neonUtilities::stackByTable(paste0('raw/filesToStack', prodid), nCores=3,
        savepath='semiprocessed') #error here. cannot stack.
    # zz = neonUtilities::stackDataFilesParallel('semiprocessed/', 3, sp)
    stackfile = dir('semiprocessed/stackedFiles/', full.names=TRUE)
    d = read_csv(stackfile, guess_max=10000)
    d
}


#leftovers from streampulse data processing for neon. was going to adapt these
#to the neonUtilities workflow, but it's too wonky

# #download list of available datasets for the current data product
# write(paste('Checking for new', prods_abb[p], 'data.'),
#     '../../logs_etc/NEON/NEON_ingest.log', append=TRUE)
# req = GET(paste0("http://data.neonscience.org/api/v0/products/", prod_codes[p]))
# txt = content(req, as="text")
# neondata = fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
#
# #get available urls, sites, and dates
# urls = unlist(neondata$data$siteCodes$availableDataUrls)
# avail_sets = str_match(urls, '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})')
#
# #determine which are new and worth grabbing (represented in DO dataset)
# sets_to_grab = vector()
# for(ii in 1:nrow(avail_sets)){
#     avail_sitemo = paste(avail_sets[ii,2], avail_sets[ii,3])
#     if(! avail_sitemo %in% retrieved_sets){
#         sets_to_grab = append(sets_to_grab, ii)
#     }
# }
# sets_to_grab = as.data.frame(avail_sets[sets_to_grab,],
#     stringsAsFactors=FALSE)
#
# if(prods_abb[p] == 'DO'){
#     relevant_sitemonths = c(relevant_sitemonths,
#         do.call(paste, sets_to_grab[,2:3]))
# } else {
#     in_DO = do.call(paste, sets_to_grab[,2:3]) %in% relevant_sitemonths
#     sets_to_grab = sets_to_grab[in_DO,]
# }
