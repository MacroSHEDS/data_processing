#fabricate datasets for macrosheds portal (the ones that take ages to build)

#which domain are we fabricating data for?
network = 'lter'
domain = 'hbef'

#if there are already datasets in these folders, and you might want to keep them.
#   move them somewhere safe now:
#   precipitation, precip_chemistry, precip_flux_inst, stream_flux_inst

library(tidyr)
library(plyr)
library(tidyverse)
library(lubridate)
library(feather)
library(glue)
library(errors)
library(tinsel)

source('src/global/global_helpers.R')

derive_dirs = list.dirs(glue('data/{n}/{d}/derived',
                             n = network,
                             d = domain),
                        full.names = FALSE,
                        recursive = FALSE)
derive_dirs = derive_dirs[derive_dirs != 'documentation']

munge_dirs = list.dirs(glue('data/{n}/{d}/munged',
                            n = network,
                            d = domain),
                       full.names = FALSE,
                       recursive = FALSE)
munge_dirs = munge_dirs[munge_dirs != 'documentation']

prodnames_m = prodname_from_prodname_ms(munge_dirs)


prodnames_d = prodname_from_prodname_ms(derive_dirs)

setwd('~/git/macrosheds/data_acquisition/')
read_feather('data/lter/hbef/derived/stream_flux_inst__ms003/')
