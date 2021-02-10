library(feather)
library(readr)
library(tidyverse)

df =read_feather('macrosheds/data_acquisition/data/lter/niwot/derived/stream_flux_inst__ms005/ALBION.feather')
pf =read_feather('macrosheds/portal/data/niwot/stream_flux_inst/ALBION.feather')
identical(df, pf)

ra=read_csv('macrosheds/data_acquisition/data/lter/niwot/raw/stream_chemistry__103/sitename_NA/albisolu.nc.data.csv')
dplyr::filter(ra, date > as.Date('2009-06-24'), date < as.Date('2009-06-28'))
fivenum(as.numeric(ra$Si), na.rm=T)

apr25Si = 75.8 / 1000000  * 28.086 * 1000

q=read_csv('macrosheds/data_acquisition/data/lter/niwot/raw/discharge__102/sitename_NA/albdisch.nc.data.csv')
dplyr::filter(q, date > as.Date('2009-06-24'), date < as.Date('2009-06-26'))

apr25q = 42725 * 1000 / 86400

#mg/L       L/s
flux = apr25Si * apr25q

#kg/d
flux = flux / 1000000 * 60 * 60 * 24

#kg/ha/d
flux / 715
#kg/ha/yr
flux / 715 * 365
