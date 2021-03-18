
aa = read_feather('data/lter/hjandrews/derived/precip_flux_inst__ms902/GSLOOK.feather')
qq = read_csv('data/lter/hjandrews/raw/precip_chemistry__4022/sitename_NA/CP00201.csv')
qq %>%
    select(SITECODE, DATE_TIME, UTP) %>%
    filter(DATE_TIME > as.POSIXct('2016-10-19 00:00:00', tz='UTC') &
               DATE_TIME < as.POSIXct('2017-04-05 00:00:00', tz='UTC'))
print(tail(aa, 15), n=15)
