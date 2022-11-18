library(tidyverse)
# library(feather)

#TODO
#look for other errors
#format just like current raw data (include vars missing from this set)
#conform colnames, etc
#notify kim and hjalmar
#update the raw data we have on gdrive
#rebuild krycklan

d = readxl::read_xlsx('~/Downloads/ICP-data.xlsx') %>%
    select(-`Depth (meters, for groundwater)`) %>%
    filter(! is.na(Date),
           ProjectID %in% c(101, 201))

#are there '<LOD' flags that are missing DLs? (YES) ####
sum(is.na(filter(d, Value == '<LOD')$LOD))

#are any associated with vars that have known DLs at a later date? (YES) ####
zz = d %>%
    filter(Value == '<LOD') %>%
    group_by(Parameter) %>%
    summarize(n = length(unique(LOD)),
              unqs = paste(unique(LOD), collapse = ', ')) %>%
    print(n = 100)

#identify date bounds to DLs that have them ####

bounded_lod_params = zz %>%
    filter(n == 2) %>%
    pull(Parameter)

d %>%
    filter(Parameter %in% bounded_lod_params) %>%
    filter(! is.na(LOD)) %>%
    group_by(Parameter, LOD) %>%
    summarize(m = min(Date)) %>%
    ungroup() %>%
    print(n = 100)

#figure out detection limits ####

# n detection limits by parameter (across precip and stream chem)
d %>%
    filter(! is.na(LOD)) %>%
    group_by(Parameter) %>%
    summarize(n = length(unique(LOD))) %>%
    print(n=100) #cool, just one each

#detection limits by parameter
lods = d %>%
    filter(! is.na(LOD)) %>%
    group_by(Parameter) %>%
    summarize(LOD = unique(LOD)) %>%
    mutate(Parameter = c(str_match(Parameter, '[^ ]+'))) %>%
    print(n=100) %>%
    write_csv('/tmp/krycklan_lods.csv')

# reformat the new data to match the old. join diffs of each set ####

# meanlod = function(x){
#
#

#stream chem
sc_new = d %>%
    filter(ProjectID == 101) %>%
    mutate(Date = as.Date(Date),
           Value = as.numeric(Value)) %>%
    group_by(SiteID, Parameter, Date) %>%
    summarize(Value = mean(Value, na.rm = TRUE),
              LOD = any(! is.na(LOD))) %>%
    ungroup()
HERE: VERIFY THAT LOD == true CORRESPONDS TO VALUE == na
WHERE IT DOESN'T, THAT MEANS THERE WAS A REAL VALUE FOR A REPLICATE



    select(-ProjectID, -ProjectName, -SiteName, -LOD) %>%
    group_by(SiteID, Parameter, Date) %>%
    summarize(Value = mean(Value, na.rm = TRUE)) %>%
    ungroup() %>%
    pivot_wider(names_from = Parameter, values_from = Value)

sc_old = read_csv('data/krycklan/krycklan/raw/stream_chemistry__VERSIONLESS003/sitecode_NA/KCS 101 data 2021-06-08.csv')
old_cols_keep = setdiff(colnames(sc_old), unique(sc_new$Parameter))
# setdiff(unique(sc_new$Parameter), colnames(sc_old))
sc_old %>%
    select(all_of(old_cols_keep))

# scraps ####

xx = read_feather('macrosheds/data_acquisition/data/krycklan/krycklan/derived/stream_chemistry__ms001/Site1.feather')

dxr = read_csv('macrosheds/data_acquisition/data/krycklan/krycklan/raw/stream_chemistry__VERSIONLESS003/sitecode_NA/KCS 101 data 2021-06-08.csv')



gg = filter(d, ! is.na(LOD))
unique(gg$Parameter)

#min dates by param LOD (new)
gg2 = gg %>%
    group_by(Parameter, LOD) %>%
    summarize(m = min(Date)) %>%
    ungroup() %>%
    arrange(Parameter) %>%
    print(n=100)

#min dates by param (new)
d2= d %>%
    filter(! is.na(Date)) %>%
    group_by(Parameter) %>%
    summarize(m = min(Date))

full_join(gg2, d2, by = c('Parameter'))

#min dates by param LOD (old)
jj = dxr %>%
    select(Date, SiteID, where(is.character)) %>%
    pivot_longer(-c(Date, SiteID), names_to = 'Parameter', values_to = 'val') %>%
    filter(val == '<LOD') %>%
    group_by(Parameter) %>%
    summarize(m = min(Date))

full_join(gg2, jj, by = c('Parameter'))

#min dates by param (old)
oo = dxr %>%
    select(Date, SiteID, where(is.character)) %>%
    pivot_longer(-c(Date, SiteID), names_to = 'Parameter', values_to = 'val') %>%
    group_by(Parameter) %>%
    summarize(m = min(Date, na.rm=T))

pp = full_join(d2, oo, by = c('Parameter'))
sort(pp$Parameter)
print(pp, n=100)
