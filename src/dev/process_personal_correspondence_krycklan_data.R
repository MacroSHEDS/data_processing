library(tidyverse)
# library(feather)

#TODO
#rebuild krycklan

setwd('macrosheds/data_acquisition/')

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

#identify date bounds for DLs that have them ####

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


#are there ever LOD values at the same time as measured values? (YES) ####

ggo = d %>%
    group_by(SiteID, Parameter, Date) %>%
    summarize(haslod = any(Value == '<LOD'),
              hasval = any(! Value %in% c('<LOD', 'NA'))) %>%
    filter(haslod & hasval)

ggoo = left_join(ggo, d, by = c('SiteID', 'Parameter', 'Date')) %>%
    select(ProjectID, SiteID, Parameter, Date, Value, LOD) %>%
    arrange(ProjectID, SiteID, Parameter, Date) %>%
    print(n=200)

# reformat the new data to match the old. join diffs of each set. write ####

dd = d %>%
    mutate(Date = as.Date(Date),
           LODbool = Value == '<LOD',
           Value = as.numeric(Value)) %>%
    group_by(ProjectID, SiteID, Parameter, Date) %>%
    summarize(Value = mean(Value, na.rm = TRUE), #if there's a measured value coupled with an LOD, ignore the LOD
              # LOD = any(! is.na(LOD)) && all(is.na(Value))) %>% #if there's a measured value coupled with an LOD, don't keep the LOD flag
              LOD = any(LODbool) && all(is.na(Value))) %>% #if there's a measured value coupled with an LOD, don't keep the LOD flag
    ungroup() %>%
    filter(! (is.na(Value) & ! LOD)) #these values can't be estimated, so drop 'em.

# dd[is.na(dd$Value) + dd$LOD == 1,]

dd = dd %>%
    mutate(Value = as.character(Value),
           Value = ifelse(LOD, '<LOD', Value)) %>%
    select(-LOD) %>%
    pivot_wider(names_from = Parameter, values_from = Value)

sc_new = filter(dd, ProjectID == 101) %>% select(-ProjectID) %>% arrange(Date)
pc_new = filter(dd, ProjectID == 201) %>% select(-ProjectID) %>% arrange(Date)

sc_old = read_csv('data/krycklan/krycklan/raw/stream_chemistry__VERSIONLESS003/sitename_NA/KCS 101 data 2021-06-08.csv')
old_cols_keep = setdiff(colnames(sc_old), unique(d$Parameter))
# setdiff(unique(d$Parameter), colnames(sc_old))

sc_new = sc_old %>%
    select(all_of(old_cols_keep)) %>%
    full_join(sc_new, by = c('SiteID', 'Date')) %>%
    arrange(Date)

pc_old = read_csv('data/krycklan/krycklan/raw/precip_chemistry__VERSIONLESS002/sitename_NA/KCS 201 data 2021-06-08.csv')
old_cols_keep = setdiff(colnames(pc_old), unique(d$Parameter))
# setdiff(unique(d$Parameter), colnames(sc_old))

pc_new = pc_old %>%
    select(all_of(old_cols_keep)) %>%
    full_join(pc_new, by = c('SiteID', 'Date')) %>%
    arrange(Date)

write_csv(sc_new, 'data/krycklan/krycklan/raw/stream_chemistry__VERSIONLESS003/sitename_NA/KCS 101 data 2022-11-19.csv')
write_csv(pc_new, 'data/krycklan/krycklan/raw/precip_chemistry__VERSIONLESS002/sitename_NA/KCS 201 data 2022-11-19.csv')

# scraps ####

xx = read_feather('macrosheds/data_acquisition/data/krycklan/krycklan/derived/stream_chemistry__ms001/Site1.feather')

dxr = read_csv('macrosheds/data_acquisition/data/krycklan/krycklan/raw/stream_chemistry__VERSIONLESS003/sitename_NA/KCS 101 data 2021-06-08.csv')



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
