
vv = as_tibble(dataRetrieval::parameterCdFile)

v = filter(vv, grepl('hosph', parameter_nm))

unique(v$parameter_group_nm)

# filter(v, parameter_group_nm == 'Organics, Pesticide') %>% #N
# filter(v, parameter_group_nm == 'Organics, Other') %>% #N
# filter(v, parameter_group_nm == 'Information') %>% #N
# filter(v, parameter_group_nm == 'Stable Isotopes') %>% #N
    # pull(parameter_nm) %>%
    # sort()

v = filter(v, parameter_group_nm == 'Nutrient')

unique(v$srsname)
unique(v$parameter_units)

select(v, parameter_cd, parameter_nm, srsname, parameter_units) %>%
    arrange(parameter_units) %>%
    print(n=200)

v = filter(v, grepl('g/l', parameter_units, ignore.case = TRUE))

select(v, parameter_cd, parameter_nm, srsname, parameter_units) %>%
    arrange(parameter_nm) %>%
    print(n=200)

select(v, parameter_cd, parameter_nm, srsname, parameter_units) %>%
    arrange(parameter_nm) %>%
    pull(parameter_nm) %>%
    paste(., collapse = '\n') %>% cat()
