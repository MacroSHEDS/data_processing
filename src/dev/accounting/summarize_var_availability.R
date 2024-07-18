
# summarizes availability of stream chemistry variables at stream gauge sites
# (not at mere stream sampling points)

# setup ####

# FIRST: source acquisition_master.R down to main loop

ff = system("find data -type f -path '*/derived/stream_chemistry*/*.feather'", intern = TRUE)

qsites = site_data %>%
    filter(site_type == 'stream_gauge',
           in_workflow == 1) %>%
    pull(site_code)


# config ####

vv = c('SiO2', 'SiO2_Si', 'Si')
vv = c('DOC')
vv = c('NO3', 'NO3_N', 'NO3_NO2', 'NO3_NO2_N')
vv = c('NH4', 'NH4_N', 'NH3_NH4', 'NH3_NH4_N')
vv = c('TN', 'TDN', 'TPN', 'TON', 'TIN', 'TKN', 'TDKN')
vv = c('pH')

# run ####

smry1 = tibble()
for(f in ff){

    site = str_extract(basename(f), '.*(?=\\.feather$)')
    if(! site %in% qsites) next

    dmn = str_extract(f, 'data/[^/]+/([^/]+)', group = 1)

    smry1 = read_feather(f) %>%
        mutate(var = drop_var_prefix(var)) %>%
        filter(var %in% !!vv) %>%
        group_by(var) %>%
        summarize(domain = !!dmn,
                  site_code = first(site_code),
                  n_records = n(),
                  first = min(datetime),
                  last = max(datetime),
                  .groups = 'drop') %>%
        bind_rows(smry1)
}

smry1 = arrange(smry1, var, domain, site_code, first)

smry2 = smry1 %>%
    group_by(var) %>%
    summarize(var = first(var),
              n_domains = length(unique(domain)),
              n_sites = length(unique(site_code)),
              n_records = sum(n_records),
              .groups = 'drop')

print(smry1, n = 1000)
smry2

# print ####

print(Si_A, n = 1000)
Si_B

print(DOC_A, n = 1000)
DOC_B

print(NO3_A, n = 1000)
NO3_B

print(NH4_A, n = 1000)
NH4_B

print(TN_A, n = 1000)
TN_B

print(pH_A, n = 1000)
pH_B

# X, Y, etc. all present ####

vv = c('NH4_N', 'NO3_N')

smry3 = tibble()
for(f in ff){

    site = str_extract(basename(f), '.*(?=\\.feather$)')
    if(! site %in% qsites) next

    dmn = str_extract(f, 'data/[^/]+/([^/]+)', group = 1)

    d = read_feather(f, columns = 'var') %>%
        mutate(var = drop_var_prefix(var)) %>%
        pull()

    if(all(vv %in% d)){
        smry3 = bind_rows(smry3, tibble(domain = dmn, site_code = site))
    }
}

summarize(smry3,
          n_domains = length(unique(domain)),
          n_sites = length(unique(site_code)))
