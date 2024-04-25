library(macrosheds)

ms_root <- 'macrosheds/r_package/data/macrosheds/'

ms_download_core_data(
    ms_root,
    domains = 'all'
)

q <- ms_load_product(
    ms_root,
    prodname = "discharge"
) %>%
    left_join(select(ms_site_data, site_code, domain))

chem <- ms_load_product(
    ms_root,
    prodname = "stream_chemistry"
) %>%
    left_join(select(ms_site_data, site_code, domain))

chem_filt <- chem %>%
    filter(domain == 'niwot',
           ms_drop_var_prefix(var) %in% c('Ca', 'NO3_N')) %>%
    semi_join(q, by = 'site_code')
q_filt <- q %>%
    filter(domain == 'niwot') %>%
    semi_join(chem_filt, by = 'site_code')

flux <- ms_calc_flux(
    chemistry = chem_filt,
    q = q_filt
)
