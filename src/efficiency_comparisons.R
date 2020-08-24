#munge pipeline 1 (averaging duplicates) ####
ff = expression({dd[1:s,] %>%
    filter_at(vars(-site_name, -datetime, -ms_status),
              any_vars(! is.na(.))) %>%
    group_by(datetime, site_name) %>%
    summarize(
        discharge = case_when(
            length(discharge) > 1 ~ mean(discharge, na.rm=TRUE),
            TRUE ~ discharge),
        ms_status = numeric_any(ms_status)) %>%
    ungroup()
})
gg = expression({d[1:s, ] %>%
    filter_at(vars(-site_name, -datetime, -ms_status),
              any_vars(! is.na(.))) %>%
    group_by(datetime, site_name) %>%
    summarize(
        discharge = case_when(
            length(discharge) > 1 ~ mean(discharge, na.rm=TRUE),
            TRUE ~ discharge),
        ms_status = numeric_any(ms_status)) %>%
    ungroup()
})
compare_efficiency(ff, gg, 10, 1e5,
                   outfile='plots/efficiency_comparisons/munge_pipeline_1.png')

#munge pipeline 1 (first non-NA duplicate) ####
ff = expression({dd[1:s,] %>%
    filter_at(vars(-site_name, -datetime, -ms_status),
              any_vars(! is.na(.))) %>%
    group_by(datetime, site_name) %>%
    mutate(discharge = first(na.omit(discharge))) %>%
    summarize(
        discharge = first(na.omit(discharge)),
        ms_status = numeric_any(ms_status)) %>%
    ungroup()})
gg = expression({d[1:s,] %>%
    filter_at(vars(-site_name, -datetime, -ms_status),
              any_vars(! is.na(.))) %>%
    group_by(datetime, site_name) %>%
    mutate(discharge = first(na.omit(discharge))) %>%
    summarize(
        discharge = first(na.omit(discharge)),
        ms_status = numeric_any(ms_status)) %>%
    ungroup()})
compare_efficiency(ff, gg, 10, 1e5,
                   outfile='plots/efficiency_comparisons/first_nonNA_duplicate.png')

#synchronize_timestep ####
ff = expression(synchronize_timestep(ms_df = d[1:s,],
                                     desired_interval = '1 day',
                                     impute_limit = 30))
gg = expression(synchronize_timestep(ms_df = dd[1:s,],
                                     desired_interval = '1 day',
                                     impute_limit = 30))
compare_efficiency(ff, gg, 10, 1e6,
                   outfile='plots/efficiency_comparisons/synchronize_timestep.png')

#apply_detection_limit_t ####
ff = expression(apply_detection_limit_t(dd[1:s,], network, domain))
gg = expression(apply_detection_limit_t(d[1:s,], network, domain))
compare_efficiency(ff, gg, 10, 1e6,
                   outfile='plots/efficiency_comparisons/apply_detection_limit_t.png')
