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

#munge pipeline 1 (least-NA row duplicate via rowwise) ####

ff <- expression({dd[1:s,] %>%
    filter_at(vars(-site_name, -datetime, -ms_status),
              any_vars(! is.na(.))) %>%
    rowwise(datetime, site_name) %>%
    mutate(NAsum = sum(is.na(c_across(-ms_status)))) %>%
    ungroup() %>%
    arrange(datetime, site_name, NAsum) %>%
    select(-NAsum) %>%
    distinct(datetime, site_name, .keep_all = TRUE) %>%
    arrange(site_name, datetime)})
gg <- expression({d[1:s,] %>%
    filter_at(vars(-site_name, -datetime, -ms_status),
              any_vars(! is.na(.))) %>%
    rowwise(datetime, site_name) %>%
    mutate(NAsum = sum(is.na(c_across(-ms_status)))) %>%
    ungroup() %>%
    arrange(datetime, site_name, NAsum) %>%
    select(-NAsum) %>%
    distinct(datetime, site_name, .keep_all = TRUE) %>%
    arrange(site_name, datetime)})
compare_efficiency(ff, gg, 10, 1e5,
                   outfile='plots/efficiency_comparisons/Xleast_NA_rowwise.png')

#synchronize_timestep ####
ff = expression(synchronize_timestep(ms_df = d[1:s,])
gg = expression(synchronize_timestep(ms_df = dd[1:s,])
compare_efficiency(ff, gg, 10, 1e6,
                   outfile='plots/efficiency_comparisons/synchronize_timestep.png')

#apply_detection_limit_t ####
ff = expression(apply_detection_limit_t(dd[1:s,], network, domain))
gg = expression(apply_detection_limit_t(d[1:s,], network, domain))
compare_efficiency(ff, gg, 10, 1e6,
                   outfile='plots/efficiency_comparisons/apply_detection_limit_t.png')

#status as binary integer ####

ff = expression({
    query_status(rep(c(101, 111, 000, 010), s))
})
gg = expression({
    as.logical(rep(c(1, 1, 0, 0), s))
})

compare_efficiency(ff, gg, 10, 1e6,
                   outfile='plots/efficiency_comparisons/bin_int_status.png')
