
#next time we encounter wqp data, copy the approach from the loch_vale kernels.
#collect all sites' available params together and compare to
#src/webb/loch_vale/wqp_codes_previously_seen.csv. anything new (not in that csv)
#needs to be carefully evaluated and prioritized in the case of many-to-one,
#wqp-to-macrosheds mappings. there are many instances where wqp distinguishes
#between filtered and unfiltered. in some of these cases, more scrutiny may
#be required once new variants arise "in the wild". at the time of this writing
#(2024-02-19), we do not distinguish dissolved/particulate nutrients, except for
#those that can be expressed as acronyms, e.g. "TDN", "POC"

# write_csv(wqp_codes, 'src/webb/loch_vale/wqp_mappings.csv', quote = 'all')
wqp_codes <- read_csv('src/webb/loch_vale/wqp_mappings.csv') %>%
    # arrange(ms_varcode, priority)
    arrange(param_code)

retrieve_wqp_chem <- function(nwis_site_code, siteparams, set_details){

    #also, next time this AND the munger below should be modified
    #to accommodate multiple sites. then only one kernel is required
    #for all domain-sites from which wqp data can be pulled

    if(any(duplicated(select(wqp_codes, ms_varcode, priority)))){
        stop('ambiguous priority values in wqp_mappings.csv')
    }

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    siteparams <- siteparams %>%
        inner_join(wqp_codes, by = c(parm_cd = 'param_code')) %>%
        group_by(ms_varcode) %>%
        filter(priority == min(priority)) %>%
        ungroup() %>%
        distinct(parm_cd) %>%
        pull(parm_cd)

    siteWQ <- readWQPqw(siteNumbers = paste0('USGS-', nwis_site_code),
                        parameterCd = siteparams)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    write_csv(siteWQ, file = rawfile)
}

process_wqp_chem <- function(rawfile, ms_site_code){

    d <- read.delim(rawfile, sep = ',', colClasses = 'character') %>%
        as_tibble()

    pcodes_present <- unique(d$USGSPCode)

    if(! all(na.omit(d$ResultMeasure.MeasureUnitCode == d$DetectionQuantitationLimitMeasure.MeasureUnitCode))){
        stop('need to account for result/dl unit mismatch')
    }

    if(any(! is.na(d$MeasureQualifierCode))){
        stop('evaluate contents of MeasureQualifierCode')
    }

    comment_redflags <- c('exceed', 'improper', 'contaminat', 'error',
                          'subsample', 'violat', 'fail', 'invalid', 'questionable',
                          'suspect', 'adjust', 'interfere')

    d <- d %>%
        # select(DetectionQuantitationLimitMeasure.MeasureValue, #dl
        #        ResultDetectionConditionText, #detected/not-detected
        #        # MeasureQualifierCode, #comment/flag?
        #        ResultValueTypeName, #estimated/actual
        #        ResultCommentText, #free form
        #        ResultStatusIdentifier, #accepted/historical/preliminary
        #        ResultMeasureValue, #data
        #        ActivityStartDateTime, #datetime UTC
        #        USGSPCode) %>% #parameter code
        mutate(ms_status_ = case_when
               (
                   ResultStatusIdentifier == 'Preliminary' ~ 1,
                   ResultValueTypeName == 'Estimated' ~ 1,
                   grepl(paste(comment_redflags, collapse = '|'),
                         ResultCommentText) ~ 1,
                   TRUE ~ 0
               ),
               ResultMeasureValue = if_else
               (
                   ! is.na(ResultDetectionConditionText) & ResultDetectionConditionText == 'Not Detected',
                   paste0('<', DetectionQuantitationLimitMeasure.MeasureValue),
                   ResultMeasureValue
               ),
               site_code = ms_site_code
        ) %>%
        inner_join(wqp_codes, by = c(USGSPCode = 'param_code')) %>%
        select(ResultMeasureValue,
               ActivityStartDateTime,
               ms_varcode,
               ms_status_,
               site_code) %>%
        filter(! is.na(ActivityStartDateTime))

    if(any(is.na(d$ms_status_))) stop('oi')
    z <- select(d, ResultMeasureValue) %>% mutate(ResultMeasureValue = sub('<', '', ResultMeasureValue))
    nonnum <- get_nonnumerics(z)
    if(length(na.omit(nonnum))) stop('codes in data column')

    d_wide <- pivot_wider(d,
                          names_from = ms_varcode,
                          values_from = ResultMeasureValue)

    d <- ms_read_raw_csv(preprocessed_tibble = d_wide,
                         datetime_cols = c(ActivityStartDateTime = '%Y-%m-%dT%H:%M:%S'),
                         datetime_tz = 'GMT',
                         site_code_col = 'site_code',
                         data_cols = unique(d$ms_varcode),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         convert_to_BDL_flag = '<#*#',
                         keep_bdl_values = TRUE,
                         summary_flagcols = 'ms_status_')

    d <- ms_cast_and_reflag(d,
                            summary_flags_clean = list(ms_status_ = '0'),
                            summary_flags_dirty = list(ms_status_ = '1'),
                            variable_flags_bdl = 'BDL')

    #if e.g. NH4 and NH4_N are reported, just keep the latter.
    conv_cules <- c('NO3', 'SO4', 'PO4', 'SiO2', 'SiO3', 'NH4', 'NH3', 'NO3_NO2')
    conv_cules_main <- paste(conv_cules,
                             substr(conv_cules, 1, 1),
                             sep = '_')
    redundants <- which(conv_cules %in% drop_var_prefix(d$var) &
                            sapply(conv_cules_main, function(x) any(grepl(x, d$var)))) %>%
        names() %>%
        str_extract('^(.+)(?=_\\w+$)')

    d <- filter(d, ! drop_var_prefix(var) %in% redundants)

    #record detection limits
    bdl_vars <- d %>%
        mutate(var = drop_var_prefix(var)) %>%
        filter(ms_status == 2) %>%
        distinct(var) %>%
        pull()

    detlim_vars <- wqp_codes %>%
        filter(param_code %in% pcodes_present,
               ms_varcode %in% bdl_vars) %>%
        select(ms_varcode, unit_from = unit) %>%
        deframe()

    update_detlims(d, detlim_vars)

    #determine necessary conversions
    required_conversions <- wqp_codes %>%
        filter(param_code %in% pcodes_present) %>%
        left_join(ms_vars,
                  by = c('ms_varcode' = 'variable_code'),
                  suffix = c('_usgs', '_ms')) %>%
        select(ms_varcode, starts_with('unit')) %>%
        filter(unit_ms != unit_usgs)

    d <- ms_conversions(d,
                        convert_units_from = setNames(required_conversions$unit_usgs,
                                                      required_conversions$ms_varcode),
                        convert_units_to = setNames(required_conversions$unit_ms,
                                                    required_conversions$ms_varcode))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    write_ms_file(d = d,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = ms_site_code,
                  level = 'munged',
                  shapefile = FALSE)
}
