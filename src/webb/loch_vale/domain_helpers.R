#yeah, this is how we're doin' it
wqp_codes <- tibble(
   param_code = c('00010',     '00095',  '00300', '00408',    '00413', '00409', '90410', '00600', '00618', '00631',     '00665', '00681', '00694', '00915', '00925', '00930', '00935', '00940', '00945', '00950', '00955', '01080', '39087', '49570', '50624',     '62854', '00602', '63041',    '70301', '71846', '75978',     '82082',  '82085',  '82086',  '82690',    '01046', '01056', '70507', '00191', '00405', '00613', '01106', '29803', '32209', '71851', '71856', '71870', '00080', '00608'),
   ms_varcode = c('temp',      'spCond', 'DO',    'pH',       'ANC',   'ANC',   'ANC',   'TN',    'NO3_N', 'NO3_NO2_N', 'TP',    'DOC',   'TPC',   'Ca',    'Mg',    'Na',    'K',     'Cl',    'SO4',   'F',     'Si',    'Sr',    'alk',   'TPN',   'abs254_cm', 'TDN',   'TDN',   'd18O_NO3', 'TDS',   'NH4',   '87Sr_86Sr', 'dD',     'd18O',   'd34S',   'd15N_NO3', 'Fe',    'Mn',    'SRP',   'H',     'CO2',   'NO2_N', 'Al',    'alk',   'Chla',  'NO3',   'NO2',   'Br',    'color', 'NH4_N'),
   unit =       c('degrees C', 'uS/cm',  'mg/L',  'unitless', 'mg/L',  'ueq/L', 'mg/L',  'mg/L',  'mg/L',  'mg/L',      'mg/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'ug/L',  'mg/L',  'mg/L',  'AU/cm',     'mg/L',  'mg/L',  'permil',   'mg/L',  'mg/L',  'unitless',  'permil', 'permil', 'permil', 'permil',   'ug/L',  'ug/L',  'mg/L',  'mg/L',  'mg/L',  'mg/L',  'ug/L',  'mg/L',  'ug/L',  'mg/L',  'mg/L',  'mg/L',  'PCU',   'mg/L'),
   priority =   c(1 ,          1 ,       1 ,      1 ,         2 ,      1,       3,       1 ,      1 ,      1 ,          1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1,       1 ,      1 ,          1,       2,       1 ,         1 ,      1 ,      1 ,          1 ,       1 ,       1 ,       1 ,         1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      1 ,      2,       1 ,       1 ,       1 ,      1 ,    1,       1)
)

retrieve_wqp_chem <- function(nwis_site_code, siteparams, set_details){

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
               site_code)

    if(any(is.na(d$ms_status_))) stop('oi')
    z = select(d, ResultMeasureValue) %>% mutate(ResultMeasureValue = sub('<', '', ResultMeasureValue))
    if(length(get_nonnumerics(z))) stop('codes in data column')

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

    #handle necessary conversions
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
