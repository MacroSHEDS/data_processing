
#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_9001 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9030 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9002 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9003 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9007 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9009 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9010 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9011 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9013 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_9014 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9015 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9018 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9022 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9021 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9027 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9016 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9029 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9024 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9023 <- retrieve_mcmurdo

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_0_9017 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_24 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_20 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_21 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_78 <- retrieve_mcmurdo

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_9030 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9002 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9003 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9007 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9009 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9010 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9011 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9013 <- munge_mcmurdo_discharge

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_9014 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9015 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9018 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9022 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9021 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9027 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9016 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9029 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9024 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9023 <- munge_mcmurdo_discharge

#stream_chemistry; discharge: STATUS=READY
#. handle_errors
process_1_9017 <- munge_mcmurdo_discharge

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_24 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = c('date_time' = '%m/%e/%y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_code_col = 'strmgageid',
                         alt_site_code = list(
                             anderson_h1 = 'andrsn_h1',
                             lawson_b3 = 'lawson',
                             huey_f2 = 'huey',
                             crescent_f8 = 'crescent', #not certain
                             garwood_flats = 'garwood', #not certain
                             vguerard_f6 = 'vguerard'
                             # onyx_lwright = 'onyx', #not certain
                             # worm_altran = 'worm', #not certain
                         ),
                         data_cols =  c('doc_mgl' = 'DOC'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'doc_comments',
                         set_to_NA = '',
                         is_sensor = FALSE)

    # Warning for sites are all sites that do not have a defined lat/long and move
    # over time so no including it in site_data

    smry_flg_grep <- grep(
        'detection|Detection|DL|no detect|ND|<0.1 mg/L C|Not detected|Not detected|<0.1 mg/L',
        unique(d$doc_comments),
        value = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_bdl = list(doc_comments = smry_flg_grep))

    d <- filter_single_samp_sites(d)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_20 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        filter(strmgageid != '',
               strmgageid != 'garwood',
               strmgageid != 'miers',
               strmgageid != 'delta_upper',
               strmgageid != 'lizotte_mouth',
               strmgageid != 'vguerard_lower') %>%
        rename(li = li_mgl,
               na = na_mgl,
               k = k_mgl,
               mg = mg_mgl,
               ca = ca_mgl,
               'f' = f_mgl,
               cl = cl_mgl,
               br = br_mgl,
               so4 = so4_mgl,
               si = si_mgl)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('date_time' = '%m/%e/%y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_code_col = 'strmgageid',
                         alt_site_code = list(
                             anderson_h1 = 'andrsn_h1',
                             lawson_b3 = 'lawson',
                             huey_f2 = 'huey',
                             crescent_f8 = 'crescent', #not certain
                             garwood_flats = 'garwood', #not certain
                             vguerard_f6 = 'vguerard'
                             # onyx_lwright = 'onyx', #not certain
                             # worm_altran = 'worm', #not certain
                         ),
                         data_cols =  c('li' = 'Li',
                                        'na' = 'Na',
                                        'k' = 'K',
                                        'mg' = 'Mg',
                                        'ca' = 'Ca',
                                        'f' = 'F',
                                        'cl' = 'Cl',
                                        'br' = 'Br',
                                        'so4' = 'SO4',
                                        'si' = 'Si'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#_comments',
                         summary_flagcols = 'sample_comments',
                         is_sensor = FALSE)

    var_dirty_string <- c('Not Enough water for the sample to be run fully',
                          'Not Quantified',
                          'value outside calibration limit and not diluted to be within calibration limit',
                          'Br not quantified', 'not quantified', 'Not Quantified', 'Ca not quantified',
                          'K not quantified', 'Li not quantified')

    BDL_flags <- unique(c(' Li not quantified <0.001mg/L', 'Li level below detection limit',
                          'not detected', 'ND', 'ND <0.001', 'no detect',
                          ' F not quantified <0.02mg/L', 'F level below detection limit',
                          '< 0.1 mg/L', 'Br not quantified <0.04mg/L', 'ND < 0.04 mg/L',
                          'Br level below detection limit', 'ND < 0.04', 'ND < 0.04 mg/L',
                          'Br < 0.1', 'Br not quantified <0.04mg/L', 'ND < 0.1 mg/L',
                          'ND < 0.02 mg/L', '< 0.1 mg/L', 'F level below detection limit',
                          ' F not quantified <0.02mg/L', 'ND < 0.0005 mg/L', 'Li level below detection limit',
                          'Li ND < 0.001 mg/L', 'ND < 0.0007 mg/L', 'Li < 0.001',
                          'Li not quantified <0.001mg/L', 'ND < 0.0007 mg/L',
                          'Li < 0.001', 'Li not quantified <0.001mg/L'))

    summary_dirty_string <- c('suspect Cl contamination because of high Cl concentrations and high Cl to Na ratios and excess anion equivalents',
                              'questionable location ID- no name on sample- time does not match COC form')

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = var_dirty_string,
                            variable_flags_bdl = BDL_flags,
                            variable_flags_to_drop = 'REMOVE',
                            summary_flags_dirty = list('sample_comments' = summary_dirty_string),
                            summary_flags_to_drop = list('sample_comments' = 'DROP'))

    d <- filter_single_samp_sites(d)

    d <- ms_conversions(d,
                        convert_units_from = c('SO4' = 'mg/l'),
                        convert_units_to = c('SO4' = 'mg/l'))

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_21 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)


    d <- read.csv(rawfile, colClasses = 'character') %>%
        rename(n_no3 = n_no3_ugl,
               n_no2 = n_no2_ugl,
               n_nh4 = n_nh4_ugl,
               srp = srp_ugl)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('date_time' = '%m/%e/%y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_code_col = 'strmgageid',
                         alt_site_code = list(
                             anderson_h1 = 'andrsn_h1',
                             lawson_b3 = 'lawson',
                             huey_f2 = 'huey',
                             crescent_f8 = 'crescent', #not certain
                             garwood_flats = 'garwood', #not certain
                             vguerard_f6 = 'vguerard'
                             # onyx_lwright = 'onyx', #not certain
                             # worm_altran = 'worm', #not certain
                         ),
                         data_cols =  c('n_no3' = 'NO3_N',
                                        'n_no2' = 'NO2_N',
                                        'n_nh4' = 'NH4_N',
                                        'srp' = 'orthophosphate_P'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#_comments',
                         is_sensor = FALSE)

    all_comments <- c(unique(d$`GN_NO3_N__|flg`), unique(d$`GN_NO2_N__|flg`),
                      unique(d$`GN_NH4_N__|flg`), unique(d$`GN_orthophosphate_P__|flg`))

    BDL_flags <- grep('ND|below detection limit|non-detect|Not detected|no detect|Not Detected|detection limit =',
                      all_comments, value = T)

    d <- ms_cast_and_reflag(d,
                            variable_flags_bdl = BDL_flags,
                            variable_flags_dirty = 'dirty',
                            variable_flags_to_drop = 'REMOVE')

    d <- filter_single_samp_sites(d)

    d <- ms_conversions(d,
                        convert_units_from = c('NO3_N' = 'ug/l',
                                               'NO2_N' = 'ug/l',
                                               'NH4_N' = 'ug/l',
                                               'orthophosphate_P' = 'ug/l'),
                        convert_units_to = c('NO3_N' = 'mg/l',
                                             'NO2_N' = 'mg/l',
                                             'NH4_N' = 'mg/l',
                                             'orthophosphate_P' = 'mg/l'))

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_78 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)


    d <- read.csv(rawfile, colClasses = 'character', skip = 26) %>%
        filter(STRMGAGEID != '',
               STRMGAGEID != 'garwood',
               STRMGAGEID != 'miers',
               STRMGAGEID != 'delta_upper',
               STRMGAGEID != 'lizotte_mouth',
               STRMGAGEID != 'vguerard_lower',
               STRMGAGEID != 'uvg_f21')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('DATE_TIME' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_code_col = 'STRMGAGEID',
                         data_cols =  c('TN..mg.L.' = 'TN'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         summary_flagcols = 'TN.COMMENTS',
                         is_sensor = FALSE)

    all_comments <- c(unique(d$TN.COMMENTS))

    BDL_flags <- grep('ND|below detection limit|non-detect|Not detected|no detect|Not Detected|detection limit =',
                      all_comments, value = T)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_bdl = list(TN.COMMENTS = BDL_flags),
                            summary_flags_dirty = list(TN.COMMENTS = 'dirty'),
                            summary_flags_to_drop = list(TN.COMMENTS = 'REMOVE'))

    d <- filter_single_samp_sites(d)

    d <- ms_conversions(d,
                        convert_units_from = c('NO3_N' = 'ug/l',
                                               'NO2_N' = 'ug/l',
                                               'NH4_N' = 'ug/l',
                                               'orthophosphate_P' = 'ug/l'),
                        convert_units_to = c('NO3_N' = 'mg/l',
                                             'NO2_N' = 'mg/l',
                                             'NH4_N' = 'mg/l',
                                             'orthophosphate_P' = 'mg/l'))

    return(d)
}

#derive kernels ####

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- stream_gauge_from_site_data

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__9002',
                                           'discharge__9003',
                                           'discharge__9007',
                                           'discharge__9009',
                                           'discharge__9010',
                                           'discharge__9011',
                                           'discharge__9013',
                                           'discharge__9015',
                                           'discharge__9016',
                                           'discharge__9017',
                                           'discharge__9018',
                                           'discharge__9021',
                                           'discharge__9022',
                                           'discharge__9023',
                                           'discharge__9024',
                                           'discharge__9027',
                                           'discharge__9029',
                                           'discharge__9030'))

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__24',
                                           'stream_chemistry__20',
                                           'stream_chemistry__21',
                                           'stream_chemistry__78',
                                           'stream_chemistry__9002',
                                           'stream_chemistry__9003',
                                           'stream_chemistry__9007',
                                           'stream_chemistry__9009',
                                           'stream_chemistry__9010',
                                           'stream_chemistry__9011',
                                           'stream_chemistry__9013',
                                           'stream_chemistry__9014',
                                           'stream_chemistry__9015',
                                           'stream_chemistry__9016',
                                           'stream_chemistry__9017',
                                           'stream_chemistry__9018',
                                           'stream_chemistry__9021',
                                           'stream_chemistry__9022',
                                           'stream_chemistry__9023',
                                           'stream_chemistry__9024',
                                           'stream_chemistry__9027',
                                           'stream_chemistry__9029',
                                           'stream_chemistry__9030'))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_stream_flux

