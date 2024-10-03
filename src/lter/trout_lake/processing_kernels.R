#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_276 <- function(set_details, network, domain){

    if(grepl('Site Information', set_details$component)){
        loginfo('This component is blocklisted', logger = logger_module)
        return(generate_blocklist_indicator())
    }

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_276 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    dd <- read.csv(rawfile) %>% tibble() %>%
        mutate(site_name = sub(' ', '_', tolower(site_name)))

    trout_lake_chem_var_info <- list( #item 2 blank if no conversion needed
        'anc' = c('ueq/L', 'eq/L', 'ANC'),
        'c13_c12_ratio' = c('permil', '', 'd13C'),
        "ca" = c('mg/L', '', 'Ca'),
        "cl" = c('mg/L', '', 'Cl'),
        'co2' = c('mg/L', '', 'CO2'),
        "co3" = c('mg/L', '', 'CO3'),
        'color' = c('PCU', '', 'color'),
        'conductance_field' = c('uS/cm', '', 'spCond'),
        'dic' = c('mg/L', '', 'DIC'),
        #DKN
        "do" = c('mg/L', '', 'DO'),
        "doc" = c('mg/L', '', 'DOC'),
        "don" = c('mg/L', '', 'DON'),
        'dp' = c('mg/L', '', 'TDP'),
        'f' = c('mg/L', '', 'F'),
        "fe" = c('mg/L', '', 'Fe'),
        'h_ion' = c('mg/L', '', 'H'),
        "hco3" = c('mg/L', '', 'HCO3'),
        "k" = c('mg/L', '', 'K'),
        "mg" = c('mg/L', '', 'Mg'),
        "mn" = c('mg/L', '', 'Mn'),
        'n_mixed' = c('mg/L', '', 'TN'),
        'n_mixed_1' = c('mg/L', '', 'TDN'),
        "na" = c('mg/L', '', 'Na'),
        #various dissolved nitrogen and phosphorus species
        'nh3_nh4_2' = c('mg/L', '', 'NH3_NH4_N'),
        'no2' = c('mg/L', '', 'NO2_N'),
        'no3' = c('mg/L', '', 'NO3_N'),
        'no32_2' = c('mg/L', '', 'NO3_NO2_N'),
        'o18_o16_ratio' = c('permil', '', 'd18O'),
        'o2sat' = c('%', '', 'DO_sat'),
        'ph_field' = c('unitless', '', 'pH'),
        'po4' = c('mg/L', '', 'PO4'),
        "s" = c('ug/L', 'mg/L', 'S'),
        #SAR
        "si" = c('mg/L', '', 'SiO2'),
        "so4" = c('mg/L', '', 'SO4'),
        #SPC
        "sr" = c('ug/L', 'mg/L', 'Sr'),
        'tds' = c('mg/L', '', 'TDS'),
        'tkn' = c('mg/L', '', 'TKN'),
        'toc' = c('mg/L', '', 'TOC'),
        'ton' = c('mg/L', '', 'TON'),
        'tp_2' = c('mg/L', '', 'TP'),
        'tpc' = c('mg/L', '', 'TPC'),
        'tpn' = c('mg/L', '', 'TPN')
    )

    d <- ms_read_raw_csv(preprocessed_tibble = dd,
                         datetime_cols = c('sampledate' = '%Y-%m-%d',
                                              'sample_time' = '%H:%M:%S'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'site_name',
                         data_cols = sapply(trout_lake_chem_var_info,
                                            function(x) x[3]),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    units_to_convert <- Filter(function(x) x[2] != '',
                               trout_lake_chem_var_info)
    names(units_to_convert) <- sapply(units_to_convert,
                                      function(x) x[3]) %>% unname()

    d <- ms_conversions_(d,
                        convert_units_from = sapply(units_to_convert,
                                                    function(x) x[1]),
                        convert_units_to = sapply(units_to_convert,
                                                  function(x) x[2]))
}

#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    nwis_codes <- site_data %>%
        filter(network == !!network,
               domain == !!domain,
               site_type == 'stream_gauge') %>%
        select(site_code, colocated_gauge_id) %>%
        mutate(colocated_gauge_id = str_extract(colocated_gauge_id, '[0-9]+')) %>%
        tibble::deframe()

    pull_usgs_discharge(network = network,
                        domain = domain,
                        prodname_ms = prodname_ms,
                        sites = nwis_codes,
                        time_step = rep('daily', length(nwis_codes)))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- stream_gauge_from_site_data
