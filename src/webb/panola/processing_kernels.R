
#retrieval kernels ####

#discharge; stream_chemistry; precipitation; precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain){

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}',
                    rd = raw_data_dest,
                    c = set_details$component)

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          skip = FALSE,
                          overwrite = TRUE)

    deets_out <- collect_retrieval_details(set_details$url)

    return(deets_out)
}

#munge kernels ####

p1v001_discharge <- function(zipf){

    raw_csv <- read.csv(unz(zipf, "3_PMRW_Streamflow_WY86-17.csv"),
                        header = TRUE, sep = ",") %>%
        as_tibble() %>%
        mutate(site = 'mountain_creek_tributary')

    d <- ms_read_raw_csv(preprocessed_tibble = raw_csv,
                         datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols =  c('Streamflow' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c("Quality_Cd"),
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(Quality_Cd = c('1', '2')),
                            summary_flags_dirty = list(Quality_Cd = c('3', '4')))

    return(d)
}

p1v001_stream_chemistry <- function(zipf){

    d <- read.csv(unz(zipf, '4_PMRW_StreamWaterQuality_WY86-17.csv'),
                  header = TRUE, sep = ",") %>%
        as_tibble() %>%
        mutate(site = 'mountain_creek_tributary')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols =  c(
                             "pH"  =  "pH",
                             "ANC_Conc" = "ANC",
                             "Ca_Conc" = "Ca",
                             "Mg_Conc" = "Mg",
                             "Na_Conc" = "Na",
                             "K_Conc" = "K",
                             "SO4_Conc" = "SO4",
                             "NO3_Conc" = "NO3",
                             "Cl_Conc" = "Cl",
                             "Si_Conc" = "Si",
                             "DOC_Conc" = "DOC"
                         ),
                         set_to_NA = ".",
                         data_col_pattern = '#V#',
                         convert_to_BDL_flag = '<#*#',
                         is_sensor = FALSE,
                         keep_bdl_values = TRUE)

    d <- ms_cast_and_reflag(d, variable_flags_bdl = 'BDL')

    var_deets <- list(
        'ANC' = c('ueq/l', 'eq/l'),
        'Ca' = c('ueq/l', 'mg/l'),
        'Mg' = c('ueq/l', 'mg/l'),
        'Na' = c('ueq/l', 'mg/l'),
        'K' = c('ueq/l', 'mg/l'),
        'SO4' = c('ueq/l', 'mg/l'),
        'NO3' = c('ueq/l', 'mg/l'),
        'Cl' = c('ueq/l', 'mg/l'),
        'Si' = c('umol/l', 'mg/l'),
        'DOC' = c('umol/l', 'mg/l')
    )

    update_detlims(d, var_deets)

    d <- ms_conversions(d,
                        convert_units_from = sapply(var_deets, function(x) x[1]),
                        convert_units_to = sapply(var_deets, function(x) x[2]))

    return(d)
}

p1v001_precip_chemistry <- function(zipf){

    d <- read.csv(unz(zipf, '2_PMRW_PrecipitationWaterQuality_WY86-17.csv'),
                  header = TRUE, sep = ",") %>%
        as_tibble() %>%
        # mutate(site = 'mountain_creek_tributary')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols =  c(
                             "pH"  =  "pH",
                             "ANC_Conc" = "ANC",
                             "Ca_Conc" = "Ca",
                             "Mg_Conc" = "Mg",
                             "Na_Conc" = "Na",
                             "K_Conc" = "K",
                             "SO4_Conc" = "SO4",
                             "NO3_Conc" = "NO3",
                             "Cl_Conc" = "Cl",
                             "Si_Conc" = "Si",
                             "DOC_Conc" = "DOC"
                         ),
                         set_to_NA = ".",
                         data_col_pattern = '#V#',
                         convert_to_BDL_flag = '<#*#',
                         is_sensor = FALSE,
                         keep_bdl_values = TRUE)

    d <- ms_cast_and_reflag(d, variable_flags_bdl = 'BDL')

    var_deets <- list(
        'ANC' = c('ueq/l', 'eq/l'),
        'Ca' = c('ueq/l', 'mg/l'),
        'Mg' = c('ueq/l', 'mg/l'),
        'Na' = c('ueq/l', 'mg/l'),
        'K' = c('ueq/l', 'mg/l'),
        'SO4' = c('ueq/l', 'mg/l'),
        'NO3' = c('ueq/l', 'mg/l'),
        'Cl' = c('ueq/l', 'mg/l'),
        'Si' = c('umol/l', 'mg/l'),
        'DOC' = c('umol/l', 'mg/l')
    )

    update_detlims(d, var_deets)

    d <- ms_conversions(d,
                        convert_units_from = sapply(var_deets, function(x) x[1]),
                        convert_units_to = sapply(var_deets, function(x) x[2]))

    return(d)
}

#discharge; stream_chemistry; precipitation; precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <-function(network, domain, prodname_ms, site_code, component){

    raw_data_loc <- glue('data/{n}/{d}/raw/{p}/{s}',
                         n = network,
                         d = domain,
                         p = 'discharge__VERSIONLESS001',
                         s = site_code)

    zipf <- glue('{rd}/{c}',
                 rd = raw_data_loc,
                 c = component)

    prodname <- prodname_from_prodname_ms(prodname_ms)
    if(prodname == 'discharge'){
        d <- p1v001_discharge(zipf)
    } else if(prodname == 'stream_chemistry'){
        d <- p1v001_stream_chemistry(zipf)
    } else if(prodname == 'precipitation'){
        d <- p1v001_precipitation(zipf)
    } else if(prodname == 'precip_chemistry'){
        d <- p1v001_precip_chemistry(zipf)
    }

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in sites){

        d_site <- filter(d, site_code == !!s)

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = s,
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms002 <- stream_gauge_from_site_data

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- precip_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_precip_pchem_pflux
