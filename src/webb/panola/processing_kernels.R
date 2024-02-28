
#retrieval kernels ####

#discharge; stream_chemistry; CUSTOMprecipitation; precip_chemistry; CUSTOMprecip_flux_inst_scaled; CUSTOMstream_flux_inst_scaled: STATUS=READY
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

    d <- read.csv(unz(zipf, "3_PMRW_Streamflow_WY86-17.csv"),
                        header = TRUE, sep = ",") %>%
        as_tibble() %>%
        mutate(site = 'mountain_creek_tributary')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols =  c('Streamflow' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = "Quality_Cd",
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

    stop('update panola var_deets to no longer require the second unit form')
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

p1v001_CUSTOMprecipitation <- function(zipf){

    d <- read.csv(unz(zipf, '1_PMRW_Precipitation_WY86-18.csv'),
                        header = TRUE, sep = ",") %>%
        as_tibble() %>%
        mutate(site = 'granite_etc')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols =  c('Precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         summary_flagcols = "Quality_Cd",
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(Quality_Cd = c('1', '2')),
                            summary_flags_dirty = list(Quality_Cd = c('3', '4')))

    d$val <- d$val * 10 #cm -> mm

    return(d)
}

p1v001_precip_chemistry <- function(zipf){

    d <- read.csv(unz(zipf, '2_PMRW_PrecipitationWaterQuality_WY86-17.csv'),
                  header = TRUE, sep = ",") %>%
        as_tibble() %>%
        mutate(site = 'granite_etc')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols = c(
                             "ANC_Conc" = "ANC",
                             "H_Conc" = "H",
                             "Ca_Conc" = "Ca",
                             "Mg_Conc" = "Mg",
                             "Na_Conc" = "Na",
                             "K_Conc" = "K",
                             "NH4_Conc" = "NH4",
                             "SO4_Conc" = "SO4",
                             "NO3_Conc" = "NO3",
                             "Cl_Conc" = "Cl"
                         ),
                         set_to_NA = ".",
                         data_col_pattern = '#V#',
                         convert_to_BDL_flag = '<#*#',
                         summary_flagcols = 'Notes',
                         is_sensor = FALSE,
                         keep_bdl_values = TRUE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_clean = list(Notes = ''),
                            summary_flags_to_drop = list(Notes = 'this string will never match'), #mark any string as "dirty"
                            variable_flags_bdl = 'BDL')

    var_deets <- list(
        'ANC' = c('ueq/l', 'eq/l'),
        'H' = c('ueq/l', 'mg/l'),
        'Ca' = c('ueq/l', 'mg/l'),
        'Mg' = c('ueq/l', 'mg/l'),
        'Na' = c('ueq/l', 'mg/l'),
        'K' = c('ueq/l', 'mg/l'),
        'NH4' = c('ueq/l', 'mg/l'),
        'SO4' = c('ueq/l', 'mg/l'),
        'NO3' = c('ueq/l', 'mg/l'),
        'Cl' = c('ueq/l', 'mg/l')
    )

    update_detlims(d, var_deets)

    d <- ms_conversions(d,
                        convert_units_from = sapply(var_deets, function(x) x[1]),
                        convert_units_to = sapply(var_deets, function(x) x[2]))

    return(d)
}

p1v001_CUSTOMprecip_flux_inst_scaled <- function(zipf){

    d <- read.csv(unz(zipf, '8_PMRW_WetDepositionSoluteFluxes_Daily_WY86-17.csv'),
                        header = TRUE, sep = ",") %>%
        as_tibble() %>%
        mutate(site = 'mountain_creek_tributary')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols =  c('H_WetDep' = 'H',
                                        'Ca_WetDep' = 'Ca',
                                        'Mg_WetDep' = 'Mg',
                                        'Na_WetDep' = 'Na',
                                        'K_WetDep' = 'K',
                                        'NH4_WetDep' = 'NH4',
                                        'SO4_WetDep' = 'SO4',
                                        'NO3_WetDep' = 'NO3',
                                        'Cl_WetDep' = 'Cl'),
                         data_col_pattern = '#V#',
                         set_to_NA = '.',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    var_deets <- list(
        'H' = c('ueq/l', 'kg/l'),
        'Ca' = c('ueq/l', 'kg/l'),
        'Mg' = c('ueq/l', 'kg/l'),
        'Na' = c('ueq/l', 'kg/l'),
        'K' = c('ueq/l', 'kg/l'),
        'NH4' = c('ueq/l', 'kg/l'),
        'SO4' = c('ueq/l', 'kg/l'),
        'NO3' = c('ueq/l', 'kg/l'),
        'Cl' = c('ueq/l', 'kg/l')
    )

    d <- ms_conversions(d,
                        convert_units_from = sapply(var_deets, function(x) x[1]),
                        convert_units_to = sapply(var_deets, function(x) x[2]))

    d$val <- d$val * 10000 #m^-2 -> ha^-1
    d$datetime <- floor_date(d$datetime, unit = 'day')

    writedir <- 'data/webb/panola/munged/CUSTOMprecip_flux_inst_scaled__VERSIONLESS001'
    dir.create(writedir,
               showWarnings = FALSE,
               recursive = TRUE)

    write_feather(d, file.path(writedir, 'mountain_creek_tributary.feather'))

    return(d)
}

p1v001_CUSTOMstream_flux_inst_scaled <- function(zipf, colname){

    d <- read.csv(unz(zipf, '11_PMRW_StreamwaterSoluteFluxes_Daily_WY86-16.csv'),
                        header = TRUE, sep = ",") %>%
        as_tibble() %>%
        select(Date, Solute, !!colname) %>%
        pivot_wider(names_from = 'Solute',
                    values_from = !!colname) %>%
        mutate(site = 'mountain_creek_tributary')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y'),
                         datetime_tz = 'America/New_York', #assumed. could be utc
                         site_code_col = 'site',
                         data_cols =  c('ANC', 'Ca', 'Mg', 'Na', 'K', 'SO4',
                                        'NO3', 'Cl', 'Si', 'DOC'),
                         data_col_pattern = '#V#',
                         set_to_NA = '.',
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    d <- filter(d, var != 'GN_ANC') #no idea how to convert this

    var_deets <- list(
        # 'ANC' = c('ueq/l', 'kg/l'),
        'Ca' = c('ueq/l', 'kg/l'),
        'Mg' = c('ueq/l', 'kg/l'),
        'Na' = c('ueq/l', 'kg/l'),
        'K' = c('ueq/l', 'kg/l'),
        'SO4' = c('ueq/l', 'kg/l'),
        'NO3' = c('ueq/l', 'kg/l'),
        'Cl' = c('ueq/l', 'kg/l'),
        'Si' = c('umol/l', 'kg/l'),
        'DOC' = c('umol/l', 'kg/l')
    )

    d <- ms_conversions(d,
                        convert_units_from = sapply(var_deets, function(x) x[1]),
                        convert_units_to = sapply(var_deets, function(x) x[2]))

    d$val <- d$val * 10000 #m^-2 -> ha^-1
    d$datetime <- floor_date(d$datetime, unit = 'day')

    writedir <- 'data/webb/panola/munged/CUSTOMstream_flux_inst_scaled__VERSIONLESS001'
    writedir <- paste(writedir, colname, sep = '_')
    dir.create(writedir,
               showWarnings = FALSE,
               recursive = TRUE)

    write_feather(d, file.path(writedir, 'mountain_creek_tributary.feather'))
}

#discharge; stream_chemistry; CUSTOMprecipitation; precip_chemistry; CUSTOMprecip_flux_inst_scaled; CUSTOMstream_flux_inst_scaled: STATUS=READY
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
    } else if(prodname == 'CUSTOMprecipitation'){
        d <- p1v001_CUSTOMprecipitation(zipf)
    } else if(prodname == 'precip_chemistry'){
        d <- p1v001_precip_chemistry(zipf)
    } else if(prodname == 'CUSTOMprecip_flux_inst_scaled'){
        p1v001_CUSTOMprecip_flux_inst_scaled(zipf)
        return()
    } else if(prodname == 'CUSTOMstream_flux_inst_scaled'){
        p1v001_CUSTOMstream_flux_inst_scaled(zipf, colname = 'Flx_RefTot')
        p1v001_CUSTOMstream_flux_inst_scaled(zipf, colname = 'Flx_RefMod')
        p1v001_CUSTOMstream_flux_inst_scaled(zipf, colname = 'Flx_ClmTot')
        p1v001_CUSTOMstream_flux_inst_scaled(zipf, colname = 'Flx_ClmMod')
        return()
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
