#retrieval kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(network, domain, set_details){

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

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <-  function(network, domain, set_details){

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component)

    R.utils::downloadFile(url = url,
                          filename = rawfile,
                          skip = FALSE,
                          overwrite = TRUE)

    deets_out <- collect_retrieval_details(set_details$url)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS006 <- function(network, domain, set_details){

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    siteNumb <- "401723105400000"

    #next time: use this chunk to identify params we might want.
    #if there are new ones, add them to wqp_codes (currently in loch_vale domain_helpers.R)
    #and then make access to wqp_codes more universal.
    siteparams <- dataRetrieval::whatNWISdata(siteNumber = siteNumb) %>%
        filter(count_nu > 10,
               ! is.na(parm_cd))

    #use this to lookup parameter metadata
    zz = readNWISpCode(unique(siteparams$parm_cd))

    #once you've modified wqp_codes, keep going.
    #btw, if there is a next time, this whole procedure needs to be encapsulated
    #and a more thorough look through the potential codes must be done.
    #!! SEE process_0_VERSIONLESS008 for more thorough handling
    siteparams <- siteparams %>%
        filter(parm_cd %in% wqp_codes$param_code) %>%
        distinct(parm_cd) %>%
        pull(parm_cd)

    siteWQ <- readWQPqw(siteNumbers = paste0('USGS-', siteNumb),
                        parameterCd = siteparams)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    test <- write_csv(siteWQ, file = rawfile)

    access_time <- with_tz(Sys.time(), tzone = 'UTC')
    deets_out <- list(url = 'retrieved via dataRetrieval::readWQPqw',
                      access_time = as.character(access_time),
                      last_mod_dt = access_time)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS007 <- function(network, domain, set_details){

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    siteNumb <- "401707105395000"

    #next time: use this chunk to identify params we might want.
    #if there are new ones, add them to wqp_codes (currently in loch_vale domain_helpers.R)
    #and then make access to wqp_codes more universal.
    siteparams <- dataRetrieval::whatNWISdata(siteNumber = siteNumb) %>%
        filter(count_nu > 10,
               ! is.na(parm_cd))

    #use this to lookup parameter metadata
    zz = readNWISpCode(unique(siteparams$parm_cd))

    #once you've modified wqp_codes, keep going.
    #btw, if there is a next time, this whole procedure needs to be encapsulated
    #and a more thorough look through the potential codes must be done.
    #!! SEE process_0_VERSIONLESS008 for more thorough handling
    siteparams <- siteparams %>%
        filter(parm_cd %in% wqp_codes$param_code) %>%
        distinct(parm_cd) %>%
        pull(parm_cd)

    siteWQ <- readWQPqw(siteNumbers = paste0('USGS-', siteNumb),
                        parameterCd = siteparams)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    test <- write_csv(siteWQ, file = rawfile)

    access_time <- with_tz(Sys.time(), tzone = 'UTC')
    deets_out <- list(url = 'retrieved via dataRetrieval::readWQPqw',
                      access_time = as.character(access_time),
                      last_mod_dt = access_time)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS008 <- function(network, domain, set_details){

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    siteNumb <- "401733105392404"

    #next time: use this chunk to identify params we might want.
    #if there are new ones, add them to wqp_codes (currently in loch_vale domain_helpers.R)
    #and then make access to wqp_codes more universal.
    siteparams <- dataRetrieval::whatNWISdata(siteNumber = siteNumb) %>%
        filter(count_nu > 10,
               ! is.na(parm_cd))

    #use this to lookup parameter metadata
    zz = readNWISpCode(unique(siteparams$parm_cd))

    #once you've modified wqp_codes, keep going.
    #btw, if there is a next time, this whole procedure needs to be encapsulated
    #and a more thorough look through the potential codes must be done
    siteparams <- siteparams %>%
        filter(parm_cd %in% wqp_codes$param_code) %>%
        distinct(parm_cd) %>%
        pull(parm_cd)

    siteWQ <- readWQPqw(siteNumbers = paste0('USGS-', siteNumb),
                        parameterCd = siteparams)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    test <- write_csv(siteWQ, file = rawfile)

    access_time <- with_tz(Sys.time(), tzone = 'UTC')
    deets_out <- list(url = 'retrieved via dataRetrieval::readWQPqw',
                      access_time = as.character(access_time),
                      last_mod_dt = access_time)

    return(deets_out)
}


#munge kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = c('dateoff' = '%Y-%m-%d %H:%M'),
                         datetime_tz = 'GMT',
                         site_code_col = 'siteID',
                         data_cols =  c('subppt' = 'precipitation'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'invalcode',
                         is_sensor = FALSE,
                         sampling_type = 'I',
                         set_to_NA = '-9.990')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list(invalcode = 'p           '),
                            summary_flags_clean = list(invalcode = c('            ',
                                                                     'l           ',
                                                                     'v           ',
                                                                     'n           ')))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d, precip_interp_method = 'mean_nocb')

    write_ms_file(d = d,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = 'CO98',
                  level = 'munged',
                  shapefile = FALSE)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    loch_vale_pchem_var_info <- list(
        "ph"     = c("unitless", "unitless", "pH"),
        "Conduc" = c("uS/cm", "uS/cm", "spCond"),
        "Ca"     = c("mg/L", "mg/L", "Ca"),
        "Mg"     = c("mg/L", "mg/L", "Mg"),
        "K "     = c("mg/L", "mg/L", "K"),
        "Na"     = c("mg/L", "mg/L", "Na"),
        "NH4"    = c("mg/L", "mg/L", "NH4"),
        "NO3"    = c("mg/L", "mg/L", "NO3"),
        "Cl"     = c("mg/L", "mg/L", "Cl"),
        "SO4"    = c("mg/L", "mg/L", "SO4")
        # "Br"     = c("mg/L", "mg/L", "Br")
    )

    d <- ms_read_raw_csv(rawfile,
                         datetime_cols = c('dateoff' = '%Y-%m-%d %H:%M'),
                         datetime_tz = 'GMT',
                         site_code_col = 'siteID',
                         data_cols = sapply(loch_vale_pchem_var_info, function(x) x[3]),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = 'flag#V#',
                         summary_flagcols = "invalcode",
                         is_sensor = FALSE,
                         sampling_type = 'I',
                         set_to_NA = c("-9", "-9.000"))

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = '<',
                            variable_flags_clean = ' ', #technically bdl, but can't find actual DLs, and they're filled in for us
                            summary_flags_dirty = list(invalcode = c('b           ', 'e           ', 'i           ')),
                            summary_flags_clean = list(invalcode = '            '))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d, precip_interp_method = 'mean_nocb')

    write_ms_file(d = d,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = 'CO98',
                  level = 'munged',
                  shapefile = FALSE)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component) {
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_raw <- 'data/webb/loch_vale/raw/stream_chemistry__VERSIONLESS006/sitename_NA/loch_vale_chem_andrews_creek.csv'

    d <- read.delim(temp_raw, sep = ',') %>%
        as_tibble()

    #for UV 254, two different units reported, only using the one that matches variables sheet
    d <- d %>%
        subset(!(ResultMeasure.MeasureUnitCode == "L/mgDOC*m" & grepl("UV 254", CharacteristicName)))

    loch_vale_andrews_creek_var_info <- list(
        "pH" = c('unitless', 'unitless', "pH"),
        "Calcium"=c("mg/l", "mg/L", "Ca"),
        "Magnesium"=c("mg/l", "mg/L", "Mg"),
        "Sodium"=c("mg/l", "mg/L", "Na"),
        "Potassium"=c("mg/l", "mg/L", "K"),
        "Chloride"=c("mg/l", "mg/L", "Cl"),
        "Sulfate"=c("mg/l", "mg/L", "SO4"),
        "Silica"=c("mg/l", "mg/L", "SiO2"),
        "Total dissolved solids"=c("mg/l", "mg/L", "TDS"),
        "Specific conductance"=c("uS/cm @25C", "uS/cm", "spCond"),
        "Strontium-87/Strontium-86, ratio"=c("unitless", "unitless","87Sr_86Sr"),
        "Strontium"=c("mg/l","mg/L","Sr"),
        "Temperature, water"=c("deg C", "degrees C", "temp"),
        "Mercury"=c("ng/l", "mg/L", "Hg"), # watch units
        "Carbon dioxide"=c("mg/l","ppm","CO2"),#watch units
        "Ammonia and ammonium" = c("mg/l as N","mg/L","NH3_NH4_N"),
        "Nitrate" = c("mg/l as N", "mg/L", "NO3_N" ),
        "Nitrogen"=c("mg/l","mg/L","TPN"), #total particulate nitrogen
        "Inorganic nitrogen (nitrate and nitrite)"=c("mg/l as N","mg/L","DIN"),  #dissolved
        "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"=c("mg/l", "mg/L", "TDN"),  #dissolved
        "Deuterium/Hydrogen ratio"  =c("unitless","unitless","dH2_H"),  #is that really what this is? or is it dD?
        "Oxygen-18/Oxygen-16 ratio" =c("unitless", "unitless", "d18O_d16O"), ##this is of nitrate and separately sulfate
        "Nitrogen-15/14 ratio"      =c("unitless","unitless", "d15N_d14N"), ##this is of nitrate
        "Sulfur-34/Sulfur-32 ratio" =c("unitless","unitless", "d34S_d32S"), ##this is of sulfate
        ##somewhere around here they also have d18O_NO3
        "Organic carbon"=c("mg/l","mg/L","DOC"),  #dissolved
        "Carbon"=c("mg/l","mg/L","TPC"), #total particulate carbon
        "Phosphorus"=c("mg/l as p", "mg/L", "TP"), #total
        "UV 254"=c("units/cm","AU/cm","abs254"),
        "Oxygen"=c("mg/l","mg/L","DO") #dissolved
    )

    loch_vale_stream_chem_var <- list()

    for (i in seq_along(loch_vale_andrews_creek_var_info)) {
        og_name <- names(loch_vale_andrews_creek_var_info[i])
        ms_name <- loch_vale_andrews_creek_var_info[[i]][[3]]
        loch_vale_stream_chem_var[[og_name]] <- ms_name
    }

    name_map <- unlist(loch_vale_stream_chem_var)
    pivoted_name_map <- loch_vale_stream_chem_var
    names(pivoted_name_map) <- unlist(loch_vale_stream_chem_var)

    d <- d %>%
        mutate(CharacteristicName = name_map[CharacteristicName]) %>%
        filter(!is.na(CharacteristicName)) %>%
        distinct()

    d <- d %>%
        rename(dl_col = DetectionQuantitationLimitMeasure.MeasureValue) %>%
        mutate(dl_col = as.numeric(dl_col))

    d <-pivot_wider(d, names_from = CharacteristicName, values_from = c(ResultMeasureValue, dl_col))

    # NOTE: manually changing site code to macrosheds canonical version, i.e. from uSGS numeric code to "amdrews_creek"
    d$MonitoringLocationIdentifier = 'andrews_creek'

    #browser()
    # NOTE: must fix detection limit application below
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('ActivityStartDate' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'MonitoringLocationIdentifier',
                         data_cols =  pivoted_name_map,
                         data_col_pattern = 'ResultMeasureValue_#V#',
                         is_sensor = FALSE,
                         numeric_dl_col_pattern = "dl_col_#V#",
                         summary_flagcols = c('ResultValueTypeName')
    )

    # NOTE: there must be USGS edit codes which contain QC information?
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(ResultValueTypeName = c('Actual')),
                            summary_flags_dirty = list(ResultValueTypeName = c('Estimated')))

    andrews_creek_aq_chem_units_old = c()
    andrews_creek_aq_chem_units_new = c()

    for(i in 1:length(loch_vale_andrews_creek_var_info)) {
        og_name <- names(loch_vale_andrews_creek_var_info[i])
        og_units <- loch_vale_andrews_creek_var_info[[i]][1]
        ms_name <- loch_vale_andrews_creek_var_info[[i]][3]
        ms_units <-loch_vale_andrews_creek_var_info[[i]][2]
        andrews_creek_aq_chem_units_old[ms_name] = og_units
        andrews_creek_aq_chem_units_new[ms_name] = ms_units
    }

    sd <- ms_conversions(d,
                         convert_units_from = andrews_creek_aq_chem_units_old,
                         convert_units_to = andrews_creek_aq_chem_units_new
    )

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS007 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.delim(rawfile, sep = ',') %>%
        as_tibble()

    #manually applying MS Detection Limit (when below dl, take half of dl)
    #for this USGS data, whenever there is NA for the measure,
    #a detection limit is reported, if not below, no detection limit reported
    d <- d %>%
        rename(dl_col = DetectionQuantitationLimitMeasure.MeasureValue) %>%
        mutate(
            ResultMeasureValue =  ifelse(!is.na(dl_col) & is.na(ResultMeasureValue) & ResultDetectionConditionText == 'Not Detected', as.numeric(dl_col)/2, ResultMeasureValue)
        )

    #for UV 254, two different units reported, only using the one that matches variables sheet
    d <- d %>%
        subset(!(ResultMeasure.MeasureUnitCode == "L/mgDOC*m" & grepl("UV 254", CharacteristicName)))

    loch_vale_icy_brook_var_info <- list(
        "pH" = c('unitless', 'unitless', "pH"),
        "Calcium"=c("mg/l", "mg/L", "Ca"),
        "Magnesium"=c("mg/l", "mg/L", "Mg"),
        "Sodium"=c("mg/l", "mg/L", "Na"),
        "Potassium"=c("mg/l", "mg/L", "K"),
        "Chloride"=c("mg/l", "mg/L", "Cl"),
        "Sulfate"=c("mg/l", "mg/L", "SO4"),
        "Silica"=c("mg/l", "mg/L", "SiO2"),
        "Total Dissolved Solids"=c("mg/l", "mg/L", "TDS"),
        "Specific Conductance"=c("uS/cm @25C", "uS/cm", "spCond"),
        "Strontium-87/Strontium-86, ratio"=c("unitless", "unitless","87Sr_86Sr"),
        "Strontium"=c("mg/l","mg/L","Sr"),
        "Temperature, water"=c("deg C", "degrees C", "temp"),
        "Mercury"=c("ng/l", "mg/L", "Hg"), # watch units
        "Carbon dioxide"=c("mg/l","ppm","CO2"), #watch units
        "Ammonia and ammonium" = c("mg/l as N","mg/L" ,"NH3_NH4_N"),
        "Nitrate" = c("mg/l as N", "mg/L", "NO3_N" ),
        "Nitrogen"=c("mg/l","mg/L","TPN"), #total particulate nitrogen
        "Inorganic nitrogen (nitrate and nitrite)"=c("mg/l as N","mg/L","DIN"),  #dissolved
        "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"=c("mg/l", "mg/L", "TDN"),  #dissolved
        "Deuterium/Hydrogen ratio"  =c("unitless","unitless","dH2_H"),
        "Oxygen-18/Oxygen-16 ratio" =c("unitless", "unitless", "d18O_d16O"),
        "Nitrogen-15/14 ratio"      =c("unitless","unitless", "d15N_d14N"),
        "Sulfur-34/Sulfur-32 ratio" =c("unitless","unitless", "d34S_d32S"),
        "Organic carbon"=c("mg/l","mg/L","DOC"),  #dissolved
        "Carbon"=c("mg/l","mg/L","TPC"), #total particulate carbon
        "Phosporus"=c("mg/l as p", "mg/L", "TP"), #total
        "UV 254"=c("units/cm","AU/cm","abs254"),
        "Oxygen"=c("mg/l","mg/L","DO") #dissolved
    )

    loch_vale_stream_chem_var <- list()

    for (i in seq_along(loch_vale_icy_brook_var_info)) {
        og_name <- names(loch_vale_icy_brook_var_info[i])
        ms_name <- loch_vale_icy_brook_var_info[[i]][[3]]
        loch_vale_stream_chem_var[[og_name]] <- ms_name
    }

    # NOTE: manually changing site code to macrosheds canonical version, i.e. from uSGS numeric code to "amdrews_creek"
    d$MonitoringLocationIdentifier = 'icy_brook'

    d <- pivot_wider(d, names_from = CharacteristicName, values_from = c(ResultMeasureValue, dl_col))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('ActivityStartDate' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'MonitoringLocationIdentifier',
                         data_cols =  loch_vale_stream_chem_var,
                         data_col_pattern = 'ResultMeasureValue_#V#',
                         is_sensor = FALSE
                         # numeric_dl_col_pattern = "DetectionQuantitationLimitMeasure.MeasureValue_#V#"
    )

    # NOTE: there must be USGS edit codes which contain QC information?
    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS008 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.delim(rawfile, sep = ',') %>%
        as_tibble()

    #manually applying MS Detection Limit (when below dl, take half of dl)
    #for this USGS data, whenever there is NA for the measure,
    #a detection limit is reported, if not below, no detection limit reported
    d <- d %>%
        rename(dl_col = DetectionQuantitationLimitMeasure.MeasureValue) %>%
        mutate(
            ResultMeasureValue =  ifelse(!is.na(dl_col) & is.na(ResultMeasureValue) & ResultDetectionConditionText == 'Not Detected', as.numeric(dl_col)/2, ResultMeasureValue)
        )

    #for UV 254, two different units reported, only using the one that matches variables sheet
    d <- d %>%
        subset(!(ResultMeasure.MeasureUnitCode == "L/mgDOC*m" & grepl("UV 254", CharacteristicName)))

    loch_vale_loch_outlet_var_info <- list(
        "pH" = c("unitless", "unitless", "pH"),
        "Alkalinity" = c("mg/l CaCO3", "mg/L", "alk"),
        "Calcium"=c("mg/l", "mg/L", "Ca"),
        "Magnesium"=c("mg/l", "mg/L", "Mg"),
        "Sodium"=c("mg/l", "mg/L", "Na"),
        "Potassium"=c("mg/l", "mg/L", "K"),
        "Chloride"=c("mg/l", "mg/L", "Cl"),
        "Sulfate"=c("mg/l", "mg/L", "SO4"),
        "Silica"=c("mg/l", "mg/L", "SiO2"),
        "Total Dissolved Solids"=c("mg/l", "mg/L", "TDS"),
        "Specific Conductance"=c("uS/cm @25C", "uS/cm", "spCond"),
        "Strontium-87/Strontium-86, ratio"=c("unitless", "unitless","87Sr_86Sr"),
        "Strontium"=c("mg/l","mg/L","Sr"),
        "Temperature, water"=c("deg C", "degrees C", "temp"),
        "Mercury"=c("ng/l", "mg/L", "Hg"), # watch units
        "Carbon dioxide"=c("mg/l","ppm","CO2"), #watch units
        "Ammonia and ammonium" = c("mg/l as N","mg/L" ,"NH3_NH4_N"),
        "Nitrate" = c("mg/l as N", "mg/L", "NO3_N" ),
        "Nitrogen"=c("mg/l","mg/L","TPN"), #total particulate nitrogen
        "Inorganic nitrogen (nitrate and nitrite)"=c("mg/l as N","mg/L","DIN"),  #dissolved
        "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"=c("mg/l", "mg/L", "TDN"),  #dissolved
        "Deuterium/Hydrogen ratio"  =c("unitless","unitless","dH2_H"),
        "Oxygen-18/Oxygen-16 ratio" =c("unitless", "unitless", "d18O_d16O"),
        "Nitrogen-15/14 ratio"      =c("unitless","unitless", "d15N_d14N"),
        "Sulfur-34/Sulfur-32 ratio" =c("unitless","unitless", "d34S_d32S"),
        "Organic carbon"=c("mg/l","mg/L","DOC"),  #dissolved
        "Carbon"=c("mg/l","mg/L","TPC"), #total particulate carbon
        "Phosporus"=c("mg/l as p", "mg/L", "TP"), #total
        "UV 254"=c("units/cm","AU/cm","abs254"),
        "Oxygen"=c("mg/l","mg/L","DO") #dissolved
    )

    loch_vale_stream_chem_var <- list()

    for (i in seq_along(loch_vale_loch_outlet_var_info)) {
        og_name <- names(loch_vale_loch_outlet_var_info[i])
        ms_name <- loch_vale_loch_outlet_var_info[[i]][[3]]
        loch_vale_stream_chem_var[[og_name]] <- ms_name
    }

    d <- pivot_wider(d, names_from = CharacteristicName, values_from = c(ResultMeasureValue, dl_col))

    # NOTE: manually changing site code to macrosheds canonical version, i.e. from uSGS numeric code to "amdrews_creek"
    d$MonitoringLocationIdentifier = 'the_loch_outlet'

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('ActivityStartDate' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'MonitoringLocationIdentifier',
                         data_cols =  loch_vale_stream_chem_var,
                         data_col_pattern = 'ResultMeasureValue_#V#',
                         is_sensor = FALSE
                         # numeric_dl_col_pattern = "DetectionQuantitationLimitMeasure.MeasureValue_#V#"
    )

    # NOTE: there must be USGS edit codes which contain QC information?
    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
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

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c(
                         'stream_chemistry__VERSIONLESS006',
                         'stream_chemistry__VERSIONLESS007',
                         'stream_chemistry__VERSIONLESS008'))

}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms005 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms006 <- stream_gauge_from_site_data

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_precip_pchem_pflux


