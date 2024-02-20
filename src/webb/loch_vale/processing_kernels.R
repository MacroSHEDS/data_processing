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
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    R.utils::downloadFile(url = set_details$url,
                          filename = rawfile,
                          skip = FALSE,
                          overwrite = TRUE)

    deets_out <- collect_retrieval_details(set_details$url)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS006 <- function(network, domain, set_details){

    nwis_site_code <- '401723105400000'

    #next time: use this chunk to identify params we might want.
    #if there are new ones, add them to wqp_codes (currently in loch_vale domain_helpers.R)
    #and then make access to wqp_codes more universal.
    siteparams <- dataRetrieval::whatNWISdata(siteNumber = nwis_site_code) %>%
        as_tibble() %>%
        filter(count_nu > 10,
               ! is.na(parm_cd))

    #use this to cross-reference parameter metadata
    # View(readNWISpCode(unique(siteparams$parm_cd)))

    retrieve_wqp_chem(nwis_site_code, siteparams, set_details)

    access_time <- with_tz(Sys.time(), tzone = 'UTC')
    deets_out <- list(url = 'retrieved via dataRetrieval::readWQPqw',
                      access_time = as.character(access_time),
                      last_mod_dt = access_time)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS007 <- function(network, domain, set_details){

    nwis_site_code <- '401707105395000'

    #next time: use this chunk to identify params we might want.
    #if there are new ones, add them to wqp_codes (currently in loch_vale domain_helpers.R)
    #and then make access to wqp_codes more universal.
    siteparams <- dataRetrieval::whatNWISdata(siteNumber = nwis_site_code) %>%
        as_tibble() %>%
        filter(count_nu > 10,
               ! is.na(parm_cd))

    #use this to cross-reference parameter metadata
    # View(readNWISpCode(unique(siteparams$parm_cd)))

    retrieve_wqp_chem(nwis_site_code, siteparams, set_details)

    access_time <- with_tz(Sys.time(), tzone = 'UTC')
    deets_out <- list(url = 'retrieved via dataRetrieval::readWQPqw',
                      access_time = as.character(access_time),
                      last_mod_dt = access_time)

    return(deets_out)

    access_time <- with_tz(Sys.time(), tzone = 'UTC')
    deets_out <- list(url = 'retrieved via dataRetrieval::readWQPqw',
                      access_time = as.character(access_time),
                      last_mod_dt = access_time)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS008 <- function(network, domain, set_details){

    nwis_site_code <- '401733105392404'

    #next time: use this chunk to identify params we might want.
    #if there are new ones, add them to wqp_codes (currently in loch_vale domain_helpers.R)
    #and then make access to wqp_codes more universal.
    siteparams <- dataRetrieval::whatNWISdata(siteNumber = nwis_site_code) %>%
        as_tibble() %>%
        filter(count_nu > 10,
               ! is.na(parm_cd))

    #use this to cross-reference parameter metadata
    # View(readNWISpCode(unique(siteparams$parm_cd)))

    retrieve_wqp_chem(nwis_site_code, siteparams, set_details)

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
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component){

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
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    process_wqp_chem(rawfile = rawfile,
                     ms_site_code = 'andrews_creek')

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

    process_wqp_chem(rawfile = rawfile,
                     ms_site_code = 'icy_brook')

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

    process_wqp_chem(rawfile = rawfile,
                     ms_site_code = 'the_loch_outlet')

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

    return()
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


