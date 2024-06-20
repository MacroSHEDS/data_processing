#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain){

    prod <- 'discharge'
    site <- set_details$site_code
    component <- set_details$component
    prodname_ms <- set_details$prodname_ms

    default_to = getOption('timeout')
    options(timeout=100000)

    # discharge data is 10-20 years of 15m sensor data provided as a straight xlsx file
    # the downloads are tricky, and often 504 timeout or other errors (inconsistently)
    # might be able to use different link, example: http://wq.swwdmn.org/downloads/new?opts=100th-st/gauge&site_id=100th-st&site_label=100th%20St

    # set interval to sleep after each download (seconds)
    sleep_time = 360

    # each file loaded into a site folder
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site,
                    c = component)

    site_download_url <- get_url_swwd_prod(site, prodname = prod)

    stop('manually downloaded swwd Q in 2024, but it has updated. just note the download takes forever.')

    dl <- R.utils::downloadFile(
        url = site_download_url,
        filename = rawfile,
        skip = FALSE,
        overwrite = TRUE,
        method = 'libcurl')

    options(timeout = default_to)

    # after all is said and done
    res <- httr::HEAD(site_download_url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(site_download_url, ''),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    # short resting period, avoid issues with swwd site failing downloads bc
    # too many subsequent requests. seems wiating a short window can help
    writeLines(glue::glue('   sleeping for {sleep_time} seconds'))
    Sys.sleep(sleep_time)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain){

    site <- set_details$site_code
    component <- set_details$component
    prodname_ms <- set_details$prodname_ms

    # each file loaded into a site folder
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site,
                    c = component)

    site_download_url <- get_url_swwd_prod(site)

    dl <- R.utils::downloadFile(
        url = site_download_url,
        filename = rawfile,
        skip = FALSE,
        overwrite = TRUE,
        method = 'libcurl')

    res <- httr::HEAD(site_download_url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(site_download_url, ''),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component
                    )

    raw_xlsx <- readxl::read_xlsx(rawfile) %>%
      mutate(
        site_code = !!site_code
      )

    # standardize discharge column names
    q_names <- names(raw_xlsx)[grepl('Discharge \\(cfs\\)', names(raw_xlsx))]
    if(length(q_names) == 2) {
      names(raw_xlsx)[grepl('Discharge \\(cfs\\)', names(raw_xlsx))] <- c('discharge', 'discharge_flag')
    } else if(length(q_names) == 1) {
      names(raw_xlsx)[grepl('Discharge \\(cfs\\)', names(raw_xlsx))] <- c('discharge')
    } else {
      warning('too many discharge columns in raw SWWD data')
    }


    # hey! if this kernel is being run again, make sure to check the flag columns
    # in the original data, as there may be new flag info
    d <- ms_read_raw_csv(preprocessed_tibble = raw_xlsx,
                         datetime_cols = c('Date' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'America/Chicago',
                         site_code_col = 'site_code',
                         data_cols =  c('discharge'),
                         data_col_pattern = '#V#',
                         # sampling regime:
                         # sensor vs non-sensor
                         # installed (IS) vs grab (GN)
                         is_sensor = TRUE
                         ## summary_flagcols = c('ESTCODE', 'EVENT_CODE')
                         )

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA
                            )

    #convert cfs to liters/s
    # NOTE: we should have handling?
    d <- d %>%
        mutate(val = val * 28.317)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    # conforming time interval to daily
    d <- synchronize_timestep(d)

    write_ms_file(d = d,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = site_code,
                      level = 'munged',
                      shapefile = FALSE)
    return()
}


## mwopk = mwo_pkernel_setup(prodcode = 'VERSIONLESS002')
## prodname_ms <- paste0(mwopk$prodname, '__', mwopk$prodcode)
## site_code <- mwopk$site_code
## component <- mwopk$components

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    header <- readxl::read_xlsx(rawfile, .name_repair = ~make.unique(.x, sep = "_var"))

    raw_xlsx <- readxl::read_xlsx(rawfile, skip = 1) %>%
      slice(2:nrow(.))

    colnames(raw_xlsx) <- colnames(header)
    raw_xlsx <- raw_xlsx[, !is.na(colnames(raw_xlsx))]

    d_old_names <- colnames(header)

    ## NOTE: special character was microgram symbol, removing all
    d_new_names <- unname(sapply(d_old_names, function(x) gsub('\\s*\\([^\\)]+\\)', "", x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub('_var1', '_varflag', x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub(' ', '_', x)))

    # NOTE: columns come in with utf character for microliter "u", replace?
    # NOTE: var and q columns have two-row header style, must fix

    # NOTE: duplicate column names for value and flag
    colnames(raw_xlsx) <- d_new_names

    # remove duplicate conducatcne (going form last keeps uS and removes mg/l conducatcne)
    d <- raw_xlsx[, !duplicated(colnames(raw_xlsx), fromLast = TRUE)]
    d <- d[,-1]

    d <- d %>%
      mutate(
          site_code = !!site_code
      )

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here to be just the old_var = new_var structure
    mwo_chem_cols = c()
    for(i in 1:length(mwo_vars)) {
      og_name <- names(mwo_vars[i])
      ms_name <- mwo_vars[[i]][3]
      mwo_chem_cols[og_name] = ms_name
    }

    d <- ms_read_raw_csv(preprocessed_tibble =  d,
                         datetime_cols = c('Date' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'America/Chicago',
                         site_code_col = 'site_code',
                         data_cols =  mwo_chem_cols,
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#_varflag'
                         )

    d <- ms_cast_and_reflag(d, #verify these work as expected (dirty + bdl). carries clean
                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            variable_flags_dirty   = c('~'),
                            # will turn the *ms_status* column to 2 (e.g. below detection limit)
                            variable_flags_bdl   = c('<'),
                            )
    # replace all BDL observations with half DL value
    d <- d %>%
      mutate(val = case_when(ms_status == 2 ~ val/2, TRUE ~ val))

    d <- d %>%
      mutate(ms_status = case_when(ms_status == 2 ~ 1, TRUE ~ ms_status))

    # apply uncertainty
    # NOTE; check_range breaks if multiple variable entries overlapping
    # NOTE; fix this, then get_hdetlim should also work normally
    ## d <- ms_check_range(d)
    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')


    d <- synchronize_timestep(d)

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here as var = old_units and var = new_units lists
    swwd_chem_units_old = c()
    swwd_chem_units_new = c()

    for(i in 1:length(mwo_vars)) {
      og_name <- names(mwo_vars[i])
      og_units <- mwo_vars[[i]][1]
      # pack ms untis
      ms_name <- mwo_vars[[i]][3]
      ms_units <-mwo_vars[[i]][2]
      # pack lists w units
      swwd_chem_units_old[ms_name] = og_units
      swwd_chem_units_new[ms_name] = ms_units
    }

    d <- ms_conversions(d,
                        convert_units_from = swwd_chem_units_old,
                        convert_units_to = swwd_chem_units_new)

    write_ms_file(d = d,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = site_code,
                  level = 'munged',
                  shapefile = FALSE)

    return()
}

#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux
