#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {
  prod <- 'discharge'
  site <- set_details$site_code
  component <- set_details$component
  prodname_ms <- set_details$prodname_ms

  # discharge data is 10-20 years of 15m sensor data provided as a straight xlsx file
  # the downloads are tricky, and often 504 timeout or other errors (inconsistently)
  # TODO fix this, ideas:
  #   - always can increase timeout
  #   - change mode to wb, or other mode
  #   - error out to gdrive download?
  #   - use different link, example: http://wq.swwdmn.org/downloads/new?opts=100th-st/gauge&site_id=100th-st&site_label=100th%20St
  #   - increase Sys.sleep() 'pause' between downloads, it does seem first downloads work more often

  default_to = getOption('timeout')
  options(timeout=1000000)

  # set interval to sleep after each download (seconds)
  sleep_time = 120


  # each file loaded into a site folder
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site,
                    c = component)

  site_download_url <- get_url_swwd_prod(site, prodname = prod)

  tm = Sys.time()
  wrn_msg <- glue::glue('{p}: downloading {s} in {n} {d}, time: {t}',
                        p = prodname_ms,
                        s = site,
                        n = network,
                        d = domain,
                        t = tm
                        )
  writeLines(wrn_msg)

  dl <- tryCatch(
    expr = {
      R.utils::downloadFile(
              url = site_download_url,
              filename = rawfile,
              skip = FALSE,
              overwrite = TRUE,
              method = 'libcurl')
    },
    error = function(e){
      tm = Sys.time()
      wrn_msg <- glue::glue('{file} not downloaded for {s} in {n} {d}, time: {t}. skipping to next site',
                            file = rawfile,
                            s = site,
                            n = network,
                            d = domain,
                            t = tm
                            )
      warning(wrn_msg)
    })

  if(inherits(dl, "error")) next

  options(timeout=default_to)

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
process_0_VERSIONLESS002 <- function(set_details, network, domain) {

  site <- set_details$site_code
  site <- set_details$site_code
  component <- set_details$component
  prodname_ms <- set_details$prodname_ms

  default_to = getOption('timeout')
  options(timeout=100000)

  # each file loaded into a site folder
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site,
                    c = component)

  site_download_url <- get_url_swwd_prod(site)

  tm = Sys.time()
  wrn_msg <- glue::glue('{p}: downloading {s} in {n} {d}, time: {t}',
                        p = prodname_ms,
                        s = site,
                        n = network,
                        d = domain,
                        t = tm
                        )
  writeLines(wrn_msg)

  dl <- tryCatch(
    expr = {
      R.utils::downloadFile(
              url = site_download_url,
              filename = rawfile,
              skip = FALSE,
              overwrite = TRUE,
              method = 'libcurl')
    },
    error = function(e){
      tm = Sys.time()
      wrn_msg <- glue::glue('{file} not downloaded for {s} in {n} {d}, time: {t}. skipping to next site',
                            file = rawfile,
                            s = site,
                            n = network,
                            d = domain,
                            t = tm
                            )
      warning(wrn_msg)
      next
    })

  if(inherits(dl, "error")) next

  options(timeout=default_to)
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

  return(deets_out)
}

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- download_from_googledrive

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    browser()

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/discharge.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    raw_xlsx <- readxl::read_xlsx(rawfile) %>%
      mutate(
        site_code = !!site_code
      ) %>%
      rename(
        discharge = 'Discharge (cfs)...8'
      )

    # hey! if this kernel is being run again, make sure to check the flag columns
    # in the original data, as there may be new flag info
    d <- ms_read_raw_csv(preprocessed_tibble = raw_xlsx,
                         datetime_cols = list('Date' = '%Y-%m-%d %H:%M:%S'),
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

    sites <- unique(d$site_code)

    d_site <- d %>%
        filter(site_code == !!sites[s])

    write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/stream_chemistry.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    header <- readxl::read_xlsx(rawfile, n_max = 1)
    raw_xlsx <- readxl::read_xlsx(rawfile, skip = 1) %>%
      slice(2:nrow(.)) %>%
      mutate(
        site_code = 'trout_brook'
      )

    # NOTE: duplicate column names for value and flag
    colnames(raw_xlsx) <- colnames(header)

    d <- ms_read_raw_csv(preprocessed_tibble =  raw_xlsx,
                         datetime_cols = list('Date' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'America/Chicago',
                         site_code_col = 'site_code',
                         data_cols =  c(pH='pH',
                                        `Specific conductance (mg/l)` = 'spCond',
                                        # NOTE: Phosphate?
                                        ## `Phosphorus as P (mg/l)` = 'P',
                                        `Nitrate (NO3) as N (mg/l)` = 'NO3_N'
                                        ),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#CODE',
                         summary_flagcols = c('TYPE'))

    d <- ms_cast_and_reflag(d,
                            variable_flags_to_drop = 'N',
                            variable_flags_dirty = c('*', 'Q', 'D*', 'C', 'D', 'DE',
                                                     'DQ', 'DC'),
                            variable_flags_clean =
                                c('A', 'E'),
                            summary_flags_to_drop = list(
                                TYPE = c('N', 'YE')),
                            summary_flags_dirty = list(
                                TYPE = c('C', 'S', 'A', 'P', 'B')
                            ),
                            summary_flags_clean = list(TYPE = c('QB', 'QS', 'QL',
                                                                'QA', 'F', 'G')))

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    unlink(temp_dir, recursive = TRUE)

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

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.shp',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    mwo_ws_shp <- sf::read_sf(rawfile)

    mwo_site_codes <- site_data %>%
      filter(network == !!network,
             domain == !!domain,
             ) %>%
      pull(site_code)

    d_ws <- mwo_ws_shp %>%
      filter(Monitored == "Y",
             MAP_CODE1 %in% mwo_site_codes) %>%
      select(
        site_code = MAP_CODE1,
        geometry
      ) %>%
      sf::st_transform(4326) %>%
      mutate(
        area = units::set_units(sf::st_area(.), "hectares") # meters (m) to hectares (ha)
      )

    for(i in 1:nrow(d_ws)) {
        wb <- d_ws[i,]
        site_code <- wb$site_code

        write_ms_file(d = wb,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = site_code,
                      level = 'munged',
                      shapefile = TRUE,
                      link_to_portal = FALSE)
    }
}

#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux
