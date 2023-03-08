## #retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- download_from_googledrive

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain) {
    # query built on MCES portal, this by WS 20230307
    mces_query <- "https://eims.metc.state.mn.us/Download?startDate=01-01-0001&endDate=12-31-9999&siteIds=FC0002;EA0008;SA0082;VA0010;RI0013;VR0020;BA0022;BS0019;BR0003;SI0001;CR0009;CA0017;BL0035;BE0020;NM0018;PU0039&excludeParameterIds=2191;2196;2194;2273;2197;2208;2093;2147;2199;2236;2251;2253;2255;2257;2259;2261;2274;2263;2265;2266;2268;2270&format=csv"

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    R.utils::downloadFile(
             ## url = "http://eims.metc.state.mn.us/Download?startDate=01-01-0001&endDate=12-31-9999&siteIds=FC0002;EA0008;SA0082;VA0010;RI0013;VR0020;BA0022;BS0019;BR0003;SI0001;CR0009;CA0017;BL0035;BE0020;NM0018;PU0039&excludeParameterIds=2191;2196;2194;2273;2197;2208;2093;2147;2199;2236;2251;2253;2255;2257;2259;2261;2274;2263;2265;2266;2268;2270&format=csv",
             url = set_details$url,
             filename = rawfile,
             skip = FALSE,
             overwrite = TRUE,
             method = 'libcurl')


    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, ''),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

## #munge kernels ####
#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d_sheets <- readxl::excel_sheets(rawfile)

    for(sheet in d_sheets) {
        this_d <- readxl::read_xlsx(rawfile, sheet = sheet)

        if(any(grepl('Discharge', names(this_d)))) {
          sheet_site_first_name <- strsplit(stringr::str_trim(as.character(noquote(sheet))), ' ')[[1]][1]

          sheet_site <- site_data %>%
            filter(network == !!network,
                   domain == !!domain,
                   grepl(sheet_site_first_name, full_name)
                   ) %>%
            pull(site_code)

          this_d <- this_d %>%
              mutate(site_code = sheet_site)

          if(!exists('d_sheets_combined')) {
            d_sheets_combined <- this_d
          } else {
            d_sheets_combined <- rbind(d_sheets_combined, this_d)
          }
        }
    }

    q_lps = d_sheets_combined %>%
      select(matches('Discharge')) * 28
    colnames(q_lps) <- 'discharge'

    d <- d_sheets_combined %>%
      select(-matches('Discharge')) %>%
      cbind(q_lps) %>%
      mutate(
        quality_flag = case_when(grepl('Valid', Quality) ~ 'Valid',
                                 grepl('Suspect', Quality) ~ 'Suspect',
                                 grepl('Estimated', Quality) ~ 'Estimated')
      )

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = "%Y-%m-%d"),
                         datetime_tz = 'US/Central',
                         site_code_col = 'site_code',
                         data_cols =  c('discharge' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'quality_flag',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_clean   = c('quality_flag' = 'Valid'),
                            summary_flags_to_drop = c('quality_flag' = 'Estimated'),
                            summary_flags_dirty   = c('quality_flag' = 'Suspect'),
                            varflag_col_pattern = NA
                            )

    # apply uncertainty
    d <- ms_check_range(d)
    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')


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

    unlink(temp_dir, recursive = TRUE)

    return()



}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile)
    d <- d[5:nrow(d),]
    colnames(d) <- d[1,]
    d <- d[-1,]
    raw.d <- d

    mces_site_codes <- site_data %>%
      filter(network == !!network,
             domain == !!domain,
             ) %>%
      pull(site_code, full_name)

    d <- d %>%
      filter(nchar(NAME) > 1) %>%
      mutate(
        site_code = lapply(NAME, FUN = mces_site_lookup)
        ) %>%
      select(
        site_code,
        date = END_DATE_TIME,
        var = PARAMETER,
        sign_varflag = SIGN,
        val = RESULT,
        units = UNITS,
        quality_varflag = QUALIFIER
      ) %>%
      mutate(
        quality_varflag = case_when(sign_varflag == '<' ~ 'BDL', TRUE ~ quality_varflag)
      )
    head(d)

    # data provides units - lets make a quick function to pair each variable with
    # the units reported by data source (hopefully none have multiple)
    mces_var_units <- list()
    for(var in unique(d$var)) {
      d_var_units <- d %>%
        filter(var == !!var) %>%
        pull(units) %>%
        unique()
      mces_var_units[var] = d_var_units
      mces_variable_info[var][[1]][1] = d_var_units
    }

    ## mces_variable_info[var][[1]][1]
    ## mces_variable_info


    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d.m <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = "%m/%d/%Y %H:%M"),
                         datetime_tz = 'US/Central',
                         site_code_col = 'site_code',
                         data_cols =  c('discharge' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'quality_flag',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_clean   = c('quality_flag' = 'Valid'),
                            summary_flags_to_drop = c('quality_flag' = 'Estimated'),
                            summary_flags_dirty   = c('quality_flag' = 'Suspect'),
                            varflag_col_pattern = NA
                            )

    # apply uncertainty
    d <- ms_check_range(d)
    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')


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

    unlink(temp_dir, recursive = TRUE)

    return()




}

#derive kernels ####
#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- stream_gauge_from_site_data
