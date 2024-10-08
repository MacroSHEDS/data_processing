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

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- download_from_googledrive

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
              mutate(site_code = gsub('//.', '_', sheet_site))

          if(!exists('d_sheets_combined')) {
            d_sheets_combined <- this_d
          } else {
            d_sheets_combined <- rbind(d_sheets_combined, this_d)
          }
        }
    }

    d_sheets_combined <- d_sheets_combined %>%
      mutate(site_code = coalesce(unlist(mces_sitename_preferred)[site_code], site_code))

    # flag "low L" vars as BDL and ocnvert to normal?

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
                         datetime_cols = c('Date' = "%Y-%m-%d"),
                         datetime_tz = 'US/Central',
                         site_code_col = 'site_code',
                         data_cols =  c('discharge' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'quality_flag',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d, #verify these work as expected. written before updating ms_cast
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

    # read in csv and remove non-data header fluff
    d <- read.csv(rawfile)
    d <- d[5:nrow(d),]
    colnames(d) <- d[1,]
    d <- d[-1,]
    raw.d <- d

    # get all official site codes from site data gsheet
    mces_site_codes <- site_data %>%
      filter(network == !!network,
             domain == !!domain,
             ) %>%
      pull(site_code, full_name)

    # converting csv names (which are full text names) to site codes
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
        ## val = case_when(sign_varflag == '<' ~ paste0('<', val), TRUE ~ val)
      )

    # data provides units - lets make a quick function to pair each variable with
    # the units reported by data source (hopefully none have multiple)
    mces_var_units <- list()
    vars_og <- unique(d$var)
    vars <- names(mces_variable_info)

    # doing it this way as it is theoretically dynamic with changes in underlying data units
    # this pulls the units as reported in hte raw data, and compiles a list of variable names = unit
    for(var in vars) {
        d_var_units <- d %>%
          filter(var == !!var)
        if(nrow(d_var_units) > 0) {
            d_var_units <- d_var_units %>%
              pull(units) %>%
              unique()
        }
        # populate variable info objects with original unit data
        mces_var_units[var] = d_var_units
        mces_variable_info[var][[1]][1] = d_var_units
    }

    ## use variable info object to create structure specific to **ms_read_csv** data cols arg
    mces_data_cols <- c()
    for(i in 1:length(mces_variable_info)) {
      entry <- mces_variable_info[i]
      old_varname <- gsub(" |,","",names(entry))
      ms_varname <- entry[[1]][3]
      mces_data_cols[old_varname] = ms_varname
    }

    # remove comma and space from text, and
    # filter to only variables in mces_data_cols
    d <- d %>%
      mutate(
        var = gsub(" |,","", var)
      ) %>%
      filter(var %in% names(mces_data_cols))

    # NOTE: filtered vs unfiltered versions of most variables in dataset
    # we decided to keep F and UF seperate for total N and total P, and
    # otherwise lump F and UF data together, with filtered data taking precedence
    # in the event two samples overlap on the same site-date-var
    d <- d %>%
     pivot_wider(
                 id_cols = c(site_code, date),
                 values_from = c(val, quality_varflag),
                 names_from = var
                 ) %>%
     unchop(everything())

    # read this "preprocessed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('date' = "%m/%d/%Y %H:%M"),
                         datetime_tz = 'US/Central',
                         site_code_col = 'site_code',
                         data_cols =  mces_data_cols,
                         data_col_pattern = 'val_#V#',
                         ## summary_flagcols = 'quality_varflag',
                         var_flagcol_pattern = "quality_varflag_#V#",
                         ## convert_to_BDL_flag = "<#*#",
                         is_sensor = FALSE,
                         keep_bdl_values = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_clean   = c('quality_flag' = 'Valid'),
                            variable_flags_to_drop = c('quality_flag' = 'Estimated'),
                            variable_flags_dirty   = c('quality_flag' = 'Suspect'),
                            variable_flags_bdl = c('quality_flag' = 'BDL'))

    # apply uncertainty
    d <- ms_check_range(d)
    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
    d <- synchronize_timestep(d)

    ## create structure specific to **ms_conversions_** units_from and units_to args
    mces_data_conversions_from <- c()
    mces_data_conversions_to <- c()

    for(i in 1:length(mces_variable_info)) {
      entry <- mces_variable_info[i]
      ms_varname <- entry[[1]][3]
      old_units <- entry[[1]][1]
      ms_units <- entry[[1]][2]
      if(ms_varname %in% names(mces_data_conversions_from)) {
        next
      } else {
        mces_data_conversions_from[ms_varname] = old_units
        mces_data_conversions_to[ms_varname] = ms_units
      }
    }

    d <- ms_conversions_(d,
                          convert_units_from = mces_data_conversions_from,
                          convert_units_to = mces_data_conversions_to)

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

    mces_ws_shp <- sf::read_sf(rawfile)

    mces_site_codes <- site_data %>%
      filter(network == !!network,
             domain == !!domain,
             ) %>%
      pull(site_code)

    d_ws <- mces_ws_shp %>%
      filter(Monitored == "Y",
             MAP_CODE1 %in% mces_site_codes) %>%
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
#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms001 <- stream_gauge_from_site_data
#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux
