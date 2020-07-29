#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_4341 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    fext <- ifelse(set_details$component %in% c('HF00401', 'HF00407'),
                   '.txt',
                   '.csv')

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  '.csv'),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#precipitation; precip_gauge_locations: STATUS=READY
#. handle_errors
process_0_5482 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    fext <- ifelse(set_details$component == 'MS00404',
                   '.txt',
                   '.csv')

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precipitation'){

        if(! set_details$component %in% c('MS00402', 'MS00403', 'MS00404')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_gauge_locations'){

        if(! set_details$component == 'MS00401'){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  fext),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#stream_chemistry; stream_flux_inst: STATUS=READY
#. handle_errors
process_0_4021 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_chemistry'){

        if(! set_details$component %in% c('CF00201', 'CF00202', 'CF00203', 'CF00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_flux_inst'){

        if(! set_details$component %in% c('CF00204', 'CF00205', 'CF00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  '.csv'),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#precip_chemistry; precip_flux_inst: STATUS=READY
#. handle_errors
process_0_4022 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_chemistry'){

        if(! set_details$component %in% c('CP00201', 'CP00203', 'CP00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_flux_inst'){

        if(! set_details$component %in% c('CP00202', 'CP00206')){
            loginfo('Skipping redundant download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  '.csv'),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#stream_gauge_locations; ws_boundary: STATUS=READY
#. handle_errors
process_0_3239 <- function(set_details, network, domain) {

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'ws_boundary'){

        if(! set_details$component %in% c('hf01402', 'hf01404')){
            loginfo('Skipping redundant or unneeded download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_gauge_locations'){

        if(! set_details$component == 'hf01403'){
            loginfo('Skipping redundant or unneeded download',
                    logger = logger_module)
            return(generate_blacklist_indicator())
        }
    }

    rawfile=glue(raw_data_dest,
                 '/',
                 set_details$component)
    download.file(url = set_details$url,
                  destfile = rawfile,
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#munge kernels ####

#discharge: STATUS=PENDING (just copied from hbef. see comment within)
#. handle_errors
process_1_4341 <- function(network, domain, prodname_ms, site_name,
                           components){

    #discharge for hj comes from two sources, lter portal and
    #http://andlter.forestry.oregonstate.edu/data/register/default.aspx?datapage=page2_static_ascii&enid=1519&entnum=5&dbcode=HF004

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.txt',
                   n=network,
                   d=domain,
                   p=prodname_ms,
                   s=site_name,
                   c=component)
    read_csv(rawfile)

    d = sw(read_csv(rawfile, progress=FALSE,
                    col_types=readr::cols_only(
                        DATETIME='c', #can't parse 24:00
                        WS='c',
                        Discharge_ls='d'))) %>%
        # Flag='c'))) %>% #all flags are acceptable for this product
        rename(site_name = WS,
               datetime = DATETIME,
               discharge = Discharge_ls) %>%
        mutate(
            datetime = with_tz(force_tz(as.POSIXct(datetime), 'US/Eastern'), 'UTC'),
            # datetime = with_tz(as_datetime(datetime, 'US/Eastern'), 'UTC'),
            site_name = paste0('w', site_name),
            ms_status = 0) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                  any_vars(! is.na(.))) %>%
        group_by(datetime, site_name) %>%
        summarize(
            discharge = mean(discharge, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup()

    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = '15 min',
                                 impute_limit = 30))

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_5482 <- function(network, domain, prodname_ms, site_name,
                         components){

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n=network,
                    d=domain,
                    p=prodname_ms,
                    s=site_name,
                    c=components[2])

    d = sw(read_csv(rawfile1, progress=FALSE,
                    col_types=readr::cols_only(
                        DATE = 'D',
                        SITECODE = 'c',
                        # PRECIP_METHOD = 'c', #method information
                        # QC_LEVEL = 'c', #derived, gapfilled, etc
                        PRECIP_TOT_DAY = 'd',
                        PRECIP_TOT_FLAG = 'c',
                        EVENT_CODE = 'c')))

    d = ue(sourceflags_to_ms_status(d,
                                    flagstatus_mappings = list(
                                        PRECIP_TOT_FLAG = c('A', 'E'),
                                        EVENT_CODE = NA)))

    d <- d %>%
        rename(datetime = DATE,
               site_name = SITECODE,
               precip = PRECIP_TOT_DAY) %>%
        mutate(datetime = lubridate::ymd(datetime, tz = 'UTC')) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                  any_vars(! is.na(.))) %>%
        group_by(datetime, site_name) %>%
        summarize(
            precip = mean(precip, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup()

    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = '1 day',
                                 impute_limit = 30))
}

#derive kernels ####
