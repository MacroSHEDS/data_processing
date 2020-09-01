#stuff to add to all templates: ####

#this, commented.
# discharge = case_when(
#     length(discharge) > 1 ~ mean(discharge, na.rm=TRUE),
#     TRUE ~ discharge),

# handling of detection limits, uncertainty, grab vs sensor vs spatial, time
#zone, etc also note: datetime and site_name mutations must happen before
#identification of detlims (process_1_1).
# any other initial mutuations should happen after

#anything new from process_1_4341, including carry_uncertainty

#the one case where we had to build a shapefile from a tibble (hj precip gauges?)

#non_spatial munge kernel ####

#product: STATUS=PENDING
#. handle_errors
process_1_XXX <- function(network, domain, prodname_ms, site_name,
                          component){
                          # components){


    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv', #rawfile1
    # rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
                   # c = components[X])

    # read_csv(rawfile)
    d = sw(read_csv(rawfile, #rawfile1
                    progress = FALSE,
                    col_types = readr::cols_only(
                        DATETIME = 'c', #manage timezones later
                        WS = 'c',
                        # PRECIP_METHOD = 'c', #method information
                        # QC_LEVEL = 'c', #derived, gapfilled, etc
                        Discharge_ls = 'd')))

    d = ue(sourceflags_to_ms_status(d,
                                    flagstatus_mappings = list(
                                        PRECIP_TOT_FLAG = c('A', 'E'),
                                        EVENT_CODE = NA)))
    d <- d %>%
        # Flag = 'c'))) %>% #all flags are acceptable for this product
        rename(site_name = WS,
               datetime = DATETIME,
               discharge = Discharge_ls) %>%
        # rename_all(dplyr::recode, #essentially rename_if_exists
        #            precipCatch = 'precipitation_ns',
        #            flowGageHt = 'discharge_ns') %>%
        mutate(
            #if timezone doesn't change with DST, specify it as a GMT offset:
            datetime = with_tz(as_datetime(d$datetime[1],
                                          tz = 'Etc/GMT-8'),
                               tz = 'UTC'),
            #if it does observe DST, use something like this:
            # datetime = with_tz(as_datetime(d$datetime[1], 'US/Eastern'), 'UTC'),
            #if just days:
            mutate(datetime = lubridate::ymd(datetime, tz = 'UTC')) %>%
            site_name = paste0('w', site_name),
            # ms_status = 0) %>% #only if you don't need sourceflags_to_ms_status
            # ms_status = ifelse(is.na(fieldCode), FALSE, TRUE), #same
            # ms_status = as.logical(ms_status)) %>% #if you're summarizing_all
            DIC = ue(convert_unit(DIC, 'uM', 'mM')),
            NH4_N = ue(convert_molecule(NH4, 'NH4', 'N')),
            NO3_N = ue(convert_molecule(NO3, 'NO3', 'N')),
            PO4_P = ue(convert_molecule(PO4, 'PO4', 'P'))) %>%
        select(-date, -timeEST, -PO4, -NH4, -NO3, -fieldCode) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                  any_vars(! is.na(.))) %>% #probs redund with synchronize_timestep, but whatevs
        group_by(datetime, site_name) %>% #remove dupes
        summarize(
            discharge = mean(discharge, na.rm = TRUE),
            ms_status = numeric_any(ms_status)) %>%
        # summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        # mutate(ms_status = as.numeric(ms_status)) %>% #only if you had to convert it to logical
        # select(-ms_status, everything()) #old way
        relocate(ms_status, .after = last_col()) %>% #new way
        relocate(ms_interp, .after = last_col()) %>% #sometimes gotta call twice
        arrange(site_name, datetime)

    d[is.na(d)] = NA #replaces NaNs. is there a clean, pipey way to do this?

    # #variable interval
    # intv <- ifelse(grepl('precip', prodname_ms),
    #                '1 day',
    #                '1 hour')
    # d <- ue(synchronize_timestep(ms_df = d,
    #                              desired_interval = intv,
    #                              impute_limit = 30))

    #constant interval
    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = '15 min',
                                 impute_limit = 30))

    return(d)
}


#spatial munge kernel ####

#ws_boundary; stream_gauge_locations: STATUS=READY
#. handle_errors
process_1_3239 <- function(network, domain, prodname_ms, site_name,
                           components){

    component <- ifelse(prodname_ms == 'stream_gauge_locations__3239',
                        'hf01403',
                        'hf01402')

    rawdir1 = glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name)
    rawfile1 <- glue(rawdir1, '/', component)

    zipped_files <- unzip(zipfile = rawfile1,
                          exdir = rawdir1,
                          overwrite = TRUE)

    projstring <- ue(choose_projection(unprojected = TRUE))

    if(prodname_ms == 'stream_gauge_locations__3239'){

        d <- sf::st_read(rawdir1,
                         stringsAsFactors = FALSE,
                         quiet = TRUE) %>%
            select(site_name = SITECODE,
                   geometry = geometry) %>%
            sf::st_transform(projstring) %>%
            arrange(site_name) %>%
            sf::st_zm(drop = TRUE,
                      what = 'ZM')

    } else {

        d <- sf::st_read(rawdir1,
                         stringsAsFactors = FALSE,
                         quiet = TRUE) %>%
            select(site_name = WS_,
                   area = F_AREA,
                   geometry = geometry) %>%
            filter(! grepl('^[0-9][0-9]?a$', site_name)) %>% #remove areas below station
            mutate(  #for consistency with name elsewhere
                site_name = stringr::str_pad(site_name,
                                             width = 2,
                                             pad = '0'),
                site_name = paste0('GSWS', site_name),
                site_name = ifelse(site_name == 'GSWSMACK',
                                   'GSMACK',
                                   site_name),
                site_name = ifelse(site_name == 'GSWS04',
                                   'GSLOOK',
                                   site_name)) %>%
            sf::st_transform(projstring) %>%
            arrange(site_name)
    }

    unlink(zipped_files)

    return(d)
}

#spatial munge kernel starting from non-spatial data ####

#precipitation; precip_gauge_locations: STATUS=READY
#. handle_errors
process_1_5482 <- function(network, domain, prodname_ms, site_name,
                           components){

    component <- ifelse(prodname_ms == 'precip_gauge_locations__5482',
                        'MS00401',
                        'MS00403')

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n=network,
                    d=domain,
                    p=prodname_ms,
                    s=site_name,
                    c=component)

    if(prodname_ms == 'precip_gauge_locations__5482'){

        projstring <- ue(choose_projection(unprojected = TRUE))

        d <- sw(read_csv(rawfile1, progress=FALSE,
                         col_types = readr::cols_only(
                             SITECODE = 'c',
                             LATITUDE = 'd',
                             LONGITUDE = 'd'))) %>%
            rename(site_name = SITECODE)

        sp::coordinates(d) <- ~LONGITUDE+LATITUDE
        d <- sf::st_as_sf(d)
        sf::st_crs(d) <- projstring #assuming. geodetic datum not given by lter

    } else {

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

    return(d)
}
