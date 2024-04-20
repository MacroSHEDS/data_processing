#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_4341 <- function(set_details, network, domain){

    if(! grepl('^Daily', set_details$component)){
        loginfo('This component is blocklisted', logger = logger_module)
        return(generate_blocklist_indicator())
    }

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_code)

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
                  extra = "-C -", #resume download if it gets interrupted
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
                         s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    fext <- ifelse(set_details$component == 'MS00404',
                   '.txt',
                   '.csv')

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precipitation'){

        if(! set_details$component %in% c('MS00402', 'MS00403', 'MS00404')){
            logwarn('Skipping redundant download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_gauge_locations'){

        if(! set_details$component == 'MS00401'){
            logwarn('Skipping redundant download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
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

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_4021 <- function(set_details, network, domain){

    #Stream flux (stream_flux_inst__4021) was removed becuase we are not wanting
    #use theirs as of now and was causing error in ms_derive

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_chemistry'){

        if(! set_details$component %in% c('CF00201', 'CF00202', 'CF00203', 'CF00206')){
            logwarn('Skipping redundant download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_flux_inst'){

        if(! set_details$component %in% c('CF00204', 'CF00205', 'CF00206')){
            logwarn('Skipping redundant download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
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

    #precip_flux_inst__4022 was removed becuase we make our own and it was
    #causing issues for ms_derive

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_chemistry'){

        if(! set_details$component %in% c('CP00201', 'CP00203', 'CP00206')){
            logwarn('Skipping redundant download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'precip_flux_inst'){

        if(! set_details$component %in% c('CP00202', 'CP00206')){
            logwarn('Skipping redundant download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
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
                         s = set_details$site_code)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    if(prodname_from_prodname_ms(set_details$prodname_ms) == 'ws_boundary'){

        if(! set_details$component %in% c('hf01402', 'hf01404')){
            logwarn('Skipping redundant or unneeded download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
        }

    } else if(prodname_from_prodname_ms(set_details$prodname_ms) == 'stream_gauge_locations'){

        if(! set_details$component == 'hf01403'){
            logwarn('Skipping redundant or unneeded download',
                    logger = logger_module)
            return(generate_blocklist_indicator())
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

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_4020 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    fext <- ifelse(set_details$component %in% c('HT00441'),
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

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_4341 <- function(network, domain, prodname_ms, site_code, components){

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/Daily streamflow summaries.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    #look carefully at warnings from ms_read_raw_csv.
    #they may indicate insufficiencies
    d <- ms_read_raw_csv(filepath = rawfile1,
                         datetime_cols = c(DATE = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(MEAN_Q = 'discharge'),
                         data_col_pattern = '#V#',
                         alt_site_code = list('GSMACK' = 'GSWSMA'),
                         is_sensor = TRUE,
                         set_to_NA = '',
                         summary_flagcols = 'ESTCODE')

    # In 1995 HJ Andrews put a fish ladder around the GSWSMA (it is also gauged
    # called GSWSMF). A new measurment called GSWSMC is a sum of GSWSMF and
    # GSWSMA. We are calling GSWSMA, GSMACK and when the fish ladder is added
    # GSMACK will refer to GSWSMC

    # It as looks like at 	2018-09-30 16:05:00, GSWSMC is just GSWSMA X 2. May
    # be worth contacting the domain
    GSMACK <- d %>%
        filter(site_code == 'GSMACK') %>%
        filter(datetime < '1995-09-30 22:00:00')

    GSWSMC <- d %>%
        filter(site_code == 'GSWSMC') %>%
        filter(datetime >= '1995-09-30 22:00:00') %>%
        mutate(site_code = 'GSMACK')

    d <- filter(d, ! site_code %in% c('GSWSMA', 'GSWSMC', 'GSWSMF', 'GSMACK'))

    d <- rbind(d, GSWSMC, GSMACK)

    d <- ms_cast_and_reflag(
        d,
        varflag_col_pattern = NA,
        summary_flags_clean = list(ESTCODE = c('A', 'E')),
                                   # EVENT_CODE = c(NA, 'WEATHR')),
        summary_flags_to_drop = list(ESTCODE = 'sentinel')
        # summary_flags_dirty = list(ESTCODE = c('Q', 'S', 'P'))
                                   # EVENT_CODE = c('INSREM', 'MAINTE'))
    )

    #convert cfs to liters/s
    d <- mutate(d, val = val * 28.317)

    return(d)
}

#precipitation; precip_gauge_locations: STATUS=READY
#. handle_errors
process_1_5482 <- function(network, domain, prodname_ms, site_code, components){

    component <- ifelse(prodname_ms == 'precip_gauge_locations__5482',
                        'MS00401',
                        'MS00403')

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    if(prodname_ms == 'precip_gauge_locations__5482'){

        projstring <- choose_projection(unprojected = TRUE)

        d <- sw(read_csv(rawfile1, progress=FALSE,
                         col_types = readr::cols_only(
                             SITECODE = 'c',
                             LATITUDE = 'd',
                             LONGITUDE = 'd'))) %>%
             rename(site_code = SITECODE)

        sp::coordinates(d) <- ~LONGITUDE+LATITUDE
        d <- sf::st_as_sf(d)
        sf::st_crs(d) <- projstring #assuming. geodetic datum not given by lter

    } else if(prodname_ms == 'precipitation__5482'){

        d <- ms_read_raw_csv(filepath = rawfile1,
                             datetime_cols = c(DATE = '%Y-%m-%d'),
                             datetime_tz = 'UTC',
                             site_code_col = 'SITECODE',
                             data_cols =  c(PRECIP_TOT_DAY = 'precipitation'),
                             data_col_pattern = '#V#',
                             is_sensor = FALSE,
                             set_to_NA = '',
                             summary_flagcols = c('PRECIP_TOT_FLAG',
                                                  'EVENT_CODE'))

        d <- ms_cast_and_reflag(
            d,
            varflag_col_pattern = NA,
            summary_flags_clean = list(PRECIP_TOT_FLAG = c('A', 'E'),
                                       EVENT_CODE = c(NA, 'METHOD')),
            #METHOD indicates when methods change.
            summary_flags_dirty = list(PRECIP_TOT_FLAG = c('Q', 'C', 'U'),
                                       EVENT_CODE = c('INSREM', 'MAINTE'))
        )
    }

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_4021 <- function(network, domain, prodname_ms, site_code, components){

    #note: blocklisting of components has been superseded by the "component"
    #   column in products.csv. don't copy this chunk.
    if(grepl('chemistry', prodname_ms)){
        component <- 'CF00201'
    } else {
        logwarn('Blacklisting stream flux product CF00204 (for now)')
        return(generate_blocklist_indicator())
    }

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile1, colClasses = 'character') %>%
        rename(NA.CODE = NACODE)

    #look carefully at warnings from ms_read_raw_csv.
    #they may indicate insufficiencies
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(PH='pH', COND='spCond', ALK='alk',
                             SSED='suspSed', SI='Si', PARTP='TPP', PO4P='PO4_P',
                             PARTN='TPN', NH3N='NH3_N', NO3N='NO3_N', CA='Ca',
                             MG='Mg', SO4S='SO4_S', CL='Cl', ANCA='AnCaR',
                             NA.='Na', UTP='TP', 'TDP', UTN='TN', 'TDN', 'DON',
                             UTKN='TKN', TKN='TDKN', 'K', 'DOC'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#CODE',
                         summary_flagcols = c('TYPE'))

    d <- ms_cast_and_reflag(
        d,
        variable_flags_bdl = '*',
        variable_flags_to_drop = 'N',
        variable_flags_dirty = c('Q', 'D*', 'C', 'D', 'DE', 'DQ', 'DC'),
        variable_flags_clean = c('A', 'E'),
        summary_flags_to_drop = list(TYPE = c('N', 'YE')),
        summary_flags_dirty = list(TYPE = c('C', 'S', 'A', 'P', 'B')),
        summary_flags_clean = list(TYPE = c('QB', 'QS', 'QL', 'QA', 'F', 'G'))
    )

    return(d)
}

#precip_chemistry; precip_flux_inst: STATUS=READY
#. handle_errors
process_1_4022 <- function(network, domain, prodname_ms, site_code, components){

    #note: blocklisting of components has been superseded by the "component"
    #   column in products.csv. don't copy this chunk.
    if(grepl('chemistry', prodname_ms)){
        component <- 'CP00201'
    } else {
        logwarn('Blacklisting precip flux product CP00202 (for now)')
        return(generate_blocklist_indicator())
    }

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile1, colClasses = 'character') %>%
        rename(NA.CODE = NACODE)

    #huge list of sites and locations in the first data file here:
    #https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-and&identifier=5482&revision=3
    # but RCADMN, RCHI15, RCHIF7, RCHIR7 not listedRCADMN, RCHI15, RCHIF7, RCHIR7
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(PH='pH', COND='spCond', ALK='alk',
                                        SSED='suspSed', SI='Si', PARTP='TPP', PO4P='PO4_P',
                                        PARTN='TPN', NH3N='NH3_N', NO3N='NO3_N', CA='Ca',
                                        MG='Mg', SO4S='SO4_S', CL='Cl', ANCA='AnCaR',
                                        NA.='Na', UTP='TP', 'TDP', UTN='TN', 'TDN', 'DON',
                                        UTKN='TKN', TKN='TDKN', 'K', 'DOC'),
                                        # PRECIP_CM='precip_ns'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         # alt_datacol_pattern = '#V#_INPUT',
                         set_to_NA = '',
                         var_flagcol_pattern = '#V#CODE',
                         summary_flagcols = c('TYPE'))

    d <- ms_cast_and_reflag(
        d,
        variable_flags_bdl = '*',
        variable_flags_to_drop = 'N',
        variable_flags_dirty = c('Q', 'D*', 'C', 'D', 'DE', 'DQ', 'DC'),
        variable_flags_clean = c('A', 'E'),
        summary_flags_to_drop = list(TYPE = c('N', 'YE')),
        summary_flags_dirty = list(TYPE = c('C', 'S', 'A', 'P', 'B')),
        summary_flags_clean = list(TYPE = c('QB', 'QS', 'QL', 'QA', 'F', 'G'))
    )

    #HJAndrews does not collect precip and precip chemistry at the same
    #locations, so we here crudely localize pchem data to the nearest precip
    #gauges. we could do this more gracefully with idw.
    d <- d %>%
        mutate(site_code = case_when(
            site_code == 'RCADMN' ~ 'PRIMET',
            grepl('^RCHI..$', site_code, perl = TRUE) ~ 'CENMET',
            TRUE ~ '_ERR')) #may be tripped if they add a new dry dep gauge

    if(any(d$site_code == '_ERR')){
        stop(glue('hjandrews has added a new pchem gauge that we havent mapped',
                  ' to a location'))
    }

    return(d)
}

#ws_boundary; stream_gauge_locations: STATUS=READY
#. handle_errors
process_1_3239 <- function(network, domain, prodname_ms, site_code, components){

    component <- ifelse(prodname_ms == 'stream_gauge_locations__3239',
                        'hf01403',
                        'hf01402')

    rawdir1 = glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)
    rawfile1 <- glue(rawdir1, '/', component)

    zipped_files <- unzip(zipfile = rawfile1,
                          exdir = rawdir1,
                          overwrite = TRUE)

    projstring <- choose_projection(unprojected = TRUE)

    if(prodname_ms == 'stream_gauge_locations__3239'){

        d <- sf::st_read(rawdir1,
                         stringsAsFactors = FALSE,
                         quiet = TRUE) %>%
            select(site_code = SITECODE,
                   geometry = geometry) %>%
            sf::st_transform(projstring) %>%
            arrange(site_code) %>%
            sf::st_zm(drop = TRUE,
                      what = 'ZM')

    } else {

        d <- sf::st_read(rawdir1,
                         stringsAsFactors = FALSE,
                         quiet = TRUE) %>%
            select(site_code = WS_,
                   area = F_AREA,
                   geometry = geometry) %>%
            filter(! grepl('^[0-9][0-9]?a$', site_code)) %>% #remove areas below station
            mutate(  #for consistency with name elsewhere
                site_code = stringr::str_pad(site_code,
                                             width = 2,
                                             pad = '0'),
                site_code = paste0('GSWS', site_code),
                site_code = ifelse(site_code == 'GSWSMACK',
                                   'GSMACK',
                                   site_code)) %>%
                #GSWS04 is GSLOOK, but that shapefile is missing subwatersheds,
                #   so we're going to drop it here and delineate it ourselves
                # site_code = ifelse(site_code == 'GSWS04',
                #                    'GSLOOK',
                #                    site_code)) %>%
            filter(site_code != 'GSWS04') %>%
            sf::st_transform(projstring) %>%
            arrange(site_code)
    }

    unlink(zipped_files)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_4020 <- function(network, domain, prodname_ms, site_code, components){


    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/HT00441.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    d <- ms_read_raw_csv(filepath = rawfile1,
                         datetime_cols = c(DATE = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(WATERTEMP_MEAN_DAY = 'temp'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         set_to_NA = '',
                         summary_flagcols = c('WATERTEMP_MEAN_FLAG'))

    d <- ms_cast_and_reflag(
        d,
        varflag_col_pattern = NA,
        summary_flags_clean = list(WATERTEMP_MEAN_FLAG = c('A', 'E')),
        summary_flags_dirty = list(WATERTEMP_MEAN_FLAG = c('B', 'M', 'S', 'Q'))
    )

    rawfile2 = glue('data/{n}/{d}/raw/{p}/{s}/HT00451.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)


    d_ <- ms_read_raw_csv(filepath = rawfile2,
                         datetime_cols = c(DATE_TIME = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-8',
                         site_code_col = 'SITECODE',
                         data_cols =  c(WATERTEMP_MEAN = 'temp'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         summary_flagcols = c('WATERTEMP_MEAN_FLAG'))

    d_ <- ms_cast_and_reflag(d_,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(
                                WATERTEMP_MEAN_FLAG = c('A', 'E')),
                            summary_flags_dirty = list(
                                WATERTEMP_MEAN_FLAG = c('B', 'M', 'S', 'Q')))

    d__ <- d_ %>%
        mutate(day = day(datetime),
               month = month(datetime),
               year = year(datetime)) %>%
        group_by(site_code, day, month, year, var) %>%
        summarize(val = mean(val, na.rm = T),
                  ms_status = max(ms_status)) %>%
        ungroup() %>%
        mutate(datetime = ymd(paste(year, month, day, sep = '-'))) %>%
        select(site_code, datetime, var, val, ms_status)

    #Join 2 datasets by removing daily averages when sensor data is available
    # start_dates_daily <- d %>%
    #     group_by(site_code) %>%
    #     summarise(d_min = min(datetime, na.rm = T),
    #               d_max = max(datetime, na.rm = T))
    #
    # start_dates_sub <- d_ %>%
    #     group_by(site_code) %>%
    #     summarise(min = min(datetime, na.rm = T),
    #               max = max(datetime, na.rm = T))
    #
    # check <- full_join(start_dates_sub, start_dates_daily, by = 'site_code') %>%
    #     mutate(alter_data = ifelse(min <= d_min, 1, 0)) %>%
    #     filter(alter_data == 1 | is.na(alter_data))

    d <- rbind(d, d__)

    d <- d %>%
        group_by(datetime, site_code, var) %>%
        summarise(val = mean(val, na.rm = T),
                  ms_status = max(ms_status, na.rm = T))

    return(d)
}


#derive kernels ####

# #precipitation: STATUS=PAUSED
# #. handle_errors
# process_2_ms001 <- derive_precip
#
# #precip_chemistry: STATUS=PAUSED
# #. handle_errors
# process_2_ms002 <- derive_precip_chem
#
# #precip_flux_inst: STATUS=PAUSED
# #. handle_errors
# process_2_ms004 <- derive_precip_flux

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__4020',
                                           'stream_chemistry__4021'))
    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_precip_pchem_pflux
