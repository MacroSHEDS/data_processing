#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_1 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_code)

    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_13 <- function(set_details, network, domain){

    raw_data_dest <- glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                          wd = getwd(),
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    download.file(url = set_details$url,
                  destfile = glue(raw_data_dest,
                                  '/',
                                  set_details$component,
                                  '.csv'),
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#stream_chemistry; precip_chemistry: STATUS=READY
#. handle_errors
process_0_208 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_code)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    if(set_details$component == 'Analytical Methods'){
        fext <- '.pdf'
    } else {
        l1 <- all(grepl('precip', c(set_details$component,  prodname_ms)))
        l2 <- all(grepl('stream', c(set_details$component,  prodname_ms)))

        if(! sum(l1, l2)){
            loginfo('Skipping redundant download', logger=logger_module)
            return(generate_blocklist_indicator())
        }

        fext <- '.csv'
    }

    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component, fext),
        cacheOK=FALSE, method='curl')

    return()
}

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_0_107 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_code)

    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

    return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_0_100 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_code)

    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

    return()
}

#ws_boundary: STATUS=READY
#. handle_errors
process_0_94 <- function(set_details, network, domain) {

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_code)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    rawfile=glue(raw_data_dest, '/', set_details$component)
    download.file(url = set_details$url,
                  destfile = rawfile,
                  cacheOK = FALSE,
                  method = 'curl')

    return()
}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_1 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = c(DATETIME = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'Etc/GMT-5', #EST
                         site_code_col = 'WS',
                         alt_site_code = list('w1' = c('1', 'W1'),
                                              'w2' = c('2', 'W2'),
                                              'w3' = c('3', 'W3'),
                                              'w4' = c('4', 'W4'),
                                              'w5' = c('5', 'W5'),
                                              'w6' = c('6', 'W6'),
                                              'w7' = c('7', 'W7'),
                                              'w8' = c('8', 'W8'),
                                              'w9' = c('9', 'W9')),
                         data_cols = c(Discharge_ls = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_13 <- function(network, domain, prodname_ms, site_code, component){

    if(component == 'site info'){
        loginfo('Blacklisting superfluous data component')
        return(generate_blocklist_indicator())
    }

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # SAMPLE: Sensor (also manual. Use a mix of automatic gauges and standard guages)
    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = c(DATE = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT-5',
                         site_code_col = 'rainGage',
                         data_cols = c(Precip = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    return(d)
}

#stream_chemistry; precip_chemistry: STATUS=READY
#. handle_errors
process_1_208 <- function(network, domain, prodname_ms, site_code, component){

    if(component == 'Analytical Methods'){
        loginfo('Blacklisting superfluous data component')
        return(generate_blocklist_indicator())
    }

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')

    #one day we may track methods more closely, but for now (20240412) hbef
    #is the only domain distinguishing among pH and ANC methods, so consolidate
    d <- d %>%
        mutate(ANC = if_else(is.na(ANCMet), ANC960, ANCMet),
               pH = if_else(is.na(pHmetrohm), pH, pHmetrohm)) %>%
        select(-ANCMet, -ANC960, -pHmetrohm) %>%
        mutate(fieldCode = if_else(grepl('969|955|970|905', fieldCode),
                                   '969', #sometimes two codes are combined. consolidate here
                                   fieldCode))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c(date = '%Y-%m-%d',
                                           timeEST = '%H:%M'),
                         datetime_tz = 'Etc/GMT-5',
                         site_code_col = 'site',
                         alt_site_code = list('w1' = c('1', 'W1'),
                                              'w2' = c('2', 'W2'),
                                              'w3' = c('3', 'W3'),
                                              'w4' = c('4', 'W4'),
                                              'w5' = c('5', 'W5'),
                                              'w6' = c('6', 'W6'),
                                              'w7' = c('7', 'W7'),
                                              'w8' = c('8', 'W8'),
                                              'w9' = c('9', 'W9')),
                         data_cols = c('pH', 'DIC', 'spCond', 'temp',
                                       'ANC', 'Ca', 'Mg', 'K',
                                       'Na', 'TMAl', 'OMAl', Al_ICP = 'TAl', 'Al_ferron',
                                       'NH4', 'SO4', 'NO3', 'Cl',
                                       'PO4', 'DOC', 'TDN', 'DON',
                                       'SiO2', 'Mn', 'Fe', 'F',
                                       'cationCharge', 'anionCharge',
                                       'ionBalance'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         set_to_NA = -999.9,
                         summary_flagcols = 'fieldCode')

    d <- ms_cast_and_reflag(
        d,
        summary_flags_to_drop = list(fieldCode = 'sentinel'),
        summary_flags_dirty = list(fieldCode = c('969', '970', '905', '955')),
        varflag_col_pattern = NA
    )

    d <- ms_conversions(d,
                        convert_units_from = c(DIC = 'umol/l'),
                        convert_units_to = c(DIC = 'mg/l'))

    return(d)
}

#ws_boundary: STATUS=READY
#. handle_errors
process_1_94 <- function(network, domain, prodname_ms, site_code, component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n=network, d=domain, p=prodname_ms, s=site_code)

    rawfile <- glue(rawdir, '/', component)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    projstring <- choose_projection(unprojected = TRUE)

    wb <- sf::st_read(rawdir, stringsAsFactors = FALSE,
                      quiet = TRUE) %>%
        # mutate(WSHEDS0_ID = ifelse(is.na(WSHEDS0_ID), 0, WSHEDS0_ID)) %>%
        filter(! is.na(WS), #ignore encompassing, non-experimental watershed
               WS != 'WS101') %>% #and ungauged w101
        select(site_code = WSHEDS0_ID,
               area = AREA,
               perimeter = PERIMETER,
               geometry = geometry) %>%
        mutate(site_code = paste0('w', site_code)) %>%
        sf::st_transform(projstring) %>%
        arrange(site_code)

    unlink(zipped_files)

    for(i in 1:nrow(wb)){

        new_wb <- wb[i,]

        site_code <- as_tibble(new_wb) %>%
            pull(site_code)

        write_ms_file(d = new_wb,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = site_code,
                      level = 'munged',
                      shapefile = TRUE,
                      link_to_portal = FALSE)
    }

    return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_1_100 <- function(network, domain, prodname_ms, site_code, component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n=network, d=domain, p=prodname_ms, s=site_code)
    rawfile <- glue(rawdir, '/', component)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    projstring <- choose_projection(unprojected = TRUE)

    rg_all <- sf::st_read(rawdir, stringsAsFactors = FALSE,
                          quiet = TRUE) %>%
        filter(! is.na(GAGE_NUM)) %>%
        select(site_code = ID,
               geometry = geometry) %>%
        sf::st_transform(projstring) %>%
        arrange(site_code)

    unlink(zipped_files)

    for(i in 1:nrow(rg_all)){

        rg <- rg_all[i,] %>%
            sf::st_zm(drop=TRUE, what='ZM') #drop Z dimension

        write_ms_file(d = rg,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = rg$site_code,
                      level = 'munged',
                      shapefile = TRUE,
                      link_to_portal = FALSE)
    }

    return()
}

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_1_107 <- function(network, domain, prodname_ms, site_code, component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n=network, d=domain, p=prodname_ms, s=site_code)
    rawfile <- glue(rawdir, '/', component)

    zipped_files <- unzip(zipfile = rawfile,
                          exdir = rawdir,
                          overwrite = TRUE)

    projstring <- choose_projection(unprojected = TRUE)

    weirs_all <- sf::st_read(rawdir, stringsAsFactors = FALSE,
                             quiet = TRUE) %>%
        filter(! is.na(WEIR_NUM)) %>%
        select(site_code = WEIR_NUM,
               geometry = geometry) %>%
        mutate(site_code = paste0('w', site_code)) %>%
        sf::st_transform(projstring) %>%
        arrange(site_code)

    unlink(zipped_files)

    for(i in 1:nrow(weirs_all)){

        w <- weirs_all[i,] %>%
             sf::st_zm(drop=TRUE, what='ZM') #drop Z dimension

        write_ms_file(d = w,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = w$site_code,
                      level = 'munged',
                      shapefile = TRUE,
                      link_to_portal = FALSE)
    }

    return()
}

#derive kernels ####

# #precipitation: STATUS=PAUSED
# #. handle_errors
# process_2_ms001 <- derive_precip
#
# #precip_chemistry: STATUS=PAUSED
# #. handle_errors
# process_2_ms002 <- derive_precip_chem

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- derive_stream_flux

# #precip_flux_inst: STATUS=PAUSED
# #. handle_errors
# process_2_ms004 <- derive_precip_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_precip_pchem_pflux
