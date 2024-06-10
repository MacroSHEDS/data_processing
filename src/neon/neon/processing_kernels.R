#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_DP1.20093.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain)

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#stream_nitrate: STATUS=READY
#. handle_errors
process_0_DP1.20033.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain, time_index = '15')

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#stream_PAR: STATUS=READY
#. handle_errors
process_0_DP1.20042.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain, time_index = '30')

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#stream_temperature: STATUS=READY
#. handle_errors
process_0_DP1.20053.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain, time_index = '30')

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()}

#air_pressure: STATUS=PAUSED
#. handle_errors
process_0_DP1.00004 <- function(set_details, network, domain){

    data_pile <- try(neonUtilities::loadByProduct(set_details$prodcode_full,
                                                  site=set_details$site_code, startdate=set_details$component,
                                                  enddate=set_details$component, package='basic', check.size=FALSE, avg=30),
                     silent = TRUE)

    if(class(data_pile) == 'try-error'){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code,
                                          c = set_details$component)))
    }

    raw_data_dest <- glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
                          wd=getwd(), n=network, d=domain, p=prodname_ms,
                          s=set_details$site_code, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

    # out_sub = data_pile$BP_30min %>%
    #     mutate(site_code=paste0(set_details$site_code, updown)) %>%
    #     select(site_code, startDateTime, staPresMean, staPresFinalQF)

    return()
}

#stream_gases: STATUS=READY
#. handle_errors
process_0_DP1.20097.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain)

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#surface_elevation: STATUS=PAUSED
#. handle_errors
process_0_DP1.20016 <- function(set_details, network, domain){

    #stop('disabled. waiting on NEON to fix this product')

    data_pile <- try(neonUtilities::loadByProduct(set_details$prodcode_full,
                                                  site=set_details$site_code,
                                                  startdate=set_details$component,
                                                  enddate=set_details$component,
                                                  package='basic',
                                                  check.size=FALSE,
                                                  avg=5))

    if(class(data_pile) == 'try-error'){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code,
                                          c = set_details$component)))
    }

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
                         wd=getwd(), n=network, d=domain, p=prodname_ms,
                         s=set_details$site_code, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

    # out_sub = select(data_pile$EOS_5_min, startDateTime,
    #     surfacewaterElevMean, sWatElevFinalQF, verticalPosition,
    #     horizontalPosition)

    #BELOW: old code for acquiring sensor positions. relevant for decyphering
    #neon's level data and eventually estimating discharge

    # drc = glue('data/neon/neon/raw/surfaceElev_sensorpos/{s}',
    #     s=set_details$site_code)
    # dir.create(drc, showWarnings=FALSE, recursive=TRUE)
    # f = glue(drc, '/{p}.feather', p=set_details$component)
    #
    # # if(file.exists(f)){
    # #     sens_pos = read_feather(f)
    # #     sens_pos = bind_rows(data_pile$sensor_positions_20016, sens_pos) %>%
    # #         distinct()
    # # } else {
    # #     sens_pos = data_pile$sensor_positions_20016
    # # }
    #
    # write_feather(data_pile$sensor_positions_20016, f)

    return()
}

#stream_quality: STATUS=READY
#. handle_errors
process_0_DP1.20288.001 <- function(set_details, network, domain){

    #should have timeIndex options, but getTimeIndex returns c(0, 0, 100)
    #and they all give the same result (2024-02-21)
    result <- neon_retrieve(set_details, network, domain)

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_DP1.00006.001 <- function(set_details, network, domain){

    # pgauge <- terr_aquat_sitemap[[set_details$site_code]]

    result <- neon_retrieve(set_details, network, domain, time_index = '30')

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_DP4.00130.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain)

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain){

    raw_data_dest <- glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                          wd = getwd(),
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    write_loc <- file.path(raw_data_dest, 'composite_series.zip')
    r <- httr::GET(url = 'https://api.figshare.com/v2/file/download/40935107',
                   httr::write_disk(write_loc),
                   overwrite = TRUE)

    if(httr::status_code(r) != 200) stop('figshare download fail')

    unzip(write_loc, exdir = dirname(write_loc))
    file.remove(write_loc)

    r <- httr::GET(url = 'https://api.figshare.com/v2/articles/23206592')
    article_deets <- httr::content(r)

    deets_out <- list(url = 'https://figshare.com/ndownloader/files/40935107',
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = article_deets$modified_date %>%
                          strptime(format = '%Y-%m-%dT%H:%M:%S') %>%
                          with_tz('UTC'))

    return(deets_out)
}

#isotopes: STATUS=READY
#. handle_errors
process_0_DP1.20206.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain)

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#precip_isotopes: STATUS=READY
#. handle_errors
process_0_DP1.00038.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain)

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_DP1.00013.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain)

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#spCond: STATUS=PAUSED
#. handle_errors
process_0_DP1.20008.001 <- function(set_details, network, domain){

    result <- neon_retrieve(set_details, network, domain, time_index = '30')

    if(inherits(result, 'try-error')){
        return(generate_ms_exception(glue('Data unavailable for {p} {s}',
                                          p = prodname_ms,
                                          s = set_details$site_code)))
    }

    return()
}

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain){

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code,
                          c = set_details$component)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue(raw_data_dest,
                    '/NEONAquaticWatershed.zip')

    url <- 'https://www.neonscience.org/sites/default/files/NEONAquaticWatershed.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = url,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = url,
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        unzip(rawfile, exdir = dirname(rawfile))
        file.remove(rawfile)

        deets_out$url <- url
        deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                      tzone = 'UTC'))
        deets_out$last_mod_dt = last_mod_dt

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(deets_out)
    }

    return(deets_out)
}

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_DP1.20093.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    # prodname_ms = 'stream_chemistry__DP1.20093.001'
    # site_code = 'ARIK'
    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'swc_domainLabData'
    relevant_tbl2 <- 'swc_externalLabDataByAnalyte'
    if(relevant_tbl1 %in% names(rawd)){

        d <- tibble(rawd[[relevant_tbl1]])

        #dd$dataQF is always NA, even when dd$remarks suggest something egregious
        d$actual_quality_flag <- as.numeric(
            ! is.na(d$release) &
                ! grepl('replicate|SOP|protocol|cartridge',
                        d$remarks,
                        ignore.case = TRUE)
        )

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('collectDate' = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'UTC',
                             site_code_col = 'siteID',
                             data_cols =  c(alkMgPerL = 'alk',
                                            ancMeqPerL = 'ANC'),
                             data_col_pattern = '#V#',
                             summary_flagcols = 'actual_quality_flag',
                             is_sensor = FALSE,
                             sampling_type = 'G')

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                summary_flags_clean = list(actual_quality_flag = '0'),
                                summary_flags_dirty = list(actual_quality_flag = '1'))

        out_dom <- ms_conversions(d,
                                  convert_units_from = c(ANC = 'meq'),
                                  convert_units_to = c(ANC = 'eq'))
    }

    if(relevant_tbl2 %in% names(rawd)){

        d <- tibble(rawd[[relevant_tbl2]]) %>%
            mutate(analyteConcentration = as.character(analyteConcentration),
                   #NEON uses BDL to mean "below quantification limit" and ND to mean "below detection limit"
                   analyteConcentration = if_else(
                       ! is.na(belowDetectionQF) & belowDetectionQF == 'ND',
                       'BDL',
                       analyteConcentration
                   )) %>%
            select(-uid, -domainID, -namedLocation, -sampleID, -sampleCode,
                   -startDate, -laboratoryName, -analysisDate, -coolerTemp,
                   -publicationDate, -release)

        #check for unspecified units
        missing_unit <- filter(d, is.na(analyteUnits) & ! grepl('UV Abs|pH', analyte))
        message(paste('dropping', nrow(missing_unit), 'records with unspecified units (total', nrow(d), ')'))
        d <- filter(d, ! is.na(analyteUnits) | grepl('UV Abs|pH', analyte))

        #check for vars reported in more than 1 unit
        var_unit_pairs <- d %>%
            distinct(analyte, analyteUnits) %>%
            filter(! is.na(analyteUnits)) %>%
            arrange(analyte)
        if(any(duplicated(var_unit_pairs))){
            warning('we need to address this: analyte-analyteUnits mapping not 1:1')
            browser()
        }

        #sometimes TPN and TPC are reported in milligrams. not sure how to get sample volume,
        #but so far rare enough that it's probably a mistake (10 total records as of 2024-04-05)
        weird_unit <- d$analyte %in% c('TPN', 'TPC') & d$analyteUnits != 'microgramsPerLiter'
        nweird_unit <- sum(weird_unit)
        if(nweird_unit > 0){
            message(nweird_unit, ' TPN/TPC observations reported in "',
                    paste(unique(d[weird_unit, 'analyteUnits']), collapse = ', '),
                    '". these will be dropped.')
            d <- d[! weird_unit, ]
        }

        # update_neon_detlims(rawd$swc_externalLabSummaryData,
        #                     set = 'chem')

        #consolidate QA info. can't find definitions for externalLabDataQF, and
        #the values present in that column aren't obviously indicative of ms_status = 1.
        #probably could be used to identify causes of discontinuities in time series though.
        if(any(is.na(d$sampleCondition))) stop('handle NA sampleCondition')
        d <- d %>%
            mutate(sampleCondition = if_else(belowDetectionQF %in% c('BDL', 'ND'),
                                             'OK',
                                             sampleCondition),
                   sampleCondition = if_else(shipmentWarmQF == 1,
                                             'OK',
                                             sampleCondition))

        #handle replicates; create varflag cols
        d <- d %>%
            select(-analyteUnits, -belowDetectionQF, -remarks, -shipmentWarmQF,
                   -externalLabDataQF) %>%
            filter(! is.na(analyteConcentration)) %>%
            group_by(collectDate, analyte) %>%
            #if any replicates are good, ignore the bad ones. otherwise bad will have to do
            filter(! (sampleCondition != 'GOOD' & 'GOOD' %in% sampleCondition)) %>%
            #if any replicates are okay, and none is good, ignore the bad ones. otherwise bad will have to do
            filter(! (sampleCondition == 'Other' & 'OK' %in% sampleCondition & ! 'GOOD' %in% sampleCondition)) %>%
            summarize(analyteConcentration = mean(suppressWarnings(as.numeric(analyteConcentration)),
                                                  na.rm = TRUE), #converts "BDL" to NaN
                      sampleCondition = if_else(any(sampleCondition %in% c('OK', 'Other')),
                                                'OK', #OK and Other will be consolidated to ms_status = 1
                                                'GOOD'),
                      siteID = first(siteID)) %>%
            ungroup() %>%
            #restore "BDL" flags
            mutate(analyteConcentration = as.character(analyteConcentration),
                   analyteConcentration = if_else(analyteConcentration == 'NaN',
                                                  'BDL',
                                                  analyteConcentration)) %>%
            #create varflags
            rename(val = analyteConcentration,
                   flag = sampleCondition) %>%
            pivot_wider(names_from = 'analyte',
                        values_from = c('val', 'flag'))

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = list('collectDate' = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'UTC',
                             site_code_col = 'siteID',
                             data_cols =  c(
                                 'Ortho - P' = 'orthophosphate_P',
                                 'NO3+NO2 - N' = 'NO3_NO2_N',
                                 'NO2 - N' = 'NO2_N',
                                 'NH4 - N' = 'NH4_N',
                                 'specificConductance' = 'spCond',
                                 'UV Absorbance (280 nm)' = 'abs280',
                                 'UV Absorbance (254 nm)' = 'abs254',
                                 'SO4', 'TDN', 'Ca', 'TDP', 'DOC', 'TN', 'Mg',
                                 'Mn', 'TPN', 'DIC', 'TOC', 'Na', 'TSS', 'Cl',
                                 'Fe', 'HCO3', 'F', 'Br', 'TPC', 'pH', 'Si',
                                 'K', 'TP', 'TDS', 'CO3', 'ANC'),
                             data_col_pattern = 'val_#V#',
                             var_flagcol_pattern = 'flag_#V#',
                             convert_to_BDL_flag = 'BDL',
                             is_sensor = FALSE,
                             sampling_type = 'G')

        d <- ms_cast_and_reflag(d,
                                variable_flags_clean = 'GOOD',
                                variable_flags_dirty = 'OK',
                                variable_flags_bdl = 'BDL')

        conv_vars <- neon_chem_vars %>%
            filter(tolower(neon_unit) != tolower(unit))

        out_lab <- ms_conversions(
            d,
            convert_units_from = deframe(select(conv_vars, ms_var, neon_unit)),
            convert_units_to = deframe(select(conv_vars, ms_var, unit))
        )
    }

    if(! exists('out_lab') && ! exists('out_dom')){

        print(paste0('swc_externalLabDataByAnalyte and swc_domainLabData are missing for ', site_code))
        out_sub <- tibble()

    } else {

        if(! exists('out_lab')){
            print(paste0('swc_externalLabDataByAnalyte is missing for ', site_code, '. proceeding with swc_domainLabData'))
            out_sub <- out_dom
        }

        if(! exists('out_dom')){
            print(paste0('swc_domainLabData is missing for ', site_code, '. proceeding with swc_externalLabDataByAnalyte'))
            out_sub <- out_lab
        }

        if(exists('out_dom') && exists('out_lab')){
            out_sub <- rbind(out_lab, out_dom)
        }

        out_sub <- out_sub %>%
            group_by(datetime, site_code, var) %>%
            summarize(
                ms_status = min(ms_status),
                val = mean(val[ms_status == min(ms_status)],
                           na.rm = TRUE)
            ) %>%
            ungroup() %>%
            filter(! is.na(val))
    }

    return(out_sub)
}

#stream_nitrate: STATUS=READY
#. handle_errors
process_1_DP1.20033.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'NSW_15_minute'
    if(relevant_tbl1 %in% names(rawd)){
        rawd <- tibble(rawd[[relevant_tbl1]])
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(is.na(rawd$surfWaterNitrateMean))){
        return(generate_ms_exception(paste('No data for', site_code)))
    }

    #just using this to check for weirdness. nitrate is only collected at the
    #downstream sensor array (S2)
    updown <- determine_upstream_downstream(rawd)

    rawd <- neon_average_start_end_times(rawd)

    d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                         datetime_cols = list(datetime = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'UTC',
                         site_code_col = 'siteID',
                         data_cols =  c(surfWaterNitrateMean = 'NO3'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'finalQF',
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(finalQF = '0'),
                            summary_flags_to_drop = list(finalQF = 'sentinel'))

    d <- ms_conversions(d,
                        convert_units_from = c(NO3 = 'umol/L'),
                        convert_units_to = c(NO3 = 'mg/L'))

    return(d)
}

#stream_PAR: STATUS=READY
#. handle_errors
process_1_DP1.20042.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'PARWS_30min'
    if(relevant_tbl1 %in% names(rawd)){
        rawd <- tibble(rawd[[relevant_tbl1]])
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(is.na(rawd$PARMean))){
        return(generate_ms_exception(paste('No data for', site_code)))
    }

    rawd <- rename(rawd, finalQF = PARFinalQF)

    rawd <- neon_average_start_end_times(rawd)
    rawd <- neon_borrow_from_upstream(rawd, relevant_cols = 'PARMean')

    d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                         datetime_cols = list(date = '%Y-%m-%d'),
                         datetime_tz = 'UTC',
                         site_code_col = 'siteID',
                         data_cols =  c(PARMean = 'PAR'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'finalQF',
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(finalQF = '0'),
                            summary_flags_to_drop = list(finalQF = 'sentinel'))

    return(d)
}

#stream_temperature: STATUS=READY
#. handle_errors
process_1_DP1.20053.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'TSW_30min'
    if(relevant_tbl1 %in% names(rawd)){
        rawd <- tibble(rawd[[relevant_tbl1]])
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(is.na(rawd$surfWaterTempMean))){
        return(generate_ms_exception(paste('No data for', site_code)))
    }

    rawd <- neon_average_start_end_times(rawd)
    rawd <- neon_borrow_from_upstream(rawd, relevant_cols = 'surfWaterTempMean')

    d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                         datetime_cols = list(date = '%Y-%m-%d'),
                         datetime_tz = 'UTC',
                         site_code_col = 'siteID',
                         data_cols =  c(surfWaterTempMean = 'temp'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'finalQF',
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(finalQF = '0'),
                            summary_flags_to_drop = list(finalQF = 'sentinel'))

    return(d)
}

#air_pressure: STATUS=PAUSED
#. handle_errors
process_1_DP1.00004 <- function(network, domain, prodname_ms, site_code,
                                component){

    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                  n=network, d=domain, p=prodname_ms, s=site_code, c=component)

    rawfiles = list.files(rawdir)
    # write_neon_readme(rawdir, dest='/tmp/neon_readme.txt')
    # varkey = write_neon_variablekey(rawdir, dest='/tmp/neon_varkey.csv')

    relevant_tbl1 = 'BP_30min.feather'

    if(relevant_tbl1 %in% names(rawd)){
        rawd = tibble(rawd[[relevant_tbl1]])
        out_sub = sourceflags_to_ms_status(rawd, list(staPresFinalQF = 0))
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(out_sub$ms_status == 1)){
        return(generate_ms_exception('All records failed QA'))
    }

    updown = determine_upstream_downstream(out_sub)#still need? loop through ggg and table(updown)

    out_sub = out_sub %>%
        mutate(
            site_code=paste0(siteID, updown), #append "-up" to upstream site_codes
            startDateTime = force_tz(startDateTime, 'UTC')) %>% #GMT -> UTC
        group_by(startDateTime, site_code) %>%
        summarize(
            staPresMean = mean(staPresMean, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup() %>%
        select(site_code, datetime=startDateTime, airpressure=staPresMean,
               ms_status)

    out_sub <- synchronize_timestep(d = out_sub)

    return(out_sub)
}

#stream_gases: STATUS=READY
#. handle_errors
process_1_DP1.20097.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'sdg_externalLabData'

    if(relevant_tbl1 %in% names(rawd)){

        # update_neon_detlims(rawd$sdg_externalLabSummaryData,
        #                     set = 'gas')

        d <- rawd$sdg_externalLabData

        ## conversion from ppm by volume to mol/L

        sdgFormatted <- suppressWarnings(neonDissGas::def.format.sdg(
            externalLabData = d,
            fieldDataProc = rawd$sdg_fieldDataProc,
            fieldSuperParent = rawd$sdg_fieldSuperParent
        ))

        sdgConcentrations <- neonDissGas::def.calc.sdg.conc(inputFile = sdgFormatted)

        #merge computed concentrations for dissolved gases with main df
        d <- d %>%
            as_tibble() %>%
            filter(grepl('WAT', sampleID)) %>%
            mutate(sampleID = sub('\\.WAT', '', sampleID)) %>%
            left_join(select(sdgConcentrations,
                             waterSampleID,
                             contains('dissolved')),
                      by = c(sampleID = 'waterSampleID')) %>%
            #contentrations are not computed for replicates, but that's okay
            filter(! if_any(contains('dissolved'), is.na))

        ## consolidate QC info

        d <- d %>%
            mutate(
                #sampleCondition can be "OK", NA, or assorted specific things
                stts = if_else(! is.na(sampleCondition) &
                                   sampleCondition == 'OK',
                               0, 1),
                #remarks usually indicate no issue, but whenever the word "inventory"
                #appears it indicates labeling mishaps
                stts = if_else(grepl('inventor', externalRemarks),
                               1, stts),
                #these quality flags can be -1, 0, 1, or NA. only 0 is okay.
                N2OCheckStandardQF = if_else(! is.na(N2OCheckStandardQF) &
                                                 N2OCheckStandardQF == 0,
                                             0, 1),
                CO2CheckStandardQF = if_else(! is.na(CO2CheckStandardQF) &
                                                 CO2CheckStandardQF == 0,
                                             0, 1),
                CH4CheckStandardQF = if_else(! is.na(CH4CheckStandardQF) &
                                                 CH4CheckStandardQF == 0,
                                             0, 1),
                #BDL is not directly indicated, but can be determined like so
                CH4CheckStandardQF = if_else(
                    ! is.na(concentrationCH4) & concentrationCH4 <= runDetectionLimitCH4,
                    1, CH4CheckStandardQF
                ),
                N2OCheckStandardQF = if_else(
                    ! is.na(concentrationN2O) & concentrationN2O <= runDetectionLimitN2O,
                    1, N2OCheckStandardQF
                ),
                CO2CheckStandardQF = if_else(
                    ! is.na(concentrationCO2) & concentrationCO2 <= runDetectionLimitCO2,
                    1, CO2CheckStandardQF
                )
            )
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('collectDate' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'UTC',
                         site_code_col = 'siteID',
                         data_cols =  c('CO2', 'N2O', 'CH4'),
                         data_col_pattern = 'dissolved#V#',
                         var_flagcol_pattern = '#V#CheckStandardQF',
                         summary_flagcols = 'stts',
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(d,
                            variable_flags_clean = '0',
                            variable_flags_to_drop = 'sentinel',
                            summary_flags_clean = list(finalQF = '0'),
                            summary_flags_to_drop = list(finalQF = 'sentinel'))

    d <- ms_conversions(d,
                        convert_units_from = c(CO2 = 'mol/L',
                                               CH4 = 'mol/L',
                                               N2O = 'mol/L'),
                        convert_units_to = c(CO2 = 'mg/L',
                                             CH4 = 'mg/L',
                                             N2O = 'mg/L'))
}

#surface_elevation: STATUS=PAUSED
#. handle_errors
process_1_DP1.20016 <- function(network, domain, prodname_ms, site_code,
                                component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n=network, d=domain, p=prodname_ms, s=site_code, c=component)

    rawfiles <- list.files(rawdir)

    relevant_tbl <- 'EOS_5_min.feather'

    if(relevant_tbl %in% names(rawd)) {

        rawd <- tibble(rawd[[relevant_tbl]])

        out_sub <- rawd %>%
            mutate(ms_status = ifelse(sWatElevFinalQF == 1, 1, 0)) %>%
            mutate(ms_status = ifelse(is.na(ms_status), 0, ms_status)) %>%
            mutate(var = 'stage_height') %>%
            select(site_code = siteID, datetime = endDateTime, var, val = surfacewaterElevMean, ms_status)

    } else {
        return(generate_ms_exception('Missing discharge files'))
    }

    return(out_sub)

}

#stream_quality: STATUS=READY
#. handle_errors
process_1_DP1.20288.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'waq_instantaneous'
    if(relevant_tbl1 %in% names(rawd)){
        rawd <- tibble(rawd[[relevant_tbl1]])
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    na_test <- rawd %>%
        select(specificConductance, dissolvedOxygen, pH, chlorophyll, turbidity, fDOM) %>%
        is.na() %>%
        all()

    if(na_test){
        return(generate_ms_exception(paste('No data for', site_code)))
    }

    #make column naming consistent
    rawd <- rename_with(rawd, ~sub('specificConductance', 'specificCond', .))
    rawd <- rename_with(rawd, ~sub('localDissolvedOxygen', 'localDO', .))

    rawd <- neon_average_start_end_times(rawd)
    rawd <- neon_borrow_from_upstream(
        rawd,
        relevant_cols = c('specificCond', 'dissolvedOxygen', 'localDOSat',
                          'pH', 'chlorophyll', 'turbidity', 'fDOM')
    )

    d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                         datetime_cols = list(date = '%Y-%m-%d'),
                         datetime_tz = 'UTC',
                         site_code_col = 'siteID',
                         data_cols =  c(specificCond = 'spCond',
                                        dissolvedOxygen = 'DO',
                                        localDOSat = 'DO_sat',
                                        pH = 'pH',
                                        chlorophyll = 'Chla',
                                        turbidity = 'turbid',
                                        fDOM = 'FDOM'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#FinalQF',
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            variable_flags_clean = '0',
                            variable_flags_to_drop = 'sentinel')

    d <- ms_conversions(d,
                        convert_units_from = c(Chla = 'ug/L'),
                        convert_units_to = c(Chla = 'mg/L'))

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_DP1.00006.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    # pgauges <- terr_aquat_sitemap[[site_code]]

    # d <- tibble()
    # for(pgauge in pgauges){

    if(site_code == 'MCRA'){
        #MCRA precip data are borrowed from hjandrews during derive
        return(tibble())
    }

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)
                   # s = pgauge)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'PRIPRE_30min' #primary collector
    relevant_tbl2 <- 'SECPRE_30min' #secondary collector

    if(relevant_tbl1 %in% names(rawd)){

        rawd1 <- tibble(rawd[[relevant_tbl1]])

        d1 <- ms_read_raw_csv(preprocessed_tibble = rawd1,
                              datetime_cols = list(endDateTime = '%Y-%m-%d %H:%M:%S'),
                              datetime_tz = 'UTC',
                              site_code_col = 'siteID',
                              data_cols =  c(priPrecipBulk = 'precipitation'),
                              data_col_pattern = '#V#',
                              summary_flagcols = 'priPrecipFinalQF',
                              is_sensor = TRUE,
                              sampling_type = 'I',
                              keep_empty_rows = TRUE)

        d1 <- ms_cast_and_reflag(
            d1,
            varflag_col_pattern = NA,
            summary_flags_clean = list(priPrecipFinalQF = '0'),
            summary_flags_to_drop = list(priPrecipFinalQF = 'sentinel'),
            keep_empty_rows = TRUE
        )
    }

    if(relevant_tbl2 %in% names(rawd)){

        rawd2 <- tibble(rawd[[relevant_tbl2]])

        d2 <- ms_read_raw_csv(preprocessed_tibble = rawd2,
                              datetime_cols = list(endDateTime = '%Y-%m-%d %H:%M:%S'),
                              datetime_tz = 'UTC',
                              site_code_col = 'siteID',
                              data_cols =  c(secPrecipBulk = 'precipitation'),
                              data_col_pattern = '#V#',
                              summary_flagcols = c('secPrecipValidCalQF',
                                                   'secPrecipRangeQF',
                                                   'secPrecipSciRvwQF'),
                              is_sensor = TRUE,
                              sampling_type = 'I',
                              keep_empty_rows = TRUE)

        d2 <- ms_cast_and_reflag(
            d2,
            varflag_col_pattern = NA,
            summary_flags_clean = list(secPrecipValidCalQF = '0',
                                       secPrecipRangeQF = '0',
                                       secPrecipSciRvwQF = c('0', NA)),
            summary_flags_to_drop = list(secPrecipValidCalQF = 'sentinel',
                                         secPrecipRangeQF = 'sentinel',
                                         secPrecipSciRvwQF = 'sentinel'),
            keep_empty_rows = TRUE
        )
    }

    if(! relevant_tbl1 %in% names(rawd) && ! relevant_tbl2 %in% names(rawd)){
        return(generate_ms_exception('Missing precip files'))
    }

    if(relevant_tbl1 %in% names(rawd) && relevant_tbl2 %in% names(rawd)){

        d <- full_join(d1, d2,
                       by = c('datetime', 'site_code', 'var'),
                       suffix = c('1', '2'))

        ## borrow secondary precip values wherever:
        ## primary is questionable and secondary is not (case A), or
        ## primary is missing and secondary is available (case B).
        ## borrow status of secondary precip if used.

        d$ms_status1[is.na(d$ms_status1)] <- 1
        d$ms_status2[is.na(d$ms_status2)] <- 1

        borrow_inds_a <- d$ms_status1 == 1 & d$ms_status2 == 0 & ! is.na(d$val2)
        borrow_inds_b <- is.na(d$val1)
        borrow_inds <- borrow_inds_a | borrow_inds_b

        d$val1[borrow_inds] <- d$val2[borrow_inds]
        d$ms_status1[borrow_inds] <- d$ms_status2[borrow_inds]

        d$val2 <- d$ms_status2 <- NULL
        d <- rename(d, val = val1, ms_status = ms_status1)

    } else {

        if(relevant_tbl1 %in% names(rawd)) d <- d1
        if(relevant_tbl2 %in% names(rawd)) d <- d2
    }

        # d <- bind_rows(d, d_)
    # }

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code,
                                     component){

    for(site_code_ in neon_streams){

        rawfile <- glue('data/{n}/{d}/raw/{p}/sitename_NA/composite_series/{s}.csv',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code_)

        rawd <- read_csv(rawfile, show_col_types = FALSE) %>%
            mutate(site = site_code_)

        d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                             datetime_cols = list(datetime = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'UTC',
                             site_code_col = 'site',
                             data_cols =  c(discharge_Ls = 'discharge'),
                             data_col_pattern = '#V#',
                             summary_flagcols = 'source',
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(d,
                                varflag_col_pattern = NA,
                                summary_flags_clean = list(source = 'NEON'),
                                summary_flags_to_drop = list(source = 'sentinel'))

        d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
        d <- synchronize_timestep(d)

        d$ms_interp[d$ms_status == 1] <- 1

        write_ms_file(d = d,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = site_code_,
                      level = 'munged',
                      shapefile = FALSE)
    }
}

#isotopes: STATUS=READY
#. handle_errors
process_1_DP1.20206.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'asi_externalLabH2OIsotopes'
    if(relevant_tbl1 %in% names(rawd)){
        rawd <- tibble(rawd[[relevant_tbl1]])
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(is.na(rawd$d2HWater))){
        return(generate_ms_exception(paste('No data for', site_code)))
    }

    rawd$remark_summary <- as.numeric(
        ! is.na(rawd$externalRemarks) &
            grepl('vial|volume| id',
                  rawd$externalRemarks,
                  ignore.case = TRUE)
    )

    d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                         datetime_cols = list('collectDate' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'UTC',
                         site_code_col = 'siteID',
                         data_cols =  c(d2HWater = 'dD',
                                        d18OWater = 'd18O'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c('remark_summary',
                                              # 'isotopeH2OExternalLabQF',
                                              'sampleCondition'),
                         is_sensor = FALSE,
                         sampling_type = 'G')

    d <- ms_cast_and_reflag(
        d,
        varflag_col_pattern = NA,
        summary_flags_clean = list(remark_summary = '0',
                                   sampleCondition = 'OK'),
        summary_flags_to_drop = list(remark_summary = 'sentinel',
                                     sampleCondition = 'sentinel')
    )

    return(d)
}

#precip_isotopes: STATUS=READY
#. handle_errors
process_1_DP1.00038.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl1 <- 'wdi_isoPerSample'
    if(relevant_tbl1 %in% names(rawd)){
        rawd <- tibble(rawd[[relevant_tbl1]])
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(is.na(rawd$d2HWater))){
        return(generate_ms_exception(paste('No data for', site_code)))
    }

    rawd$remark_summary <- as.numeric(
        ! is.na(rawd$externalRemarks) &
            grepl('vial|volume| id',
                  rawd$externalRemarks,
                  ignore.case = TRUE)
    )

    d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                         datetime_cols = list('collectDate' = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'UTC',
                         site_code_col = 'siteID',
                         data_cols =  c(d2HWater = 'dD',
                                        d18OWater = 'd18O'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c('remark_summary',
                                              'sampleCondition'),
                         is_sensor = FALSE,
                         sampling_type = 'G',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(
        d,
        varflag_col_pattern = NA,
        summary_flags_clean = list(remark_summary = '0',
                                   sampleCondition = 'OK'),
        summary_flags_to_drop = list(remark_summary = 'sentinel',
                                     sampleCondition = 'sentinel'),
        keep_empty_rows = TRUE
    )

    return(d)
}

#spCond: STATUS=PAUSED
#. handle_errors
process_1_DP1.20008.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

}

#discharge: STATUS=READY
#. handle_errors
process_1_DP4.00130.001 <-function(network, domain, prodname_ms, site_code,
                                   component){

    #stationHorizontalID seems to have stabilized as of first publication of composite Q.
    #only one per site when composite-era data are filtered out.

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    if(site_code == 'TOMB'){
        relevant_tbl1 <- 'csd_continuousDischargeUSGS'
    } else {
        relevant_tbl1 <- 'csd_continuousDischarge'
    }

    if(relevant_tbl1 %in% names(rawd)){
        rawd <- tibble(rawd[[relevant_tbl1]])
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(is.na(rawd$maxpostDischarge))){
        return(generate_ms_exception(paste('No data for', site_code)))
    }

    composite_q <- read_feather(glue('data/neon/neon/munged/discharge__VERSIONLESS002/{site_code}.feather'))
    last_composite_date <- as.Date(max(composite_q$datetime))
    rm(composite_q)

    rawd <- rawd %>%
        mutate(endDate = with_tz(endDate, 'UTC')) %>%
        filter(!(siteID == !!site_code & endDate <= !!last_composite_date))

    if(site_code == 'TOMB'){

        d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                             datetime_cols = list(endDate = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'UTC',
                             site_code_col = 'siteID',
                             data_cols =  c(usgsDischarge = 'discharge'),
                             data_col_pattern = '#V#',
                             summary_flagcols = c('dischargeFinalQF',
                                                  'dischargeFinalQFSciRvw',
                                                  'usgsValueQualCode'),
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(
            d,
            varflag_col_pattern = NA,
            summary_flags_clean = list(dischargeFinalQF = '0',
                                       dischargeFinalQFSciRvw = '0',
                                       usgsValueQualCode = 'A'),
            summary_flags_to_drop = list(dischargeFinalQF = 'sentinel',
                                         dischargeFinalQFSciRvw = 'sentinel',
                                         usgsValueQualCode = 'sentinel')
        )

    } else {

        d <- ms_read_raw_csv(preprocessed_tibble = rawd,
                             datetime_cols = list(endDate = '%Y-%m-%d %H:%M:%S'),
                             datetime_tz = 'UTC',
                             site_code_col = 'siteID',
                             data_cols =  c(maxpostDischarge = 'discharge'),
                             data_col_pattern = '#V#',
                             summary_flagcols = c('dischargeFinalQF',
                                                  'dischargeFinalQFSciRvw'),
                             is_sensor = TRUE,
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(
            d,
            varflag_col_pattern = NA,
            summary_flags_clean = list(dischargeFinalQF = '0',
                                       dischargeFinalQFSciRvw = '0'),
            summary_flags_to_drop = list(dischargeFinalQF = 'sentinel',
                                         dischargeFinalQFSciRvw = 'sentinel')
        )
    }

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_DP1.00013.001 <- function(network, domain, prodname_ms, site_code,
                                    component){

    rawdir <- glue('data/{n}/{d}/raw/{p}/{s}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_code)

    neonprodcode <- prodcode_from_prodname_ms(prodname_ms) %>%
        str_split_i('\\.', i = 2)

    rawd <- try({stackByTable_keep_zips(glue('{rawdir}/filesToStack{neonprodcode}'))})
    if(inherits(rawd, 'try-error')) return(generate_ms_exception('No data for site'))

    relevant_tbl <- 'wdp_chemLab'
    if(! relevant_tbl %in% names(rawd)){
        return(generate_ms_exception('Relevant file missing'))
    }

    d <- tibble(rawd[[relevant_tbl]])

    if(! all(c('precipNitrate', 'pH', 'precipMagnesiumFlag', 'precipConductivity') %in% colnames(d))){
        stop('Known column names missing. There has been a change')
    }

    #consolidate free-hand remarks into flag (sampleCondition doesn't capture everything)
    d$actual_quality_flag <- as.numeric(
        ! is.na(d$labConditionRemarks) &
            grepl('pollen|small|spec|bug|matter|dirt|leak|only',
                  d$labConditionRemarks,
                  ignore.case = TRUE)
    )

    #detection limits...
    #first thought: BDL gets replaced with detection limit even if there are other concerns.
    #second thought: but rly, this is just for recording DLs, as values are supplied for bdl records.
    #final thought: ugh, detlims not even reported.
    # d <- mutate(
    #     d,
    #     across(ends_with('Flag', ignore.case = FALSE),
    #            ~if_else(grepl('below detection limit', ., ignore.case = TRUE),
    #                     'Below detection limit',
    #                     .)
    #     )
    # )
    #
    # update_neon_detlims(rawd$)

    #NOTE: NEON does not necessarily flag samples that sat in the collector for several months
    #or even a year (see DELA sample with setDate == '2020-03-05 17:00:00').
    #it's possible that their QA process accounts for this? Anyway, will leave
    #it to end-user to make assumptions

    ##next, convert "setDate" and "collectDate" to a single date column

    #cull columns, convert datetimes to dates
    d <- d %>%
        select(siteID, setDate, collectDate, starts_with('precip'),
               pH, actual_quality_flag, sampleCondition) %>%
        mutate(setDate = as.Date(setDate),
               collectDate = as.Date(collectDate))

    #fill in days during which collectors were operating
    d$date <- Map(seq, from = d$setDate, to = d$collectDate, by = 'day')

    #fill in missing days between a collection and the following deployment
    d <- d %>%
        unnest(date) %>%
        relocate(date) %>%
        arrange(date) %>%
        tidyr::complete(date = seq(min(.$date), max(.$date), by = 'day')) %>%
        arrange(date)

    #associate each measurement with its collectionDate, not setDate (collections almost always in evening)
    d <- d %>%
        group_by(date) %>%
        filter(n() == 1 | date == collectDate) %>%
        ungroup() %>%
        select(-setDate, -collectDate)

    d <- ms_read_raw_csv(
        preprocessed_tibble = d,
        datetime_cols = list('date' = '%Y-%m-%d'),
        datetime_tz = 'UTC',
        site_code_col = 'siteID',
        data_cols =  c(Calcium = 'Ca',
                       Magnesium = 'Mg',
                       Potassium = 'K',
                       Sodium = 'Na',
                       Ammonium = 'NH4',
                       Nitrate = 'NO3',
                       Sulfate = 'SO4',
                       Phosphate = 'PO4',
                       Chloride = 'Cl',
                       Bromide = 'Br',
                       pH = 'pH',
                       Conductivity = 'spCond'),
        data_col_pattern = 'precip#V#',
        alt_datacol_pattern = '#V#',
        var_flagcol_pattern = 'precip#V#Flag',
        summary_flagcols = c('actual_quality_flag',
                             'sampleCondition'),
        is_sensor = FALSE,
        sampling_type = 'G',
        ignore_missing_col_warning = TRUE,
        keep_empty_rows = TRUE
    )

    d <- ms_cast_and_reflag(
        d,
        variable_flags_clean = c(NA, 'Analytical dilution'),
        variable_flags_to_drop = 'totally unmatchable sentinel jic',
        # variable_flags_bdl = ,
        summary_flags_clean = list(actual_quality_flag = '0',
                                   sampleCondition = NA),
        summary_flags_to_drop = list(actual_quality_flag = 'sentinel',
                                     sampleCondition = 'sentinel'),
        keep_empty_rows = TRUE
    )

    return(d)
}

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code,
                                     component){

    rawdir1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    rawfile1 <- glue(rawdir1, '/NEONAquaticWatershed.zip')

    # zipped_files <- unzip(zipfile = rawfile1,
    #                       exdir = rawdir1,
    #                       overwrite = TRUE)

    projstring <- choose_projection(unprojected = TRUE)

    d <- sf::st_read(glue(rawdir1,
                          '/NEON_Aquatic_Watershed.shp'),
                     stringsAsFactors = FALSE,
                     quiet = TRUE) %>%
        filter(Science != 'Lake') %>%
        # mutate(area = WSAreaKm2 * 100) %>%
        select(site_code = SiteID, geometry = geometry) %>%
        sf::st_transform(projstring) %>%
        arrange(site_code)

    if(nrow(d) == 0) stop('no rows in sf object')

    for(i in 1:nrow(d)){

        write_ms_file(d = d[i, ],
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = d$site_code[i],
                      level = 'munged',
                      shapefile = TRUE)
    }

    return()
}

#derive kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__20093.001',
                                           'stream_quality__DP1.20288.001',
                                           # 'spCond__DP1.20008.001',
                                           'stream_nitrate__DP1.20033.001',
                                           'stream_gases__DP1.20297.001',
                                           'stream_temperature__DP1.20053.001',
                                           'stream_PAR__DP1.20042.001',
                                           'isotopes__DP1.20206.001'))

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__DP4.00130.001',
                                           'discharge__VERSIONLESS002'))

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_2_ms003 <- function(network, domain, prodname_ms){

    combine_products(
        network = network,
        domain = domain,
        prodname_ms = prodname_ms,
        input_prodname_ms = c('precip_chemistry__DP1.00013.001',
                              'precip_isotopes__DP1.00038.001')
    )
}

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- stream_gauge_from_site_data

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms005 <- precip_gauge_from_site_data

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms006 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms007 <- function(network, domain, prodname_ms){

    pchem_prodname_ms <- get_derive_ingredient(network = network,
                                               domain = domain,
                                               prodname = 'precip_chemistry')

    precip_prodname_ms <- get_derive_ingredient(network = network,
                                                domain = domain,
                                                prodname = 'precipitation')

    wb_prodname_ms <- get_derive_ingredient(network = network,
                                            domain = domain,
                                            prodname = 'ws_boundary')

    rg_prodname_ms <- get_derive_ingredient(network = network,
                                            domain = domain,
                                            prodname = 'precip_gauge_locations')

    for(site in c('MCRA', names(terr_aquat_sitemap))){

        if(site == 'MCRA'){

            #borrow precip data from hjandrews
            warning('make sure hjandrews has already been derived in this macrosheds version. precip data needed for NEON-MCRA')

            hj_pchem <- get_derive_ingredient('lter', 'hjandrews', 'precip_chemistry')
            hj_precip <- get_derive_ingredient('lter', 'hjandrews', 'precipitation')
            hj_rg <- get_derive_ingredient('lter', 'hjandrews', 'precip_gauge_locations')

            hj_pgauge_ids <- site_data %>%
                filter(domain == 'hjandrews',
                       site_type == 'rain_gauge') %>%
                pull(site_code)

            precip_pchem_pflux_idw(pchem_prodname = hj_pchem,
                                   precip_prodname = hj_precip,
                                   wb_prodname = wb_prodname_ms,
                                   pgauge_prodname = hj_rg,
                                   prodname_ms = prodname_ms,
                                   filter_sites = list(precip = hj_pgauge_ids,
                                                       wb = site),
                                   donor_domain = c('lter' = 'hjandrews'))

        } else {

            pgauges <- terr_aquat_sitemap[[site]]

            precip_pchem_pflux_idw(pchem_prodname = pchem_prodname_ms,
                                   precip_prodname = precip_prodname_ms,
                                   wb_prodname = wb_prodname_ms,
                                   pgauge_prodname = rg_prodname_ms,
                                   prodname_ms = prodname_ms,
                                   filter_sites = list(precip = pgauges,
                                                       wb = site))
        }
    }
}


