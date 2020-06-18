#retrieval kernels ####

#chem: READY
#. handle_errors
process_0_20093 <- function(set_details, network, domain){
    # set_details=s

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE)
    # write_lines(data_pile$readme_20093$X1, '/tmp/chili.txt')

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

}

#nitrate: READY
#. handle_errors
process_0_20033 <- function(set_details, network, domain){

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE)

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

}

#photosynthetically active radiation (PAR): READY
#. handle_errors
process_0_20042 <- function(set_details, network, domain){

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE, avg=5)

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

}

#water temperature: READY
#. handle_errors
process_0_20053 <- function(set_details, network, domain){

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE, avg=5)

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

}

#air pressure: READY
#. handle_errors
process_0_00004 <- function(set_details, network, domain){

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE, avg=30)

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

    # out_sub = data_pile$BP_30min %>%
    #     mutate(site_name=paste0(set_details$site_name, updown)) %>%
    #     select(site_name, startDateTime, staPresMean, staPresFinalQF)
}

#gases: READY
#. handle_errors
process_0_20097 <- function(set_details, network, domain){

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE)

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

}

#stage: OUT OF SERVICE
#. handle_errors
process_0_20016 <- function(set_details, network, domain){

    stop('disabled. waiting on NEON to fix this product')

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE, avg=5)

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

    # out_sub = select(data_pile$EOS_5_min, startDateTime,
    #     surfacewaterElevMean, sWatElevFinalQF, verticalPosition,
    #     horizontalPosition)

    #BELOW: old code for acquiring sensor positions. relevant for decyphering
    #neon's level data and eventually estimating discharge

    # drc = glue('data/neon/neon/raw/surfaceElev_sensorpos/{s}',
    #     s=set_details$site_name)
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

}

#water quality: READY
#. handle_errors
process_0_20288 <- function(set_details, network, domain){

    data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
        site=set_details$site_name, startdate=set_details$component,
        enddate=set_details$component, package='basic', check.size=FALSE)

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}/{c}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name, c=set_details$component)

    serialize_list_to_dir(data_pile, raw_data_dest)

}

#precip: NOT STARTED
#. handle_errors
process_0_ <- function(set_details, network, domain){

}

#precip chem: NOT STARTED
#. handle_errors
process_0_ <- function(set_details, network, domain){

}

#munge kernels ####

#chem: READY
#. handle_errors
process_1_20093 <- function(network, domain, ms_prodname, site_name, component){
    # ms_prodname=prod; site_name=site; component=in_comp

    # #NEON has no record of what flags might be encountered here, so build some lists
    # # saveRDS(list(shipmentWarmQF=c(), externalLabDataQF=c(), sampleCondition=c(),
    # #         analyteUnits=c(), analyte=c()),
    # #     'data/neon/temp/20093_variants.rds')
    #
    # v = readRDS('data/neon/neon/temp/20093_variants.rds')
    # v = list(shipmentWarmQF=c(set$shipmentWarmQF, v$shipmentWarmQF),
    #     externalLabDataQF=c(set$externalLabDataQF, v$externalLabDataQF),
    #     sampleCondition=c(set$sampleCondition, v$sampleCondition),
    #     vars=c(paste(set$analyte, set$analyteUnits), v$vars))
    # saveRDS(v, 'data/neon/neon/temp/20093_variants.rds')
    # table(v$vars)

    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
        n=network, d=domain, p=ms_prodname, s=site_name, c=component)

    rawfiles = list.files(rawdir)

    #this has alc and anc, which we want!
    relevant_file1 = 'swc_domainLabData.feather'
    if(relevant_file1 %in% rawfiles){
        #build this!
        NULL
    }

    relevant_file2 = 'swc_externalLabDataByAnalyte.feather'
    if(relevant_file2 %in% rawfiles){
        #out_sub = join(out_sub, this) #do this too!
        #then fix stop message below

        rawd = read_feather(glue(rawdir, '/', relevant_file2))

        out_sub = rawd %>%
            select(collectDate, analyte, analyteConcentration, analyteUnits,
                shipmentWarmQF, externalLabDataQF, sampleCondition)
    }

    if(! exists('out_sub')){
        stop(glue('No external lab data available ',
            '(still gotta parse domain lab data)')) #uncomment next when resolved

        # return(generate_ms_exception(glue('No external lab data available ',
        #     '(still gotta parse domain lab data)')))
    }

    out_sub = out_sub %>%
        filter(
            shipmentWarmQF == 0,
            externalLabDataQF != paste0('formatChange|legacyData|Preliminary ',
                'method: UV absorbance not water blank subtracted'),
            sampleCondition == 'GOOD',
            analyte != 'TSS - Dry Mass') %>%
        select(-analyteUnits, -shipmentWarmQF, -externalLabDataQF,
            -sampleCondition) %>%
        group_by(collectDate, analyte) %>%
        summarize(analyteConcentration = mean(analyteConcentration, na.rm=TRUE)) %>%
        ungroup() %>%
        tidyr::spread(analyte, analyteConcentration) %>%
        mutate_at(vars(one_of('ANC')), function(x) x / 1000) %>% #meq/L -> eq/L
        mutate_at(vars(one_of('conductivity')), function(x) x / 1e6) %>% #uS/cm -> S/cm
        mutate(collectDate = lubridate::force_tz(collectDate, 'UTC')) %>%
        rename_all(dplyr::recode, siteID='site_name', collectDate='datetime',
            conductivity='spCond', `NH4 - N`='NH4_N', `NO2 - N`='NO2_N',
            `NO3+NO2 - N`='NO3_NO2_N', `Ortho - P`='PO4_P',
            `UV Absorbance (250 nm)`='abs250',
            `UV Absorbance (280 nm)`='abs280') %>%
        select(site_name, datetime, everything())

    return(out_sub)
}

#nitrate: READY
#. handle_errors
process_1_20033 <- function(network, domain, ms_prodname, site_name, component){
    # ms_prodname=prod; site_name=site; component=in_comp

    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
        n=network, d=domain, p=ms_prodname, s=site_name, c=component)

    rawfiles = list.files(rawdir)
    # write_neon_readme(rawdir, dest='/tmp/neon_readme.txt')
    # varkey = write_neon_variablekey(rawdir, dest='/tmp/neon_varkey.csv')

    relevant_file1 = 'NSW_15_minute.feather'
    if(relevant_file1 %in% rawfiles){
        rawd = read_feather(glue(rawdir, '/', relevant_file1))
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(out_sub$finalQF == 1)){
        return(generate_ms_exception('All records failed QA'))
    }

    updown = determine_upstream_downstream(out_sub)
    N_mass = calculate_molar_mass('N')

    out_sub = out_sub %>%
        mutate(
            site_name=paste0(siteID, updown), #append "-up" to upstream site_names
            startDateTime = lubridate::force_tz(startDateTime, 'UTC'), #GMT -> UTC
            surfWaterNitrateMean = surfWaterNitrateMean * N_mass / 1000) %>% #uM/L NO3 -> mg/L N
        filter(finalQF == 0) %>% #remove flagged records
        group_by(startDateTime, site_name) %>%
        summarize(surfWaterNitrateMean = mean(surfWaterNitrateMean, na.rm=TRUE)) %>%
        ungroup() %>%
        select(site_name, datetime=startDateTime, NO3_N=surfWaterNitrateMean)

    return(out_sub)
}

#par: READY
#. handle_errors
process_1_20042 <- function(network, domain, ms_prodname, site_name, component){
    
    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                  n=network, d=domain, p=ms_prodname, s=site_name, c=component)
    
    rawfiles = list.files(rawdir)
    # write_neon_readme(rawdir, dest='/tmp/neon_readme.txt')
    # varkey = write_neon_variablekey(rawdir, dest='/tmp/neon_varkey.csv')
    
    relevant_file1 = 'PARWS_5min.feather'
    
    if(relevant_file1 %in% rawfiles){
        out_sub = read_feather(glue(rawdir, '/', relevant_file1))
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }
    
    if(all(out_sub$PARFinalQF == 1)){
        return(generate_ms_exception('All records failed QA'))
    }
    
    updown = determine_upstream_downstream(out_sub)
    
    out_sub = out_sub %>%
        mutate(
            site_name=paste0(siteID, updown), #append "-up" to upstream site_names
            startDateTime = lubridate::force_tz(startDateTime, 'UTC')) %>% #GMT -> UTC 
        filter(PARFinalQF == 0) %>% #remove flagged records
        group_by(startDateTime, site_name) %>%
        summarize(PARMean = mean(PARMean, na.rm=TRUE)) %>%
        ungroup() %>%
        select(site_name, datetime=startDateTime, PAR=PARMean)
    
    return(out_sub)
}

#water temp: READY
#. handle_errors
process_1_20053 <- function(network, domain, ms_prodname, site_name, component){
    
    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                  n=network, d=domain, p=ms_prodname, s=site_name, c=component)
    
    rawfiles = list.files(rawdir)
    # write_neon_readme(rawdir, dest='/tmp/neon_readme.txt')
    # varkey = write_neon_variablekey(rawdir, dest='/tmp/neon_varkey.csv')
    
    relevant_file1 = 'TSW_5min.feather'
    
    if(relevant_file1 %in% rawfiles){
        out_sub = read_feather(glue(rawdir, '/', relevant_file1))
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }
    
    if(all(out_sub$finalQF == 1)){
        return(generate_ms_exception('All records failed QA'))
    }
    
    updown = determine_upstream_downstream(out_sub)
    
    out_sub = out_sub %>%
        mutate(
            site_name=paste0(siteID, updown), #append "-up" to upstream site_names
            startDateTime = lubridate::force_tz(startDateTime, 'UTC')) %>% #GMT -> UTC 
        filter(finalQF == 0) %>% #remove flagged records
        group_by(startDateTime, site_name) %>%
        summarize(surfWaterTempMean = mean(surfWaterTempMean, na.rm=TRUE)) %>%
        ungroup() %>%
        select(site_name, datetime=startDateTime, temp=surfWaterTempMean)
    
    return(out_sub)
}

#air pres: PENDING (needed file: BP_30min.feather; needed column: staPresMean; flag column: staPresFinalQF)
#. handle_errors
process_1_00004 <- function(network, domain, ms_prodname, site_name, component){
    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                  n=network, d=domain, p=ms_prodname, s=site_name, c=component)
    
    rawfiles = list.files(rawdir)
    # write_neon_readme(rawdir, dest='/tmp/neon_readme.txt')
    # varkey = write_neon_variablekey(rawdir, dest='/tmp/neon_varkey.csv')
    
    relevant_file1 = 'BP_30min.feather'
    
    if(relevant_file1 %in% rawfiles){
        out_sub = read_feather(glue(rawdir, '/', relevant_file1))
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }
    
    if(all(out_sub$finalQF == 1)){
        return(generate_ms_exception('All records failed QA'))
    }
    
    updown = determine_upstream_downstream(out_sub)
    
    out_sub = out_sub %>%
        mutate(
            site_name=paste0(siteID, updown), #append "-up" to upstream site_names
            startDateTime = lubridate::force_tz(startDateTime, 'UTC')) %>% #GMT -> UTC 
        filter(finalQF == 0) %>% #remove flagged records
        group_by(startDateTime, site_name) %>%
        summarize(surfWaterTempMean = mean(surfWaterTempMean, na.rm=TRUE)) %>%
        ungroup() %>%
        select(site_name, datetime=startDateTime, temp=surfWaterTempMean)
    
    return(out_sub)
}

#gases: PENDING (started)
#. handle_errors
process_1_20097 <- function(network, domain, ms_prodname, site_name, component){

    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
        n=network, d=domain, p=ms_prodname, s=site_name, c=component)

    rawfiles = list.files(rawdir)
    # write_neon_readme(rawdir, dest='/tmp/neon_readme.txt')
    # varkey = write_neon_variablekey(rawdir, dest='/tmp/neon_varkey.csv')

    relevant_file1 = 'sdg_externalLabData.feather'
    if(relevant_file1 %in% rawfiles){
        rawd = read_feather(glue(rawdir, '/', relevant_file1))
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    if(all(out_sub$gasCheckStandardQF == 1)){
        return(generate_ms_exception('All records failed QA'))
    }

    updown = determine_upstream_downstream(out_sub)

    #these are the columns we want:
    # collectDate, concentrationCH4, concentrationCO2, concentrationN2O

    #still needs to be built...
}

#stage: PENDING (not yet needed. waiting on neon)
process_1_20016 <- function(network, domain, ms_prodname, site_name, component){
    NULL
}

#waterqual: PENDING (started)
process_1_20288 <- function(network, domain, ms_prodname, site_name, component){

    rawdir = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
        n=network, d=domain, p=ms_prodname, s=site_name, c=component)

    rawfiles = list.files(rawdir)
    # write_neon_readme(rawdir, dest='/tmp/neon_readme.txt')
    # varkey = write_neon_variablekey(rawdir, dest='/tmp/neon_varkey.csv')

    relevant_file1 = 'waq_instantaneous.feather'
    if(relevant_file1 %in% rawfiles){
        rawd = read_feather(glue(rawdir, '/', relevant_file1))
    } else {
        return(generate_ms_exception('Relevant file missing'))
    }

    #LOTS OF FLAG COLUMNS FOR THIS PRODUCT. WILL NEED A MORE ELABORATE FLAG FILTER
    # if(){
    #     return(generate_ms_exception('All records failed QA'))
    # }

    #MIGHT NOT WORK
    updown = determine_upstream_downstream(out_sub)

    #THIS CAN HELP WITH COLUMN SELECTION (COLS WITH ASSOC FLAGS ARE USEFUL COLS)
    cn = colnames(out_sub)
    cn_base = na.omit(stringr::str_match(cn, '(.*?)FinalQF$')[,2])
    cn_keep = c(cn_base,
        paste0(cn_base, 'FinalQF'),
        paste0(cn_base, 'ExpUncert'))

    #dissolvedOxygenSaturation doesn't follow the naming convention of the others.
    #rawCalibratedfDOM is additional.
    #still gotta do the usual time zone conversion, unit conversion, etc.
    out_sub = out_sub %>%
        mutate(site_name=paste0(site_name, updown)) %>%
        select(site_name, one_of('startDate', 'startDateTime'), #naming discrepancy
            one_of(cn_keep), dissolvedOxygenSaturation,
            rawCalibratedfDOM) %>%
        rename_all(dplyr::recode, startDate='startDateTime') #rename only if column exists

    return(out_sub)
}
