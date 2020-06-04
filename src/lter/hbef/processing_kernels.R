
#retrieval kernels
process_0_20093 = function(set_details){

    thisenv = environment()

    tryCatch({

        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE)
        # write_lines(data_pile$readme_20093$X1, '/tmp/chili.txt')

        # data_pile$swc_domainLabData #this has alc and anc, which we want!
        if('swc_domainLabData' %in% names(data_pile)){
            #build this!
            # out_sub = this
            NULL
        }

        if('swc_externalLabDataByAnalyte' %in% names(data_pile)){
            #out_sub = join(out_sub, this) #do this too!
            #then fix stop message below
            out_sub = data_pile$swc_externalLabDataByAnalyte %>%
                mutate(site_name=set_details$site_name) %>%
                select(collectDate, analyte, analyteConcentration, analyteUnits,
                    shipmentWarmQF, externalLabDataQF, sampleCondition)
        }

        if(! exists('out_sub')){
            stop('No external lab data available (still gotta parse domain lab data)')
        }

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)

} #chem: ready (grab interval?)
process_0_20033 = function(set_details){

    thisenv = environment()

    tryCatch({

        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE)

        updown = determine_upstream_downstream(data_pile$NSW_15_minute)

        out_sub = data_pile$NSW_15_minute %>%
            mutate(site_name=paste0(set_details$site_name, updown)) %>%
            select(site_name, startDateTime, surfWaterNitrateMean, finalQF)

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)
} #nitrate: ready
process_0_20042 = function(set_details){

    thisenv = environment()

    tryCatch({

        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE, avg=5)

        updown = determine_upstream_downstream(data_pile$PARWS_5min)

        out_sub = data_pile$PARWS_5min %>%
            mutate(site_name=paste0(set_details$site_name, updown)) %>%
            select(site_name, startDateTime, PARMean, PARFinalQF)

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)
} #par: ready (interval?)
process_0_20053 = function(set_details){

    thisenv = environment()

    tryCatch({

        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE, avg=5)

        updown = determine_upstream_downstream(data_pile$TSW_5min)

        out_sub = data_pile$TSW_5min %>%
            mutate(site_name=paste0(set_details$site_name, updown)) %>%
            select(site_name, startDateTime, surfWaterTempMean, finalQF)

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)
} #water temp: ready
process_0_00004 = function(set_details){

    thisenv = environment()

    tryCatch({

        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE, avg=30)

        updown = determine_upstream_downstream(data_pile$BP_30min)

        out_sub = data_pile$BP_30min %>%
            mutate(site_name=paste0(set_details$site_name, updown)) %>%
            select(site_name, startDateTime, staPresMean, staPresFinalQF)

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)
} #airpres: ready
process_0_20097 = function(set_details){

    thisenv = environment()

    tryCatch({

        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE)

        if('sdg_externalLabData' %in% names(data_pile)){

            d = data_pile$sdg_externalLabData

            out_sub = d %>%
                mutate(site_name=set_details$site_name) %>%
                select(site_name, collectDate, concentrationCH4, concentrationCO2,
                    concentrationN2O, gasCheckStandardQF)
        } else {
            out_sub = generate_ms_exception('external lab gas analysis missing')
        }

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)
} #gases: ready (grab interval?)
process_0_20016 = function(set_details){

    thisenv = environment()

    tryCatch({
        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE, avg=5)

        updown = determine_upstream_downstream(data_pile$EOS_5_min)

        out_sub = select(data_pile$EOS_5_min, startDateTime,
            surfacewaterElevMean, sWatElevFinalQF, verticalPosition,
            horizontalPosition)

        drc = glue('data/neon/neon/raw/surfaceElev_sensorpos/{s}',
            s=set_details$site_name)
        dir.create(drc, showWarnings=FALSE, recursive=TRUE)
        f = glue(drc, '/{p}.feather', p=set_details$component)

        # if(file.exists(f)){
        #     sens_pos = feather::read_feather(f)
        #     sens_pos = bind_rows(data_pile$sensor_positions_20016, sens_pos) %>%
        #         distinct()
        # } else {
        #     sens_pos = data_pile$sensor_positions_20016
        # }

        feather::write_feather(data_pile$sensor_positions_20016, f)

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)
} #stage: waiting on NEON
process_0_20288 = function(set_details){

    thisenv = environment()

    tryCatch({
        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE)

        cn = colnames(data_pile$waq_instantaneous)
        cn_base = na.omit(stringr::str_match(cn, '(.*?)FinalQF$')[,2])
        cn_keep = c(cn_base,
            paste0(cn_base, 'FinalQF'),
            paste0(cn_base, 'ExpUncert'))

        updown = determine_upstream_downstream(data_pile$waq_instantaneous)

        #omitting sensorDepth, buoyNAflag, etc.
        #dissolvedOxygenSaturation doesn't follow the naming convention of the
        #others. rawCalibratedfDOM is additional.
        out_sub = data_pile$waq_instantaneous %>%
            mutate(site_name=paste0(set_details$site_name, updown)) %>%
            select(site_name, one_of('startDate', 'startDateTime'),
                one_of(cn_keep), dissolvedOxygenSaturation,
                rawCalibratedfDOM) %>%
            rename_all(dplyr::recode, startDate='startDateTime')

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)

} #waterqual: ready
process_0_ = function(set_details){

} #precip: not started
process_0_ = function(set_details){

} #precip chem: not started

#munge kernels
process_1_20093 = function(set, site_name){

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

    set = set %>%
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
        mutate(
            collectDate = lubridate::force_tz(collectDate, 'UTC'),
            site_name = site_name) %>%
        rename_all(dplyr::recode, collectDate='datetime', conductivity='spCond',
            `NH4 - N`='NH4_N', `NO2 - N`='NO2_N', `NO3+NO2 - N`='NO3_NO2_N',
            `Ortho - P`='PO4_P', `UV Absorbance (250 nm)`='abs250',
            `UV Absorbance (280 nm)`='abs280') %>%
        select(site_name, datetime, everything())

    return(set)
} #chem: ready

#obsolete kernels (for parts, maybe)
process_0_DP1.20093.001_api = function(d, set_details){

    data1_ind = intersect(grep("expanded", d$data$files$name),
        grep("fieldSuperParent", d$data$files$name))
    data2_ind = intersect(grep("expanded", d$data$files$name),
        grep("externalLabData", d$data$files$name))
    data3_ind = intersect(grep("expanded", d$data$files$name),
        grep("domainLabData", d$data$files$name))

    data1 = tryCatch({
        read.delim(d$data$files$url[data1_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data2 = tryCatch({
        read.delim(d$data$files$url[data2_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data3 = tryCatch({
        read.delim(d$data$files$url[data3_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })

    if(nrow(data1)){
        data1 = select(data1, siteID, collectDate, dissolvedOxygen,
            dissolvedOxygenSaturation, specificConductance, waterTemp,
            maxDepth)
    }
    if(nrow(data2)){
        data2 = select(data2, siteID, collectDate, pH, externalConductance,
            externalANC, starts_with('water'), starts_with('total'),
            starts_with('dissolved'), uvAbsorbance250, uvAbsorbance284,
            shipmentWarmQF, externalLabDataQF)
    }
    if(nrow(data3)){
        data3 = select(data3, siteID, collectDate, starts_with('alk'),
            starts_with('anc'))
    }

    out_sub = plyr::join_all(list(data1, data2, data3), type='full') %>%
        group_by(collectDate) %>%
        summarise_each(list(~ if(is.numeric(.)){
            mean(., na.rm = TRUE)
        } else {
            first(.)
        })) %>%
        ungroup() %>%
        mutate(datetime=as.POSIXct(collectDate, tz='UTC',
            format='%Y-%m-%dT%H:%MZ')) %>%
        select(-collectDate)

    return(out_sub)
} #chem: obsolete
process_0_DP1.20288.001_api = function(d, set_details){

    data_inds = intersect(grep("expanded", d$data$files$name),
        grep("instantaneous", d$data$files$name))

    site_with_suffixes = determine_upstream_downstream(d, data_inds,
        set_details)
    if(is_ms_err(site_with_suffixes)) return(site_with_suffixes)

    #process upstream and downstream sites independently
    for(j in 1:length(data_inds)){

        site_with_suffix = site_with_suffixes[j]

        #download data
        out_sub = read.delim(d$data$files$url[data_inds[j]], sep=",",
            stringsAsFactors=FALSE)

        out_sub = resolve_neon_naming_conflicts(out_sub,
            set_details_=set_details,
            replacements=c('specificCond'='specificConductance',
                'dissolvedOxygenSat'='dissolvedOxygenSaturation'))
        if(is_ms_err(out_sub)) return(out_sub)

        out_sub = mutate(out_sub,
            datetime=as.POSIXct(startDateTime,
                tz='UTC', format='%Y-%m-%dT%H:%M:%SZ'),
            site=site_with_suffix) %>%
            select(-startDateTime)
    }

    return(out_sub)
} #waterqual: obsolete
