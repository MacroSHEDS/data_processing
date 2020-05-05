# library(logging)
# library(tidyverse)
#
# glue = glue::glue

process_0_20093 = function(set_details){

    thisenv = environment()

    tryCatch({

        data_pile = neonUtilities::loadByProduct(set_details$prodcode_full,
            site=set_details$site_name, startdate=set_details$component,
            enddate=set_details$component, package='basic', check.size=FALSE)
        # write_lines(data_pile$readme_20093$X1, '/tmp/chili.txt')

        out_sub = data_pile$swc_externalLabDataByAnalyte %>%
            mutate(site_name=set_details$site_name) %>%
            select(collectDate, analyte, analyteConcentration, analyteUnits,
                shipmentWarmQF, externalLabDataQF, sampleCondition)

        # if('swc_externalLabData' %in% datasets){
        #     data_pile$swc_externalLabData %>%
        #         # filter(qa filtering here) %>%
        #         # convert_units_here() %>%
        #         select(collectDate, pH, externalConductance,
        #             externalANC, starts_with('water'), starts_with('total'),
        #             starts_with('dissolved'), uvAbsorbance250, uvAbsorbance284,
        #             shipmentWarmQF, externalLabDataQF)
        # }

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

        out_sub = select(data_pile$EOS_5_min, startDateTime,
            surfacewaterElevMean, sWatElevFinalQF, verticalPosition,
            horizontalPosition)

        dir.create('data_acquisition/data/neon/raw/surfaceElev_sensorpos',
            showWarnings=FALSE)

        f = glue('data_acquisition/data/neon/raw/surfaceElev_sensorpos/',
            'sensorpos_{s}.feather', s=set_details$site_name)

        if(file.exists(f)){
            sens_pos = feather::read_feather(f)
            sens_pos = bind_rows(data_pile$sensor_positions_20016, sens_pos) %>%
                distinct()
        } else {
            sens_pos = data_pile$sensor_positions_20016
        }

        feather::write_feather(sens_pos, f)

    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        assign('email_err_msg', TRUE, pos=.GlobalEnv)
        assign('out_sub', generate_ms_err(), pos=thisenv)
    })

    return(out_sub)
} #stage: waiting on NEON; fix updown
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
            select(site_name, startDateTime, one_of(cn_keep),
                dissolvedOxygenSaturation, rawCalibratedfDOM)

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

process_1_20093 = function(set_details){


    out_sub = data_pile$swc_externalLabDataByAnalyte %>%
        mutate(site_name=paste0(set_details$site_name, updown)) %>%
        # filter(qa filtering here) %>%
        # convert_units_here() %>%
        select(collectDate, analyte, analyteConcentration, analyteUnits,
            shipmentWarmQF, externalLabDataQF, sampleCondition)
    # spread()

    # # identify which dsets is/are present and mget them into the following list
    # out_sub = plyr::join_all(list(data1, data2, data3), type='full') %>%
    #     group_by(collectDate) %>%
    #     summarise_each(list(~ if(is.numeric(.)){
    #         mean(., na.rm = TRUE)
    #     } else {
    #         first(.)
    #     })) %>%
    #     ungroup() %>%
    #     mutate(datetime=as.POSIXct(collectDate, tz='UTC',
    #         format='%Y-%m-%dT%H:%MZ')) %>%
    #     select(-collectDate)
    #
    # return(out_sub)

    #---

    #     dir.create('data_acquisition/data/neon/raw/chemistry')
    #
    #     f = glue('data_acquisition/data/neon/raw/chemistry/sensorpos_{s}.feather',
    #         s=set_details$site_name)
    #
    #     if(file.exists(f)){
    #         sens_pos = feather::read_feather(f)
    #         sens_pos = bind_rows(data_pile$sensor_positions_20016, sens_pos) %>%
    #             distinct()
    #     } else {
    #         sens_pos = data_pile$sensor_positions_20016
    #     }
    #
    #     feather::write_feather(sens_pos, f)
    #
    # }, error=function(e){
    #     logging::logerror(e, logger='neon.module')
    #     assign('email_err_msg', TRUE, pos=.GlobalEnv)
    #     assign('out_sub', generate_ms_err(), pos=thisenv)
    # })
} #chem: just scraps

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
