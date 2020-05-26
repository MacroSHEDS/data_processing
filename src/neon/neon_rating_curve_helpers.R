get_sensor_positions = function(site_id){

    sp = tibble()
    monthfiles = list.files(glue('raw/surfaceElev_20016/{s}', s=site_id))

    for(m in monthfiles){
        sp0 = read_feather(glue('raw/surfaceElev_sensorpos/{s}/{m}',
            s=site_id, m=m))
        sp = bind_rows(sp, sp0)
    }

    sp = sp %>%
        mutate(
            horizontalPosition = substr(HOR.VER, 1, 3),
            referenceStart = as.POSIXct(referenceStart, tz='UTC')) %>%
        select(one_of('horizontalPosition', 'referenceElevation',
            'referenceStart')) %>%
        distinct() %>%
        group_by(horizontalPosition, referenceStart) %>%
        filter(referenceElevation == first(referenceElevation)) %>%
        ungroup() %>%
        filter(! is.na(referenceStart))

    return(sp)
}

get_surface_elevation = function(site_id){

    el = tibble()
    monthfiles = list.files(glue('raw/surfaceElev_20016/{s}', s=site_id))

    for(m in monthfiles){
        el0 = read_feather(glue('raw/surfaceElev_20016/{s}/{m}',
            s=site_id, m=m))
        el = bind_rows(el, el0)
    }

    return(el)
}

calc_stage = function(sens_pos_df, surface_elev_df){

    hp = sort(unique(sens_pos_df$horizontalPosition))
    el_rebuild = tibble()

    for(j in 1:length(hp)){

        el_hpos = surface_elev_df %>%
            filter(horizontalPosition == hp[j])

        sensdates = pull(el_hpos, startDateTime)

        refdates = sens_pos_df %>%
            filter(horizontalPosition == hp[j]) %>%
            arrange(referenceStart) %>%
            pull(referenceStart)

        refcol = as.POSIXct(as.character(cut(sensdates,
            breaks=c(
                # as.POSIXct('1900-01-01', tz='UTC'),
                refdates,
                as.POSIXct('2100-01-01', tz='UTC')
            ), include.lowest=TRUE, labels=refdates)), tz='UTC')

        el_rebuild = el_hpos %>%
            bind_cols(list(refdate=refcol)) %>%
            bind_rows(el_rebuild)
    }

    stage_df = el_rebuild %>%
        arrange(horizontalPosition, startDateTime) %>%
        left_join(sens_pos_df,
            by=c('refdate'='referenceStart', 'horizontalPosition')) %>%
        mutate(stage = surfacewaterElevMean - referenceElevation)

    #remove points that don't pass QC. pare down columns
    stage_df = stage_df %>%
        filter(sWatElevFinalQF == 0) %>%
        select(-refdate, -surfacewaterElevMean, -referenceElevation,
            -sWatElevFinalQF, -verticalPosition)

    return(stage_df)
}

# x=site_zq; y=sc; rollkey_x='startDate'; rollkey_y='startDateTime'
left_rolljoin = function(x, y, rollkey_x, rollkey_y){

    x = as.data.table(x)
    y = as.data.table(y)

    #forward rolling join by datetime
    setkeyv(x, rollkey_x)
    setkeyv(y, rollkey_y)
    joined = y[x, roll=TRUE]

    joined = as_tibble(joined)

    return(joined)
}

#watch out. this rolls speccond values forward by datetimes till they match up
#with the datetimes in your zq frame. they might roll a long way.
merge_speccond_zq = function(site_id, site_zq_df){

    wq_files = list.files(glue('raw/waterqual_20288/{s}', s=site_id))
    wq = tibble()
    for(f in wq_files){
        wq = read_feather(glue('raw/waterqual_20288/{s}/{f}',
                s=site_id, f=f)) %>%
            bind_rows(wq)
    }

    #remove wonky data (deal with name inconsistency); select useful columns
    if('specificConductanceFinalQF' %in% colnames(wq) &&
        'specificCondFinalQF' %in% colnames(wq)){

        sc = wq %>%
            filter(
                is.na(specificCondFinalQF) | specificCondFinalQF == 0,
                is.na(specificConductanceFinalQF) |
                    specificConductanceFinalQF == 0,
                ! (is.na(specificCondFinalQF) &
                    is.na(specificConductanceFinalQF)),
                ! is.na(specificConductance)) %>%
            select(startDateTime, specificConductance) %>%
            mutate(startDateTime = lubridate::force_tz(startDateTime,
                tzone='UTC'))

    } else if('specificConductanceFinalQF' %in% colnames(wq)){

        sc = wq %>%
            filter(
                is.na(specificConductanceFinalQF) |
                    specificConductanceFinalQF == 0,
                ! is.na(specificConductanceFinalQF),
                ! is.na(specificConductance)) %>%
            select(startDateTime, specificConductance) %>%
            mutate(startDateTime = lubridate::force_tz(startDateTime,
                tzone='UTC'))

    } else if('specificCondFinalQF' %in% colnames(wq)){

        sc = wq %>%
            filter(
                is.na(specificCondFinalQF) | specificCondFinalQF == 0,
                ! is.na(specificCondFinalQF),
                ! is.na(specificConductance)) %>%
            select(startDateTime, specificConductance) %>%
            mutate(startDateTime = lubridate::force_tz(startDateTime,
                tzone='UTC'))
    }

    #forward rolling join, leverages data.table
    site_zq_withsc = left_rolljoin(site_zq_df, sc, 'startDate', 'startDateTime')

    return(site_zq_withsc)
}

get_zq = function(filepath){

    #this product includes variables needed for generating rating curves.
    #it also includes velocity, width, etc.
    # prodcode = 'DP1.20048.001'

    # data_pile = neonUtilities::loadByProduct(prodcode,
    #     site='all', startdate=NA, enddate=NA,
    #     package='basic', check.size=TRUE)
    # saveRDS(data_pile, 'zq_data_temp.rds')
    data_pile = readRDS(filepath)

    # readr::write_lines(data_pile$readme_20048$X1, '/tmp/neon_readme.txt')

    useful_columns = c('siteID', 'stationID', 'totalDischarge', 'totalDischargeUnits',
        'streamStage', 'streamStageUnits', 'startDate')

    zq1 = select(data_pile$dsc_fieldData, one_of(useful_columns))
    zq2 = select(data_pile$dsc_fieldDataADCP, one_of(useful_columns))

    zq = zq1 %>%
        bind_rows(zq2) %>%
        mutate(
            totalDischarge = ifelse(totalDischargeUnits == 'litersPerSecond',
                totalDischarge / 1000, totalDischarge), #convert to cms
            streamStage = ifelse(streamStageUnits == 'centimeter',
                streamStage / 100, streamStage), #convert to m
            siteID = ifelse(is.na(siteID), stationID, siteID),
            startDate = lubridate::force_tz(startDate, tzone='UTC')) %>%
        select(-streamStageUnits, -totalDischargeUnits) %>%
        filter(
            ! is.na(streamStage) & ! is.na(totalDischarge),
            streamStage < 1.5 | siteID == 'FLNT',
            totalDischarge > 0)

    return(zq)
}

fit_zq = function(zq_df, fit_form='exponential'){

    Z = zq_df$streamStage
    Q = zq_df$totalDischarge

    if(fit_form == 'exponential'){
        mod = nls(Q ~ (a * exp(b * Z)), start=list(a=0.01, b=1))
    } else if(fit_form == 'power'){
        mod = nls(Q ~ (a * Z^b), start=list(a=1, b=1))
    }

    return(mod)
}

plot_stage = function(stage_df, sens_pos_df, label){

    hp = sort(unique(sens_pos_df$horizontalPosition))
    cols = brewer.pal(8, 'Paired')

    plot(stage_df$startDateTime, stage_df$stage, type='n', xlab='Time (UTC)',
        ylab='Stage (m)', main=label)

    for(j in 1:length(hp)){
        stage_df_part = filter(stage_df, horizontalPosition == hp[j])
        points(stage_df_part$startDateTime, stage_df_part$stage, col=cols[j],
            pch='.', bty='l')
    }
}

# q_df =stg; sens_pos_df=sp; label=site
plot_discharge = function(q_df, sens_pos_df, label){

    hp = sort(unique(sens_pos_df$horizontalPosition))
    cols = brewer.pal(8, 'Paired')

    plot(q_df$startDateTime, q_df$discharge_cms, type='n', xlab='Time (UTC)',
        ylab='Discharge (cms)', main=label)
    # plot(q_df$startDateTime, q_df$discharge_cms, type='n', xlab='Time (UTC)',
    #     ylab='Discharge (cms)', main=label, ylim=c(0, 2))

    for(j in 1:length(hp)){
        q_df_part = filter(q_df, horizontalPosition == hp[j])
        lines(q_df$startDateTime, q_df$discharge_cms,
            col=cols[j], bty='l')
    }
}

plot_zq = function(zq_df, zq_mod, label){

    Z = zq_df$streamStage
    Q = zq_df$totalDischarge

    plot(Z, Q, main=label, xlab='Stage (m)', ylab='Discharge (cms)')
    curveseq = seq(-1, 5, 0.01)
    lines(curveseq, predict(zq_mod, list(Z=curveseq)), col='gray30', lty=3, lwd=1.5)
}
