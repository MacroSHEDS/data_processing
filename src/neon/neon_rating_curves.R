library(tidyverse)
library(RColorBrewer)
library(feather)
library(glue)
library(lubridate)

setwd('~/git/macrosheds/data_acquisition/data/neon/')

#prepare rating curve data ####

#this product includes variables needed for generating rating curves.
#it also includes velocity, width, etc.
prodcode = 'DP1.20048.001'

# data_pile = neonUtilities::loadByProduct(prodcode,
#     site='all', startdate=NA, enddate=NA,
#     package='basic', check.size=TRUE)
# saveRDS(data_pile, 'zq_data_temp.rds')
data_pile = readRDS('zq_data_temp.rds')

# readr::write_lines(data_pile$readme_20048$X1, '/tmp/neon_readme.txt')

useful_columns = c('siteID', 'stationID', 'totalDischarge', 'totalDischargeUnits',
    'streamStage', 'streamStageUnits', 'startDate')#, 'endDate', 'collectDate',
# 'dataQF') #commented ones explored and deemed unnecessary

zq1 = select(data_pile$dsc_fieldData, one_of(useful_columns))
zq2 = select(data_pile$dsc_fieldDataADCP, one_of(useful_columns))

zq = zq1 %>%
    bind_rows(zq2) %>%
    mutate(
        totalDischarge=ifelse(totalDischargeUnits == 'litersPerSecond',
            totalDischarge / 1000, totalDischarge), #convert to cms
        streamStage=ifelse(streamStageUnits == 'centimeter',
            streamStage / 100, streamStage), #convert to m
        siteID=ifelse(is.na(siteID), stationID, siteID)) %>%
    select(-streamStageUnits, -totalDischargeUnits) %>%
    filter(
        ! is.na(streamStage) & ! is.na(totalDischarge),
        streamStage < 1.5 | siteID == 'FLNT',
        totalDischarge > 0)

sites = unique(zq$siteID)

# zq2 = zq %>%
#     filter(siteID == 'ARIK')
# plot(zq2$streamStage, log(zq2$totalDischarge), col=zq2$)

#convert surface elev to stage and plot it####
#surface elev acquired via macrosheds data_acquisition system

sites = list.dirs('raw/surfaceElev_20016/', full.names=FALSE)
sites = sites[nchar(sites) > 0]

png('plots/zqsites_stage2.png', height=10, width=10, units='in',
    type='cairo', res=300)
cols = brewer.pal(10, 'Paired')
par(mfrow=c(5, 5), oma=c(2, 2, 0, 0), mar=c(3, 3, 3, 0))

el = sp = tibble()
# s = sites[1]
for(s in sites){

    monthfiles = list.files(glue('raw/surfaceElev_20016/{s}', s=s))
    for(m in monthfiles){
        el0 = read_feather(glue('raw/surfaceElev_20016/{s}/{m}', s=s, m=m))
        sp0 = read_feather(glue('raw/surfaceElev_sensorpos/{s}/{m}',
            s=s, m=m))

        el = bind_rows(el, el0)
        sp = bind_rows(sp, sp0)
    }

    # sp = sp %>%
    #     select_if(~(any(! is.na(.)))) %>%
    #     select(-one_of('pitch', 'roll', 'azimuth'), -ends_with('Offset')) %>%
    #     distinct()
    sp = sp %>%
        mutate(
            horizontalPosition = substr(HOR.VER, 1, 3),
            referenceStart = as.POSIXct(referenceStart, tz='UTC')) %>%
        select(one_of('siteID', 'horizontalPosition', 'referenceElevation',
            'referenceStart')) %>%
        distinct() %>%
        group_by(horizontalPosition, referenceStart) %>%
        filter(referenceElevation == first(referenceElevation)) %>%
        ungroup() %>%
        filter(! is.na(referenceStart)) %>%
        select(-siteID)

    hp = sort(unique(sp$horizontalPosition))

    #merge sensor position data
    #could also somehow vertically sync all stages with current sensor position
    el_rebuild = tibble()
    for(j in 1:length(hp)){

        el_hpos = el %>%
            filter(horizontalPosition == hp[j])

        sensdates = pull(el_hpos, startDateTime)

        refdates = sp %>%
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

    el = el_rebuild %>%
        arrange(horizontalPosition, startDateTime) %>%
        left_join(sp, by=c('refdate'='referenceStart', 'horizontalPosition')) %>%
        mutate(stage = surfacewaterElevMean - referenceElevation)

    rm(el_rebuild)
    gc()

    el = el %>%
        filter(sWatElevFinalQF == 0) %>%
        select(-refdate, -surfacewaterElevMean, -referenceElevation,
            -sWatElevFinalQF, -verticalPosition)

    #begin plot part
    # xrng = range(el2$startDateTime, na.rm=TRUE)
    # yrng = range(el2$stage, na.rm=TRUE)

    plot(el2$startDateTime, el2$stage, type='n')
    for(j in 1:length(hp)){
        el_plot = filter(el2, horizontalPosition == hp[j])
        points(el_plot$startDateTime, el_plot$stage, col=cols[j], pch='.',
            bty='l')
    }
    mtext('Time (UTC)', 1, outer=TRUE, line=1)
    mtext('Stage (m)', 2, outer=TRUE, line=1)

}

#plot surface elevation series (obsolete) ####

png('plots/zqsites_surface_elev.png', height=10, width=10, units='in',
    type='cairo', res=300)

cols = brewer.pal(6, 'Paired')
par(mfrow=c(5, 5), oma=c(2, 2, 0, 0), mar=c(3, 3, 3, 0))
for(s in sites){

    site_sub = surface_elev %>%
        filter(
            siteID == s,
            sWatElevFinalQF == 0,
            ! is.na(surfacewaterElevMean)) %>%
        mutate(HOR.VER=paste(horizontalPosition, verticalPosition, sep='.')) %>%
        select(-horizontalPosition, -verticalPosition) %>%
        arrange(startDateTime)

    if(nrow(site_sub) == 0){
        plot(1, 1, type='n', main=s, xaxt='n', yaxt='n', bty='l')
        next
    }

    hvpos = unique(site_sub$HOR.VER)
    subdfs = list()
    for(i in 1:length(hvpos)){

        pos_sub = site_sub %>%
            filter(HOR.VER == hvpos[i]) %>%
            select(startDateTime, surfacewaterElevMean, HOR.VER)

        subdfs[[i]] = pos_sub
    }

    xlims = Reduce(range,
        lapply(subdfs, function(x) range(x$startDateTime, na.rm=TRUE)))
    ylims = Reduce(range,
        lapply(subdfs, function(x) range(x$surfacewaterElevMean, na.rm=TRUE)))

    for(i in 1:length(hvpos)){

        pos_sub = subdfs[[i]]

        if(i == 1){
            plot(pos_sub$startDateTime, pos_sub$surfacewaterElevMean, bty='l',
                main=s, type='p', col=cols[i], pch='.', ylim=ylims, xlim=xlims)
        } else {
            points(pos_sub$startDateTime, pos_sub$surfacewaterElevMean,
                col=cols[i], pch='.')
        }

        mtext('Time (UTC)', 1, outer=TRUE, line=1)
        mtext('Surface Elevation (m)', 2, outer=TRUE, line=1)
    }
}

dev.off()

#plot uncorrected stage series (obsolete) ####

png('plots/zqsites_stage.png', height=10, width=10, units='in',
    type='cairo', res=300)

cols = brewer.pal(6, 'Paired')
par(mfrow=c(5, 5), oma=c(2, 2, 0, 0), mar=c(3, 3, 3, 0))
for(s in sites){

    site_sub = surface_elev %>%
        filter(
            siteID == s,
            sWatElevFinalQF == 0,
            ! is.na(surfacewaterElevMean)) %>%
        mutate(HOR.VER=paste(horizontalPosition, verticalPosition, sep='.')) %>%
        select(-horizontalPosition, -verticalPosition) %>%
        arrange(startDateTime)

    if(nrow(site_sub) == 0){
        plot(1, 1, type='n', main=s, xaxt='n', yaxt='n', bty='l')
        next
    }

    spos = glue::glue('raw/sensorpos/sensorpos_{site}.feather', site=s) %>%
        feather::read_feather() %>%
        mutate(start=as.POSIXct(start, tz='GMT')) %>%
        select(start, HOR.VER, referenceLatitude, referenceLongitude,
            referenceElevation) %>%
        filter(! is.na(start)) %>%
        distinct() %>%
        arrange(start)

    hvpos = unique(site_sub$HOR.VER)
    subdfs = list()
    for(i in 1:length(hvpos)){

        changedates = spos %>%
            filter(HOR.VER == hvpos[i]) %>%
            distinct(start) %>%
            arrange(start) %>%
            pull(start)

        # cutgroups = c(as.POSIXct('1900-01-01', tz='UTC'), changedates,
        cutgroups = c(changedates, as.POSIXct('2100-01-01', tz='UTC'))

        pos_sub = site_sub %>%
            filter(HOR.VER == hvpos[i]) %>%
            select(startDateTime, surfacewaterElevMean, HOR.VER) %>%
            mutate(adjgroup=as.POSIXct(as.character(cut(startDateTime, cutgroups,
                labels=changedates)), tz='GMT'))  %>%
            left_join(
                select(spos, HOR.VER, start, referenceElevation),
                by=c('HOR.VER'='HOR.VER', 'adjgroup'='start')) %>%
            mutate(streamStage=surfacewaterElevMean - referenceElevation)

        subdfs[[i]] = pos_sub
    }

    xlims = Reduce(range,
        lapply(subdfs, function(x) range(x$startDateTime, na.rm=TRUE)))
    ylims = Reduce(range,
        lapply(subdfs, function(x) range(x$streamStage, na.rm=TRUE)))

    for(i in 1:length(hvpos)){

        pos_sub = subdfs[[i]]

        if(i == 1){
            plot(pos_sub$startDateTime, pos_sub$streamStage, bty='l',
                main=s, type='p', col=cols[i], pch='.', ylim=ylims, xlim=xlims)
        } else {
            points(pos_sub$startDateTime, pos_sub$streamStage,
                col=cols[i], pch='.')
        }

        # if(i %in% 20:24) axis(1)
        # # if((i - 1) %% 5 == 0 | i == 25) axis(2, las=2)
        # if((i - 1) %% 5 == 0) axis(2, las=2)

        mtext('Time (UTC)', 1, outer=TRUE, line=1)
        mtext('Stage (m)', 2, outer=TRUE, line=1)
    }
}

dev.off()

#plot harmonized stage series (obsolete template) ####

png('plots/zqsites_stage_harmonized.png', height=10, width=10, units='in',
    type='cairo', res=300)

cols = brewer.pal(6, 'Paired')
par(mfrow=c(5, 5), oma=c(2, 2, 0, 0), mar=c(3, 3, 3, 0))
for(s in sites){

    site_sub = surface_elev %>%
        filter(
            siteID == s,
            sWatElevFinalQF == 0,
            ! is.na(surfacewaterElevMean)) %>%
        mutate(HOR.VER=paste(horizontalPosition, verticalPosition, sep='.')) %>%
        select(-horizontalPosition, -verticalPosition) %>%
        arrange(startDateTime)

    if(nrow(site_sub) == 0){
        plot(1, 1, type='n', main=s, xaxt='n', yaxt='n', bty='l')
        next
    }

    spos = glue::glue('raw/sensorpos/sensorpos_{site}.feather', site=s) %>%
        feather::read_feather() %>%
        mutate(start=as.POSIXct(start, tz='GMT')) %>%
        select(start, HOR.VER, referenceLatitude, referenceLongitude,
            referenceElevation) %>%
        filter(! is.na(start)) %>%
        distinct() %>%
        arrange(start)

    #HERE: MULTIPLE REFERENCES ELEVATIONS LISTED FOR SOME SENSOR POSITIONS
    #AT THE SAME TIME. RESOLVE BEFORE CONTINUING
    current_positions = spos %>%
        group_by(HOR.VER) %>%
        filter(start == max(start))
        # summarize(date=max(start))

    #CREATE REPRODUCIBLE ERROR TO SEND TO NEON
    elev_all = neonUtilities::loadByProduct('DP1.20016.001',
        site='HOPB', startdate=NA, enddate=NA,
        package='basic', check.size=FALSE, avg=5)
    elev_201901 = neonUtilities::loadByProduct('DP1.20016.001',
        site='HOPB', startdate='2019-01', enddate='2019-02',
        package='basic', check.size=FALSE, avg=5)
    elev_all$sensor_positions_20016
    elev_201901$sensor_positions_20016
    # colnames(data_pile$EOS_5_min)
    # data_pile$EOS_5_min %>%
    #     mutate(HOR.VER=paste(horizontalPosition, verticalPosition, sep='.')) %>%
    #     distinct(HOR.VER)
    #     # filter(
    #     select(-horizontalPosition, -verticalPosition) %>%
    #     arrange(startDateTime)
    # saveRDS(data_pile, 'zq_data_temp.rds')
    # data_pile = readRDS('zq_data_temp.rds')

    # data_pile2 = neonUtilities::loadByProduct('DP1.20048.001',
    #     site='HOPB', startdate=NA, enddate=NA,
    #     package='basic', check.size=TRUE)
    # data_pile2$dsc_fieldData
    # # saveRDS(data_pile, 'zq_data_temp.rds')
    # data_pile = readRDS('zq_data_temp.rds')


    #...AND BACK TO THE UNMODIFIED, COPIED CODE FROM ABOVE THAT WILL BE
    #ADAPTED TO PLOT ELEVATION-HARMONIZED STAGE DATA
    hvpos = unique(site_sub$HOR.VER)
    subdfs = list()
    for(i in 1:length(hvpos)){

        changedates = spos %>%
            filter(HOR.VER == hvpos[i]) %>%
            distinct(start) %>%
            arrange(start) %>%
            pull(start)

        # cutgroups = c(as.POSIXct('1900-01-01', tz='UTC'), changedates,
        cutgroups = c(changedates, as.POSIXct('2100-01-01', tz='UTC'))

        pos_sub = site_sub %>%
            filter(HOR.VER == hvpos[i]) %>%
            select(startDateTime, surfacewaterElevMean, HOR.VER) %>%
            mutate(adjgroup=as.POSIXct(as.character(cut(startDateTime, cutgroups,
                labels=changedates)), tz='GMT'))  %>%
            left_join(
                select(spos, HOR.VER, start, referenceElevation),
                by=c('HOR.VER'='HOR.VER', 'adjgroup'='start')) %>%
            mutate(streamStage=surfacewaterElevMean - referenceElevation)

        subdfs[[i]] = pos_sub
    }

    xlims = Reduce(range,
        lapply(subdfs, function(x) range(x$startDateTime, na.rm=TRUE)))
    ylims = Reduce(range,
        lapply(subdfs, function(x) range(x$streamStage, na.rm=TRUE)))

    for(i in 1:length(hvpos)){

        pos_sub = subdfs[[i]]

        if(i == 1){
            plot(pos_sub$startDateTime, pos_sub$streamStage, bty='l',
                main=s, type='p', col=cols[i], pch='.', ylim=ylims, xlim=xlims)
        } else {
            points(pos_sub$startDateTime, pos_sub$streamStage,
                col=cols[i], pch='.')
        }

        # if(i %in% 20:24) axis(1)
        # # if((i - 1) %% 5 == 0 | i == 25) axis(2, las=2)
        # if((i - 1) %% 5 == 0) axis(2, las=2)

        mtext('Time (UTC)', 1, outer=TRUE, line=1)
        mtext('Stage (m)', 2, outer=TRUE, line=1)
    }
}

dev.off()

#make rating curves (still legit) ####

xlims = range(zq$streamStage[zq$siteID != 'FLNT'], na.rm=TRUE)
ylims = range(log(zq$totalDischarge[zq$siteID != 'FLNT']), na.rm=TRUE)

png('plots/neon_rating_curves.png', height=10, width=10, units='in',
    type='cairo', res=300)

par(mfrow=c(5, 5), oma=c(5, 6, 0, 0), mar=c(0, 0, 3, 0))
color = alpha('steelblue4', alpha=0.3)
curveseq = seq(-1, 5, 0.01)
for(i in 1:length(sites)){

    # pane = i %% 9

    site = sites[i]
    zqsub = filter(zq, siteID == site)
    Z = zqsub$streamStage
    Q = zqsub$totalDischarge
    mod = try(nls(Q ~ (a * exp(b * Z)), start=list(a=0.01, b=1))) #exp
    if('try-error' %in% class(mod)){
        mod = nls(Q ~ (a * Z^b), start=list(a=1, b=1)) #power
    }


    if(i == 25) {
        plot(Z, log(Q), main='', las=1, pch=20, col=color, cex=2, bty='n',
            yaxt='n', ylim=c(2, 8))
        legend('topleft', legend=site, bty='n', text.font=2, cex=1.2)
    } else {
        plot(Z, log(Q), main=site, col=color, xlim=xlims, ylim=ylims,
            xaxt='n', yaxt='n', pch=20, cex=2, bty='l')
    }

    # zsort = sort(Z)
    # plotseq = round(seq(zsort[1], zsort[length(zsort)], diff(range(Z)) / 50), 2)
    lines(curveseq, log(predict(mod, list(Z=curveseq))), col='gray30', lty=3, lwd=1.5)

    # if(pane %in% c(7, 8, 9)) axis(1)
    # if(pane %in% c(1, 4, 7)) axis(2)
    if(i %in% 20:24) axis(1)
    tcks = c(0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000)
    logtcks = log(tcks)
    if((i - 1) %% 5 == 0 | i == 25) axis(2, at=logtcks, labels=tcks, las=2)

    mtext('Stage (m)', 1, outer=TRUE, line=3)
    mtext('Log Discharge (cms)', 2, outer=TRUE, line=4)
}

dev.off()
