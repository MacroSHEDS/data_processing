library(tidyverse)
library(RColorBrewer)

#plot uncorrected stage data ####
# prodcode = 'DP1.20016.001'

ef = list.files('data_acquisition/data/neon/raw/surfaceElev/', full.names=TRUE)

surface_elev = tibble::tibble()
for(i in length(ef):1){
    surface_elev = feather::read_feather(ef[i]) %>%
        mutate(siteID=str_match(ef[i], '([A-Z]{4}).feather$')[,2]) %>%
        bind_rows(surface_elev)
}

# sum(is.na(surface_elev$surfacewaterElevMean)) / nrow(surface_elev)

# data_pile = neonUtilities::loadByProduct(prodcode,
#     site='ARIK', package='basic', check.size=FALSE, avg=5)
# saveRDS(data_pile, 'data_acquisition/data/neon/z_ARIK_temp.rds')
# data_pile = readRDS('data_acquisition/data/neon/z_ARIK_temp.rds')
# unique(data_pile$EOS_5_min$horizontalPosition)

sites = unique(surface_elev$siteID)
par(mfrow=c(3,3))
cols = brewer.pal(6, 'Paired')
for(s in sites[1:3]){

    site_sub = filter(surface_elev, siteID == s) %>%
        arrange(startDateTime)

    if(length(unique(site_sub$verticalPosition)) > 1) stop('vert pos change')

    hpos = unique(site_sub$horizontalPosition)
    ylims = range(site_sub$surfacewaterElevMean, na.rm=TRUE)
    for(i in 1:length(hpos)){

        pos_sub = filter(site_sub, horizontalPosition == hpos[i])

        if(i == 1){
            plot(pos_sub$startDateTime, pos_sub$surfacewaterElevMean,
                main=s, type='p', col=cols[i], pch='.', ylim=ylims)
        } else {
            points(pos_sub$startDateTime, pos_sub$surfacewaterElevMean,
                col=cols[i], pch='.')
        }
    }
}

#make rating curves ####

#this product includes variables needed for generating rating curves.
#it also includes velocity, width, etc.
prodcode = 'DP1.20048.001'

# data_pile = neonUtilities::loadByProduct(prodcode,
#     site='all', startdate=NA, enddate=NA,
#     package='basic', check.size=TRUE)
# saveRDS(data_pile, 'data_acquisition/data/neon/zq_data_temp.rds')
data_pile = readRDS('data_acquisition/data/neon/zq_data_temp.rds')

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

xlims = range(zq$streamStage[zq$siteID != 'FLNT'], na.rm=TRUE)
ylims = range(log(zq$totalDischarge[zq$siteID != 'FLNT']), na.rm=TRUE)

# par(mfrow=c(3, 3), oma=c(4, 4, 0, 0), mar=c(0, 0, 3, 0))
# for(i in 1:length(sites[1:9])){
par(mfrow=c(5, 5), oma=c(5, 6, 0, 0), mar=c(0, 0, 3, 0))
color = alpha('steelblue4', alpha=0.3)
curveseq = seq(-1, 5, 0.01)
for(i in 1:length(sites)){

    # pane = i %% 9

    site = sites[i]
    zqsub = filter(zq, siteID == site)
    Z = zqsub$streamStage
    Q = zqsub$totalDischarge
    mod = nls(Q ~ (a * exp(b * Z)), start=list(a=0.01, b=1)) #exp
    # mod = nls(Q ~ (a * Z^b), start=list(a=1, b=1)) #power


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

# dev.off()
