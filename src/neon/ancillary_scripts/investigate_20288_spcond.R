library(tidyverse)
library(RColorBrewer)
library(feather)
library(glue)
library(lubridate)
# library(data.table)

#set paths here
setwd('~/git/macrosheds/data_acquisition/data/neon/')
helper_funcs_path = '../../src/neon/ancillary_scripts/general_helpers.R'
water_qual_path = 'raw/waterqual_20288'

source(helper_funcs_path)

sites = dir(water_qual_path, full.names=FALSE, no..=TRUE)
cols = brewer.pal(6, 'Paired')

png('plots/spcond_20288.png', height=10, width=10, units='in',
    type='cairo', res=300)
par(mfrow=c(5, 7), oma=c(2, 2, 0, 0), mar=c(3, 3, 3, 0))

for(s in sites){
    wq = get_siteprod(s, water_qual_path)

    wq = wq %>%
        select(site_name, startDateTime, starts_with('specificCond'),
            sensorDepthFinalQF) %>%
        filter_at(vars(ends_with('QF')), all_vars(. != 1 | is.na(.))) %>%
        select(-ends_with('QF'), -one_of('specificConductanceExpUncert')) %>%
        mutate(startDateTime = force_tz(startDateTime, 'UTC')) %>%
        distinct() %>%
        arrange(site_name, startDateTime)

    if(nrow(wq) == 0 ||
            ! 'specificConductance' %in% colnames(wq) ||
            all(is.na(wq$specificConductance))){
        plot(1, 1, type='n', main=s, xaxt='n', yaxt='n', bty='l')
        next
    }

    site_locs = unique(wq$site_name)

    xlims = range(wq$startDateTime, na.rm=TRUE)
    ylims = range(wq$specificConductance, na.rm=TRUE)

    plot(wq$startDateTime[wq$site_name == site_locs[1]],
        wq$specificConductance[wq$site_name == site_locs[1]],
        bty='l', main=s, type='p', col=cols[1], pch=20, ylim=ylims,
        xlim=xlims, cex=0.7, xlab='', ylab='')
    if(length(site_locs) == 2){
        points(wq$startDateTime[wq$site_name == site_locs[2]],
            wq$specificConductance[wq$site_name == site_locs[2]],
            col=cols[2], pch=20, cex=0.7)
    }

    rm(wq)
    gc()
    print(paste('site', s, 'done'))
}

# data_pile = neonUtilities::loadByProduct('DP1.20288.001',
#     site='ARIK', startdate='2018-01', enddate='2018-01',
#     package='basic', check.size=TRUE)

mtext('Time (UTC)', 1, outer=TRUE, line=1)
mtext('Specific Conductance (unit not yet verified)', 2, outer=TRUE, line=0.5)

dev.off()
