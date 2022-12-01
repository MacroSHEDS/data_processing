zz = read_combine_feathers(network = 'neon',
                           domain = 'neon',
                           prodname_ms = 'stream_quality__DP1.20288')
spc = zz %>%
    select(site_code, datetime, spCond)

sites = sort(unique(spc$site_code))
rc = ceiling(sqrt(length(sites)))

png(width=8, height=8, units='in', type='cairo', res=300,
    filename='data/neon/neon/ancillary/plots/spcond_20288_20200907.png')

par(mfrow=c(rc, rc), mar=c(0,0,0,0), oma=c(3, 3, 0, 0))
xlims = range(spc$datetime)
for(s in sites){
    spc_ = filter(spc, site_code == s) %>% arrange()
    plot(spc_$datetime, spc_$spCond, type='p', xlab='', ylab='',
        yaxt='n', bty='l', col='cadetblue', pch='.', xaxt='n',
        xlim=xlims)
    mtext(s, 3, line=-1.5)

    mtext('Time: 2016-07-29 through 2020-09-01', 1, outer=TRUE, line=1)
    mtext('Specific Conductance', 2, outer=TRUE, line=0.5)
    # mtext('Specific Conductance (uS/cm)', 2, outer=TRUE, line=0.5)
}

dev.off()
