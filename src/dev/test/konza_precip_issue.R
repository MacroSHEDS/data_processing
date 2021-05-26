raw_p = read_csv('data/lter/konza/raw/precipitation__4/sitename_NA/APT011.csv')
munged_p = read_feather('data/lter/konza/munged/precipitation__4/002C.feather')

#look at raw precip coverage ####

p = mutate(raw_p,
           datetime = mdy(RecDate),
           site_name = watershed,
           network = 'lter',
           domain = 'konza',
           var='GN_precipitation',
           val = as.numeric(ppt)) %>%
    select(datetime, site_name, network, domain, site_name, var, val)

earliest_year = lubridate::year(min(p$datetime[p$domain == dmn]))
nyears = current_year - earliest_year
yrcols = viridis(n = nyears)

sites = unique(p$site_name[p$domain == dmn])
if(dmn == 'arctic') sites = sites[! grepl('[0-9]', sites)]

plotrc = ceiling(sqrt(length(sites)))
# plotc = floor(sqrt(length(sites)))
doyseq = seq(1, 366, 30)
par(mfrow=c(plotrc, plotrc), mar=c(1,2,0,0), oma=c(0,0,2,0))

for(s in sites){

    plot(NA, NA, xlim=c(1, 366), ylim=c(0, nyears), xaxs='i', yaxs='i',
         ylab = '', xlab = '', yaxt='n', cex.axis=0.6, xaxt='n', xpd=NA)
    axis(1, doyseq, doyseq, tick=FALSE, line = -2, cex.axis=0.8)
    axis(2, 1:nyears, earliest_year:(current_year - 1), las=2, cex.axis=0.6,
         hadj=0.7)

    psub = p %>%
        filter(domain == dmn, site_name == s) %>%
        mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
               yr_offset = lubridate::year(datetime) - earliest_year)

    #         cat(paste(dmn, s, '\n', paste(unique(extract_var_prefix(psub$var)), collapse= ', '), '\n'))
    #         # cat(paste(dmn, s, '\n', ms_determine_data_interval(psub)))
    #     }
    # }

    lubridate::year(psub$datetime) <- 1972
    yrs = unique(psub$yr_offset)

    for(i in 1:length(yrs)){
        pss = psub %>%
            filter(yr_offset == yrs[i]) %>%
            arrange(doy)
        lines(pss$doy, c(scale(pss$val)) + pss$yr_offset, col=yrcols[i])
    }

    mtext(s, 3, outer=FALSE, line=-2)
}

#look at munged precip coverage (one site) ####

p = munged_p %>%
    mutate(network = 'lter',
           domain = 'konza')

earliest_year = lubridate::year(min(p$datetime[p$domain == dmn]))
nyears = current_year - earliest_year
yrcols = viridis(n = nyears)

sites = unique(p$site_name[p$domain == dmn])
if(dmn == 'arctic') sites = sites[! grepl('[0-9]', sites)]

plotrc = ceiling(sqrt(length(sites)))
# plotc = floor(sqrt(length(sites)))
doyseq = seq(1, 366, 30)
par(mfrow=c(plotrc, plotrc), mar=c(1,2,0,0), oma=c(0,0,2,0))

for(s in sites){

    plot(NA, NA, xlim=c(1, 366), ylim=c(0, nyears), xaxs='i', yaxs='i',
         ylab = '', xlab = '', yaxt='n', cex.axis=0.6, xaxt='n', xpd=NA)
    axis(1, doyseq, doyseq, tick=FALSE, line = -2, cex.axis=0.8)
    axis(2, 1:nyears, earliest_year:(current_year - 1), las=2, cex.axis=0.6,
         hadj=0.7)

    psub = p %>%
        filter(domain == dmn, site_name == s) %>%
        mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
               yr_offset = lubridate::year(datetime) - earliest_year)

    #         cat(paste(dmn, s, '\n', paste(unique(extract_var_prefix(psub$var)), collapse= ', '), '\n'))
    #         # cat(paste(dmn, s, '\n', ms_determine_data_interval(psub)))
    #     }
    # }

    lubridate::year(psub$datetime) <- 1972
    yrs = unique(psub$yr_offset)

    for(i in 1:length(yrs)){
        pss = psub %>%
            filter(yr_offset == yrs[i]) %>%
            arrange(doy)
        lines(pss$doy, c(scale(pss$val)) + pss$yr_offset, col=yrcols[i])
    }

    mtext(s, 3, outer=FALSE, line=-2)
}
