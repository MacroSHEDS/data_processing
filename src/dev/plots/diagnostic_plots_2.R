# Run the setup portion of acquisition_master (the part before the main loop)
# to load necessary packages and helper functions.

#TODO: tabulate siteyears dropped from each summary due to presence of NAs
#change all the coverage plots so that they scale by site (like:
# qsub = q %>%
#     filter(domain == dmn, site_code == s) %>%
#     mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
#            yr_offset = lubridate::year(datetime) - earliest_year,
#            val = errors::drop_errors(val),
#            val = scale(val))

q_interp_limit = 5 #240
p_interp_limit = 5
# chem_interp_limit = 1 #not yet in use
flux_interp_limit = 14 #240

#setup ####

library(RColorBrewer)
library(viridis)

ws_areas <- site_data %>%
    filter(as.logical(in_workflow)) %>%
    select(network, domain, site_code, ws_area_ha)

pals = c('Blues', 'Greens', 'Greys', 'Reds', 'Purples',
         'PuRd', 'YlOrBr', 'OrRd', 'Oranges', 'YlGnBu')
palettes = RColorBrewer::brewer.pal.info %>%
    mutate(name = row.names(.)) %>%
    filter(name %in% pals)
    # filter(category == 'seq') %>%
palettes = palettes[order(pals), ]
palettes = bind_rows(palettes, palettes, palettes)
rownames(palettes) = 1:nrow(palettes)
nlty = ceiling(nrow(palettes) / 6)
palettes$lty = rep(1:nlty, each = 6, length.out = nrow(palettes))

domains = unique(site_data$domain[site_data$in_workflow == 1])
# dmncolors = c(RColorBrewer::brewer.pal(12, 'Paired'),
#               # RColorBrewer::brewer.pal(8, 'Accent')[6], #redund
#               RColorBrewer::brewer.pal(8, 'Dark2')[c(4, 8)],
#               RColorBrewer::brewer.pal(9, 'Pastel1')[7],
#               # RColorBrewer::brewer.pal(8, 'Pastel2')[-c(1:6)],
#               RColorBrewer::brewer.pal(8, 'Spectral')[7],
#               RColorBrewer::brewer.pal(8, 'Greens')[8])
dmncolors = c('brown4', 'brown1', 'blueviolet', 'blue4', 'dodgerblue3', 'blanchedalmond',
              'bisque4', 'khaki1', 'gray70', 'gray25', 'aquamarine4',
              'darkorange2', 'darkolivegreen4', 'darkolivegreen1', 'darkmagenta',
              'darkgoldenrod1', 'cyan3', 'deeppink',
              'darkred', 'green1', 'palevioletred4', 'peru', 'yellow', 'springgreen2',
              'mediumorchid3', 'white', 'skyblue', 'burlywood4', 'cornflowerblue')
# dmncolors = viridis::(n = length(domains))
# plot(1:15, 1:15, col=dmncolors, cex=5, pch=20) #test color distinctiveness
dmncolors = dmncolors[1:length(domains)]

dir.create(paste0('plots/diagnostic_plots_',  vsn), recursive = TRUE,
           showWarnings = FALSE)

# [OBSOLETE] all Q (line plot with log Y) ####

# q_dirs <- list_all_product_dirs('discharge',
#                                 location = 'data_acquisition')
# log_ticks = c(0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000)
#
# png(width=11, height=9, units='in', type='cairo', res=300,
#     filename=paste0('plots/diagnostic_plots_',  vsn, '/Q_all.png'))
#
# yr_seq = 1948:2021
# ylims = log(c(0.1, 10000))
# plot(yr_seq, rep(1, length(yr_seq)), ylim=ylims, type='n', yaxs='i',
#      xaxs = 'i', ylab='Runoff (mm/yr)', xlab='Year', xaxt='n',
#      main='Annual Runoff Totals', yaxt='n')
# axis(2, at=log(log_ticks), labels = log_ticks)
# axis(1, seq(1950, 2020, 5))
#
# legend_map = tibble(domain = character(), colors = list())
# plotted_site_tally = omitted_site_tally = 0
# for(i in 1:length(q_dirs)){
#
#     pd = q_dirs[i]
#
#     ntw_dmn_prd <- str_match(string = pd,
#                              pattern = '^data/(.+?)/(.+?)/derived/(.+?)$')[, 2:4]
#
#     # summarize(d, min=min(date), max=max(date), n=n())->ranges #just for testing
#     d <- list.files(pd, full.names = TRUE) %>%
#         purrr::map_dfr(read_feather) %>%
#         select(-val_err) %>%
#         # mutate(val = errors::set_errors(val, val_err),
#         group_by(site_code,
#                  date = lubridate::as_date(datetime)) %>%
#         summarize(val = mean(val, na.rm = TRUE),
#                   .groups = 'drop') %>%
#         ungroup() %>%
#         arrange(site_code, date) %>%
#         group_by(site_code) %>%
#         mutate(
#             val = if(sum(! is.na(val)) > 1)
#                 {
#                     imputeTS::na_interpolation(val,
#                                                maxgap = q_interp_limit)
#                 } else val) %>%
#         ungroup() %>%
#         mutate(year = lubridate::year(date),
#                val = val * 86400)
#
#     print(tapply(d$val, list(d$site_code, d$year), function(x) sum(! is.na(x))))
#     # tapply(d$val, list(d$site_code, d$year), function(x) sum(is.na(x)))
#
#     d = d %>%
#         group_by(site_code, year) %>%
#         summarize(val = sum(val, na.rm = FALSE),
#                   .groups = 'drop') %>%
#         arrange(site_code, year) %>%
#         mutate(network = ntw_dmn_prd[1],
#                domain = ntw_dmn_prd[2]) %>%
#         left_join(ws_areas,
#                   by = c('network', 'domain', 'site_code')) %>%
#         mutate(ws_area_mm2 = ws_area_ha * 10000 * 1e6,
#                val = log(val / 1000 * 1e9 / ws_area_mm2),
#                ntw_dmn_sit = paste(network, domain, site_code,
#                                    sep = ' > ')) %>%
#         select(year, ntw_dmn_sit, val) %>%
#         arrange(ntw_dmn_sit, year)
#
#     n_available_colors = palettes[i, 'maxcolors']
#     colors = RColorBrewer::brewer.pal(n = n_available_colors,
#                                       name = palettes[i, 'name'])
#
#     legend_map = bind_rows(legend_map,
#                            tibble(domain = paste(ntw_dmn_prd[1:2], collapse=' > '),
#                                   colors = list(colors[3:9])))
#
#     sites = unique(d$ntw_dmn_sit)
#     for(j in 1:length(sites)){
#         site = sites[j]
#         color_ind = j %% (n_available_colors - 2) + 2
#         color_ind = ifelse(color_ind == 2, n_available_colors, color_ind)
#         ds = filter(d, ntw_dmn_sit == !!site)
#         maxq = exp(max(ds$val, na.rm = TRUE))
#         if(i == 1 && j == 1){
#             print('These sites have annual runoff values over 2000 mm:')
#         }
#         if(maxq > 2000) message(site, ': ', round(maxq, 1))
#         if(any(! is.na(ds$val))){
#             plotted_site_tally = plotted_site_tally + 1
#         } else {
#             omitted_site_tally = omitted_site_tally + 1
#             print(paste(site, 'omitted'))
#         }
#         lines(ds$year, ds$val, col=alpha(colors[color_ind], 0.7), lwd = 2.5)
#     }
#
# }
# graphics::text(x=median(yr_seq), y=quantile(ylims, 0.9),
#     labels=paste0('sites: ', plotted_site_tally), adj=0.5)
#
# defpar = par(lend=1)
# for(k in 3:9){
#     if(k == 3){
#         legend(x=1949, y=log(50), legend=legend_map$domain, bty = 'n',
#                col = sapply(legend_map$colors, function(x) x[k]),
#                lty=1, seg.len=1, lwd=5, x.intersp=6.5)
#                # lty=1, seg.len=as.numeric(paste0('0.', k)), lwd=5)
#     } else {
#         legend(x=1946 + k, y=log(50), legend=rep('', nrow(legend_map)), bty = 'n',
#                # fill = sapply(legend_map$colors, function(x) x[k]),
#                col = sapply(legend_map$colors, function(x) x[k]),
#                lty=1, seg.len=1, lwd=5)
#     }
# }
# par(defpar)
#
# dev.off()

# Q by domain (line plots stitched, log Y) ####

seasonal_Q <- c('boulder', 'east_river', 'krycklan', 'arctic', 'bonanza', 'niwot')
q_dirs <- list_all_product_dirs('discharge',
                                location = 'data_acquisition')
log_ticks = c(0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000)

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/Q_by_domain_interp',
                q_interp_limit, '.pdf'))

yr_seq = 1948:2021
linetypes = 1:6
log_ticks = c(0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000)

for(i in 1:length(q_dirs)){

    pd = q_dirs[i]

    ntw_dmn_prd <- str_match(string = pd,
                             pattern = '^data/(.+?)/(.+?)/derived/(.+?)$')[, 2:4]

    d <- list.files(pd, full.names = TRUE) %>%
        purrr::map_dfr(read_feather) %>%
        select(-val_err) %>%
        # mutate(val = errors::set_errors(val, val_err),
        group_by(site_code,
                 date = lubridate::as_date(datetime)) %>%
        summarize(val = mean(val, na.rm = TRUE),
                  .groups = 'drop') %>%
        arrange(site_code, date) %>%
        group_by(site_code) %>%
        mutate(
            val = if(sum(! is.na(val)) > 1)
            {
                imputeTS::na_interpolation(val,
                                           maxgap = q_interp_limit)
            } else val) %>%
        ungroup()

    # gg = na_interpolation(dd$val, maxgap=40)
    # plot(dd$val, type='l')
    # plot(gg, type='l')
    #
    # yrcols = viridis(n = 39)
    # dd = filter(d, site_code == 'MARTINELLI') %>%
    #     mutate(doy = as.numeric(strftime(date, format = '%j', tz='UTC'))) %>%
    #     group_split(year = year(date)) %>% as.list()
    # plot(1, 1, type='n', xlim=c(1, 366), ylim=c(0, 100))
    # for(i in 1:length(dd)){
    #     yy = dd[[i]]
    #     lines(yy$doy, c(scale(yy$val)) + i, col=yrcols[i])
    # }

    # zz = filter(d, site_code == 'MARTINELLI', year(date) == 1994) %>% arrange(date)
    # zz = filter(d, site_code == 'ALBION', year(date) == 1999) %>% arrange(date)
    # plot(zz$date, zz$val)

    d <- d %>%
        mutate(year = lubridate::year(date),
               val = val * 86400) %>%
        filter(!is.na(val)) %>%
        group_by(site_code, year) %>%
        summarize(val = sum(val, na.rm = TRUE),
                  n = n(),
                  .groups = 'drop') %>%
        # arrange(site_code, year) %>%
        mutate(network = ntw_dmn_prd[1],
               domain = ntw_dmn_prd[2]) 

    if(ntw_dmn_prd[2] %in% seasonal_Q){
        d <- d %>%
            filter(n > 60)
    }
    if((!ntw_dmn_prd[2] %in% seasonal_Q) && ntw_dmn_prd[2] != 'mcmurdo'){
        d <- d %>%
            filter(n > 330)
    }
    if(ntw_dmn_prd[2] == 'mcmurdo'){
        d <- d %>%
            filter(n > 30)
    }
    d <- d %>%
        left_join(ws_areas,
                  by = c('network', 'domain', 'site_code')) %>%
        mutate(ws_area_mm2 = ws_area_ha * 10000 * 1e6,
               val = log(val / 1000 * 1e9 / ws_area_mm2),
               ntw_dmn_sit = paste(network, domain, site_code,
                                   sep = ' > ')) %>%
        select(year, ntw_dmn_sit, val) %>%
        arrange(ntw_dmn_sit, year)

    hi_q = d %>%
        mutate(val = exp(val),
               site = stringr::str_match(ntw_dmn_sit, '> (\\w+)$')[, 2],
               domain = stringr::str_match(ntw_dmn_sit, '> (\\w+) >')[, 2],
               # domain = stringr::str_split(ntw_dmn_sit, ' > ')[[1]][2],
               yearval = paste(year, round(val, 1), sep=': ')) %>%
        filter(val > 2000)

    if(nrow(hi_q)){
        sitechunks = split(hi_q, hi_q$site)
        for(sc in sitechunks){
            message(sc$domain[1], ' > ', sc$site[1], ':\n\t',
                    paste(sc$yearval, collapse = '\n\t'))
        }
    }

    legend_map = tibble(site = character(), color = character(), lty=numeric())
    n_available_colors = palettes[i, 'maxcolors']
    colors = RColorBrewer::brewer.pal(n = n_available_colors,
                                      name = palettes[i, 'name'])

    ylims = log(c(0.1, 10000))

    if(is.infinite(ylims[2])){
        plot(1, 1, type='n', xlab='', ylab='',
             main=paste0('Annual Runoff Totals (', ntw_dmn_prd[1],
                              ' > ', ntw_dmn_prd[2], ')'))
        graphics::text(x=1, y=1, labels='!! :O', adj=0.5)
        next
    }

    plot(yr_seq, rep(1, length(yr_seq)), ylim=ylims, yaxt='n',
         type='n', yaxs='i', xaxs = 'i', ylab='Runoff (mm/yr)', xlab='Year',
         xaxt='n', main=paste0('Annual Runoff Totals (', ntw_dmn_prd[1],
                               ' > ', ntw_dmn_prd[2], ')'))
    axis(1, seq(1950, 2020, 5))
    axis(2, at=log(log_ticks), labels = log_ticks)

    sites = unique(d$ntw_dmn_sit)
    j = 0
    while(j < length(sites)){
        j = j + 1
        site = sites[j]
        ds = filter(d, ntw_dmn_sit == !!site)
        if(all(is.na(ds$val))) next

        color_ind = j %% (n_available_colors - 3) + 3
        color_ind = ifelse(color_ind == 3, n_available_colors, color_ind)
        ltype = (j - 1) %/% length(linetypes) + 1
        legend_map = bind_rows(legend_map,
                               tibble(site = !!site,
                                      color = colors[color_ind],
                                      lty=ltype))
        lines(ds$year, ds$val, col=alpha(colors[color_ind], 0.7), lwd = 2.5,
              lty=ltype)
    }

    defpar = par(lend=1)
    if(length(legend_map$site)){
        legend(x=1949, y=quantile(ylims, 0.97), legend=legend_map$site, bty = 'n',
               col = legend_map$color,
               lty=legend_map$lty, seg.len=4, lwd=3)
    }

    par(defpar)
}

dev.off()

# concentration of Ca, Si, Cl, NO3-N, DOC and SO4 (box plot with log Y, dark) ####
stream_gauges <- site_data %>%
    filter(in_workflow == 1,
           site_type == 'stream_gauge') %>%
    pull(site_code)
chemvars = c('Ca', 'Si', 'Cl', 'NO3_N', 'DOC', 'SO4_S', 'Na', 'PO4_P')
# chemvars = c('Ca', 'Si', 'SiO2_Si', 'Cl', 'NO3_N', 'DOC', 'SO4_S')
ylim_maxes = log(c(10000, 10000, 100000, 10000, 10000, 10000, 10000, 10))
# ylim_maxes = log(c(455, 25, 25, 600, 20, 85, 600))
ylim_mins = log(rep(0.001, length(ylim_maxes)))
# ylim_mins = c(-10, -0.5, -0.5, -10, -0.5, -1, -10)
log_ticks = c(0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000, 100000)

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/chem_boxplot.pdf'))
par(mar=c(14, 4, 4, 1))

for(i in 1:length(chemvars)){

    chemvar = chemvars[i]
    unit = ms_vars$unit[ms_vars$variable_code == chemvar]
    if(chemvar == 'Si'){
        d1 = load_product('stream_chemistry', filter_vars=chemvar)
        d2 = load_product('stream_chemistry', filter_vars='SiO2_Si')
        sio2_dmns = unique(d2$domain)
        sio2_dmns = sio2_dmns[! sio2_dmns == 'arctic'] #arctic doesn't really have SiO2
        d2$var = sub('SiO2_Si', 'Si', d2$var)
        d = bind_rows(d1, d2)
    } else {
        d = load_product('stream_chemistry', filter_vars=chemvar)
    }
    
    # d <- d %>%
    #     filter(site_code %in% !!stream_gauges)

    d_by_max = d %>%
        filter(!is.na(val)) %>%
        group_by(network, domain, site_code) %>%
        summarize(maxval = max(val, na.rm = T),
                  medianval = median(val, na.rm = T),
                  n = n(),
                  .groups = 'drop') %>%
        mutate(ntw_dmn_sit = paste(network, domain, site_code,
                                   sep = ' > ')) %>%
        filter(n > 30)

    site_order = d_by_max$ntw_dmn_sit[order(d_by_max$medianval)]
    
    sites_with_data <- d_by_max$site_code

    d_boxplot = d %>%
        filter(site_code %in% !!sites_with_data) %>%
        mutate(val = errors::drop_errors(val)) %>%
        group_by(network, domain, site_code) %>%
        summarize(box_stats = list(boxplot.stats(val)),
                  .groups = 'drop') %>%
        mutate(color = dmncolors[match(domain, domains)],
               ntw_dmn_sit = paste(network, domain, site_code,
                                   sep = ' > '))

    included_domain_inds = which(domains %in% unique(d_by_max$domain))
    excluded_domains = domains[-included_domain_inds]
    included_domains = domains[included_domain_inds]
    included_dmncolors = dmncolors[included_domain_inds]

    d_boxplot = d_boxplot[rev(order(match(d_boxplot$ntw_dmn_sit, site_order))), ]

    nsites = nrow(d_boxplot)
    ylims = c(ylim_mins[i], ylim_maxes[i])
    plot(1:nsites, rep(0, nsites), ylim=ylims, type='n', yaxt='n',
         ylab=paste0(chemvar, ' (', unit, ')'), xlab='', xaxt='n', yaxs='i',
         main=paste0('Concentration (', chemvar, ')'), xlim=c(1, nsites))
    axis(2, at=log(log_ticks), labels = log_ticks)
    corners = par("usr")
    rect(corners[1], corners[3], corners[2], corners[4], col = 'black')

    for(j in 1:nsites){
        color = slice(d_boxplot, j) %>% pull(color)
        stats = (slice(d_boxplot, j) %>% pull(box_stats))[[1]]
        outliers = log(stats$out)
        box_whisk = log(stats$stats)
        segments(x0=j, x1=j, y0=box_whisk[1], y1=box_whisk[2], col=color,
                 lwd=2, lend=3)
        points(x=j, y=box_whisk[3], col=color, pch=20)
        segments(x0=j, x1=j, y0=box_whisk[4], y1=box_whisk[5], col=color,
                 lwd=2, lend=3)
        points(x=rep(j, length(outliers)), y=outliers, col=color, pch=20, cex=0.2)
    }

    axis_seq_1 = seq(1, nsites, 2)
    axis_seq_2 = seq(2, nsites, 2)
    axis(1, at=axis_seq_1, labels=d_boxplot$site_code[axis_seq_1],
         las=2, cex.axis=0.6, tcl=-0.6)
    axis(1, at=axis_seq_2, labels=d_boxplot$site_code[axis_seq_2],
         las=2, cex.axis=0.6, tcl=-0.6, line=6.5, tick=FALSE)
    axis(1, at=axis_seq_2, labels=rep('', length(axis_seq_2)), tcl=-7)
    legend('topright', legend=included_domains, lty=1, col=included_dmncolors, bty='n', lwd=3,
           text.col='white', ncol=ceiling(length(included_domains) / 7))
    newline_seq = try(seq(6, length(excluded_domains), 6), silent=TRUE)
    if(! inherits(newline_seq, 'try-error')){
        excluded_domains[newline_seq] = paste0('\n', excluded_domains[newline_seq])
    }
    graphics::text(x=quantile(1:nsites, 0.15),
                   y=quantile(ylims, 0.95), adj=0, col='green',
                   labels=paste0('Sites: ', length(unique(d_boxplot$ntw_dmn_sit))))
    graphics::text(x=quantile(1:nsites, 0.15),
                   y=quantile(ylims, 0.9), adj=0, col='pink',
                   labels=paste0('Missing domains: ',
                                 paste(excluded_domains, collapse = ', ')))
    if(chemvar == 'Si'){
        graphics::text(x=quantile(1:nsites, 0.15),
                       y=quantile(ylims, 0.8), adj=0, col='yellow',
                       labels=bquote(bold('Reporting SiO2-Si from these domains:' ~
                                     .(paste(sio2_dmns, collapse = ', ')))))
    }
}

dev.off()


# Q vs. flux of Ca, Si, Cl, NO3-N, DOC and SO4 (scatter plot with log axes, dark) ####

fluxvars = c('Ca', 'Si', 'Cl', 'NO3_N', 'DOC', 'SO4_S')
# ylim_maxes = log(c(1500, 300, 3000, 11000, 800, 4000, 700))
ylim_maxes = log(rep(11000, 6))
ylim_mins = log(rep(0.000001, 6))
xlim_maxes = log(rep(10000, 6))
# xlim_mins = log(c(10, 10, 10, 10, 10, 0.1, 10))
xlim_mins = log(rep(1, 6))
# ylim_maxes = c(455, 25, 25, 600, 20, 85, 600)
# ylim_mins = log(rep(0.1, length(fluxvars)))
# legend_position = c('topleft', 'topright', 'topright', 'topright', 'topleft', 'topright', 'topleft')
legend_position = rep('topleft', length(fluxvars))
log_ticks = c(0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000, 100000)

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/Qinterp', q_interp_limit,
                '_vs_FLUXinterp', flux_interp_limit, '.pdf'))
par(mar=c(14, 4, 4, 1))

q_dirs <- list_all_product_dirs('discharge',
                                location = 'data_acquisition')

flux_ranges = q_ranges = tibble()
for(k in 1:length(fluxvars)){

    fluxvar = fluxvars[k]

    plot(1, 1, xlim=c(xlim_mins[k], xlim_maxes[k]), ylim=c(ylim_mins[k], ylim_maxes[k]), type='n', yaxs='i',
         xaxs = 'i', ylab='Annual Flux (kg/ha)', xlab='Annual Runoff (mm)',
         main=paste0('Annual Q vs. Flux (', fluxvar, ')'), xaxt='n', yaxt='n')
    axis(1, at=log(log_ticks), labels = log_ticks, cex.axis=0.8)
    axis(2, at=log(log_ticks), labels = log_ticks, cex.axis=0.8)
    corners = par("usr")
    rect(corners[1], corners[3], corners[2], corners[4], col = 'black')

    if(fluxvar == 'Si'){
        d1 = load_product('stream_flux_inst_scaled', filter_vars=fluxvar)
        d2 = load_product('stream_flux_inst_scaled', filter_vars='SiO2_Si')
        sio2_dmns = unique(d2$domain)
        sio2_dmns = sio2_dmns[! sio2_dmns == 'arctic'] #arctic doesn't really have SiO2
        d2$var = sub('SiO2_Si', 'Si', d2$var)
        dflux = bind_rows(d1, d2)
    } else {
        dflux = load_product('stream_flux_inst_scaled', filter_vars=fluxvar)
    }

    included_domains = c()
    for(i in 1:length(q_dirs)){

        pd = q_dirs[i]

        ntw_dmn_prd = str_match(string = pd,
                                 pattern = '^data/(.+?)/(.+?)/derived/(.+?)$')[, 2:4]

        dflux_var_year = dflux %>%
            filter(domain == ntw_dmn_prd[2]) %>%
            mutate(ntw_dmn_sit = paste(network, domain, site_code,
                                       sep = ' > '),
                   date = lubridate::as_date(datetime),
                   # year = lubridate::year(datetime),
                   # month = lubridate::month(datetime),
                   val = errors::drop_errors(val)) %>%
            # group_by(ntw_dmn_sit, year, month) %>%
            # summarise(val = mean(val, na.rm = TRUE),
            #           .groups = 'drop') %>%

            group_by(ntw_dmn_sit, date) %>%
            summarize(val = mean(val, na.rm = TRUE),
                      .groups = 'drop') %>%
            mutate(year = year(date)) %>%

            arrange(ntw_dmn_sit, year) %>%
            group_by(ntw_dmn_sit) %>%
            mutate(
                val = if(sum(! is.na(val)) > 1)
                {
                    imputeTS::na_interpolation(val,
                                               maxgap = flux_interp_limit)
                } else val) %>%
            ungroup() %>%

            # mutate(days_in_month = case_when(month %in% c(1,3,5,7,8,10,12) ~ 31,
            #                                  month %in% c(4,6,9,11) ~ 30,
            #                                  month == 2 ~ 28)) %>%
            # mutate(val = val * days_in_month) %>%

            group_by(ntw_dmn_sit, year) %>%
            summarize(val = sum(val, na.rm = FALSE), #
                      .groups = 'drop') %>%
            tidyr::extract(col = ntw_dmn_sit,
                           into = c('network', 'domain', 'site_code'),
                           regex = '(.+?) > (.+?) > (.+)',
                           remove = FALSE) %>%
            left_join(ws_areas,
                      by = c('network', 'domain', 'site_code')) %>%
            mutate(val = log(val / ws_area_ha)) %>%
            select(year, ntw_dmn_sit, flux = val) %>%
            arrange(ntw_dmn_sit, year)

        dflow = list.files(pd, full.names = TRUE) %>%
            purrr::map_dfr(read_feather) %>%
            select(-val_err) %>%
            group_by(site_code,
                     date = lubridate::as_date(datetime)) %>%
            summarize(val = mean(val, na.rm = TRUE),
                      .groups = 'drop') %>%

            arrange(site_code, date) %>%
            group_by(site_code) %>%
            mutate(
                val = if(sum(! is.na(val)) > 1)
                {
                    imputeTS::na_interpolation(val,
                                               maxgap = q_interp_limit)
                } else val) %>%
            ungroup() %>%

            mutate(year = lubridate::year(date),
                   val = val * 86400) %>%
            group_by(site_code, year) %>%
            summarize(val = sum(val, na.rm = FALSE), #
                      .groups = 'drop') %>%
            arrange(site_code, year) %>%
            mutate(network = ntw_dmn_prd[1],
                   domain = ntw_dmn_prd[2]) %>%
            left_join(ws_areas,
                      by = c('network', 'domain', 'site_code')) %>%
            mutate(ws_area_mm2 = ws_area_ha * 10000 * 1e6,
                   val = log(val / 1000 * 1e9 / ws_area_mm2),
                   ntw_dmn_sit = paste(network, domain, site_code,
                                       sep = ' > ')) %>%
            select(year, ntw_dmn_sit, flow = val) %>%
            arrange(ntw_dmn_sit, year)

        d = left_join(dflux_var_year, dflow, by = c('ntw_dmn_sit', 'year')) %>%
            tidyr::extract(col = 'ntw_dmn_sit',
                           into = 'domain',
                           regex = '.+? > (.+?) > .+') %>%
            mutate(color = dmncolors[match(domain, domains)])

        if(any(! is.na(d$flux) & ! is.na(d$flow))){
            included_domains = c(included_domains, ntw_dmn_prd[2])
        }

        points(d$flow, d$flux, pch=1, cex=0.8, lwd=2, col=d$color)

        flux_ranges = bind_rows(flux_ranges,
                                tibble(network = ntw_dmn_prd[1],
                                       domain = ntw_dmn_prd[2],
                                       var = fluxvar,
                                       min = min(d$flux, na.rm = TRUE),
                                       max = max(d$flux, na.rm = TRUE)))
        q_ranges = bind_rows(q_ranges,
                             tibble(network = ntw_dmn_prd[1],
                                    domain = ntw_dmn_prd[2],
                                    var = paste0('Q_of_', fluxvar, '_flux_mmyr'),
                                    min = min(d$flow, na.rm = TRUE),
                                    max = max(d$flow, na.rm = TRUE)))
    }

    legend(legend_position[k], legend=included_domains, pch=1, cex=0.8, pt.lwd=2,
           col=dmncolors[match(included_domains, domains)], bty='n',
           text.col='white')

    if(chemvar == 'Si'){
        mtext(bquote(bold('Reporting SiO2-Si from these domains:' ~
                              .(paste(sio2_dmns, collapse = ', ')))),
              side=3, col='yellow', line=-2)
    }
}

dev.off()

# all flux (line plot with log Y) ####

fluxvars = c('Ca', 'Si', 'Cl', 'NO3_N', 'DOC', 'SO4_S')
# ylim_maxes = rep(10000, length(fluxvars))
# ylim_maxes = c(455, 25, 25, 600, 20, 85, 600)
# ylim_maxes = log(c(1500, 300, 3000, 11000, 800, 4000, 700))
ylim_maxes = log(rep(11000, 6))
# xlim_mins = log(c(10, 10, 10, 10, 10, 0.1, 10))
# ylim_maxes = c(3000, 500, 4000, 11000, 1000, 4000, 1000)
# ylim_mins = log(c(10, 0.5, 0.5, 10, 0.5, 1, 10)) * -1
ylim_mins = log(rep(0.00009, 6))
# ylim_mins = log(rep(0.1, length(fluxvars)))
legend_position = c('bottomleft', 'topleft', 'topleft', 'topleft', 'topleft', 'bottomleft')

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/flux_interp',
                flux_interp_limit, '.pdf'))
par(mar=c(14, 4, 4, 1))

q_dirs <- list_all_product_dirs('discharge',
                                location = 'data_acquisition')

yr_seq = 1948:2021
log_ticks = c(0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000)
for(k in 1:length(fluxvars)){

    fluxvar = fluxvars[k]

    plot(yr_seq, rep(1, length(yr_seq)), ylim=c(ylim_mins[k], ylim_maxes[k]),
         type='n', yaxs='i', xaxs = 'i', ylab='Flux (kg/ha/yr)', xlab='Year',
         xaxt='n', yaxt='n',
         main=paste0('Annual Flux (', fluxvar, ')'))
    axis(1, seq(1950, 2020, 5))

    if(fluxvar == 'Si'){
        d1 = load_product('stream_flux_inst_scaled', filter_vars=fluxvar)
        d2 = load_product('stream_flux_inst_scaled', filter_vars='SiO2_Si')
        sio2_dmns = unique(d2$domain)
        sio2_dmns = sio2_dmns[! sio2_dmns == 'arctic'] #arctic doesn't really have SiO2
        d2$var = sub('SiO2_Si', 'Si', d2$var)
        dflux = bind_rows(d1, d2)
    } else {
        dflux = load_product('stream_flux_inst_scaled', filter_vars=fluxvar)
    }

    axis(2, at=log(log_ticks), labels = log_ticks)

    included_domains = c()
    plotted_site_tally = 0
    palette_cnt = 0
    legend_map = tibble(domain = character(), colors = list())
    for(i in 1:length(q_dirs)){

        ntw_dmn_prd = str_match(string = q_dirs[i],
                                pattern = '^data/(.+?)/(.+?)/derived/(.+?)$')[, 2:4]

        dflux_var_year = dflux %>%
            filter(domain == ntw_dmn_prd[2]) %>%
            mutate(ntw_dmn_sit = paste(network, domain, site_code,
                                       sep = ' > '),
                   date = lubridate::as_date(datetime),
                   # year = lubridate::year(datetime),
                   # month = lubridate::month(datetime),
                   val = errors::drop_errors(val)) %>%
            group_by(ntw_dmn_sit, date) %>%
            summarize(val = mean(val, na.rm = TRUE),
                      .groups = 'drop') %>%
            mutate(year = year(date)) %>%
            arrange(ntw_dmn_sit, year) %>%
            group_by(ntw_dmn_sit) %>%
            mutate(
                val = if(sum(! is.na(val)) > 1)
                {
                    imputeTS::na_interpolation(val,
                                               maxgap = flux_interp_limit)
                } else val) %>%
            ungroup() %>%
            group_by(ntw_dmn_sit, year) %>%
            summarize(val = sum(val, na.rm = FALSE), #
                      .groups = 'drop') %>%
            tidyr::extract(col = ntw_dmn_sit,
                           into = c('network', 'domain', 'site_code'),
                           regex = '(.+?) > (.+?) > (.+)',
                           remove = FALSE) %>%
            left_join(ws_areas,
                      by = c('network', 'domain', 'site_code')) %>%
            mutate(val = log(val / ws_area_ha)) %>%
            # filter(! is.na(val)) %>%
            select(year, ntw_dmn_sit, domain, flux = val) %>%
            arrange(ntw_dmn_sit, year)

        # zz = filter(dflux_var_year, ntw_dmn_sit == 'lter > niwot > GREEN4')
        # plot(zz$year, zz$flux, type='l', xlim=c(1990, 2021))

        if(! nrow(dflux_var_year)){
            message(paste0('no ', fluxvar, ' flux for domain: ', ntw_dmn_prd[2]))
            next
        }

        palette_cnt = palette_cnt + 1

        included_domains = c(included_domains, ntw_dmn_prd[2])

        n_available_colors = palettes[palette_cnt, 'maxcolors']
        colors = RColorBrewer::brewer.pal(n = n_available_colors,
                                          name = palettes[palette_cnt, 'name'])
        lty = palettes[palette_cnt, 'lty']

        legend_map = bind_rows(legend_map,
                               tibble(domain = paste(ntw_dmn_prd[1:2], collapse=' > '),
                                      colors = list(colors[3:9])))

        sites = unique(dflux_var_year$ntw_dmn_sit)
        for(j in 1:length(sites)){
            site = sites[j]
            color_ind = j %% (n_available_colors - 2) + 2
            color_ind = ifelse(color_ind == 2, n_available_colors, color_ind)
            ds = filter(dflux_var_year, ntw_dmn_sit == !!site)
            if(any(! is.na(ds$flux))) plotted_site_tally = plotted_site_tally + 1
            lines(ds$year, ds$flux, col=alpha(colors[color_ind], 0.7), lwd = 2.5,
                  lty = lty)
        }
    }

    graphics::text(x=median(yr_seq), y=quantile(c(0, ylim_maxes[k]), 0.9),
                   labels=paste0('sites: ', plotted_site_tally), adj=0.5)

    defpar = par(lend=1)
    for(j in 3:9){
        if(j == 3){
            legend(x=1949, y=quantile(c(0, ylim_maxes[k]), 0.95),
                   legend=legend_map$domain, bty = 'n',
                   col = sapply(legend_map$colors, function(x) x[j]),
                   lty=1, seg.len=1, lwd=5, x.intersp=6.5)
            legend(x=1947, y=quantile(c(0, ylim_maxes[k]), 0.95),
                   legend=rep('', nrow(legend_map)), bty = 'n',
                   col = 'black',
                   lty=palettes$lty, seg.len=2, lwd=1, x.intersp=6.5)
        } else {
            legend(x=1946 + j, y=quantile(c(0, ylim_maxes[k]), 0.95),
                   legend=rep('', nrow(legend_map)), bty = 'n',
                   col = sapply(legend_map$colors, function(x) x[j]),
                   lty=1, seg.len=1, lwd=5)
        }
    }
    par(defpar)

    if(fluxvar == 'Si'){
        mtext(bquote(bold('Reporting SiO2-Si from these domains:' ~
                              .(paste(sio2_dmns, collapse = ', ')))),
              side=3, col='tomato2', line=-4)
    }
}

dev.off()


# all Q (box plot with log Y, dark) ####

q_dirs <- list_all_product_dirs('discharge',
                                location = 'data_acquisition')

plotted_site_tally = 0
d_by_max = d_boxplot = tibble()
for(i in 1:length(q_dirs)){

    pd = q_dirs[i]

    ntw_dmn_prd <- str_match(string = pd,
                             pattern = '^data/(.+?)/(.+?)/derived/(.+?)$')[, 2:4]

    d = list.files(pd, full.names = TRUE) %>%
        purrr::map_dfr(read_feather) %>%
        select(-val_err) %>%
        # mutate(val = errors::set_errors(val, val_err),
        group_by(site_code,
                 date = lubridate::as_date(datetime)) %>%
        summarize(val = mean(val, na.rm = TRUE),
                  .groups = 'drop') %>%
        arrange(site_code, date) %>%
        group_by(site_code) %>%
        mutate(
            val = if(sum(! is.na(val)) > 1)
            {
                imputeTS::na_interpolation(val,
                                           maxgap = q_interp_limit)
            } else val) %>%
        ungroup() %>%
        mutate(year = lubridate::year(date),
               val = val * 86400) %>%
        group_by(site_code, year) %>%
        summarize(val = sum(val, na.rm = FALSE), #
                  .groups = 'drop') %>%
        arrange(site_code, year) %>%
        mutate(network = ntw_dmn_prd[1],
               domain = ntw_dmn_prd[2]) %>%
        left_join(ws_areas,
                  by = c('network', 'domain', 'site_code')) %>%
        mutate(ws_area_mm2 = ws_area_ha * 10000 * 1e6,
               val = val / 1000 * 1e9 / ws_area_mm2,
               ntw_dmn_sit = paste(network, domain, site_code,
                                   sep = ' > ')) %>%
        select(year, ntw_dmn_sit, val) %>%
        arrange(ntw_dmn_sit, year)

    d_by_max = d %>%
        group_by(ntw_dmn_sit) %>%
        summarize(maxval = max(val),
                  .groups = 'drop') %>%
        filter(! is.na(maxval)) %>%
        mutate(domain = ntw_dmn_prd[2]) %>%
        bind_rows(d_by_max)

    d_boxplot = d %>%
        filter(ntw_dmn_sit %in% unique(d_by_max$ntw_dmn_sit)) %>%
        group_by(ntw_dmn_sit) %>%
        summarize(box_stats = list(boxplot.stats(val)),
                  .groups = 'drop') %>%
        mutate(color = dmncolors[ntw_dmn_prd[2] == domains],
               site_code = str_match(ntw_dmn_sit, '.+? > .+? > (.+)$')[, 2]) %>%
        bind_rows(d_boxplot)
}

d_boxplot = filter(d_boxplot,
                   ntw_dmn_sit %in% d_by_max$ntw_dmn_sit)

site_order = d_by_max$ntw_dmn_sit[order(d_by_max$maxval)]

included_domain_inds = which(domains %in% unique(d_by_max$domain))
excluded_domains = domains[-included_domain_inds]
included_domains = domains[included_domain_inds]
included_dmncolors = dmncolors[included_domain_inds]

d_boxplot = d_boxplot[rev(order(match(d_boxplot$ntw_dmn_sit, site_order))), ]

ylims = log(c(0.1, 2e5))
y_ticks = c(0.1, 1, 10, 100, 1000, 10000, 1e5)

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/Q_boxplot_interp',
                q_interp_limit, '.pdf'))
# png(width=11, height=9, units='in', type='cairo', res=300,
#     filename=paste0('plots/diagnostic_plots_',  vsn, '/Q_boxplot.png'))
par(mar=c(14, 4, 4, 1))

nsites = nrow(d_boxplot)
plot(1:nsites, rep(1, nsites), ylim=ylims, type='n',
     ylab='Runoff (mm/yr)', xlab='', xaxt='n', yaxs='i',
     main=paste0('Annual Runoff'), xlim=c(1, nsites), yaxt='n')
axis(2, log(y_ticks), y_ticks)
corners = par("usr")
rect(corners[1], corners[3], corners[2], corners[4], col = 'black')

for(j in 1:nsites){
    color = slice(d_boxplot, j) %>% pull(color)
    stats = (slice(d_boxplot, j) %>% pull(box_stats))[[1]]
    outliers = log(stats$out)
    box_whisk = log(stats$stats)
    segments(x0=j, x1=j, y0=box_whisk[1], y1=box_whisk[2], col=color,
             lwd=2, lend=3)
    points(x=j, y=box_whisk[3], col=color, pch=20)
    segments(x0=j, x1=j, y0=box_whisk[4], y1=box_whisk[5], col=color,
             lwd=2, lend=3)
    points(x=rep(j, length(outliers)), y=outliers, col=color, pch=20, cex=0.2)
}

axis_seq_1 = seq(1, nsites, 2)
axis_seq_2 = seq(2, nsites, 2)
axis(1, at=axis_seq_1, labels=d_boxplot$site_code[axis_seq_1],
     las=2, cex.axis=0.6, tcl=-0.6)
axis(1, at=axis_seq_2, labels=d_boxplot$site_code[axis_seq_2],
     las=2, cex.axis=0.6, tcl=-0.6, line=6.5, tick=FALSE)
axis(1, at=axis_seq_2, labels=rep('', length(axis_seq_2)), tcl=-7)
legend('topright', legend=included_domains, lty=1, col=included_dmncolors, bty='n', lwd=3,
       text.col='white', ncol=2)
newline_seq = try(seq(6, length(excluded_domains), 6), silent=TRUE)
if(! inherits(newline_seq, 'try-error')){
    excluded_domains[newline_seq] = paste0('\n', excluded_domains[newline_seq])
}
graphics::text(x=quantile(1:nsites, 0.15),
               y=quantile(ylims, 0.95), adj=0, col='green',
               labels=paste0('Sites: ', length(unique(d_boxplot$ntw_dmn_sit))))
graphics::text(x=quantile(1:nsites, 0.15),
               y=quantile(ylims, 0.9), adj=0, col='pink',
               labels=paste0('Missing domains: ',
                             paste(excluded_domains, collapse = ', ')))

dev.off()

#annual Q coverage by domain and site ####

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/Q_coverage.pdf'))

q = load_product('discharge')
dmns = unique(q$domain)
current_year = lubridate::year(Sys.Date())

for(dmn in dmns){

    earliest_year = lubridate::year(min(q$datetime[q$domain == dmn]))
    nyears = current_year - earliest_year
    yrcols = viridis(n = nyears)

    sites = unique(q$site_code[q$domain == dmn])
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

        qsub = q %>%
            filter(domain == dmn, site_code == s) %>%
            mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
                   yr_offset = lubridate::year(datetime) - earliest_year)

        lubridate::year(qsub$datetime) <- 1972
        yrs = unique(qsub$yr_offset)

        for(i in 1:length(yrs)){
            qss = qsub %>%
                filter(yr_offset == yrs[i]) %>%
                arrange(doy)
            lines(qss$doy, c(scale(drop_errors(qss$val))) + qss$yr_offset, col=yrcols[i])
        }

        mtext(s, 3, outer=FALSE, line=-2)
    }

    mtext(paste0(dmn, ' (DOY vs. Year)'), 3, outer=TRUE)
}

dev.off()

#annual P coverage by domain and site ####

#for this to work properly, reload the version of load_entire_product that's in
#   the public export dataset. it can start from any root (and you'll need to
#   start from the public export root to avoid reading raw precip gauge data)

p = load_product(prodname = 'precipitation')

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/P_coverage.pdf'))

dmns = unique(p$domain)
current_year = lubridate::year(Sys.Date())

for(dmn in dmns){

    earliest_year = lubridate::year(min(p$datetime[p$domain == dmn]))
    nyears = current_year - earliest_year
    yrcols = viridis(n = nyears)

    sites = unique(p$site_code[p$domain == dmn])
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
            filter(domain == dmn, site_code == s) %>%
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
            lines(pss$doy, c(scale(drop_errors(pss$val))) + pss$yr_offset, col=yrcols[i])
        }

        mtext(s, 3, outer=FALSE, line=-2)
    }

    mtext(paste0(dmn, ' (DOY vs. Year)'), 3, outer=TRUE)
}

dev.off()

#annual precip flux coverage by domain and site (for select variables) ####

#for this to work properly, reload the version of load_entire_product that's in
#   the public export dataset. it can start from any root (and you'll need to
#   start from the public export root to avoid reading raw precip gauge data)

p = load_product(prodname = 'precip_flux_inst_scaled',
                        filter_vars = 'Ca')

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/pflux_Ca_coverage.pdf'))

dmns = unique(p$domain)
current_year = lubridate::year(Sys.Date())

for(dmn in dmns){

    earliest_year = lubridate::year(min(p$datetime[p$domain == dmn]))
    nyears = current_year - earliest_year
    yrcols = viridis(n = nyears)

    sites = unique(p$site_code[p$domain == dmn])
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
            filter(domain == dmn, site_code == s) %>%
            mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
                   yr_offset = lubridate::year(datetime) - earliest_year)

        lubridate::year(psub$datetime) <- 1972
        yrs = unique(psub$yr_offset)

        for(i in 1:length(yrs)){
            pss = psub %>%
                filter(yr_offset == yrs[i]) %>%
                arrange(doy)
            lines(pss$doy, c(scale(drop_errors(pss$val))) + pss$yr_offset, col=yrcols[i])
        }

        mtext(s, 3, outer=FALSE, line=-2)
    }

    mtext(paste0(dmn, ' (DOY vs. Year)'), 3, outer=TRUE)
}

dev.off()

#annual stream flux coverage by domain, site, var ####

ignore_vars <- c('NO3_NO2_N', 'NH4_NH3_N', 'PON', 'POC', 'TIP', 'TIN',
                 'DOP', 'CO3', 'NO2_N', 'Ta', 'Pt', 'Te', 'W', 'Sm', 'Sc',
                 'Rh', 'Re', 'Pd', 'Nd', 'UTP', 'UTN', 'suspSed', 'Sn', 'Th',
                 'Ti', 'Zr', 'Se', 'Rb', 'Ni', 'Li', 'Ge', 'Cs', 'Tl', 'Cd',
                 'Yb', 'Y', 'V', 'U', 'Tm', 'Ti49', 'Tb', 'Sn118', 'Sm147',
                 'Se78', 'Sb', 'Pr', 'Pb', 'Ni60', 'Nd14', 'Mo', 'Lu', 'La',
                 'Ho', 'Gd157', 'Eu', 'Er', 'Dy', 'Cr', 'Co', 'Ce', 'Be', 'Ba',
                 'B', 'As', 'Ag', 'Tc', 'NO2', 'H', 'Hf', 'Ga', 'Nb', 'Bi',
                 'Au', 'Nd145', 'NH3_N', 'TC', 'Gd', 'Hg', 'I', 'S', 'Al_ICP',
                 'OMAl', 'TMAl', 'TIC', 'SRP')

load_entire_product <- function(macrosheds_root,
                                prodname,
                                sort_result = FALSE,
                                filter_vars){

    require(tidyverse)
    require(feather)
    require(errors)

    #WARNING: this could easily eat up 20 GB RAM for a product like discharge.
    #As the dataset grows, that number will increase. This warning only applies
    #if the dataset version has full temporal granularity (not daily).

    # macrosheds_root: character. The path to the macrosheds dataset's parent
    #    directory, e.g. '~/stuff/macrosheds_dataset_v0.3'
    # prodname: character. read and combine all files associated with this prodname
    #    across all networks and domains. Available prodnames are:
    #    discharge, stream_chemistry, stream_flux_inst, precipitation,
    #    precip_chemistry, precip_flux_inst.
    # sort_result: logical. If TRUE, output will be sorted by site_code, var,
    #    datetime. this may take a few additional minutes for some products in
    #    the full 15m dataset.
    # filter_vars: character vector. for products like stream_chemistry that include
    #    multiple variables, this filters to just the ones specified (ignores
    #    variable prefixes). To see a catalog of variables, visit macrosheds.org

    list_all_product_dirs <- function(macrosheds_root, prodname){

        prodname_dirs <- list.dirs(path = macrosheds_root,
                                   full.names = TRUE,
                                   recursive = TRUE)

        prodname_dirs <- grep(pattern = paste0('derived/', prodname, '__'),
                              x = prodname_dirs,
                              value = TRUE)

        return(prodname_dirs)
    }

    drop_var_prefix <- function(x){

        unprefixed <- substr(x, 4, nchar(x))

        return(unprefixed)
    }

    avail_prodnames <- c('discharge', 'stream_chemistry', 'stream_flux_inst_scaled',
                         'precipitation', 'precip_chemistry', 'precip_flux_inst_scaled')

    if(! prodname %in% avail_prodnames){
        stop(paste0('prodname must be one of: ',
                    paste(avail_prodnames,
                          collapse = ', ')))
    }

    prodname_dirs <- list_all_product_dirs(macrosheds_root = macrosheds_root,
                                           prodname = prodname)

    d <- tibble()
    for(pd in prodname_dirs){

        rgx <- '/([a-zA-Z0-9\\-\\_]+)/([a-zA-Z0-9\\-\\_]+)/derived.+'
        network_domain <- str_match(string = pd,
                                    pattern = rgx)[, 2:3]

        d0 <- list.files(pd, full.names = TRUE) %>%
            purrr::map_dfr(read_feather)

        if(! missing(filter_vars)){
            d0 <- filter(d0,
                         drop_var_prefix(var) %in% filter_vars)
        }

        d <- d0 %>%
            mutate(val = errors::set_errors(val, val_err),
                   network = network_domain[1],
                   domain = network_domain[2]) %>%
            select(-val_err) %>%
            select(datetime, network, domain, site_code, var, val, ms_status,
                   ms_interp) %>%
            bind_rows(d)
    }

    if(nrow(d) == 0){

        if(missing(filter_vars)){
            stop('No results. Make sure macrosheds_root is correct.')
        } else {
            stop(paste('No results. Make sure macrosheds_root is correct and',
                       'filter_vars includes variable codes from the catalog',
                       'on macrosheds.org'))
        }
    }

    if(sort_result){
        d <- arrange(d,
                     site_code, var, datetime)
    }

    return(d)
}

f = load_entire_product(macrosheds_root = '~/git/macrosheds/data_processing/macrosheds_dataset_v1/',
                        prodname = 'stream_flux_inst_scaled')

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/stream_flux_coverage.pdf'))

dmns = unique(f$domain)
current_year = lubridate::year(Sys.Date())

for(dmn in dmns){

    earliest_year = lubridate::year(min(f$datetime[f$domain == dmn]))
    nyears = current_year - earliest_year
    yrcols = viridis(n = nyears)

    sites = unique(f$site_code[f$domain == dmn])
    if(dmn == 'arctic') sites = sites[! grepl('[0-9]', sites)]

    plotrc = ceiling(sqrt(length(sites)))
    # plotc = floor(sqrt(length(sites)))
    doyseq = seq(1, 366, 30)
    par(mfrow=c(plotrc, plotrc), mar=c(1,2,0,0), oma=c(0,0,2,0))

    for(s in sites){

        vars <- unique(filter(f, domain == dmn, site_code == s)$var)
        if(any(extract_var_prefix(vars) %in% c('GS', 'IS'))) stop('oi')
        # vars <- drop_var_prefix(vars)
        vars <- sort(vars[! drop_var_prefix(vars) %in% ignore_vars])

        for(v in vars){

            plot(NA, NA, xlim=c(1, 366), ylim=c(0, nyears), xaxs='i', yaxs='i',
                 ylab = '', xlab = '', yaxt='n', cex.axis=0.6, xaxt='n', xpd=NA)
            axis(1, doyseq, doyseq, tick=FALSE, line = -2, cex.axis=0.8)
            axis(2, 1:nyears, earliest_year:(current_year - 1), las=2, cex.axis=0.6,
                 hadj=0.7)

            psub = f %>%
                filter(domain == dmn, site_code == s, var == !!v) %>%
                mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
                       yr_offset = lubridate::year(datetime) - earliest_year)

            lubridate::year(psub$datetime) <- 1972
            yrs = unique(psub$yr_offset)

            for(i in 1:length(yrs)){
                pss = psub %>%
                    filter(yr_offset == yrs[i]) %>%
                    arrange(doy)
                lines(pss$doy, c(scale(drop_errors(pss$val))) + pss$yr_offset, col=yrcols[i])
            }

            mtext(paste(s, v, sep='; '), 3, outer=FALSE, line=-2)
        }
        mtext(paste0(dmn, ' (DOY vs. Year)'), 3, outer=TRUE)
    }
}

dev.off()

#annual stream conc coverage by domain, site, var ####

ignore_vars <- c('NO3_NO2_N', 'NH4_NH3_N', 'PON', 'POC', 'TIP', 'TIN',
                 'DOP', 'CO3', 'NO2_N', 'Ta', 'Pt', 'Te', 'W', 'Sm', 'Sc',
                 'Rh', 'Re', 'Pd', 'Nd', 'UTP', 'UTN', 'suspSed', 'Sn', 'Th',
                 'Ti', 'Zr', 'Se', 'Rb', 'Ni', 'Li', 'Ge', 'Cs', 'Tl', 'Cd',
                 'Yb', 'Y', 'V', 'U', 'Tm', 'Ti49', 'Tb', 'Sn118', 'Sm147',
                 'Se78', 'Sb', 'Pr', 'Pb', 'Ni60', 'Nd14', 'Mo', 'Lu', 'La',
                 'Ho', 'Gd157', 'Eu', 'Er', 'Dy', 'Cr', 'Co', 'Ce', 'Be', 'Ba',
                 'B', 'As', 'Ag', 'Tc', 'NO2', 'H', 'Hf', 'Ga', 'Nb', 'Bi',
                 'Au', 'Nd145', 'NH3_N', 'TC', 'Gd', 'Hg', 'I', 'S', 'Al_ICP',
                 'OMAl', 'TMAl', 'TIC', 'SRP')

load_entire_product <- function(macrosheds_root,
                                prodname,
                                sort_result = FALSE,
                                filter_vars){

    require(tidyverse)
    require(feather)
    require(errors)

    #WARNING: this could easily eat up 20 GB RAM for a product like discharge.
    #As the dataset grows, that number will increase. This warning only applies
    #if the dataset version has full temporal granularity (not daily).

    # macrosheds_root: character. The path to the macrosheds dataset's parent
    #    directory, e.g. '~/stuff/macrosheds_dataset_v0.3'
    # prodname: character. read and combine all files associated with this prodname
    #    across all networks and domains. Available prodnames are:
    #    discharge, stream_chemistry, stream_flux_inst, precipitation,
    #    precip_chemistry, precip_flux_inst.
    # sort_result: logical. If TRUE, output will be sorted by site_code, var,
    #    datetime. this may take a few additional minutes for some products in
    #    the full 15m dataset.
    # filter_vars: character vector. for products like stream_chemistry that include
    #    multiple variables, this filters to just the ones specified (ignores
    #    variable prefixes). To see a catalog of variables, visit macrosheds.org

    list_all_product_dirs <- function(macrosheds_root, prodname){

        prodname_dirs <- list.dirs(path = macrosheds_root,
                                   full.names = TRUE,
                                   recursive = TRUE)

        prodname_dirs <- grep(pattern = paste0('derived/', prodname, '__'),
                              x = prodname_dirs,
                              value = TRUE)

        return(prodname_dirs)
    }

    drop_var_prefix <- function(x){

        unprefixed <- substr(x, 4, nchar(x))

        return(unprefixed)
    }

    avail_prodnames <- c('discharge', 'stream_chemistry', 'stream_flux_inst_scaled',
                         'precipitation', 'precip_chemistry', 'precip_flux_inst_scaled')

    if(! prodname %in% avail_prodnames){
        stop(paste0('prodname must be one of: ',
                    paste(avail_prodnames,
                          collapse = ', ')))
    }

    prodname_dirs <- list_all_product_dirs(macrosheds_root = macrosheds_root,
                                           prodname = prodname)

    d <- tibble()
    for(pd in prodname_dirs){

        rgx <- '/([a-zA-Z0-9\\-\\_]+)/([a-zA-Z0-9\\-\\_]+)/derived.+'
        network_domain <- str_match(string = pd,
                                    pattern = rgx)[, 2:3]

        d0 <- list.files(pd, full.names = TRUE) %>%
            purrr::map_dfr(read_feather)

        if(! missing(filter_vars)){
            d0 <- filter(d0,
                         drop_var_prefix(var) %in% filter_vars)
        }

        d <- d0 %>%
            mutate(val = errors::set_errors(val, val_err),
                   network = network_domain[1],
                   domain = network_domain[2]) %>%
            select(-val_err) %>%
            select(datetime, network, domain, site_code, var, val, ms_status,
                   ms_interp) %>%
            bind_rows(d)
    }

    if(nrow(d) == 0){

        if(missing(filter_vars)){
            stop('No results. Make sure macrosheds_root is correct.')
        } else {
            stop(paste('No results. Make sure macrosheds_root is correct and',
                       'filter_vars includes variable codes from the catalog',
                       'on macrosheds.org'))
        }
    }

    if(sort_result){
        d <- arrange(d,
                     site_code, var, datetime)
    }

    return(d)
}

f = load_entire_product(macrosheds_root = '~/git/macrosheds/data_processing/macrosheds_dataset_v1/',
                        prodname = 'stream_chemistry')

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/stream_chemistry_coverage.pdf'))

dmns = unique(f$domain)
current_year = lubridate::year(Sys.Date())

for(dmn in dmns){

    earliest_year = lubridate::year(min(f$datetime[f$domain == dmn]))
    nyears = current_year - earliest_year
    yrcols = viridis(n = nyears)

    sites = unique(f$site_code[f$domain == dmn])
    if(dmn == 'arctic') sites = sites[! grepl('[0-9]', sites)]

    plotrc = ceiling(sqrt(length(sites)))
    # plotc = floor(sqrt(length(sites)))
    doyseq = seq(1, 366, 30)
    par(mfrow=c(plotrc, plotrc), mar=c(1,2,0,0), oma=c(0,0,2,0))

    for(s in sites){

        vars <- unique(filter(f, domain == dmn, site_code == s)$var)
        vars <- grep('^I[SN]_', vars, invert = TRUE, value = TRUE)
        print(s)
        print(vars)
        if(any(extract_var_prefix(vars) %in% c('GS', 'IS'))) stop('oi')
        # vars <- drop_var_prefix(vars)
        vars <- sort(vars[! drop_var_prefix(vars) %in% ignore_vars])

        for(v in vars){

            plot(NA, NA, xlim=c(1, 366), ylim=c(0, nyears), xaxs='i', yaxs='i',
                 ylab = '', xlab = '', yaxt='n', cex.axis=0.6, xaxt='n', xpd=NA)
            axis(1, doyseq, doyseq, tick=FALSE, line = -2, cex.axis=0.8)
            axis(2, 1:nyears, earliest_year:(current_year - 1), las=2, cex.axis=0.6,
                 hadj=0.7)

            psub = f %>%
                filter(domain == dmn, site_code == s, var == !!v) %>%
                mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
                       yr_offset = lubridate::year(datetime) - earliest_year)

            lubridate::year(psub$datetime) <- 1972
            yrs = unique(psub$yr_offset)

            for(i in 1:length(yrs)){
                pss = psub %>%
                    filter(yr_offset == yrs[i]) %>%
                    arrange(doy)
                lines(pss$doy, c(scale(drop_errors(pss$val))) + pss$yr_offset, col=yrcols[i])
            }

            mtext(paste(s, v, sep='; '), 3, outer=FALSE, line=-2)
        }
        mtext(paste0(dmn, ' (DOY vs. Year)'), 3, outer=TRUE)
    }
}

dev.off()

#annual NO3-N concentration coverage by domain and site ####

#for this to work properly, reload the version of load_entire_product that's in
#   the public export dataset. it can start from any root (and you'll need to
#   start from the public export root to avoid reading raw precip gauge data)

p = load_entire_product(macrosheds_root = '~/git/macrosheds/data_processing/macrosheds_dataset_v0.4/',
                        prodname = 'stream_chemistry',
                        filter_vars = 'NO3_N')

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/streamchem_NO3-N_coverage.pdf'))

dmns = unique(p$domain)
current_year = lubridate::year(Sys.Date())

for(dmn in dmns){

    earliest_year = lubridate::year(min(p$datetime[p$domain == dmn]))
    nyears = current_year - earliest_year
    yrcols = viridis(n = nyears)

    sites = unique(p$site_code[p$domain == dmn])
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
            filter(domain == dmn, site_code == s) %>%
            mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
                   yr_offset = lubridate::year(datetime) - earliest_year)

        lubridate::year(psub$datetime) <- 1972
        yrs = unique(psub$yr_offset)

        for(i in 1:length(yrs)){
            pss = psub %>%
                filter(yr_offset == yrs[i]) %>%
                arrange(doy)
            lines(pss$doy, c(scale(drop_errors(pss$val))) + pss$yr_offset, col=yrcols[i])
        }

        mtext(s, 3, outer=FALSE, line=-2)
    }

    mtext(paste0(dmn, ' (DOY vs. Year)'), 3, outer=TRUE)
}

dev.off()

#Q and air temp (just Niwot and Plum for now) ####

library(geoknife)
library(data.table)

webdatasets = query('webdata') #return all sets
# grep('NCEP', title(webdatasets), value = TRUE)[5]
# grep('NCEP', title(webdatasets))[5]
grep('emperature', title(webdatasets), value = F)[204]
webdata(webdatasets[475])
geoknife::query(webdatasets[475], 'variables')
# webdata(webdatasets[99])
# webdata(webdatasets[452])
webdata('topowx')

qair_sites <- site_data %>%
    filter(domain %in% c('niwot', 'plum'),
           site_type == 'stream_gauge',
           in_workflow == TRUE) %>%
    select(site_code, longitude, latitude) %>%
    data.table::transpose(make.names = 'site_code')
    # sf::st_as_sf(coords = c('longitude', 'latitude'), crs = 4326)

# stencil = simplegeom(as(niwot_sites, 'Spatial'))
stencil = simplegeom(qair_sites)
fabric = webdata(list(times = as.POSIXct(c('1981-01-01', '2020-12-31')),
    url = 'https://cida.usgs.gov/thredds/dodsC/topowx',
    variables = 'tmax'))

job = geoknife(stencil, fabric, wait = FALSE)
successful(job)
running(job)
# job = cancel(job)
tmax = result(job)

fabric = webdata(list(times = as.POSIXct(c('1981-01-01', '2020-12-31')),
    url = 'https://cida.usgs.gov/thredds/dodsC/topowx',
    variables = 'tmin'))
job = geoknife(stencil, fabric, wait = FALSE)
tmin = result(job)


# library(RCurl)
# url <- 'ftp://ftp.cdc.noaa.gov/Projects/NARR/Dailies/monolevel/air.sfc.1981.nc'
# userpwd <- "anonymous:anonymous@"
# filenames <- getURL(url,
#                     userpwd = userpwd)
#                     # ftp.use.epsv = FALSE,
#                     # dirlistonly = TRUE)
# airt <- getURLContent(url,
#                       userpwd = userpwd)

# library(ncdf4)
# library(raster)
#
# wgs84_wkt = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs+ towgs84=0,0,0'
# download.file('ftp://anonymous:anonymous@ftp.cdc.noaa.gov/Projects/NARR/Dailies/monolevel/air.sfc.1981.nc',
#               destfile = '/tmp/air.sfc.1981.nc')
# airt_ncdf <- nc_open('/tmp/air.sfc.1981.nc')
# lonvec <- ncvar_get(airt_ncdf, 'lon')
# latvec <- ncvar_get(airt_ncdf, 'lat', verbose = FALSE)
# timevec <- ncvar_get(airt_ncdf, 'time')
# airt <- ncvar_get(airt_ncdf, 'air')
# missing_data_val = ncatt_get(airt_ncdf, 'air', '_FillValue')$value
# nc_close(airt_ncdf)
# airt[airt == missing_data_val] = NA
#
# airt_timeslice <- airt[, , 1]
# airr <- raster(t(airt_timeslice),
#                xmn = min(lonvec),
#                xmx = max(lonvec),
#                ymn = min(latvec),
#                ymx = max(latvec),
#                crs = CRS(wgs84_wkt)) %>%
#     raster::flip(direction = 'y')
#
# plot(airr)

# u = 'https://cida.usgs.gov/thredds/ncss/topowx?var=tmax&var=tmin&north=51.1916&west=-125.0000&east=-66.6750&south=24.1166&disableLLSubset=on&disableProjSubset=on&horizStride=1&time_start=1948-01-01T12%3A00%3A00Z&time_end=2016-12-31T12%3A00%3A00Z&timeStride=1'
# r = httr::GET(u)
# json = httr::content(r, as="text", encoding="UTF-8")
# d = try(jsonlite::fromJSON(json), silent=TRUE)

pdf(width=11, height=9, onefile=TRUE,
    file=paste0('plots/diagnostic_plots_',  vsn, '/Q_and_airtemp.pdf'))

q = load_entire_product('discharge')
dmns = unique(q$domain)
current_year = lubridate::year(Sys.Date())

# for(dmn in dmns){
for(dmn in c('niwot', 'plum')){

    earliest_year = lubridate::year(min(q$datetime[q$domain == dmn]))
    nyears = current_year - earliest_year
    yrcols = viridis(n = nyears)

    sites = unique(q$site_code[q$domain == dmn])
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

        qsub = q %>%
            filter(domain == dmn, site_code == s) %>%
            mutate(val = errors::drop_errors(val),
                   val = scale(val)[, 1])

        q_year_range = range(year(qsub$datetime))

        tmax_site = as_tibble(tmax) %>%
            select(datetime = DateTime, !!s) %>%
            filter(datetime >= as.POSIXct(paste0(q_year_range[1], '-01-01')),
                   datetime <= as.POSIXct(paste0(q_year_range[2], '-12-31'))) %>%
            rename(tmax = !!s) %>%
            mutate(datetime = floor_date(datetime, unit = 'days'))

        qsub = full_join(qsub, tmax_site, by = 'datetime') %>%
            mutate(doy = as.numeric(strftime(datetime, format = '%j', tz='UTC')),
                   yr_offset = lubridate::year(datetime) - earliest_year) %>%
            arrange(datetime)

        # lubridate::year(qsub$datetime) <- 1972
        yrs = unique(qsub$yr_offset)

        for(i in 1:length(yrs)){

            qss = qsub %>%
                filter(yr_offset == yrs[i]) %>%
                arrange(doy)

            qss_subzero_max = mutate(qss,
                                     val = ifelse(tmax <= 0,
                                                  min(val, na.rm = TRUE),
                                                  NA))

            lines(x = qss_subzero_max$doy,
                  y = qss_subzero_max$val + qss_subzero_max$yr_offset,
                  col = 'gray85', lwd = 5, lend = 2)
            lines(qss$doy, qss$val + qss$yr_offset, col=yrcols[i])
        }

        mtext(s, 3, outer=FALSE, line=-2)
    }

    mtext(paste0(dmn, ' (DOY vs. Year)'), 3, outer=TRUE)
}

dev.off()

save.image('NA_Q_image.rda')

