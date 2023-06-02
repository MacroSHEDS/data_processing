#visualize coverage by variable, site, and time ####

turboplotblaster <- function(prodname_ms){

    zz = read_combine_feathers(network = 'neon',
                               domain = 'neon',
                               prodname_ms = prodname_ms)

    today_date = format(Sys.Date(), format='%Y%m%d')

    vars = colnames(zz)
    vars = vars[! vars %in% c('datetime', 'site_code', 'ms_status', 'ms_interp')]

    for(v in vars){

        spc = zz %>%
            select(site_code, datetime, !!v)

        sites = sort(unique(spc$site_code))
        rc = ceiling(sqrt(length(sites)))

        png(width=8, height=8, units='in', type='cairo', res=300,
            filename=glue('data/neon/neon/ancillary/plots/coverage/{vv}_{t}.png',
                          vv = v, t=today_date))

        par(mfrow=c(rc, rc), mar=c(0,0,0,0), oma=c(3, 3, 0, 0))
        xlims = range(spc$datetime)

        for(s in sites){
            spc_ = filter(spc, site_code == s) %>% arrange()
            plot(spc_$datetime, spc_[[v]], type='p', xlab='', ylab='',
                yaxt='n', bty='l', col='cadetblue', pch='.', xaxt='n',
                xlim=xlims)
            mtext(s, 3, line=-1.5)

            mtext(glue('Time: {st} through {en}',
                       st = as.Date(xlims[1]),
                       en = as.Date(xlims[2])),
                  1, outer=TRUE, line=1)
            mtext(v, 2, outer=TRUE, line=0.5)
        }

        dev.off()

    }
}

turboplotblaster('stream_quality__DP1.20288')
turboplotblaster('stream_chemistry__DP1.20093')
turboplotblaster('stream_nitrate__DP1.20033')
turboplotblaster('stream_PAR__DP1.20042')
turboplotblaster('stream_gases__DP1.20097')
turboplotblaster('air_pressure__DP1.00004')
turboplotblaster('stream_temperature__DP1.20053')

# get % coverage by site, variable, year ####
get_siteyear_coverage = function(){

    filez = list.files('data/neon/neon/munged/')
    grand_tb = tibble()

    for(p in filez){

        zz = read_combine_feathers(network = 'neon',
                                   domain = 'neon',
                                   prodname_ms = p)

        vars = colnames(zz)
        vars = vars[! vars %in% c('datetime', 'site_code', 'ms_status', 'ms_interp')]

        sites = unique(zz$site_code)

        grabvars = c()
        big_tb = tibble()
        yearsec = 365 * 24 * 60 * 60

        for(v in vars){
            for(s in sites){

                wee_tb <- zz %>%
                    select(site_code, datetime, !!v) %>%
                    filter(site_code == s) %>%
                    group_by(year = year(datetime)) %>%
                    summarize(
                        mode_interval_sec = Mode(diff(as.numeric(datetime))),
                        mode_magnitude = sum(diff(as.numeric(datetime)) == mode_interval_sec),
                        nsamp = n(),
                        max_possible_samp = yearsec / mode_interval_sec,
                        coverage_pct = nsamp / max_possible_samp * 100) %>%
                    ungroup()

                grabvars = wee_tb$mode_interval_sec > 50000
                wee_tb$grab = FALSE
                wee_tb$grab[grabvars] = TRUE
                wee_tb$max_possible_samp[grabvars] = NA
                wee_tb$var = v
                wee_tb$site = s

                big_tb = bind_rows(big_tb, wee_tb)
            }
        }

        big_tb = big_tb %>%
            select(var, site, year, coverage_pct, mode_magnitude, grab,
                   nsamp, mode_interval_sec, max_possible_samp) %>%
            arrange(var, site, year)

        grand_tb = bind_rows(grand_tb, big_tb)

    }

    return(grand_tb)
}

sitevaryear_cov = get_siteyear_coverage()

write_csv(sitevar_cov_post2018,
          'data/neon/neon/ancillary/data/coverage_by_site-variable-year.csv')

# get % coverage by site, variable (2016-17) ####
sitevar_cov_201617 = sitevaryear_cov %>%
    filter(
        grab == FALSE,
        year %in% c(2016, 2017),
        mode_magnitude > 20) %>%
    group_by(site, var) %>%
    summarize(coverage_201617 = mean(coverage_pct))

write_csv(sitevar_cov_201617,
          'data/neon/neon/ancillary/data/coverage_by_site-variable_201617.csv')

# get % coverage by variable (2016-17) ####
sort(unique(sitevar_cov_201617$var))

var_cov_201617 = sitevar_cov_201617 %>%
    mutate(cov_rnd = round(coverage_201617, 1)) %>%
    group_by(var) %>%
    summarize(
        # min_coverage_pct = min(cov_rnd),
        # quant_25_coverage_pct = quantile(cov_rnd, probs = 0.25),
        nsites = n(),
        median_coverage_pct = quantile(cov_rnd, probs = 0.5),
        quant_75_coverage_pct = quantile(cov_rnd, probs = 0.75),
        max_coverage_pct = max(cov_rnd),
        nsites_with_90pct_cov = sum(cov_rnd >= 90))

write_csv(var_cov_201617,
          'data/neon/neon/ancillary/data/coverage_by_variable_201617.csv')

# get % coverage by site, variable (2018-19) ####
sitevar_cov_201819 = sitevaryear_cov %>%
    filter(
        grab == FALSE,
        year %in% c(2018, 2019),
        mode_magnitude > 20) %>%
    group_by(site, var) %>%
    summarize(coverage_201819 = mean(coverage_pct))

write_csv(sitevar_cov_201819,
          'data/neon/neon/ancillary/data/coverage_by_site-variable_201819.csv')

# get % coverage by variable (2018-19) ####
sort(unique(sitevar_cov_201819$var))

var_cov_201819 = sitevar_cov_201819 %>%
    mutate(cov_rnd = round(coverage_201819, 1)) %>%
    group_by(var) %>%
    summarize(
        # min_coverage_pct = min(cov_rnd),
        # quant_25_coverage_pct = quantile(cov_rnd, probs = 0.25),
        nsites = n(),
        median_coverage_pct = quantile(cov_rnd, probs = 0.5),
        quant_75_coverage_pct = quantile(cov_rnd, probs = 0.75),
        max_coverage_pct = max(cov_rnd),
        nsites_with_90pct_cov = sum(cov_rnd >= 90))

write_csv(var_cov_201819,
          'data/neon/neon/ancillary/data/coverage_by_variable_201819.csv')
