#run acquisition_master.R down to main loop

#do our totals for Panola match those determined by Aulenbach et al.? ####
#start with discharge

o = read_feather('data/webb/panola/derived/discharge__ms005/mountain_creek_tributary.feather') %>%
    group_by(site_code, var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = 'day'))
t = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/3_PMRW_Streamflow_WY86-17.csv') %>%
    mutate(datetime = as_datetime(Date, format = '%m/%d/%Y %H:%M:%S'))

plot(o$datetime, o$val, type = 'l', lwd = 2, xlim = as.POSIXct(c('1990-01-01', '1991-01-01')))
lines(t$datetime, t$Streamflow, col = 'red')

#very different maxima

t_dy = t %>%
    group_by(year = year(datetime), month = month(datetime), day = day(datetime)) %>%
    summarize(mean_daily = mean(Streamflow, na.rm = T)) %>%
    ungroup() %>%
    mutate(tot_daily = mean_daily * 86400) %>%
    group_by(year) %>%
    summarize(tot = sum(tot_daily)) %>%
    print(n=10)

o %>%
    group_by(year = year(datetime)) %>%
    summarize(tot = sum(val * 86400))

#but annual totals are similar when summarizing by day and then year

#what about daily?

t %>%
    group_by(year = year(datetime), month = month(datetime), day = day(datetime)) %>%
    summarize(mean_daily = mean(Streamflow, na.rm = T))

o

#legit. how different are yearly totals when summarizing the raw data by hour rather than day?

t_hr = t %>%
    group_by(year = year(datetime), month = month(datetime), day = day(datetime),
             hour = hour(datetime)) %>%
    summarize(mean_hrly = mean(Streamflow, na.rm = T)) %>%
    ungroup() %>%
    mutate(tot_hrly = mean_hrly * 3600) %>%
    group_by(year) %>%
    summarize(tot = sum(tot_hrly)) %>%
    print(n=10)

left_join(t_dy, t_hr, by = 'year') %>%
    rowwise() %>%
    mutate(pct_diff = (tot.x - tot.y) / mean(c(tot.x, tot.y)) * 100) %>%
    ungroup() %>%
    print(n=100)

#oh boy. huge percent differences when yearly totals are calculated this way.
#compare to aulenbach runoff totals

tt = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/12_PMRW_StreamwaterSoluteFluxes_WaterYear_WY86-16.csv') %>%
    distinct(Water_Year, Runoff) %>%
    rename(wy = Water_Year)

ws_area = filter(site_data, domain == 'panola', site_type == 'stream_gauge') %>%
    pull(ws_area_ha)

o %>%
    mutate(runoff = discharge_to_runoff(val, ws_area_ha = ws_area)) %>%
    mutate(wy = ifelse(month(datetime) >= 10, year(datetime) + 1, year(datetime))) %>%
    # print(n=100) %>%
    group_by(wy) %>%
    summarize(macrosheds_runoff = sum(runoff)) %>%
    left_join(tt, by = 'wy') %>%
    rowwise() %>%
    mutate(pct_diff = (macrosheds_runoff - Runoff) / mean(c(macrosheds_runoff, Runoff)) * 100) %>%
    ungroup() %>%
    pull(pct_diff)

#can we compute runoff the same way?

tt1 = t %>%
    arrange(datetime) %>%
    mutate(time_diff_sec = as.numeric(difftime(lead(datetime, 1), datetime, units = "secs")),
           time_diff_sec = ifelse(is.na(time_diff_sec), 0, time_diff_sec),
           liters = Streamflow * time_diff_sec) %>%

    mutate(liters = (Streamflow + lead(Streamflow, default = last(Streamflow))) / 2 * time_diff_sec) %>%
    filter(! is.na(liters)) %>%

    select(datetime, liters) %>%
    group_by(year = year(datetime), month = month(datetime), day = day(datetime)) %>%
    summarize(liters = sum(liters)) %>%
    ungroup() %>%
    mutate(lps = liters / 86400) %>%
    mutate(runoff = discharge_to_runoff(lps, ws_area_ha = ws_area)) %>%
    mutate(wy = ifelse(month >= 10, year + 1, year)) %>%
    group_by(wy) %>%
    summarize(ann_runoff = sum(runoff)) %>%
    left_join(tt, by = 'wy') %>%
    rowwise() %>%
    mutate(pct_diff = (ann_runoff - Runoff) / mean(c(ann_runoff, Runoff)) * 100) %>%
    ungroup() %>%
    print(n=10)


#the new ms approach
first_midnight <- floor_date(min(t$datetime), unit = 'day') + days()
last_midnight <- floor_date(max(t$datetime), unit = 'day') - days()
tt2 = t %>%
    select(datetime, val = Streamflow) %>%
    filter(! is.na(val)) %>%
    tidyr::complete(datetime = seq(first_midnight, last_midnight, by = 'day'),
                    fill = list(site_code = t$site_code[1],
                                var = t$var[1],
                                ms_status = 0,
                                ms_interp = 0)) %>%
    arrange(datetime) %>%
    mutate(val = imputeTS::na_interpolation(val, maxgap = 1, rule = 1),
           next_dt = lead(datetime, 1, default = last(datetime)),
           time_diff = as.numeric(difftime(next_dt, datetime, units = 'sec')),
           #value for each interval becomes the average between it and the
           #next, weighted by the duration of the interval in seconds.
           liters = (val + lead(val, default = last(val))) / 2 * time_diff) %>%
    group_by(datetime = floor_date(datetime, unit = 'day')) %>%
    summarize(val = sum(liters),
              .groups = 'drop') %>%
    mutate(val = val / 86400) %>%

    mutate(runoff = discharge_to_runoff(val, ws_area_ha = ws_area)) %>%
    mutate(wy = ifelse(month(datetime) >= 10, year(datetime) + 1, year(datetime))) %>%
    mutate(runoff = imputeTS::na_interpolation(runoff, maxgap = Inf, rule = 1)) %>%
    group_by(wy) %>%
    summarize(ann_runoff = sum(runoff)) %>%
    left_join(tt, by = 'wy') %>%
    rowwise() %>%
    mutate(pct_diff = (ann_runoff - Runoff) / mean(c(ann_runoff, Runoff)) * 100) %>%
    ungroup() %>%
    print(n=10)

#okay, final look at panola derived runoff loads after overhauling Q aggregation ####
#pretty sure this is redundant with the last thing above

o = read_feather('data/webb/panola/derived/discharge__ms005/mountain_creek_tributary.feather') %>%
    group_by(site_code, var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = 'day'))

tt = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/12_PMRW_StreamwaterSoluteFluxes_WaterYear_WY86-16.csv') %>%
    distinct(Water_Year, Runoff) %>%
    rename(wy = Water_Year)

ws_area = filter(site_data, domain == 'panola', site_type == 'stream_gauge') %>%
    pull(ws_area_ha)

q_ann = o %>%
    mutate(runoff = discharge_to_runoff(val, ws_area_ha = ws_area)) %>%
    mutate(wy = ifelse(month(datetime) >= 10, year(datetime) + 1, year(datetime))) %>%
    # print(n=100) %>%
    group_by(wy) %>%
    summarize(macrosheds_runoff = sum(runoff, na.rm = T)) %>%
    left_join(tt, by = 'wy') %>%
    rowwise() %>%
    mutate(pct_diff = (macrosheds_runoff - Runoff) / mean(c(macrosheds_runoff, Runoff)) * 100) %>%
    ungroup() %>%
    print()


#what about precip, pchem, pflux? ####
#start with pflux ####

convert_Ca <- function(ca_ueq_m2) {
    ca_kg_ha <- ca_ueq_m2 * 20.04 * 10^(-5)
    return(ca_kg_ha)
}
convert_unit(convert_to_gl(x = 10, input_unit = 'ueq/L', molecule = 'Ca'),
             'ug/L', 'g/L')
10 * 20.04 * 1e-6
#okay, our converter is working
ms_conversions(tibble(datetime = as.POSIXct(1), site_code = 'a', var = 'GN_Ca', val = 10),
               convert_units_from = c(Ca = 'ueq/L'), convert_units_to = c(Ca = 'g/L'))
#yup, def chill.

o = read_feather('data/webb/panola/derived/precip_flux_inst__ms902/mountain_creek_tributary.feather') %>%
    group_by(site_code, var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = 'day')) %>%
    ungroup()
t = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/8_PMRW_WetDepositionSoluteFluxes_Daily_WY86-17.csv') %>%
    mutate(Date = as_date(Date, format = '%m/%d/%Y'))

oo = filter(o, var == 'GN_Ca') %>% mutate(date = as.Date(datetime)) %>% select(date, val)
tz = select(t, date = Date, val_aul = Ca_WetDep) %>%
    mutate(val_aul = as.numeric(val_aul),
           val_aul = convert_Ca(val_aul))
zzz = full_join(oo, tz, by = 'date') %>%
    rowwise() %>%
    mutate(pct_diff = (val - val_aul) / mean(c(val, val_aul)) * 100) %>%
    ungroup()
fivenum(zzz$pct_diff, na.rm = T)
fivenum(filter(zzz, abs(pct_diff) != 200)$pct_diff, na.rm = T)

#ugh, off by about 200% across the board ####
#actually off by a lot more than that. this is just what happens when one number is
#close to 0 and the other is orders of magnitude larger.

#is it precip that's causing the discrepancy?

op = read_feather('data/webb/panola/derived/precipitation__ms900/mountain_creek_tributary.feather') %>%
    group_by(site_code, var) %>%
    mutate(date = as_date(datetime)) %>%
    tidyr::complete(date = seq(min(date), max(date), by = 'day')) %>%
    ungroup() %>%
    select(date, val)
tp = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/1_PMRW_Precipitation_WY86-18.csv') %>%
    mutate(datetime = as_datetime(Date, format = '%m/%d/%Y %H:%M:%S', tz = 'Etc/GMT+5'),
           datetime = with_tz(datetime, 'UTC')) %>%
    group_by(date = date(datetime)) %>%
    summarize(p_aul = sum(Precip)) %>%
    ungroup() %>%
    mutate(p_aul = p_aul * 10)
prism = read_feather('data/webb/panola/ws_traits/cc_precip/raw_mountain_creek_tributary.feather') %>%
    filter(var == 'cc_precip_median') %>%
    select(date = datetime, val) %>%
    inner_join(tp, by = 'date') %>%
    rowwise() %>%
    mutate(pct_diff = (val - p_aul) / mean(c(val, p_aul)) * 100) %>%
    ungroup()

zp = full_join(op, tp, by = 'date') %>%
    rowwise() %>%
    mutate(pct_diff = (val - p_aul) / mean(c(val, p_aul)) * 100) %>%
    ungroup() %>%
    filter(! is.na(p_aul) & ! is.na(val))

fivenum(filter(zp, abs(pct_diff) != 200)$pct_diff, na.rm = T)
zp[zp$pct_diff > 198,]
zp[zp$pct_diff < -198,]

sum(abs(zp$pct_diff) > 100)

#annual precip?
p_ann = zp %>%
    mutate(wy = ifelse(month(date) >= 10, year(date) + 1, year(date))) %>%
    group_by(wy) %>%
    summarize(ms = sum(val, na.rm = T),
              aul = sum(p_aul, na.rm = T),
              .groups = 'drop') %>%
    rowwise() %>%
    mutate(pct_diff = (ms - aul) / mean(c(ms, aul)) * 100) %>%
    ungroup()

#all good. so there's just a huge P-Q discrep in aulenbach?
p_ann
q_ann
#yes. because P often exceeds Q by a large margin. it's just the other way around that you need to watch out for

#lots of big discreps in precip. likely cause. let's see about pchem ####

convert_Ca2 <- function(ca_ueq_L) {
    ca_mg_L <- ca_ueq_L * 20.04 * 10^(-6) * 1000
    return(ca_mg_L)
}

opc = read_feather('data/webb/panola/derived/precip_chemistry__ms901/mountain_creek_tributary.feather') %>%
    group_by(site_code, var) %>%
    mutate(date = as_date(datetime)) %>%
    tidyr::complete(date = seq(min(date), max(date), by = 'day')) %>%
    ungroup() %>%
    select(date, val, var)
tpc = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/2_PMRW_PrecipitationWaterQuality_WY86-17.csv') %>%
    mutate(Date = as_date(Date, format = '%m/%d/%Y %H:%M'))

oopc = filter(opc, var == 'GN_Ca') %>% select(date, val)
ttpc = select(tpc, date = Date, val_aul = Ca_Conc) %>%
    mutate(val_aul = as.numeric(val_aul),
           val_aul = convert_Ca2(val_aul)) %>%
    group_by(date) %>%
    summarize(val_aul = mean(val_aul, na.rm =T)) %>%
    ungroup()
zz = full_join(oopc, ttpc, by = 'date') %>%
    rowwise() %>%
    mutate(pct_diff = (val - val_aul) / mean(c(val, val_aul)) * 100) %>%
    ungroup()
zz_ = zz %>%
    filter(! is.na(pct_diff))
fivenum(zz_$pct_diff, na.rm = T)
fivenum(filter(zz_, abs(pct_diff) != 200)$pct_diff, na.rm = T)

zz[zz$pct_diff > 100,]
zz[zz$pct_diff < -100,]

sum(abs(zz$pct_diff) > 100)

#okay, so it's precip causing all the trouble? ####

zp
zp$pct_diff

#why are we so wrong about 1985-10-21?

gg = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/1_PMRW_Precipitation_WY86-18.csv') %>%
    mutate(Date = as_datetime(Date, format = '%m/%d/%Y %H:%M:%S'),
           Precip = Precip * 10)
chez_dates = as.Date(c('1985-10-19', '1985-10-20', '1985-10-21', '1985-10-22', '1985-10-23'))
qrg = (filter(gg, as.Date(Date) %in% chez_dates))
filter(op, date %in% chez_dates)
sum(qrg$Precip)

#this is after ms_read_csv
filter(d, as.Date(datetime) %in% chez_dates)

#the 1985-10-21 discrepancy is just because of datetime shifts
#so what's up with pflux? no explanation for consistency of discrepancy

plot(density(na.omit(zzz$pct_diff)))
chez_dates = as.Date(c('1985-10-14', '1985-10-15', '1985-10-16', '1985-10-17', '1985-10-18'))
#pflux
filter(zzz, date %in% chez_dates)
#precip
filter(zp, date %in% chez_dates)
#pchem
filter(zz, date %in% chez_dates)
filter(tpc, between(Date, as.Date('1985-10-01'), as.Date('1985-11-01')))
# ms_conversions(tibble(datetime = as.POSIXct(1), site_code = 'a', var = 'GN_Ca', val = 1.99),
#                convert_units_from = c(Ca = 'ueq/L'), convert_units_to = c(Ca = 'mg/L'))

#interesting. it may be that aulenbach used linear interpolation for precip chemistry, whereas
#we use nocb. aulenbach would have then interpolated Ca on 10-16 as hasf of what we did. still, that
#   should produce at worst a 100% discrep. but let's see if the numbers are different for Cl

var__ = 'GN_Cl'

conv_ = function(zfg, molec, frm, to){
    convert_unit(convert_to_gl(x = zfg, input_unit = frm, molecule = molec),
                 frm, to)
}

#pflux
oo = filter(o, var == var__) %>% mutate(date = as.Date(datetime)) %>% select(date, val)
tz = select(t, date = Date, val_aul = SO4_WetDep) %>%
    mutate(val_aul = as.numeric(val_aul),
           val_aul = conv_(val_aul, 'Cl', 'ueq/L', 'kg/L') / 1000)
zzz = full_join(oo, tz, by = 'date') %>%
    rowwise() %>%
    mutate(pct_diff = (val - val_aul) / mean(c(val, val_aul)) * 100) %>%
    ungroup()

#pchem
oopc = filter(opc, var == var__) %>% select(date, val)
ttpc = select(tpc, date = Date, val_aul = SO4_Conc) %>%
    mutate(val_aul = as.numeric(val_aul),
           val_aul = conv_(val_aul, 'Cl', 'ueq/L', 'mg/L')) %>%
    group_by(date) %>%
    summarize(val_aul = mean(val_aul, na.rm =T)) %>%
    ungroup()
zz = full_join(oopc, ttpc, by = 'date') %>%
    rowwise() %>%
    mutate(pct_diff = (val - val_aul) / mean(c(val, val_aul)) * 100) %>%
    ungroup()

# chez_dates = seq(as.Date('1985-10-01'), as.Date('1985-10-30'), 'day')
#compare
filter(zzz, date %in% chez_dates) %>% print(n=100)
#precip
filter(zp, date %in% chez_dates) %>% print(n=100)
#pchem
filter(zz, date %in% chez_dates) %>% print(n=100)

#argh! no idea why the numbers are so different. let's look at annual loads ####

a = read_feather('data/webb/panola/derived/precip_flux_inst__ms902/mountain_creek_tributary.feather') %>%
    mutate(wy = ifelse(month(datetime) >= 10, year(datetime) + 1, year(datetime))) %>%
    group_by(wy, var) %>%
    summarize(val = sum(val, na.rm = T)) %>%
    ungroup()
b = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/10_PMRW_WetDepositionSoluteFluxes_WaterYear_WY86-16.csv')

aa = filter(a, var == 'GN_Ca') %>% select(-var)

bb = select(b, wy = Water_Year, val_aul = Ca_WetDep) %>%
    mutate(val_aul = conv_(as.numeric(val_aul), 'Ca', 'meq/L', 'kg/L') * 10000)

omg = full_join(aa, bb, by = 'wy') %>%
    rowwise() %>%
    mutate(pct_diff = (val - val_aul) / mean(c(val, val_aul)) * 100) %>%
    ungroup()
tail(omg)


#zomg, i'm working with unscaled pflux! revisit this after postprocess! ####

a = read_feather('data/webb/panola/derived/precip_flux_inst_scaled__ms902/mountain_creek_tributary.feather') %>%
    mutate(wy = ifelse(month(datetime) >= 10, year(datetime) + 1, year(datetime))) %>%
    group_by(wy, var) %>%
    summarize(val = sum(val, na.rm = T)) %>%
    ungroup()
b = read_csv('data/webb/panola/raw/discharge__VERSIONLESS001/sitename_NA/10_PMRW_WetDepositionSoluteFluxes_WaterYear_WY86-16.csv')

aa = filter(a, var == 'GN_Ca') %>% select(-var)

bb = select(b, wy = Water_Year, val_aul = Ca_WetDep) %>%
    mutate(val_aul = conv_(as.numeric(val_aul), 'Ca', 'meq/L', 'kg/L') * 10000)

omg = full_join(aa, bb, by = 'wy') %>%
    rowwise() %>%
    mutate(pct_diff = (val - val_aul) / mean(c(val, val_aul)) * 100) %>%
    ungroup()

#wow, all of this panicking was for nought ####

#okay, let's look at neon MCRA versus hjandrews MACK ####

#pflux

x = read_feather('data/lter/hjandrews/derived/precip_flux_inst_scaled__ms902/GSMACK.feather') %>%
    filter(var == 'GN_Ca')
y = read_feather('data/neon/neon/derived/precip_flux_inst_scaled__ms902/MCRA.feather') %>%
    filter(var == 'GN_Ca')

x = select(x, datetime, val)
y = select(y, datetime, val)

yyy = full_join(x, y, by = 'datetime') %>%
    rowwise() %>%
    mutate(pct_diff = (val.x - val.y) / mean(c(val.x, val.y)) * 100) %>%
    ungroup()

na.omit(yyy$pct_diff) %>% fivenum()

sum(abs(yyy$pct_diff) > 50, na.rm=T)
yyy = filter(yyy, ! is.na(pct_diff))
yyy[yyy$pct_diff > 100,]

#not bad. what about annual totals?

xtot = x %>%
    group_by(yr = year(datetime)) %>%
    summarize(val = sum(val, na.rm = T))
ytot = y %>%
    group_by(yr = year(datetime)) %>%
    summarize(val = sum(val, na.rm = T))

yyy = full_join(xtot, ytot, by = 'yr') %>%
    rowwise() %>%
    mutate(pct_diff = (val.x - val.y) / mean(c(val.x, val.y)) * 100) %>%
    ungroup()

na.omit(yyy$pct_diff) %>% fivenum()

sum(abs(yyy$pct_diff) > 50, na.rm=T)
yyy = filter(yyy, ! is.na(pct_diff))
yyy[yyy$pct_diff > 100,]

#jolly good. and sflux?

x = read_feather('data/lter/hjandrews/derived/stream_flux_inst_scaled__ms001/GSMACK.feather') %>%
    filter(var == 'GN_Ca')
y = read_feather('data/neon/neon/derived/stream_flux_inst_scaled__ms006/MCRA.feather') %>%
    filter(var == 'GN_Ca')

x = select(x, datetime, val)
y = select(y, datetime, val)

yyy = full_join(x, y, by = 'datetime') %>%
    rowwise() %>%
    mutate(pct_diff = (val.x - val.y) / mean(c(val.x, val.y)) * 100) %>%
    ungroup()

na.omit(yyy$pct_diff) %>% fivenum()

sum(abs(yyy$pct_diff) > 50, na.rm=T)
yyy = filter(yyy, ! is.na(pct_diff))
yyy[yyy$pct_diff > 100,]
yyy[yyy$pct_diff < 100,]

#oh right, these are actually different streams. nvm
