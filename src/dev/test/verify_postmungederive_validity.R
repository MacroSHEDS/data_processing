#run acquisition_master.R down to main loop

#do our totals for Panola match those determined by Aulenbach et al.? ####

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

o %>%
    mutate(runoff = discharge_to_runoff(val, ws_area_ha = ws_area)) %>%
    mutate(wy = ifelse(month(datetime) >= 10, year(datetime) + 1, year(datetime))) %>%
    # print(n=100) %>%
    group_by(wy) %>%
    summarize(macrosheds_runoff = sum(runoff, na.rm = T)) %>%
    left_join(tt, by = 'wy') %>%
    rowwise() %>%
    mutate(pct_diff = (macrosheds_runoff - Runoff) / mean(c(macrosheds_runoff, Runoff)) * 100) %>%
    ungroup()
