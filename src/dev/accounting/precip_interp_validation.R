# run doe/walker_branch process_1_VERSIONLESS001 to generate `d` (skip this) ####

#obs vs interp
d[duplicated(d$datetime) | duplicated(d$datetime, fromLast = TRUE),] %>%
    arrange(datetime)

plot(interped$val, not_interped$val, xlim = c(0, 70), ylim = c(0, 70))
abline(a=0, b=1)
abline(lm(not_interped$val ~ interped$val))

#just the non-zeros
qz = cbind(interped = drop_errors(interped$val), not_interped = drop_errors(not_interped$val)) %>%
    as_tibble() %>%
    filter(interped != 0, not_interped != 0)

plot(qz$interped, qz$not_interped, xlim = c(0, 70), ylim = c(0, 70))
abline(a=0, b=1)
abline(lm(qz$not_interped ~ qz$interped))

#binary
qx = cbind(interped = drop_errors(interped$val), not_interped = drop_errors(not_interped$val)) %>%
    as_tibble() %>%
    mutate(interped = as.numeric(interped > 0),
           not_interped = as.numeric(not_interped > 0))

qxx = apply(qx, 1, sum)
#0 good, 2, good, 1 bad
table(qxx)
(214 + 211) / nrow(qx) * 100
#58% of interped precip days at least correctly identify whether there was precip or not. not very good.


obs = read_feather('data/usfs/santee/munged/precipitation__VERSIONLESS001/Lotti.feather')
obs = read_feather('data/lter/hbef/munged/precipitation__13/RG11.feather')
obs = slice_tail(obs, n = 100)
    # filter(year(datetime) == 2017) %>%
sitecode = obs$site_code[1]
obs = select(obs, datetime, val)

siterow = site_data %>%  filter(site_code == sitecode)
precip_fill = get_ldas_precip(lat = siterow$latitude,
                              lon = siterow$longitude,
                              startdate = as.Date(min(obs$datetime)),
                              enddate = as.Date(max(obs$datetime)))

zz = mutate(precip_fill, datetime = as.POSIXct(date)) %>%
    select(-date)
zzz = left_join(obs, zz)

plot(zzz$val, zzz$precip_mm)
abline(a=0, b=1)
abline(lm(zzz$val ~ zzz$precip_mm))

# ----
zz = mutate(zz, precip_mm = lead(precip_mm))
zzz = left_join(obs, zz)

plot(zzz$val, zzz$precip_mm)
abline(a=0, b=1)
abline(lm(zzz$val ~ zzz$precip_mm))

#no need to run anything first ####

#get_ldas_precip WORKS in south carolina ----
obs = read_feather('data/usfs/santee/munged/precipitation__VERSIONLESS001/Lotti.feather')
obs = slice_tail(obs, n = 100)
# filter(year(datetime) == 2017) %>%
sitecode = obs$site_code[1]
obs = select(obs, datetime, val)

siterow = site_data %>%  filter(site_code == sitecode)
precip_fill = get_ldas_precip(lat = siterow$latitude,
                              lon = siterow$longitude,
                              startdate = as.Date(min(obs$datetime)),
                              enddate = as.Date(max(obs$datetime)))

zz = mutate(precip_fill, datetime = as.POSIXct(date)) %>%
    select(-date)
zzz = left_join(obs, zz)

plot(zzz$val, zzz$precip_mm)
abline(a=0, b=1)
abline(lm(zzz$val ~ zzz$precip_mm))
print(zzz, n=100)

#get_ldas_precip DOES NOT WORK in new hampshire
obs = read_feather('data/lter/hbef/munged/precipitation__13/RG11.feather')
obs = slice_tail(obs, n = 100)
# filter(year(datetime) == 2017) %>%
sitecode = obs$site_code[1]
obs = select(obs, datetime, val)

siterow = site_data %>%  filter(site_code == sitecode)
precip_fill = get_ldas_precip(lat = siterow$latitude,
                              lon = siterow$longitude,
                              startdate = as.Date(min(obs$datetime)),
                              enddate = as.Date(max(obs$datetime)))

zz = mutate(precip_fill, datetime = as.POSIXct(date)) %>%
    select(-date)
zzz = left_join(obs, zz)

plot(zzz$val, zzz$precip_mm)
abline(a=0, b=1)
abline(lm(zzz$val ~ zzz$precip_mm))
print(zzz, n=100)

# but it works with lead-1! ----

zz = mutate(zz, precip_mm = lead(precip_mm))
zzz = left_join(obs, zz)

plot(zzz$val, zzz$precip_mm)
abline(a=0, b=1)
abline(lm(zzz$val ~ zzz$precip_mm))

# func for this ####

chez_precip <- function(fpath, lead1 = FALSE){

    obs = read_feather(fpath)
    obs = slice_tail(obs, n = 100)
    # filter(year(datetime) == 2017) %>%
    sitecode = obs$site_code[1]
    obs = select(obs, datetime, val)

    siterow = site_data %>% filter(site_code == sitecode)
    precip_fill = get_ldas_precip(lat = siterow$latitude,
                                  lon = siterow$longitude,
                                  startdate = as.Date(min(obs$datetime)),
                                  enddate = as.Date(max(obs$datetime)))

    zz = mutate(precip_fill, datetime = as.POSIXct(date)) %>%
        select(-date)

    if(lead1){
        zz = mutate(zz, precip_mm = lead(precip_mm))
    }

    zzz = left_join(obs, zz)

    plot(zzz$val, zzz$precip_mm)
    abline(a=0, b=1)
    abline(lm(zzz$val ~ zzz$precip_mm))
    print(zzz, n=100)
}
chez_precip('data/usfs/santee/munged/precipitation__VERSIONLESS001/Lotti.feather')
chez_precip('data/usfs/santee/munged/precipitation__VERSIONLESS001/Lotti.feather', T)
chez_precip('data/lter/hbef/munged/precipitation__13/RG11.feather')
