# domains with and without precip interp ####

#domains with at least some precip interpolation:
#    "bear"      "krycklan"  "sleepers"  "loch_vale" "santee"    "bonanza", plum, neon
#domains without precip interp:
#    east_river"    "walker_branch" "krew"          "hbef
#is it true that all the others are without precip entirely?

zz = system('find data/ -type f -name "*.feather" -path "*/precipitation*/*"',
       intern = TRUE)

dd = tibble()
for(f in zz){
    domain = str_extract(f, 'data/[a-z_]+/([a-z_]+)', group = 1)
    site = str_extract(f, '/([\\w\\.\\-_]+?)\\.feather$', group = 1)
    d = read_feather(f)
    interps = sum(d$ms_interp == 1)
    # interps = tryCatch(sum(d$ms_interp == 1),
                       # warning = function(w){
                       #     print(domain)
                       #     print(site)
                       # })
    dd <- bind_rows(dd, tibble(domain = domain,
                               site = site,
                               interps = interps))
    # readLines(n=1)
}
dd->dd_

good_dmns = filter(dd, interps == 0) %>%
    print(n=500) %>%
    pull(domain)
bad_dmns = filter(dd, interps > 1) %>%
    print(n=500) %>%
    pull(domain)

intersect(good_dmns, bad_dmns)
setdiff(good_dmns, bad_dmns)
setdiff(bad_dmns, good_dmns)

#### ? ####
filter(domain_detection_limits, domain == 'loch_vale')->zz
unique(zz$prodcode)

sapply(half_detlims_all$prodcode, function(x){
    any(str_split(x, '\\|')[[1]] == prodname_ms)
}) %>% keep(isTRUE) %>%
    names() %>%
    unique()

# ? ####

ggplot(d) +
    geom_line(aes(x = datetime, y = val, group = site_code)) +
    facet_wrap(~site_code)
ggplot(d_) +
    geom_line(aes(x = datetime, y = val, group = site_code)) +
    facet_wrap(~site_code)

d %>%
    filter(site_code == 'HQ', datetime < as.Date('1990-01-01')) %>%
    ggplot() +
    geom_line(aes(x = datetime, y = val, group = site_code)) +
    facet_wrap(~site_code)
d_ %>%
    filter(site_code == 'HQ', datetime < as.Date('1990-01-01')) %>%
    ggplot() +
    geom_line(aes(x = datetime, y = val, group = site_code)) +
    facet_wrap(~site_code)

## catalina and plum are missing the interp column for precip ####
#this is legit. they use munge_time_component
x = read_feather('data/lter/plum/munged/precipitation__140/governors_academy.feather')
read_feather('data/lter/plum/munged/precipitation__179/mbl_marshview.feather')
read_feather('data/lter/plum/munged/stream_chemistry__104/egypt_river.feather')

# do these domains really lack precip? ####
#no, but what's the deal?
some_precip_interp <- c('bear', 'krycklan', 'sleepers', 'loch_vale', 'santee',
                        'bonanza', 'plum', 'santa_barbara', 'luquillo', 'konza',
                        'hjandrews', 'arctic', 'niwot', 'catalina_jemez', 'boulder',
                        'shale_hills', 'neon')
#check these for konzaness. konza is the gut check. same with catalina
no_precip_interp <- c('east_river', 'walker_branch', 'krew', 'hbef', 'santa_barbara',
                      'baltimore', 'konza', 'hjandrews', 'catalina_jemez',
                      'calhoun', 'shale_hills', 'neon')
boths <- intersect(some_precip_interp, no_precip_interp)
nones <- setdiff(no_precip_interp, some_precip_interp)

setdiff(network_domain$domain, c(some_precip_interp, no_precip_interp)) %>%
    paste(collapse = "', '")
no_precip <- c('mcmurdo', 'trout_lake', 'acton_lake', 'mces', 'swwd',
               'streampulse', 'fernow', 'suef', 'usgs', 'panola')

# investigate interp method for each domain ####

zz = system('find data/bear/bear -type f -name "*.feather" -path "*munged/precipitation*/*"',
zz = system('find data/webb/sleepers -type f -name "*.feather" -path "*munged/precipitation*/*"',
zz = system('find data/webb/loch_vale -type f -name "*.feather" -path "*munged/precipitation*/*"',
zz = system('find data/usfs/santee -type f -name "*.feather" -path "*munged/precipitation*/*"',
zz = system('find data/lter/plum -type f -name "*.feather" -path "*munged/precipitation*/*"',
zz = system('find data/neon -type f -name "*.feather" -path "*munged/precipitation*/*"',
            intern = TRUE)

# api_key <- read_lines('~/keys/noaa_climate_api')
x11()
for(f in zz){
    domain = str_extract(f, 'data/[a-z_]+/([a-z_]+)', group = 1)
    site = str_extract(f, '/([\\w\\.\\-_]+?)\\.feather$', group = 1)
    d = read_feather(f)
    print(domain)
    print(paste('interps:', sum(d$ms_interp == 1)))

    par(mfrow = c(6, 1), mar = c(2, 3, 2, 3))

    #view first year
    frst = filter(d, year(datetime) == min(year(datetime)) + 1)
    lst = filter(d, year(datetime) == max(year(datetime)) - 1)
    # frst = filter(d, year(datetime) == min(year(datetime)) + 1)
    # lst = filter(d, year(datetime) == max(year(datetime)) - 1)
    plot(frst$datetime, frst$val, main = site, col = as.factor(frst$ms_interp))
    plot(lst$datetime, lst$val, main = site, col = as.factor(lst$ms_interp))
    xx = readLines(n=1)

    if(xx == 's') next
    if(xx == 'p'){
        print('middate >'); middt = readLines(n=1)
        pp = get_nldas_precip(middate = middt, lat = site_data$latitude[site_data$domain == domain][1],
                              lon = site_data$longitude[site_data$domain == domain][1], window_size = 10)
        print(pp, n=1000)
    }

    #view second  and penultimate month
    frst = filter(d, floor_date(datetime, "month") == min(floor_date(datetime, "month")) + months(1))
    lst = filter(d, ceiling_date(datetime, "month") == max(ceiling_date(datetime, "month")) - months(1))
    plot(frst$datetime, frst$val, main = paste(site, year(frst$datetime[1])), col = as.factor(frst$ms_interp))
    plot(lst$datetime, lst$val, main = paste(site, year(lst$datetime[1])), col = as.factor(lst$ms_interp))
    xx = readLines(n=1)

    if(xx == 's') next
    if(xx == 'p'){
        print('middate >'); middt = readLines(n=1)
        pp = get_nldas_precip(middate = middt, lat = site_data$latitude[site_data$domain == domain][1],
                             lon = site_data$longitude[site_data$domain == domain][1], window_size = 10)
        print(pp, n=1000)
    }

    print('any button for next plot')
    catch <- readLines(n=1)

    #view 7th and 7th-to-last month
    frst = filter(d, floor_date(datetime, "month") == min(floor_date(datetime, "month")) + months(6))
    lst = filter(d, ceiling_date(datetime, "month") == max(ceiling_date(datetime, "month")) - months(6))
    plot(frst$datetime, frst$val, main = paste(site, year(frst$datetime[1])), col = as.factor(frst$ms_interp))
    plot(lst$datetime, lst$val, main = paste(site, year(lst$datetime[1])), col = as.factor(lst$ms_interp))
    xx = readLines(n=1)

    if(xx == 's') next
    if(xx == 'p'){
        print('middate >'); middt = readLines(n=1)
        pp = get_nldas_precip(middate = middt, lat = site_data$latitude[site_data$domain == domain][1],
                              lon = site_data$longitude[site_data$domain == domain][1], window_size = 10)
        print(pp, n=1000)
    }

    print('any button for next site')
    catch <- readLines(n=1)
}

# compare pchem ####

# zzz=read_feather('data/webb/sleepers/munged/precip_chemistry__VERSIONLESS002/R-29.feather')
# zzz=read_feather('data/webb/loch_vale/munged/precip_chemistry__VERSIONLESS002/CO98.feather')
zzz=read_feather('data/usfs/santee/munged/precip_chemistry__VERSIONLESS004/SEFHQ_pluvio.feather')
ggyr = min(year(zzz$datetime)) + 1
ggyrl = max(year(zzz$datetime)) - 1
ggmo = 4
ggyearmo = ymd(paste(ggyr, ggmo, '01', sep = '-'))
ggyearmol = ymd(paste(ggyrl, ggmo, '01', sep = '-'))
dd = filter(zzz, year(datetime) == ggyr)
ddl = filter(zzz, year(datetime) == ggyrl)

#precip month view
frst = filter(d, floor_date(datetime, "month") == ggyearmo)
lst = filter(d, floor_date(datetime, "month") == ggyearmol)
plot(frst$datetime, frst$val, main = paste(site, year(frst$datetime[1])), col = as.factor(frst$ms_interp))
plot(lst$datetime, lst$val, main = paste(site, year(lst$datetime[1])), col = as.factor(lst$ms_interp))

#pchem month view
frst = filter(dd, floor_date(datetime, "month") == ggyearmo) %>% filter(var == dd$var[1])
lst = filter(dd, floor_date(datetime, "month") == ggyearmol) %>% filter(var == dd$var[1])
plot(frst$datetime, frst$val, main = paste(site, year(frst$datetime[1])), col = as.factor(frst$ms_interp))
plot(lst$datetime, lst$val, main = paste(site, year(lst$datetime[1])), col = as.factor(lst$ms_interp))

#year-view
#P
frst = filter(d, year(datetime) == ggyr)
lst = filter(d, year(datetime) == ggyr)
plot(frst$datetime, frst$val, main = site, col = as.factor(frst$ms_interp))
plot(lst$datetime, lst$val, main = site, col = as.factor(lst$ms_interp))

#pchem
frst = filter(dd, var == dd$var[1])
lst = filter(dd, var == dd$var[1])
plot(frst$datetime, frst$val, main = site, col = as.factor(frst$ms_interp))
plot(lst$datetime, lst$val, main = site, col = as.factor(lst$ms_interp))
