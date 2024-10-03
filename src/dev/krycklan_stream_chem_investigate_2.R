x0 = read_csv('data/krycklan/krycklan/raw/stream_chemistry__VERSIONLESS003/sitename_NA/KCS 101 data 2021-06-08.csv', col_types = 'Dnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn')
# x0 = read.csv('data/krycklan/krycklan/raw/stream_chemistry__VERSIONLESS003/sitename_NA/KCS 101 data 2021-06-08.csv') %>%
#     filter(SiteID == '1')
# cbind(x0$Date, x0$Br.µg.l) %>% head(n = 375) %>% tail(n=30)

# '([A-Za-z0-9µ\\.]+)' = '([A-Za-z_0-9]+)'
# \\.([A-Za-z0-9µ]+)\\.([A-Za-z0-9µ]+)`,$
x = rename(x0, spCond = `EC µS/cm`,
DOC = `DOC mg C/l`,
NO3_NO2_N = `NO2-N/NO3-N µg N/l`,
DIC = `DIC mg/l`,
TP = `tot-P µg P/l`,
DIN = `DIN µg/l`,
pH = `pH (@25°C)`,
TOC = `TOC mg C/l`,
NO2_N = `NO2-N µg N/l`,
NH4_N = `NH4-N µg N/l`,
NO3_N = `NO3-N µg N/l`,
TN = `tot-N mg N/l`,
SO4 = `SO4-SO4 µg SO4/l`,
CH4_C = `CH4-C µg/l`,
PO4_P = `PO4-P µg P/l`,
SO4_S = `S-SO4 µg S/l`,
CO2_C = `CO2-C mg/l`,
Cu = `Cu µg/l`,
Cs = `Cs µg/l`,
Mn = `Mn µg/l`,
Na = `Na µg/l`,
Bi = `Bi µg/l`,
Co = `Co µg/l`,
Cl = `Cl µg/l`,
Cd = `Cd µg/l`,
Ca = `Ca µg/l`,
Ce = `Ce µg/l`,
B = `B µg/l`,
Br = `Br µg/l`,
Be = `Be µg/l`,
Cr = `Cr µg/l`,
P = `P µg/l`,
Os = `Os µg/l`,
Ru = `Ru µg/l`,
Nd = `Nd µg/l`,
#'Gd µg/l' = 'Gd',
Ti = `Ti µg/l`,
Tb = `Tb µg/l`,
Se = `Se µg/l`,
Li = `Li µg/l`,
Y = `Y µg/l`,
Nb = `Nb µg/l`,
Tm = `Tm µg/l`,
Sc = `Sc µg/l`,
Sm = `Sm µg/l`,
V = `V µg/l`,
Dy = `Dy µg/l`,
Te = `Te µg/l`,
Rb = `Rb µg/l`,
Ta = `Ta µg/l`,
Zn = `Zn µg/l`,
Gd = `Gd µg/l`,
Tl = `Tl µg/l`,
Lu = `Lu µg/l`,
Si = `Si µg/l`,
Mg = `Mg µg/l`,
`F` = `F µg/l`,
U = `U µg/l`,
Pt = `Pt µg/l`,
Er = `Er µg/l`,
Zr = `Zr µg/l`,
K = `K µg/l`,
Hg = `Hg µg/l`,
Ho = `Ho µg/l`,
Th = `Th µg/l`,
I = `I µg/l`,
Fe = `Fe µg/l`,
Au = `Au µg/l`,
Ni = `Ni µg/l`,
Hf = `Hf µg/l`,
Sn = `Sn µg/l`,
Sr = `Sr µg/l`,
S = `S µg/l`,
La = `La µg/l`,
Ir = `Ir µg/l`,
Pb = `Pd µg/l`,
#'V µg/l_1' = 'V',
Pd = `Pb µg/l`,
Al = `Al µg/l`,
Ag = `Ag µg/l`,
Ba = `Ba µg/l`,
Ge = `Ge µg/l`,
Mo = `Mo µg/l`,
Pr = `Pr µg/l`,
As = `As µg/l`,
Yb = `Yb µg/l`,
Sb = `Sb µg/l`,
Eu = `Eu µg/l`,
Rh = `Rh µg/l`,
W = `W µg/l`,
Re = `Re µg/l`,
Ga = `Ga µg/l`)

# x2 = read_feather('data/krycklan/krycklan/derived/stream_chemistry__ms001/Site1.feather')

sfs = list.files('data/krycklan/krycklan/derived/stream_chemistry__ms001', full.names = T)
sites = str_match(sfs, '(Site[0-9]+)\\.feather')[, 2]
sites_as_num = str_match(sites, 'Site([0-9]{1,2})')[, 2]

for(i in seq_along(sites)){
    site = sites[i]
    f = sfs[i]
    x_site = sites_as_num[i]
    x_sub = filter(x, SiteID == !!x_site)
    x2 = read_feather(f)
    vars = unique(x2$var)
    png(width=8, height = 8, units='in', type='cairo', res=300,
        filename = glue('plots/krycklan_stream_chem_comparison/{site}.png'))
    if(x_site %in% as.character(61:65)){
        par(mfrow=c(4,4), oma = c(0,0,0,0), mar = c(2,2,2,2))
    } else {
        par(mfrow=c(10,10), oma = c(0,0,0,0), mar = c(0,0,0,0))
    }
    for(v in vars){
        var_nopref = str_split(v, '_', n = 2)[[1]][2]
        if(! var_nopref %in% colnames(x_sub)){
            print(paste('skipping', var_nopref))
            next
        }
        xx = mutate(x_sub, datetime = as_datetime(Date)) %>%
            select(datetime, !!var_nopref) %>%
            mutate(across(-datetime, as.numeric)) %>%
            arrange(datetime)
        xx2 = filter(x2, var == !!v) %>% arrange(datetime)
        if(! var_nopref %in% c('DOC', 'pH', 'spCond', 'TN', 'TOC')){
            xx2 = mutate(xx2, val = val * 1000)
        }
        if(nrow(xx2) && nrow(xx)){
            ylm = c(0, max(c(xx[[var_nopref]], xx2$val), na.rm=T))
            xlm = c(min(c(xx$datetime, xx2$datetime)), max(c(xx$datetime, xx2$datetime)))
            plot(xx$datetime, xx[[var_nopref]], type = 'b', ylim = ylm, xlim = xlm, xaxt='n', yaxt='n',
                 main = '', xlab = 'date', ylab=paste(var_nopref, 'µg/L'))
            lines(xx2$datetime, xx2$val, col = 'red')
            points(xx2$datetime, xx2$val, col = 'red', pch = ',')
            mtext(paste(site, var_nopref), 3, line = -2, col='blue', cex=0.8)
        }
    }
    dev.off()
    # readLines(n=1)
}

# filter(xx, !is.na(Br))
# xx2
# select(x_sub, Date, Br) %>% filter(! is.na(Br)) %>% arrange(Date)
#
# qq=select(x, SiteID, Date, Br) %>% filter(! is.na(Br)) %>% arrange(SiteID, Date)
# gg1=duplicated(qq[, c('SiteID', 'Date')])
# gg2=duplicated(qq[, c('SiteID', 'Date')], fromLast = TRUE)
# qq[gg1|gg2,]
#
# select(x, SiteID, Date, Br) %>% filter(! is.na(Br)) %>% arrange(SiteID, Date) %>%
#     group_by(SiteID, Date) %>%
#     summarize(Br = mean(Br, na.rm=T)) %>%
#     ungroup() %>%
#     pivot_wider(names_from = SiteID, values_from = Br)


# do the same, but just for silicon ####

dupes = tibble()
png(width=8, height = 8, units='in', type='cairo', res=300,
    filename = glue('plots/krycklan_stream_si_comparison.png'))
par(mfrow=c(5,5), oma = c(0,3,0,0), mar = c(2,2,0,0))
for(i in seq_along(sites)){
    site = sites[i]
    f = sfs[i]
    x_site = sites_as_num[i]
    x_sub = filter(x, SiteID == !!x_site)
    x2 = read_feather(f)

    var_nopref = 'Si'
    xx = mutate(x_sub, datetime = as_datetime(Date)) %>%
        select(datetime, !!var_nopref) %>%
        mutate(across(-datetime, as.numeric)) %>%
        arrange(datetime)
    xx2 = filter(x2, var == 'GN_Si') %>% arrange(datetime)
    xx2 = mutate(xx2, val = val * 1000)
    if(nrow(xx2) && nrow(xx)){
        ylm = c(0, max(c(xx[[var_nopref]], xx2$val), na.rm=T))
        xlm = c(min(c(xx$datetime, xx2$datetime)), max(c(xx$datetime, xx2$datetime)))
        plot(xx$datetime, xx[[var_nopref]], type = 'b', ylim = ylm, xlim = xlm,# xaxt='n', yaxt='n',
             main = '', xlab = 'date', ylab=paste(var_nopref, 'µg/L'), xaxs='i', yaxs='i')
        lines(xx2$datetime, xx2$val, col = 'red')
        points(xx2$datetime, xx2$val, col = 'red', pch = ',')
        mtext(site, 3, line = -2, col = 'blue')
    }

    zz = full_join(xx, select(xx2, datetime, val, ms_status), by = 'datetime')
    greatest_diff = mutate(zz, diff = abs(Si - val), diff = ifelse(is.na(diff), 0, diff))%>% arrange(desc(diff)) %>% slice(1) %>% pull(diff)
    print(paste(site, greatest_diff))

    if(any(duplicated(xx$datetime))){
            dupes = filter(xx, ! is.na(Si)) %>%
                filter(duplicated(datetime) | duplicated(datetime, fromLast = TRUE)) %>%
                arrange(datetime) %>%
            mutate(site = !!site) %>%
            relocate(site, .after='datetime') %>%
                bind_rows(dupes)
    }

}
mtext('Si (µg/L)', 2, line = 1, outer = T)
dev.off()

dupes = mutate(dupes, date = as.Date(datetime)) %>% select(-datetime)
dupes = select(dupes, date, site, Si) %>% arrange(site, date)
write_csv(dupes, '~/temp/duplicate_si_records.csv')

# filter(xx, datetime < as.Date('2020-05-16'), datetime > as.Date('2020-05-14'))
# filter(xx2, datetime < as.Date('2020-05-16'), datetime > as.Date('2020-05-14'))
# filter(xx, datetime < as.Date('2002-06-11'), datetime > as.Date('2002-06-09'))
# filter(x0, Date < as.Date('2002-06-11'), Date > as.Date('2002-06-09')) %>% View()
# filter(zz, datetime < as.Date('2011-08-16'), datetime > as.Date('2011-08-14'))
