#verify highres Q legitness
q1 = read_feather('data_hbef_highres/lter/hbef/derived/discharge__ms003/w1.feather')
q2 = read_feather('data_0.4/lter/hbef/derived/discharge__ms003/w1.feather')

plot(q1$datetime, q1$val, pch='.')
points(q2$datetime, q2$val, pch='.', col='red')

dtrng = as.POSIXct(c('2000-01-01', '2000-12-31'))
ylims = c(0, 80)

plot(q1$datetime, q1$val, pch='.', xlim=dtrng, ylim=ylims)
points(q2$datetime, q2$val, pch=20, col='red', xlim=dtrng, ylim=ylims)

#verify highres chem legitness
q1 = read_feather('data_hbef_highres/lter/hbef/derived/precip_chemistry__ms901/w1.feather')
q2 = read_feather('data_0.4/lter/hbef/derived/precip_chemistry__ms901/w1.feather')

plot(q1$datetime, q1$val, pch=20)
points(q2$datetime, q2$val, pch=20, col='red')
plot(q2$datetime, q2$val, pch=20)
points(q1$datetime, q1$val, pch=20, col='red')

dtrng = as.POSIXct(c('2000-01-01', '2000-12-31'))
ylims = c(0, 80)

plot(q1$datetime, q1$val, pch='.', xlim=dtrng, ylim=ylims)
points(q2$datetime, q2$val, pch='.', col='red', xlim=dtrng, ylim=ylims)
plot(q2$datetime, q2$val, pch=20, col='red', xlim=dtrng, ylim=ylims)
points(q1$datetime, q1$val, pch=20, xlim=dtrng, ylim=ylims)

anti_join(q1, q2)
anti_join(q2, q1)

anti_join(q1, q2, by='datetime')
anti_join(q2, q1, by='datetime')

sum(is.na((anti_join(q2, q1, by='datetime')$val)))

q1[q1$datetime == as.POSIXct('2012-09-11 00:00:00', tz='UTC') & q1$var == 'GN_Al_ICP',]
q2[q2$datetime == as.POSIXct('2012-09-11 00:00:00', tz='UTC') & q2$var == 'GN_Al_ICP',]

filter(q2,
       datetime > as.POSIXct('2012-09-10 23:00:00', tz='UTC'),
       var == 'GN_Al_ICP',
       datetime < as.POSIXct('2012-09-11 01:00:00', tz='UTC'))

dts = anti_join(q1, q2)$datetime
for(i in seq_along(dts)){
    dt = dts[i]
    q1[q1$datetime == dt,]

    #aargh! continue this later. need to find out if all the records from q1 that
    #don't exist in q2 are different in ms_interp only, and then find out why.
    #note that both "q" datasets are just chemistry in this case
