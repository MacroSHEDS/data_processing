# problematic datetime rounding ####

#to use: run lter/hbef/process_1_1 and save the adjusted output as dd
png(height=4, width=8, filename='plots/problematic_datetime_rounding.png',
    units='in', res=300,)
# par(mfrow=c(2, 1))
ylm = c(0, 50)
xlm0 = c(30, 50)
xlm = c(d$datetime[xlm0[1]], d$datetime[xlm0[2]])
cols = as.factor(dd$ms_interp)
levels(cols) = c('blue', 'red')
cols = as.character(cols)
plot(d$datetime, d$discharge, type='p', xlim=xlm, ylim=ylm)
points(dd$datetime, dd$discharge, col=cols, pch=20)
# plot(dd$datetime, dd$discharge, type='p', xlim=xlm, col=cols, ylim=ylm)
# plot(dd$datetime, dd$discharge, type='l', col=cols)
dev.off()
