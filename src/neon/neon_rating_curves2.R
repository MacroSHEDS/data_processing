#building incomplete Z-Q rating curves for 3 neon sites: COMO, FLNT, BLDE.
#using long intervals during which sensors were not moved.

library(tidyverse)
library(RColorBrewer)
library(feather)
library(glue)
library(lubridate)
library(data.table)

setwd('~/git/macrosheds/data_acquisition/data/neon/')

source('../../src/neon/neon_rating_curve_helpers.R')
zq = get_zq('zq_data_temp.rds')

#FLNT ####

site = 'FLNT'

#get stage; plot it; grab only downstream sensor (102, vs. upstream=101); replot
sp = get_sensor_positions(site)
el = get_surface_elevation(site)
stg = calc_stage(sp, el)

plot_stage(stg, sp, site)

#rating curve (fit and plot)
site_zq = filter(zq, siteID == site)
mod = fit_zq(site_zq, 'power')
plot_zq(site_zq, mod, site)

#generate discharge predictions and plot
stg$discharge_cms = predict(mod, list(Z=stg$stage))
plot_discharge(stg, sp, site)

#make speccond-Q rating curve
site_zq = merge_speccond_zq(site, site_zq) #no speccond for this site

#COMO ####

site = 'COMO'

#get stage; plot it; grab only downstream sensor (102, vs. upstream=101); replot
sp = get_sensor_positions(site)
el = get_surface_elevation(site)
stg = calc_stage(sp, el)

plot_stage(stg, sp, site)
stg = filter(stg, horizontalPosition == '102')
plot_stage(stg, sp, site)

#rating curve (fit and plot)
site_zq = filter(zq, siteID == site)
mod = fit_zq(site_zq, 'exponential')
plot_zq(site_zq, mod, site)

#clean/filter data (input required here); refit; replot
COMO_cut_date = '2018-02-01'
COMO_problem_points = 0.23280

stg = filter(stg, startDateTime > as.POSIXct(COMO_cut_date, tz='UTC'))
plot_stage(stg, sp, site)

site_zq = filter(site_zq,
    startDate > COMO_cut_date,
    ! totalDischarge %in% COMO_problem_points)

mod = fit_zq(site_zq, 'exponential')
plot_zq(site_zq, mod, site)

#generate discharge predictions and plot
stg$discharge_cms = predict(mod, list(Z=stg$stage))
plot_discharge(stg, sp, site)

#make speccond-Q rating curve?
site_zq = merge_speccond_zq(site, site_zq) #speccond unusable for this site.

#BLDE ####

site = 'BLDE'

#get stage; plot it; grab only downstream sensor (102, vs. upstream=101); replot
sp = get_sensor_positions(site)
el = get_surface_elevation(site)
stg = calc_stage(sp, el)

plot_stage(stg, sp, site)
stg = filter(stg, horizontalPosition == '102')
plot_stage(stg, sp, site)

#rating curve (fit and plot)
site_zq = filter(zq, siteID == site)
mod = fit_zq(site_zq, 'exponential')
plot_zq(site_zq, mod, site)

#clean/filter data (input required here); refit; replot
BLDE_cut_date = '2019-07-01'

stg = filter(stg, startDateTime < as.POSIXct(BLDE_cut_date, tz='UTC'))
plot_stage(stg, sp, site)

site_zq = filter(site_zq, startDate > BLDE_cut_date)

mod = fit_zq(site_zq, 'exponential')
plot_zq(site_zq, mod, site)

#generate discharge predictions and plot
stg$discharge_cms = predict(mod, list(Z=stg$stage))
plot_discharge(stg, sp, site)

#make speccond-Q rating curve?
site_zq = merge_speccond_zq(site, site_zq) #no speccond for this site
