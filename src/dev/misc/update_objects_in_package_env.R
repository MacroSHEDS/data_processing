# Include this code at the top of your data processing scripts to update
# site and variable metadata in the macrosheds package for V2.
# Once I update the package, you won't need this anymore.

library(macrosheds)

rdata_path <- '/path/to/ms_package_quickfix' #UPDATE PATH

load(file.path(rdata_path, 'ms_site_data.RData'))
load(file.path(rdata_path, 'ms_vars_ws_attr.RData'))
load(file.path(rdata_path, 'ms_vars_ts.RData'))
load(file.path(rdata_path, 'ms_var_catalog.RData'))

nv <- as.environment('package:macrosheds')

for(ms_data in c('ms_vars_ts', 'ms_vars_ws', 'ms_site_data', 'ms_var_catalog')){
    unlockBinding(ms_data, nv)
    assign(ms_data, get(ms_data), envir = nv)
    lockBinding(ms_data, nv)
}

# Unrelated: the same example code that was included in the email body

# macrosheds::ms_download_core_data #normally you'd get data this way, but instead just unzip macrosheds_dataset_v2_almost_complete.zip
# macrosheds::ms_load_product(macrosheds_root = '/path/to/unzipped/macrosheds_dataset_v2',
#                             prodname = 'precipitation') #local gauge(s) precip, interpolated to each watershed
#                             prodname = 'ws_attr_timeseries:climate') #includes PRISM precip and air temp
#                             prodname = 'ws_attr_summaries') #watershed area, elevation, annual summaries of everything (except CAMELS attributes, coming soon)
#                             prodname = 'ws_attr_CAMELS_Daymet_forcings') #V2 not yet available
#                             prodname = 'ws_attr_CAMELS_summaries') #V2 not yet available
