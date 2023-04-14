#don't source this file. it was used in the creation of macrosheds/data_processing/eml,
#but everything in there should now be edited manually or piecemeal

library(EMLassemblyline)
library(tidyverse)
library(glue)
library(EDIutils)

# setup** ####

setwd('~/git/macrosheds/data_acquisition')

wd <- file.path('eml', 'eml_templates')
ed <- file.path('eml', 'eml_out')
dd <- file.path('eml', 'data_links')

# unlink(dd, recursive = TRUE)
dir.create(wd, recursive = TRUE, showWarnings = FALSE)
dir.create(ed, recursive = TRUE, showWarnings = FALSE)
dir.create(dd, recursive = TRUE, showWarnings = FALSE)

# eml dictionaries (as needed) ####

view_unit_dictionary()
zz = EML::get_unitList()
grep('meter', zz$units$id, value=T)
dplyr::filter(zz$units, id == 'number')
unit_types = sort(unique(zz$unitTypes$id))
more_unit_types = sort(unique(zz$units$unitType))
units = sort(unique(zz$units$id))
filter(zz$units, unitType == 'time') %>% pull(id)

# generate eml templates. these need to be manually modified ####

#manually edit all files after running these lines

# template_directories(wd, 'macrosheds') #might be convenient for simple projects
template_core_metadata(wd, 'CCBY', '.txt') #requires license arg, but we don't use a standard license

template_table_attributes(wd, dd, 'ws_attr_timeseries.csv')
template_table_attributes(wd, dd, 'ws_attr_summaries.csv')
template_table_attributes(wd, dd, 'timeseries_hbef.csv') #this one needs to be manually copied for all domains after filling it out
template_table_attributes(wd, dd, 'CAMELS_compliant_ws_attr_summaries.csv')
template_table_attributes(wd, dd, 'CAMELS_compliant_Daymet_forcings.csv')
template_table_attributes(wd, dd, 'sites.csv')
template_table_attributes(wd, dd, 'variables_timeseries.csv')
template_table_attributes(wd, dd, 'range_check_limits.csv')
template_table_attributes(wd, dd, 'detection_limits.csv')
template_table_attributes(wd, dd, 'variables_ws_attr_timeseries.csv')
template_table_attributes(wd, dd, 'variable_category_codes_ws_attr.csv')
template_table_attributes(wd, dd, 'variable_data_source_codes_ws_attr.csv')
template_table_attributes(wd, dd, 'data_irregularities.csv')
template_table_attributes(wd, dd, 'disturbance_record.csv')
template_table_attributes(wd, dd, 'attribution_and_intellectual_rights_timeseries.csv')
template_table_attributes(wd, dd, 'attribution_and_intellectual_rights_ws_attr.csv')
template_table_attributes(wd, dd, 'data_coverage_breakdown.csv')
template_table_attributes(wd, dd, 'variable_sample_regimen_codes_timeseries.csv')

template_geographic_coverage(wd, dd, 'sites.csv',
                             lat.col = 'latitude', lon.col = 'longitude',
                             site.col = 'site_code')
template_provenance(wd)
# template_annotations()

template_categorical_variables(wd, dd)

# template_arguments() #for custom inputs?

# copy the first timeseries template (hbef) to account for all the other domains ####

ts_templts <- list.files('eml/data_links', pattern = '^timeseries')
ts_templts <- grep('hbef\\.csv$', ts_templts, value = TRUE, invert = TRUE)
ts_dmns <- str_match(ts_templts, '^timeseries_([a-z_0-9]+)\\.csv$')[, 2]
for(td in ts_dmns){
    file.copy(from = 'eml/eml_templates/attributes_timeseries_hbef.txt',
              to = glue('eml/eml_templates/attributes_timeseries_{td}.txt'),
              overwrite = TRUE)
}

var_cat_map <- c('stream_chemistry' = 'Stream chemistry',
                 'precip_chemistry' = 'Precipitation chemistry',
                 'precipitation' = 'Precipitation depth',
                 'discharge' = 'Stream discharge')

for(td in ts_dmns){
    read_tsv(glue('eml/eml_templates/catvars_timeseries_{td}.txt')) %>%
        mutate(definition = unname(var_cat_map)[match(code, names(var_cat_map))]) %>%
        write_tsv(glue('eml/eml_templates/catvars_timeseries_{td}.txt'))
}

# template_categorical_variables(wd, dd)

