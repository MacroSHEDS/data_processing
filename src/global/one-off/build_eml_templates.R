#don't source this file. it was used in the creation of macrosheds/data_processing/eml,
#but everything in there should now be edited manually or piecemeal

stop('if this is for macrosheds > v1, generalize paths')

library(EMLassemblyline)
library(tidyverse)

# setup ####

setwd('~/git/macrosheds/data_acquisition')

conf <- jsonlite::fromJSON('config.json',
                           simplifyDataFrame = FALSE)

site_data <- googlesheets4::read_sheet(
    conf$site_data_gsheet,
    na = c('', 'NA'),
    col_types = 'ccccccccnnnnnccccc'
)

wd <- file.path('eml', 'eml_templates')
ed <- file.path('eml', 'eml_out')
dd <- file.path('eml', 'data_links')

unlink(dd, recursive = TRUE)
dir.create(wd, recursive = TRUE)
dir.create(ed, recursive = TRUE)
dir.create(dd, recursive = TRUE)

# eml dictionaries ####

view_unit_dictionary()
zz = EML::get_unitList()
grep('pascal', zz$units$id, value=T)
dplyr::filter(zz$units, id == 'kilogramsPerSquareMeter')
unit_types = sort(unique(zz$unitTypes$id))
more_unit_types = sort(unique(zz$units$unitType))
units = sort(unique(zz$units$id))
filter(zz$units, unitType == 'time') %>% pull(id)

# collect files that will be included. link them to a single directory ####

ts_tables <- list.files('macrosheds_figshare_v1/2_timeseries_data', pattern = '\\.csv$',
                        recursive = TRUE, full.names = TRUE)

files_to_link <- c(ts_tables,
                   list.files('macrosheds_figshare_v1/1_watershed_attribute_data',
                              full.names = TRUE, recursive = TRUE),
                   'macrosheds_figshare_v1/3_CAMELS-compliant_watershed_attributes/CAMELS_compliant_ws_attr.csv',
                   'macrosheds_figshare_v1/4_CAMELS-compliant_Daymet_forcings/CAMELS_compliant_Daymet_forcings.csv')
basenames <- basename(files_to_link)
link_locs <- file.path(dd, basenames)

descriptions <- basenames
descriptions <- str_replace(descriptions,
                            '^timeseries_([a-z_]+)\\.csv$',
                            'Time-series (streamflow, precip if available, chemistry) for domain: \\1')
descriptions <- str_replace(descriptions,
                            '^ws_attr_summaries\\.csv$',
                            'Watershed attribute data, summarized across time, for all domains')
descriptions <- str_replace(descriptions,
                            '^ws_attr_timeseries\\.csv$',
                            'Watershed attribute data, temporally explicit, for all domains')
descriptions <- str_replace(descriptions,
                            '^CAMELS_compliant_ws_attr\\.csv$',
                            'Watershed attribute data, temporally explicit, for all domains, and interoperable with the CAMELS dataset (https://ral.ucar.edu/solutions/products/camels)')
descriptions <- str_replace(descriptions,
                            '^CAMELS_compliant_Daymet_forcings\\.csv$',
                            'Daymet climate forcings for all domains; interoperable with the CAMELS dataset (https://ral.ucar.edu/solutions/products/camels)')

for(i in seq_along(files_to_link)){
    suppressWarnings(file.link(files_to_link[i], link_locs[i]))
}

# generate eml templates. these need to be manually modified ####

#manually edit all files after running these lines

# template_directories(wd, 'macrosheds') #might be convenient for simple projects
template_core_metadata(wd, 'CCBY', '.txt') #requires license arg, but we don't use a standard license

template_table_attributes(wd, 'eml/data_links', 'ws_attr_timeseries.csv')
template_table_attributes(wd, 'eml/data_links', 'ws_attr_summaries.csv')
template_table_attributes(wd, 'eml/data_links', 'timeseries_hbef.csv') #this one needs to be manually copied for all domains after filling it out
template_table_attributes(wd, 'eml/data_links', 'CAMELS_compliant_ws_attr.csv')
template_table_attributes(wd, 'eml/data_links', 'CAMELS_compliant_Daymet_forcings.csv')

template_categorical_variables(wd, dd) #Describes categorical variables of a data table (if any columns are classified as categorical in table attributes template).
template_geographic_coverage() #Describes where the data were collected.
template_provenance() #Describes source datasets. Explicitly listing the DOIs and/or URLs of input data help future users understand in greater detail how the derived data were created and may some day be able to assign attribution to the creators of referenced datasets.
template_annotations()

# template_arguments() #for custom inputs?

# copy the first timeseries template (hbef) to account for all the other domains ####

ts_templts <- list.files('eml/data_links', pattern = '^timeseries')
ts_templts <- grep('hbef\\.csv$', ts_templts, value = TRUE, invert = TRUE)
ts_dmns <- str_match(ts_templts, '^timeseries_([a-z_0-9]+)\\.csv$')[, 2]
for(td in ts_dmns){
    file.copy(from = 'eml/eml_templates/attributes_timeseries_hbef.txt',
              to = glue('eml/eml_templates/attributes_timeseries_{td}.txt'))
}

# write EML ####

# temporal_coverage <- map(ts_tables, ~range(read_csv(.)$datetime)) %>%
#     reduce(~c(min(c(.x[1], .y[1])), max(c(.x[2], .y[2]))))
temporal_coverage <- c("1945-07-01", "2022-04-16")

make_eml(wd, dd, ed,
         dataset.title = 'MacroSheds',
         temporal.coverage = as.Date(temporal_coverage),
         geographic.description = NULL,#not needed if geographic_coverage.txt exists,
         geographic.coordinates = NULL,#same,
         maintenance.description = 'ongoing',
         data.table = basenames,
         # data.table.name = data.table,
         data.table.description = descriptions,
         data.table.quote.character = rep('"', length(files_to_link)),
         data.table.url = NULL,
         other.entity = 'eml/data_links/shapefiles.zip',
         # other.entity.name = other.entity,
         other.entity.description = 'Watershed boundaries, stream gauge locations, and precip gauge locations, for all domains',
         other.entity.url = NULL,
         user.id = conf$edi_user_id,
         user.domain = NULL, #pretty sure this doesn't apply to us
         package.id = NULL)
