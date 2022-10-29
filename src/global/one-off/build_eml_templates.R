#don't source this file. it was used in the creation of macrosheds/data_processing/eml,
#but everything in there should now be edited manually or piecemeal

stop('if this is for macrosheds > v1, generalize paths')

library(EMLassemblyline)
library(tidyverse)

setwd('~/git/macrosheds/data_acquisition')

conf <- jsonlite::fromJSON('config.json',
                           simplifyDataFrame = FALSE)

wd <- file.path('eml', 'eml_templates')
ed <- file.path('eml', 'eml_out')
dd <- file.path('eml', 'data_links')

unlink(dd, recursive = TRUE)
dir.create(wd, recursive = TRUE)
dir.create(ed, recursive = TRUE)
dir.create(dd, recursive = TRUE)

# template_directories(wd, 'macrosheds') #not needed ever

template_core_metadata(wd, 'CCBY', '.txt') #requires license arg, but we don't use a standard license
    #manually edit all files

# d0 <- 'macrosheds_figshare_v1/2_timeseries_data'
# d1 <- list.files(d0)
# for(d in d1){
#     d2 <- list.files(file.path(d0, d))
#     for(d in d2){
#         d3 <- list.files(file.path(d0, d1, d))
#         grep('locations|boundary|documentation')
template_table_attributes(wd, 'macrosheds_figshare_v1/2_timeseries_data/bear/bear/discharge', 'WB.csv')

# template_table_attributes(wd, 'macrosheds_figshare_v1/2_timeseries_data/', '')
template_table_attributes(wd, 'macrosheds_figshare_v1/1_watershed_attribute_data', 'ws_attr_summaries.csv')
fs = list.files('macrosheds_figshare_v1/3_CAMELS-compliant_watershed_attributes')
for(f in fs){
    template_table_attributes(wd, 'macrosheds_figshare_v1/3_CAMELS-compliant_watershed_attributes', f)
}

#won't identify categorical variables on its own. go in an manually edit "character"
template_categorical_variables(wd, dd) #Describes categorical variables of a data table (if any columns are classified as categorical in table attributes template).
template_geographic_coverage() #Describes where the data were collected.
template_provenance() #Describes source datasets. Explicitly listing the DOIs and/or URLs of input data help future users understand in greater detail how the derived data were created and may some day be able to assign attribution to the creators of referenced datasets.
template_annotations()

# template_arguments()

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
