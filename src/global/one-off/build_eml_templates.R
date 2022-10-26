#don't source this file. it was used in the creation of macrosheds/data_processing/eml,
#but everything in there should now be edited manually or piecemeal

library(EMLassemblyline)

setwd('~/git/macrosheds/data_acquisition')

wd <- file.path('eml', 'eml_templates')
ed <- file.path('eml', 'eml_out')
dd <- file.path('eml', 'data_links')

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

files_to_link <- c('macrosheds_figshare_v1/1_watershed_attribute_data/ws_attr_summaries.csv',
                   'macrosheds_figshare_v1/2_timeseries_data/bear/bear/discharge/EB.csv')
link_locs <- file.path(dd, basename(files_to_link))

for(i in seq_along(files_to_link)){
    suppressWarnings(file.link(files_to_link[i], link_locs[i]))
}

make_eml(wd, dd, ed,
         dataset.title = 'MacroSheds',
         temporal.coverage = c('2012-05-01', '2014-11-30'),
         geographic.description = NULL,#not needed if geographic_coverage.txt exists,
         geographic.coordinates = NULL,#same,
         maintenance.description = 'ongoing',
         data.table = c('EB.csv', 'ws_attr_summaries.csv'),
         # data.table.name = data.table, #takes care of itself
         data.table.description = c('omg'),
         data.table.quote.character = c('"'),
         data.table.url = NULL,
         other.entity = NULL, #list any non-table (zip, shp, R, etc) file here
         # other.entity.name = other.entity,
         other.entity.description = NULL,
         other.entity.url = NULL,
         # provenance = c('knb-lter-cap.46.3'), #deprecated. use template_provenance()
         user.id = NULL,
         user.domain = NULL, #pretty sure this doesn't apply to me
         package.id = NULL)
