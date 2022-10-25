#don't source this file. it was used in the creation of macrosheds/data_processing/eml,
#but everything in there should now be edited manually or piecemeal

library(EMLassemblyline)

setwd('~/git/macrosheds/data_acquisition')

wd <- file.path('eml', 'eml_templates')
ed <- file.path('eml', 'eml_out')
# dd <- '~/temp/emltest/data'

dir.create(wd, recursive = TRUE)
dir.create(ed, recursive = TRUE)
# dir.create(dd)

template_directories(wd, 'macrosheds')

template_core_metadata(wd, 'CCBY', '.txt') #requires license arg, but we don't use a standard license
    #manually edit all files

# d0 <- 'macrosheds_figshare_v1/2_timeseries_data'
# d1 <- list.files(d0)
# for(d in d1){
#     d2 <- list.files(file.path(d0, d))
#     for(d in d2){
#         d3 <- list.files(file.path(d0, d1, d))
#         grep('locations|boundary|documentation')

template_table_attributes(wd, 'macrosheds_figshare_v1/2_timeseries_data/bear/bear/discharge', 'EB.csv')

#won't identify categorical variables on its own. go in an manually edit "character"
template_categorical_variables(wd, dd) #Describes categorical variables of a data table (if any columns are classified as categorical in table attributes template).
template_geographic_coverage() #Describes where the data were collected.
template_provenance() #Describes source datasets. Explicitly listing the DOIs and/or URLs of input data help future users understand in greater detail how the derived data were created and may some day be able to assign attribution to the creators of referenced datasets.
template_annotations()

# template_arguments()
