#don't source this file. it was used in the creation of macrosheds/data_processing/eml,
#but everything in there should now be edited manually or piecemeal
#ADDENDUM: ...except data_links, which are generated here. these should be generated
#in prepare_for_edi. sections with ** belong there. also heed the stop message
#and all TEMPORARY flags below.

stop('if this is for macrosheds > v1, generalize paths and revisit TEMPORARY below')

#REPLACE NA VALUES IN PROVENANCE.TXT WITH BLANKS

#TEMPORARY**
rm_networks <- c('webb', 'mwo', 'neon')

#update this if necessary**
non_lter_networks_on_edi <- c('bear')

#also look through creator_name1 and contact_name1 on https://docs.google.com/spreadsheets/d/1x38OiUPhD7C3m0vBj2kRZO_ORQrks4aOo0DDrFBjY7I/edit#gid=1195899788
#and make sure they all follow acceptable formats for separate_names helper below. (2 or 3 name components, space separated)

library(EMLassemblyline)
library(tidyverse)
library(glue)
library(googlesheets4)
library(EDIutils)

# setup** ####

setwd('~/git/macrosheds/data_acquisition')

conf <- jsonlite::fromJSON('config.json',
                           simplifyDataFrame = FALSE)

wd <- file.path('eml', 'eml_templates')
ed <- file.path('eml', 'eml_out')
dd <- file.path('eml', 'data_links')

# unlink(dd, recursive = TRUE)
dir.create(wd, recursive = TRUE, showWarnings = FALSE)
dir.create(ed, recursive = TRUE, showWarnings = FALSE)
dir.create(dd, recursive = TRUE, showWarnings = FALSE)

googlesheets4::read_sheet(
    conf$disturbance_record_gsheet,
    na = c('', 'NA'),
    col_types = 'c'
) %>%
    filter(! network %in% rm_networks) %>%
    rename(ws_status = watershed_type, pulse_or_chronic = disturbance_type,
           disturbance = disturbance_def, details = disturbance_ex) %>%
    write_csv(file.path(dd, 'disturbance_record.csv'))

googlesheets4::read_sheet(
    conf$univ_prods_gsheet,
    na = c('', 'NA'),
    col_types = 'c'
) %>%
    select(prodname, primary_source = data_source, retrieved_from_GEE = type,
           doi, license, citation, url, addtl_info = notes) %>%
    mutate(retrieved_from_GEE = ifelse(retrieved_from_GEE == 'gee', TRUE, FALSE)) %>%
    write_csv(file.path(dd, 'attribution_and_intellectual_rights_ws_attr.csv'))

# helpers** ####

get_edi_identifier <- function(x){

    #x is a vector of EDI URLs. expects a form like
    #https://portal.edirepository.org/nis/mapbrowse?scope=edi&identifier=618&revision=1 or
    #https://portal.edirepository.org/nis/mapbrowse?scope=edi&identifier=621 or
    #https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-bes&identifier=900&revision=450 or
    #https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-bes&identifier=900

    x <- str_replace(x,
                     'https://portal.edirepository.org/nis/mapbrowse\\?scope=([a-z\\-]+)&identifier=([0-9]+)(?:&revision=([0-9]+))?$',
                     '\\1.\\2.\\3')

    x <- sub('\\.$', '.1', x)

    return(x)
}

find_resource_title <- function(x){

    #x: a vector of citation strings

    out <- sapply(x, function(xx){

        xx <- gsub('Mt. ', 'Mt ', xx)

        xspl <- strsplit(xx, '\\. ')[[1]]
        xspl <- xspl[! grepl('National Ecological Observatory Network', xspl)]
        xspl <- xspl[! grepl('Forest Service', xspl)]
        xspl <- xspl[! grepl('Susquehanna', xspl)]
        xspl <- xspl[! grepl('Dataset accessed from', xspl)]
        xspl <- xspl[! grepl('Williams', xspl)]

        xspl <- gsub('\\.$', '', xspl)

        xspl[which.max(nchar(xspl))]

    }, USE.NAMES = FALSE)

    return(out)
}

split_names <- function(x){

    #expects a vector of names, e.g.
    #Jane Doe
    #J. Doe
    #Jane H. Doe
    #J. H. Doe

    xsep <- lapply(x, function(xx){
        if(is.na(xx)) return(c(NA_character_, NA_character_, NA_character_))
        splt <- strsplit(xx, '\\ ')[[1]]
        if(length(splt) > 3) stop('non-NA names must have 1-3 space-separated components')
        if(length(splt) == 1) splt <- c(splt, '', '')
        if(length(splt) == 2) splt <- c(splt[1], '', splt[2])
        if(nchar(splt[2]) > 1) splt[2] <- toupper(substr(splt[2], 1, 1))
        splt
    })

    xsep <- do.call(rbind, xsep)

    return(xsep)
}

sw = suppressWarnings

# eml dictionaries (as needed) ####

view_unit_dictionary()
zz = EML::get_unitList()
grep('dimensionless', zz$units$id, value=T)
dplyr::filter(zz$units, id == 'number')
unit_types = sort(unique(zz$unitTypes$id))
more_unit_types = sort(unique(zz$units$unitType))
units = sort(unique(zz$units$id))
filter(zz$units, unitType == 'time') %>% pull(id)

# collect files that will be included. link them to a single directory** ####

##build provenance table

prov <- readxl::read_xlsx('macrosheds_figshare_v1/0_documentation_and_metadata/01b_attribution_and_intellectual_rights_complete.xlsx',
                                skip = 5) %>%
    mutate(dataPackageID = NA_character_, systemID = NA_character_, title = NA_character_,
           givenName = NA_character_, middleInitial = NA_character_,
           surName = NA_character_, role = NA_character_,
           onlineDescription = NA_character_) %>%
    select(dataPackageID, systemID, title, givenName,
           middleInitial, surName, role, organizationName = network,
           email = contact, onlineDescription = citation, url = link, contact_name1, creator_name1)

#TEMP
prov <- filter(prov, ! organizationName %in% rm_networks)

lter_sites <- prov$organizationName == 'lter'
other_edi_sites <- prov$organizationName %in% non_lter_networks_on_edi

prov$systemID[lter_sites | other_edi_sites] <- 'EDI'

# old_urls_replaced <- prov$url
# old_urls_replaced[lter_sites] <- sub('https://portal.lternet.edu/nis/mapbrowse\\?packageid=([a-z\\-]+)\\.([0-9]+)\\.([0-9]+)',
#     'https://portal.edirepository.org/nis/mapbrowse?scope=\\1&identifier=\\2&revision=\\3',
#     prov$url[lter_sites])
# write_csv(tibble(x=old_urls_replaced), '~/temp/old_urls_replaced.csv')

prov$dataPackageID[lter_sites | other_edi_sites] <-
    get_edi_identifier(prov$url[lter_sites | other_edi_sites])

prov$title[! lter_sites] <- find_resource_title(prov$onlineDescription[! lter_sites])
prov2 <- prov

prov[! lter_sites, c('givenName', 'middleInitial', 'surName')] <- split_names(prov$contact_name1[! lter_sites])
prov2[! lter_sites, c('givenName', 'middleInitial', 'surName')] <- split_names(prov$creator_name1[! lter_sites])
prov$role[! lter_sites] <- 'contact'
prov2$role[! lter_sites] <- 'creator'
prov <- select(prov, -creator_name1, -contact_name1)
prov2 <- select(prov2, -creator_name1, -contact_name1)
prov <- bind_rows(prov, prov2) %>%
    arrange(organizationName, dataPackageID, title)

# write_tsv(prov, file.path(wd, 'provenance.txt'), na = '')
write_csv(prov, file.path(wd, 'provenance.csv'))
#convert this to tsv manually

ts_tables <- list.files('macrosheds_figshare_v1/2_timeseries_data', pattern = '\\.csv$',
                        recursive = TRUE, full.names = TRUE)

files_to_link <- c(ts_tables,
                   list.files('macrosheds_figshare_v1/1_watershed_attribute_data',
                              full.names = TRUE, recursive = TRUE),
                   'macrosheds_figshare_v1/3_CAMELS-compliant_watershed_attributes/CAMELS_compliant_ws_attr.csv',
                   'macrosheds_figshare_v1/4_CAMELS-compliant_Daymet_forcings/CAMELS_compliant_Daymet_forcings.csv',
                   'macrosheds_figshare_v1/macrosheds_documentation_packageformat/site_metadata.csv',
                   'macrosheds_figshare_v1/0_documentation_and_metadata/05_timeseries_documentation/05b_timeseries_variable_metadata.csv',
                   'macrosheds_figshare_v1/0_documentation_and_metadata/05_timeseries_documentation/05e_range_check_limits.csv',
                   'macrosheds_figshare_v1/0_documentation_and_metadata/05_timeseries_documentation/05f_detection_limits_and_precision.csv',
                   'macrosheds_figshare_v1/0_documentation_and_metadata/06_ws_attr_documentation/06b_ws_attr_variable_metadata.csv',
                   'macrosheds_figshare_v1/0_documentation_and_metadata/06_ws_attr_documentation/06d_ws_attr_variable_category_codes.csv',
                   'macrosheds_figshare_v1/0_documentation_and_metadata/06_ws_attr_documentation/06e_ws_attr_data_source_codes.csv',
                   'macrosheds_figshare_v1/0_documentation_and_metadata/08_data_irregularities.csv',
                   'macrosheds_figshare_v1/macrosheds_documentation_packageformat/variable_catalog.csv'
)

basenames <- basename(files_to_link)
basenames <- sub('^0[1-9][a-z]?_', '', basenames)
basenames <- sub('site_metadata', 'sites', basenames)
basenames <- sub('timeseries_variable_metadata', 'variables_time_series', basenames)
basenames <- sub('ws_attr_variable_metadata', 'variables_ws_attr', basenames)
basenames <- sub('ws_attr_variable_category_codes', 'variable_category_codes_ws_attr', basenames)
basenames <- sub('ws_attr_data_source_codes', 'variable_data_source_codes_ws_attr', basenames)
basenames <- sub('detection_limits_and_precision', 'detection_limits', basenames)
basenames <- sub('CAMELS_compliant_ws_attr', 'CAMELS_compliant_ws_attr_summaries', basenames)
basenames <- sub('variable_catalog', 'data_coverage_breakdown', basenames)
link_locs <- file.path(dd, basenames)
basenames <- c(basenames, 'disturbance_record.csv')
basenames <- c(basenames, 'attribution_and_intellectual_rights_ws_attr.csv')

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
                            '^CAMELS_compliant_ws_attr_summaries\\.csv$',
                            'Watershed attribute data, temporally explicit, for all domains, and interoperable with the CAMELS dataset (https://ral.ucar.edu/solutions/products/camels)')
descriptions <- str_replace(descriptions,
                            '^CAMELS_compliant_Daymet_forcings\\.csv$',
                            'Daymet climate forcings for all domains; interoperable with the CAMELS dataset (https://ral.ucar.edu/solutions/products/camels)')
descriptions <- str_replace(descriptions,
                            '^sites\\.csv$',
                            'Stream site metadata')
descriptions <- str_replace(descriptions,
                            '^variables_time_series\\.csv$',
                            'Time-series variable metadata (standard units, etc.)')
descriptions <- str_replace(descriptions,
                            '^range_check_limits\\.csv$',
                            'Minimum and maximum values allowed to pass through our range filter. Values exceeding these limits are omitted from the MacroSheds dataset.')
descriptions <- str_replace(descriptions,
                            '^detection_limits\\.csv$',
                            'Primary data source detection limits')
descriptions <- str_replace(descriptions,
                            '^variables_ws_attr\\.csv$',
                            'Watershed attribute variable metadata (standard units and definitions)')
descriptions <- str_replace(descriptions,
                            '^variable_category_codes_ws_attr\\.csv$',
                            'Watershed attribute category codes (the second letter of the variable code prefix)')
descriptions <- str_replace(descriptions,
                            '^variable_data_source_codes_ws_attr\\.csv$',
                            'Watershed attribute data source codes (the first letter of the variable code prefix)')
descriptions <- str_replace(descriptions,
                            '^data_irregularities\\.csv$',
                            'Any notable inconsistencies within the MacroSheds dataset')
descriptions <- str_replace(descriptions,
                            '^disturbance_record\\.csv$',
                            'A register of known watershed experiments and significant natural disturbances')
descriptions <- str_replace(descriptions,
                            '^attribution_and_intellectual_rights_ws_attr\\.csv$',
                            'Information about fair use of watershed attribute data. See also attribution_and_intellectual_rights_ts.xlsx.')
descriptions <- str_replace(descriptions,
                            '^data_coverage_breakdown\\.csv$',
                            'Number of observations, timespan of observation, by variable and site')

for(i in seq_along(files_to_link)){
    sw(file.remove(link_locs[i]))
    sw(file.link(files_to_link[i], link_locs[i]))
}

#link additional files that will be grouped under "other entities"
sw(file.remove(file.path(dd, 'attribution_and_intellectual_rights_ts.xlsx')))
file.link('macrosheds_figshare_v1/0_documentation_and_metadata/01b_attribution_and_intellectual_rights_complete.xlsx',
          file.path(dd, 'attribution_and_intellectual_rights_ts.xlsx'))
message('manually remove the second and third sheets from attribution_and_intellectual_rights_ts.xlsx')
sw(file.remove(file.path(dd, 'data_use_agreements.docx')))
file.link('macrosheds_figshare_v1/0_documentation_and_metadata/01a_data_use_agreements.docx',
          file.path(dd, 'data_use_agreements.docx'))
sw(file.remove(file.path(dd, 'timeseries_refs.bib')))
file.link('macrosheds_figshare_v1/0_documentation_and_metadata/05_timeseries_documentation/05h_timeseries_refs.bib',
          file.path(dd, 'timeseries_refs.bib'))
sw(file.remove(file.path(dd, 'ws_attr_refs.bib')))
file.link('macrosheds_figshare_v1/0_documentation_and_metadata/06_ws_attr_documentation/06h_ws_attr_refs.bib',
          file.path(dd, 'ws_attr_refs.bib'))
sw(file.remove(file.path(dd, 'changelog.txt')))
file.link('macrosheds_figshare_v1/0_documentation_and_metadata/03_changelog.txt',
          file.path(dd, 'changelog.txt'))
sw(file.remove(file.path(dd, 'glossary.txt')))
file.link('macrosheds_figshare_v1/0_documentation_and_metadata/02_glossary.txt',
          file.path(dd, 'glossary.txt'))

#this is now taken care of during automated prep
# file.copy('macrosheds_figshare_v1/5_shapefiles', dd, recursive = TRUE)
# file.rename('eml/data_links/5_shapefiles', 'eml/data_links/shapefiles')
# setwd(dd)
# zip('shapefiles.zip', files = list.files('shapefiles', full.names = TRUE), flags = '-r9Xq')
# unlink('shapefiles', recursive = TRUE)
# setwd('../..')

#zip documentation.txt files together (doing this as an afterthought)

docfiles <- list.files('macrosheds_figshare_v1/2_timeseries_data',
                            full.names = TRUE, recursive = TRUE,
                            pattern = 'documentation_.*?\\.txt')

docdir = glue('eml/{dd}/code_autodocumentation')
dir.create(docdir)

ntws = unique(str_match(docfiles, '^macrosheds_figshare_v1/2_timeseries_data/([a-z_]+)')[, 2])
for(i in seq_along(ntws)){

    dir.create(file.path(docdir, ntws[i]))
    dmns = list.files(glue('macrosheds_figshare_v1/2_timeseries_data/{ntws[i]}'))
    dmns = grep('\\.csv', dmns, invert = TRUE, value = TRUE)
    for(j in seq_along(dmns)){

        dir.create(file.path(docdir, ntws[i], dmns[j]))
        fs = list.files(glue('macrosheds_figshare_v1/2_timeseries_data/{ntws[i]}/{dmns[j]}/documentation'), full.names = TRUE)
        fs = grep('README.txt$', fs, invert = TRUE, value = TRUE)
        file.copy(fs, file.path(docdir, ntws[i], dmns[j]))
    }
}

setwd(dd)
zip('code_autodocumentation.zip', files = list.files('code_autodocumentation', full.names = TRUE), flags = '-r9Xq')
setwd('../..')


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
template_table_attributes(wd, dd, 'variables_time_series.csv')
template_table_attributes(wd, dd, 'range_check_limits.csv')
template_table_attributes(wd, dd, 'detection_limits.csv')
template_table_attributes(wd, dd, 'variables_ws_attr.csv')
template_table_attributes(wd, dd, 'variable_category_codes_ws_attr.csv')
template_table_attributes(wd, dd, 'variable_data_source_codes_ws_attr.csv')
template_table_attributes(wd, dd, 'data_irregularities.csv')
template_table_attributes(wd, dd, 'disturbance_record.csv')
template_table_attributes(wd, dd, 'attribution_and_intellectual_rights_ws_attr.csv')
template_table_attributes(wd, dd, 'data_coverage_breakdown.csv')

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

# write EML** ####

# temporal_coverage <- map(ts_tables, ~range(read_csv(.)$datetime)) %>%
#     reduce(~c(min(c(.x[1], .y[1])), max(c(.x[2], .y[2]))))
temporal_coverage <- c("1945-07-01", "2022-04-16")

make_eml(wd, dd, ed,
         dataset.title = 'MacroSheds: a synthesis of long-term biogeochemical, hydroclimatic, and geospatial data from small watershed ecosystem studies',
         temporal.coverage = as.Date(temporal_coverage),
         geographic.description = NULL,#not needed if geographic_coverage.txt exists,
         geographic.coordinates = NULL,#same,
         maintenance.description = 'ongoing',
         data.table = basenames,
         data.table.name = basenames,
         data.table.description = descriptions,
         data.table.quote.character = rep('"', length(basenames)),
         data.table.url = NULL,
         other.entity = c('shapefiles.zip',
                          'attribution_and_intellectual_rights_ts.xlsx',
                          'data_use_agreements.docx',
                          'timeseries_refs.bib',
                          'ws_attr_refs.bib',
                          'changelog.txt',
                          'glossary.txt',
                          'code_autodocumentation.zip'),
         other.entity.name = c('shapefiles.zip',
                               'attribution_and_intellectual_rights_ts.xlsx',
                               'data_use_agreements.docx',
                               'timeseries_refs.bib',
                               'ws_attr_refs.bib',
                               'changelog.txt',
                               'glossary.txt',
                               'code_autodocumentation.zip'),
         other.entity.description = c(
             'Watershed boundaries, stream gauge locations, and precip gauge locations, for all domains.',
             'Specific license requirements and expectations associated with each primary time-series dataset. See data_use_agreements.docx and attribution_and_intellectual_right_ws_attr.csv',
             'Terms and conditions for using MacroSheds data.',
             'Complete bibliographic references for time-series data.',
             'Complete bibliographic references for watershed attribute data.',
             'List of changes made since the last version of the MacroSheds dataset.',
             'Glossary of terms related to the MacroSheds dataset.',
             'Programmatically assembled pseudo-scripts intended to help users recreate/edit specific MacroSheds data products (Also see our code on GitHub).'),
         other.entity.url = NULL,
         user.id = conf$edi_user_id,
         user.domain = NULL, #pretty sure this doesn't apply to us
         package.id = NULL)


EDIutils::
