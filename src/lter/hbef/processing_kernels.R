
#retrieval kernels
#SHOULD THERE BE JUST ONE FOR LTER?
# process_0_1_OBSOLETE = function(set_details, network, domain){
#
#     thisenv = environment()
#     result = 'success'
#
#     tryCatch({
#
#         raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
#             wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
#             s=set_details$site_name)
#         dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
#
#         download.file(url=set_details$url,
#             destfile=glue(raw_data_dest, '/', set_details$component),
#             cacheOK=FALSE, method='curl')
#
#     }, error=function(e){
#         ms_err = handle_error(e)
#         assign('result', ms_err, pos=thisenv)
#     })
#
#     return(result)
# } #discharge: ready

#. handle_errors
process_0_1 <- function(set_details, network, domain){

    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

} #discharge: ready

#munge kernels (not yet modified since borrowing from neon)
# process_1_20093 = function(set, site_name){
#
#     # #NEON has no record of what flags might be encountered here, so build some lists
#     # # saveRDS(list(shipmentWarmQF=c(), externalLabDataQF=c(), sampleCondition=c(),
#     # #         analyteUnits=c(), analyte=c()),
#     # #     'data/neon/temp/20093_variants.rds')
#     #
#     # v = readRDS('data/neon/neon/temp/20093_variants.rds')
#     # v = list(shipmentWarmQF=c(set$shipmentWarmQF, v$shipmentWarmQF),
#     #     externalLabDataQF=c(set$externalLabDataQF, v$externalLabDataQF),
#     #     sampleCondition=c(set$sampleCondition, v$sampleCondition),
#     #     vars=c(paste(set$analyte, set$analyteUnits), v$vars))
#     # saveRDS(v, 'data/neon/neon/temp/20093_variants.rds')
#     # table(v$vars)
#
#     set = set %>%
#         filter(
#             shipmentWarmQF == 0,
#             externalLabDataQF != paste0('formatChange|legacyData|Preliminary ',
#                 'method: UV absorbance not water blank subtracted'),
#             sampleCondition == 'GOOD',
#             analyte != 'TSS - Dry Mass') %>%
#         select(-analyteUnits, -shipmentWarmQF, -externalLabDataQF,
#             -sampleCondition) %>%
#         group_by(collectDate, analyte) %>%
#         summarize(analyteConcentration = mean(analyteConcentration, na.rm=TRUE)) %>%
#         ungroup() %>%
#         tidyr::spread(analyte, analyteConcentration) %>%
#         mutate_at(vars(one_of('ANC')), function(x) x / 1000) %>% #meq/L -> eq/L
#         mutate_at(vars(one_of('conductivity')), function(x) x / 1e6) %>% #uS/cm -> S/cm
#         mutate(
#             collectDate = lubridate::force_tz(collectDate, 'UTC'),
#             site_name = site_name) %>%
#         rename_all(dplyr::recode, collectDate='datetime', conductivity='spCond',
#             `NH4 - N`='NH4_N', `NO2 - N`='NO2_N', `NO3+NO2 - N`='NO3_NO2_N',
#             `Ortho - P`='PO4_P', `UV Absorbance (250 nm)`='abs250',
#             `UV Absorbance (280 nm)`='abs280') %>%
#         select(site_name, datetime, everything())
#
#     return(set)
# }

