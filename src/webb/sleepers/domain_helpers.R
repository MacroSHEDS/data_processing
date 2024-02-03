## run these before working inside retrieve_product func
## network = network
## domain = domain
## prodname_ms = prodname_ms
## site_code = site_code
## tracker = held_data
## url = prod_info$url[i]

retrieve_sleepers_product <- function(network,
                                 domain,
                                 prodname_ms,
                                 site_code,
                                 tracker,
                                 url){
    # creating a string which matches the names of processing kernels
    processing_func <- get(paste0('process_0_',
                                  # these names or based off of prod names in products.csv
                                  prodcode_from_prodname_ms(prodname_ms)))

    # tracking the "version" of the product
    rt <- tracker[[prodname_ms]][[site_code]]$retrieve

    held_dt <- as.POSIXct(rt$held_version,
                          tz = 'UTC')

    # "deets" is a list of all the information originally from a row in products.csv
    deets <- list(prodname_ms = prodname_ms,
                  site_code = site_code,
                  component = rt$component,
                  last_mod_dt = held_dt,
                  url = url)

    # these "deets" are fed as arguments to wwhatever processing kernel is currently being called
    # remember, this "retrieve_product" function is being called, in the retrieve.R script,
    # in a loop over the product names from products.csv -- this is why the products.csv prod names
    # must match the end of the procesing kernels which are written to retrieve that product

    # if you're working on pkernels and not actually running this func, uncomment and run these lines:
    ## set_details = deets
    ## network = network
    ## domain = domain

    result <- do.call(processing_func,
                      args = list(set_details = deets,
                                  network = network,
                                  domain = domain))


    new_status <- evaluate_result_status(result)

    if('access_time' %in% names(result) && any(! is.na(result$access_time))){
        deets$last_mod_dt <- result$access_time[! is.na(result$access_time)][1]
    }

    update_data_tracker_r(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          set_details = deets,
                          new_status = new_status)

    source_urls <- get_source_urls(result_obj = result,
                                   processing_func = processing_func)

    write_metadata_r(murl = source_urls,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

}

retrieve_usgs_sleepers_daily_q <- function(set_details) {
  if(grepl("w5", set_details$component) == TRUE) {
    q <- dataRetrieval::readNWISdv(siteNumbers = "01135300",
                            parameterCd = "00060")

  } else if(grepl("w3", set_details$component) == TRUE) {
    q <- dataRetrieval::readNWISdv(siteNumbers = "01135150",
                                   parameterCd = "00060")
  }

  return(q)
}

sleepers_stream_chem_var_info <- list(
    "K.ueq.L"        = c('ueq/L', 'mg/L', 'K'),
    "Mg.ueq.L"       = c('ueq/L', 'mg/L', 'Mg'),
    "Na.ueq.L"       = c('ueq/L', 'mg/L', 'Na'),
    "Ca.ueq.L"       = c('ueq/L', 'mg/L', 'Ca'),
    "Cl.ueq.L"       = c('ueq/L', 'mg/L', 'Cl'),
    "Si.umol.L"      = c('umol/L', 'mg/L', 'Si'),
    "Fe.ug.L"        = c('ug/L',  'mg/L', 'Fe'),
    "Li.ug.L"        = c('ug/L',  'mg/L', 'Li'),
    "Mn.ug.L"        = c('ug/L',  'mg/L', 'Mn'),
    "Al.ug.L"        = c('ug/L',  'mg/L', 'Al'),
    "Ba.ug.L"        = c('ug/L',  'mg/L', 'Ba'),
    "Sr.ug.L"        = c('ug/L',  'mg/L', 'Sr'),
    "NO3.ueq.L"      = c('ueq/L', 'mg/L', 'NO3'),
    "SO4.ueq.L"      = c('ueq/L', 'mg/L', 'SO4'),
    "NH4.ueq.L"      = c('ueq/L', 'mg/L', 'NH4'),
    "PO4.mg.P.L"    = c('mg/L', 'mg/L', 'PO4_P'),
    "DOC.mg.L"      = c('mg/L', 'mg/L', 'DOC'),
    "TDN.mg.L"      = c('mg/L', 'mg/L', 'TDN'),
    "TDP.mg.L"      = c('mg/L', 'mg/L', 'TDP'),
    "Temp.C"        = c('degrees C', 'degrees C', 'temp'),
    "pH"            = c('unitless', 'unitless', 'pH'),
    "SpCond.uS.cm"   = c('uS/cm', 'uS/cm', 'spCond'),
    "d13C"          = c('ppt', 'ppt', 'd13C'),
    "d18O.NO3"      = c('ppt', 'ppt', 'NO3_d18O'),
    "d2H"           = c('ppt', 'ppt', 'deuterium'),
    "d15N.NO3"      = c('ppt', 'ppt', 'NO3_d15N'),
    "d18O"          = c('ppt', 'ppt', 'd18O'),
    "87Sr.86Sr"     = c('unitless', 'unitless', 'd87Sr_d86Sr'),
    "ANC.ueq.L"      = c('ueq/l', 'eq/l', 'ANC'),
    "UV254.cm.1"    = c('AU/cm', 'AU/cm', 'abs254'),
    "SUVA.L.mg.m.1" = c('L/mgm', 'L/mgm', 'SUVA'),
    "Alo.ug.L"       = c('ug/L', 'mg/L', 'Al_om'),
    "Alm.ug.L"       = c('ug/L', 'mg/L', 'Al_m')
)

update_sleepers_detlims <- function(sleepers_stream_chem_var_info,
                                    no_bdl_vars){

    names_units <- sleepers_stream_chem_var_info %>%
        plyr::ldply() %>%
        rename(variable_original = 1, unit_original = 2,
               unit_converted = 3, variable_converted = 4) %>%
        mutate(variable_original = variable_converted)

    #update detection limits for this domain
    detlim_pre <- d %>%
        filter(! var %in% no_bdl_vars,
               val < 0) %>%
        distinct(var, val, .keep_all = TRUE) %>%
        mutate(start_date = as.Date(datetime),
               var = drop_var_prefix(var)) %>%
        select(start_date, var, val) %>%
        arrange(var, start_date) %>%
        group_by(var) %>%
        mutate(end_date = lead(start_date) - 1) %>%
        ungroup() %>%
        left_join(names_units, by = c('var' = 'variable_converted')) %>%
        rename(detection_limit_original = val,
               variable_converted = var) %>%
        mutate(domain = !!domain,
               prodcode = !!prodname_ms,
               # variable_converted = NA,
               # variable_original = ,
               detection_limit_converted = NA,
               detection_limit_original = abs(detection_limit_original),
               precision = NA,
               sigfigs = NA,
               # unit_converted = ,
               # unit_original = ,
               # start_date = ,
               # end_date = ,
               added_programmatically = FALSE
        )

    sleepers_detlims <- standardize_detection_limits(
        dls = detlim_pre,
        vs = ms_vars,
        update_on_gdrive = FALSE
    ) %>%
        mutate(added_programmatically = TRUE)

    detlims_update <- anti_join(
        sleepers_detlims, domain_detection_limits,
        by = c('domain', 'prodcode', 'variable_converted', 'variable_original',
               'detection_limit_original', 'start_date', 'end_date')
    )

    if(nrow(detlims_update)){
        ms_write_confdata(detlims_update,
                          which_dataset = 'domain_detection_limits',
                          to_where = 'remote',
                          overwrite = FALSE) #append
    }

    return(invisible())
}
