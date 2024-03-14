library(neonUtilities)

#neon name = c(macrosheds name, neon unit)
neon_chem_vars <- list(
    'SO4' = c('SO4', 'mg/l'),
    'TDN' = c('TDN', 'mg/l'),
    'NO2 - N' = c('NO2_N', 'mg/l'),
    'Ca' = c('Ca', 'mg/l'),
    'Ortho - P' = c('orthophosphate_P', 'mg/l'),
    'TDP' = c('TDP', 'mg/l'),
    'DOC' = c('DOC', 'mg/l'),
    'TN' = c('TN', 'mg/l'),
    'Mg' = c('Mg', 'mg/l'),
    'Mn' = c('Mn', 'mg/l'),
    'NH4 - N' = c('NH4_N', 'mg/l'),
    'TPN' = c('TPN', 'ug/l'), #detlim reported in mg instead
    'DIC' = c('DIC', 'mg/l'),
    'UV Absorbance (280 nm)' = c('abs280', 'unitless'),
    'TOC' = c('TOC', 'mg/l'),
    'Na' = c('Na', 'mg/l'),
    'NO3+NO2 - N' = c('NO3_NO2_N', 'mg/l'),
    'UV Absorbance (254 nm)' = c('abs254', 'unitless'),
    'TSS' = c('TSS', 'mg/l'),
    'Cl' = c('Cl', 'mg/l'),
    'Fe' = c('Fe', 'mg/l'),
    'HCO3' = c('HCO3', 'mg/l'),
    'specificConductance' = c('spCond', 'uS/cm'),
    'F' = c('F', 'mg/l'),
    'Br' = c('Br', 'mg/l'),
    'TPC' = c('TPC', 'ug/l'), #detlim reported in mg instead
    'pH' = c('pH', 'unitless'),
    'Si' = c('Si', 'mg/l'),
    # 'TSS - Dry Mass' = c('', ''), #omit
    'K' = c('K', 'mg/l'),
    'TP' = c('TP', 'mg/l'),
    'TDS' = c('TDS', 'mg/l'),
    'CO3' = c('CO3', 'mg/l'),
    'NO2 -N' = c('NO2_N', 'mg/l'),
    'ANC' = c('ANC', 'meq/l')
) %>%
    plyr::ldply() %>%
    rename(neon_var = `.id`, ms_var = V1, neon_unit = V2) %>%
    left_join(select(ms_vars, variable_code, unit),
              by = c(ms_var = 'variable_code'))

get_neon_data <- function(domain, s, tracker, silent = TRUE){

        msg <- glue('Retrieving {p}, {st}',
                    st = s$site_code,
                    p = s$prodname_ms)
        loginfo(msg, logger = logger_module)

        processing_func <- get(paste0('process_0_',
                                      s$prodcode_full))

        result <- do.call(processing_func,
                          args = list(set_details = s,
                                      network = network,
                                      domain = domain))

        new_status <- evaluate_result_status(result)

        if(new_status == 'error'){
            logging::logwarn(result, logger = logger_module)
        }

        update_data_tracker_r(network = network,
                              domain = domain,
                              tracker_name = 'held_data',
                              set_details = s,
                              new_status = new_status)
}

neon_retrieve <- function(set_details, network, domain, time_index = NULL){

    raw_data_dest <- glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                          wd = getwd(),
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms,
                          s = set_details$site_code)
                          # c = set_details$component)

    dir.create(raw_data_dest,
               recursive = TRUE,
               showWarnings = FALSE)

    result <- try({
        # neonUtilities::loadByProduct( #performs zipsByProduct and stackByTable in sequence
        neonUtilities::zipsByProduct(
            set_details$prodcode_full,
            site = set_details$site_code,
            # startdate = str_split_i(set_details$component, '_', 1),
            # enddate = str_split_i(set_details$component, '_', 2),
            package = 'expanded',
            release = 'current',
            include.provisional = FALSE,
            savepath = raw_data_dest,
            check.size = FALSE,
            timeIndex = ifelse(is.null(time_index), 'all', time_index)
        )
    }, silent = TRUE)

    return(result)
}

stackByTable_keep_zips <- function(zip_parent){

    #neonUtilities::stackByTable has parameters for keeping zips (or maybe for keeping
    #unzipped contents), but in any case they don't work as intended. This copies
    #all zips to tempdir, extracts zips into environment (which removes the original
    #zip files), and then copies the zips back, as if they were never deleted

    print(paste('stacking zips for', prodname_ms))

    tmpd <- tempdir()
    file.copy(zip_parent, tmpd, recursive = TRUE)

    sink(null_device())
    rawd <- neonUtilities::stackByTable(
        zip_parent,
        savepath = 'envt',
        saveUnzippedFiles = FALSE)
    sink()

    #can't rename cross-device, so copy and remove instead
    file.remove(zip_parent)
    # suppressWarnings(file.remove(zip_parent))
    file.copy(file.path(tmpd, basename(zip_parent)),
              dirname(zip_parent),
              recursive = TRUE)
    unlink(file.path(tmpd, basename(zip_parent)),
           recursive = TRUE)

    return(rawd)
}

munge_neon_site <- function(domain, site_code, prodname_ms, tracker, silent=TRUE){

    retrieval_log <- extract_retrieval_log(held_data, prodname_ms, site_code)

    if(nrow(retrieval_log) == 0){
        return(generate_ms_err('missing retrieval log'))
    }

    out <- tibble()
    for(k in 1:nrow(retrieval_log)){

        prodcode <- prodcode_from_prodname_ms(prodname_ms)

        processing_func <- get(paste0('process_1_', prodcode))
        in_comp <- retrieval_log[k, 'component']

        out_comp <- sw(do.call(processing_func,
                              args=list(network = network,
                                        domain = domain,
                                        prodname_ms = prodname_ms,
                                        site_code = site_code,
                                        component = in_comp)))

        if(! is_ms_err(out_comp) && ! is_ms_exception(out_comp)){
            out <- bind_rows(out, out_comp)
        }
    }

    if(nrow(out) == 0) {
        return(generate_ms_err(paste0('All data failed QA or no data is avalible at ', site_code)))
    }

    sensor <- case_when(prodname_ms == 'stream_chemistry__DP1.20093' ~ FALSE,
                        prodname_ms == 'stream_nitrate__DP1.20033' ~ TRUE,
                        prodname_ms == 'stream_temperature__DP1.20053' ~ TRUE,
                        prodname_ms == 'stream_PAR__DP1.20042' ~ TRUE,
                        prodname_ms == 'stream_gases__DP1.20097' ~ FALSE,
                        prodname_ms == 'stream_quality__DP1.20288' ~ TRUE,
                        prodname_ms == 'precip_chemistry__DP1.00013' ~ FALSE,
                        prodname_ms == 'precipitation__DP1.00006' ~ TRUE,
                        prodname_ms == 'discharge__DP4.00130' ~ TRUE,
                        prodname_ms == 'surface_elevation__DP1.20016' ~ TRUE)

    site_codes <- unique(out$site_code)
    for(y in 1:length(site_codes)) {

        d <- out %>%
            filter(site_code == !!site_codes[y])

        if(sensor){
            sampling_type <- 'I'
        } else{
            sampling_type <- 'G'
        }

        d <- identify_sampling_bypass(df = d,
                                      is_sensor =  sensor,
                                      domain = domain,
                                      network = network,
                                      prodname_ms = prodname_ms,
                                      sampling_type = sampling_type)

        d <- d %>%
            filter(!is.na(val))

        d <- remove_all_na_sites(d)

        if(nrow(d) == 0) return(NULL)

        d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

        if(nrow(d) == 0) return(NULL)

        d <- synchronize_timestep(d)

        write_ms_file(d = d,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = site_codes[y],
                      level = 'munged',
                      shapefile = FALSE,
                      link_to_portal = FALSE)
    }

    update_data_tracker_m(network=network, domain=domain,
        tracker_name='held_data', prodname_ms=prodname_ms, site_code=site_code,
        new_status='ok')

    msg = glue('munged {p} ({n}/{d}/{s})',
            p=prodname_ms, n=network, d=domain, s=site_code)
    loginfo(msg, logger=logger_module)

    return('sitemunge complete')
}

download_sitemonth_details <- function(geturl){

    d = httr::GET(geturl)
    d = jsonlite::fromJSON(httr::content(d, as="text"))

    return(d)
}

determine_upstream_downstream <- function(d_){

    updown = substr(d_$horizontalPosition, 3, 3)
    updown[updown == '1'] = '-up' #1 means upstream sensor

    #2 means downstream sensor. 0 means only one sensor? 3 means ???
    updown[updown %in% c('0', '2', '3')] = ''

    if(any(! updown %in% c('-up', ''))){
        stop('upstream/downstream indicator error')
    }

    return(updown)
}

get_avail_neon_products <- function(){

    req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/"))
    txt = httr::content(req, as="text")
    data_pile = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
    prodlist = data_pile$data$productCode

    return(prodlist)
}

get_neon_product_specs <- function(code){

    prodlist = get_avail_neon_products()

    prod_variant_inds = grep(code, prodlist)

        if(length(keep) != 1) {
            stop(glue('More than one product variant for this prodcode. Did neon ',
                      'make a v.002 data product?'))
        }

    newest_variant_ind = prodlist[prod_variant_inds] %>%
        substr(11, 13) %>%
        as.numeric() %>%
        which.max()

    prodcode_full = prodlist[prod_variant_inds[newest_variant_ind]]
    prod_version = strsplit(prodcode_full, '\\.')[[1]][3]

    return(list(prodcode_full=prodcode_full, prod_version=prod_version))
}

get_avail_neon_product_sets <- function(prodcode_full){

    #returns: tibble with url, site_code, component columns

    avail_sets <- tibble()

    req <- httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
        prodcode_full))
    txt <- httr::content(req, as = "text")
    neondata <- jsonlite::fromJSON(txt, simplifyDataFrame = TRUE,
                                   flatten = TRUE)

    urls <- unlist(neondata$data$siteCodes$availableDataUrls)

    avail_sets <- stringr::str_match(urls,
                                     '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})') %>%
        as_tibble(.name_repair='unique') %>%
        rename(url = `...1`, site_code = `...2`, component = `...3`)

    return(avail_sets)
}

populate_set_details <- function(tracker, prodname_ms, site_code, avail){

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker <- tracker[[prodname_ms]][[site_code]]$retrieve

    rgx <- '/((DP[0-9]\\.[0-9]+)\\.([0-9]+))/[A-Z]{4}/[0-9]{4}\\-[0-9]{2}$'
    rgx_capt <- str_match(avail$url, rgx)[, -1, drop = FALSE]

    retrieval_tracker <- avail %>%
        mutate(
            avail_version = as.numeric(rgx_capt[, 3]),
            prodcode_full = rgx_capt[, 1],
            prodcode_id = rgx_capt[, 2],
            prodname_ms = prodname_ms) %>%
        full_join(retrieval_tracker, by ='component') %>%
        mutate(
            held_version = as.numeric(held_version),
            needed = avail_version - held_version > 0)

    if(any(is.na(retrieval_tracker$needed))){
        stop(glue('Must run `track_new_site_components` before ',
            'running `populate_set_details`'))
    }

    return(retrieval_tracker)
}

write_neon_readme = function(raw_neonfile_dir, dest){

    readme_name = grep('readme', list.files(raw_neonfile_dir), value=TRUE)
    readme = read_feather(glue(raw_neonfile_dir, '/', readme_name))
    readr::write_lines(readme$X1, dest)
}

write_neon_variablekey = function(raw_neonfile_dir, dest){

    varkey_name = grep('variables', list.files(raw_neonfile_dir), value=TRUE)
    varkey = read_feather(glue(raw_neonfile_dir, '/', varkey_name))
    write_csv(varkey, dest)

    return(varkey)
}

update_neon_detlims <- function(neon_dls){

    detlim_pre <- neon_dls %>%
        as_tibble() %>%
        #detlims for TPC, TPN are given in mg, but those analytes are actually
        #reported in ug/L. So can't use the detlims as-is.
        filter(! (analyte %in% c('TPC', 'TPN') & analyteUnits == 'milligram'),
               ! analyte == 'TSS - Dry Mass') %>%
        mutate(analyte = if_else(analyte == 'Ortho-P', 'Ortho - P', analyte),
               analyte = if_else(analyte == 'NO2 -N', 'NO2 - N', analyte)) %>%
        #neon precision not included here, because it's analytical precision,
        #and we record mathematical precision in the detlim sheet
        select(analyte, methodDetectionLimit, analyteUnits,
               starts_with('labSpecific')) %>%
        left_join(neon_chem_vars, by = c(analyte = 'neon_var')) %>%
        filter(! neon_unit == 'unitless') %>%
        rename(detection_limit_original = methodDetectionLimit,
               variable_original = ms_var,
               unit_original = neon_unit,
               start_date = labSpecificStartDate,
               end_date = labSpecificEndDate) %>%
        mutate(domain = !!domain,
               prodcode = !!prodname_ms,
               detection_limit_converted = NA,
               precision = NA,
               sigfigs = NA,
               added_programmatically = FALSE)

    detlims <- standardize_detection_limits(
        dls = detlim_pre,
        vs = ms_vars,
        update_on_gdrive = FALSE
    ) %>%
        mutate(added_programmatically = TRUE)

    detlims_update <- anti_join(
        detlims, domain_detection_limits,
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
