
#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2010-0003.2/RDS-2010-0003.2.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = url,
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        deets_out$url <- url
        deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                      tzone = 'UTC'))
        deets_out$last_mod_dt = last_mod_dt

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(deets_out)
    }

    loginfo(glue('Nothing to do for {p}',
            p = set_details$prodname_ms),
            logger = logger_module)

    return(deets_out)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.zip',
                    rd = raw_data_dest,
                    c = set_details$component)

    url <- 'https://www.fs.usda.gov/rds/archive/products/RDS-2010-0004/RDS-2010-0004.zip'

    res <- httr::HEAD(url)
    last_mod_dt <- httr::parse_http_date(res$headers$`last-modified`) %>%
        as.POSIXct() %>%
        with_tz('UTC')

    deets_out <- list(url = NA_character_,
                      access_time = NA_character_,
                      last_mod_dt = NA_character_)

    if(last_mod_dt > set_details$last_mod_dt){

        download.file(url = url,
                      destfile = rawfile,
                      cacheOK = FALSE,
                      method = 'curl')

        deets_out$url <- url
        deets_out$access_time <- as.character(with_tz(Sys.time(),
                                                      tzone = 'UTC'))
        deets_out$last_mod_dt = last_mod_dt

        loginfo(msg = paste('Updated', set_details$component),
                logger = logger_module)

        return(deets_out)
    }

    loginfo(glue('Nothing to do for {p}',
            p = set_details$prodname_ms),
            logger = logger_module)

    return(deets_out)

}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    browser()
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)
    fils <- list.files(temp_dir, recursive = T)

    relevant_file <- grep('Data/Tenderfoot_15min_streamflow_data.csv', fils, value = T)

    # rel_file_path <- paste0(temp_dir, '\\', relevant_file)%>%
    #                     gsub("/", "\\", ., fixed=TRUE)
    rel_file_path <- file.path(temp_dir, relevant_file)

    all_q <- read.csv(rel_file_path) %>%
                tibble() %>%
                pivot_longer(.,cols = ends_with('_CFS'), names_to = "site_code", values_to = "val") %>%
                pivot_longer(., cols = ends_with('flag'), names_to = "site_flag", values_to = "flag") %>%
                mutate(datetime1 = paste(DATE, TIME, sep = " "),
                       site_code = str_extract(site_code, "[^_]+"),
                       site_flag = str_extract(site_flag, "[^_]+")) %>%
                filter(site_code == site_flag) %>%
                select(-c(site_flag, DATE, TIME))

    #DATETIME is messed up cuz of one digit thing
    d <- ms_read_raw_csv(preprocessed_tibble = all_q,
                         datetime_cols = c('datetime1' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'site_code',
                         data_cols =  c('val' = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         sampling_type = 'I',
                         summary_flagcols = 'flag')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_clean = list(flag = c(NA)),
                            summary_flags_dirty = list(flag = c("2")),
                            summary_flags_to_drop = list(flag = c("1")))

    d$val <- d$val * 28.316846592 # converting to lps

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    temp_dir <- tempdir()
    unzip(rawfile, exdir = temp_dir)

    rel_file_path <- paste0(temp_dir, '/', 'Data/Tenderfoot_waterquality_data_1992-2009.csv')

    all_chem <- read.csv(rel_file_path, colClasses = 'character')
    all_chem[all_chem=='Nitrite as N'] <- 'Nitrite Nitrogen'
    all_chem <- all_chem %>%
                    pivot_longer(cols = starts_with('X'),
                                 names_to = 'date',
                                 values_to = 'val') %>%
                    select(-Units) %>%
                    pivot_wider(names_from = 'Variable',
                                values_from = 'val')

    d <- ms_read_raw_csv(preprocessed_tibble = all_chem,
                         datetime_cols = c('date' = 'X%m.%d.%Y'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'Flume',
                         data_cols =  c('Field Water Temperature' = 'temp',
                                        'Lab Specific Conductance' = 'spCond', #umhos/cm = uS/cm
                                        #'Field Specific Conductance' = 'spCond', #only keeping lab specific conductance, field is worse quality and redundant
                                        'Calcium in Water by ICP' = 'Ca', #no conversion needed
                                        'Total Ammonia as N' = 'NH3_N', #no conversion needed
                                        'Total Phosphorus' = 'TP', #no conversion needed
                                        'pH in Water' = 'pH', #no conversion needed
                                        'Alkalinity in Water' = 'alk', #no conversion needed
                                        'Sulfate in Water' = 'SO4', #no conversion needed
                                        'Sodium  in Water by ICP' = 'Na', #no conversion needed
                                        'Potassium in Water by ICP' = 'K', #no conversion needed
                                        'Chloride in Water' = 'Cl', #no conversion needed
                                        'Total Suspended Solids at 105 C' = 'TSS', #no conversion needed
                                        'Total Kjeldahl Nitrogen' = 'TKN', #no conversion needed
                                        'Nitrate plus Nitrite as N' = 'NO3_NO2_N',#no conversion needed
                                        #'Nitrite as N' = 'NO2_N', # only keeping nitrite nitrogen, this is redundant
                                        'Magnesium in Water by ICP' = 'Mg', #no conversion needed
                                        #'Hardness, Grains' = '', #NO SUCH VAR
                                        #'Total Hardness' = , #NO SUCH VAR
                                        'Bicarbonate' = 'HCO3', #no conversion needed
                                        'Carbonate' = 'CO3', #no conversion needed
                                        'Nitrite Nitrogen' = 'NO2_N'), #no conversion needed
                         data_col_pattern = '#V#',
                         summary_flagcols = 'flag',
                         alt_site_code = list('UPTE' = c('Upper Tenderfoot'),
                                              'LOSU' = 'Lower Sun',
                                              'PACK' = 'Pack',
                                              'LOST' = 'Lower Stringer',
                                              'UPST' = 'Upper Stringer',
                                              'LOTE' = 'Lower tenderfoot',
                                              'UPSU' = 'Upper Sun',
                                              'BUBB' = 'Bubbling',
                                              'SPAA' = 'Spring Park',
                                              'PASS' = 'Passionate',
                                              'LONE' = 'Lonesome'
                                              ),
                         set_to_NA = '-9999',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d, convert_units_from = c('SO4' = 'mg/l'), convert_units_to = c('SO4' = 'mg/l')) # going from mg/l to mg/l as S

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    unlink(temp_dir, recursive = TRUE)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}


#derive kernels ####

