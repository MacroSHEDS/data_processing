#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_152 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_142 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_159 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_167 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_157 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.txt')

    return()
}

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_152 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # Bonanza appears to set values to 0 when the sample was not recorded
    # This is likely not the best approach in case there are real values that
    # are 0 and recorded with a single 0, may be worth contacting the domain
    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = c(Sample.Date = '%m/%d/%Y'),
                         datetime_tz = 'UTC',
                         # datetime_tz = 'US/Alaska',
                         site_code_col = 'Watershed',
                         alt_site_code = list('boston_creek' = 'Boston Creek'),
                         data_cols =  c('Nitrate.uM.' = 'NO3',
                                        'Sulfate.uM.' = 'SO4',
                                        'Chloride.uM.' = 'Cl',
                                        'Sodium.uM.' = 'Na',
                                        'Ammonium.uM.' = 'Al',
                                        'Magnesium.uM.' = 'Mg',
                                        'Potassium.uM.' = 'K',
                                        'Calcium.uM.' = 'Ca',
                                        'DIC.uM.' = 'DIC',
                                        'DOC.uM.' = 'DOC',
                                        'TDN.uM.' = 'TDN',
                                        'Silica.uM.' = 'SiO2',
                                        'Conductivity.uS.cm.' = 'spCond',
                                        'DON.uM.' = 'DON',
                                        'SUVA.L.mg.1.m.1.' = 'abs254',
                                        'pH' = 'pH',
                                        'Temperature.degC.' = 'temp'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         set_to_NA = c('0', ''))

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- ms_conversions(d,
                        convert_units_from = c('NO3' = 'umol/l',
                                               'SO4' = 'umol/l',
                                               'Cl' = 'umol/l',
                                               'Na' = 'umol/l',
                                               'Al' = 'umol/l',
                                               'Mg' = 'umol/l',
                                               'K' = 'umol/l',
                                               'Ca' = 'umol/l',
                                               'DIC' = 'umol/l',
                                               'DOC' = 'umol/l',
                                               'TDN' = 'umol/l',
                                               'SiO2' = 'umol/l',
                                               'DON' = 'umol/l'),
                        convert_units_to = c('NO3' = 'mg/l',
                                             'SO4' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'Al' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'K' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'DIC' = 'mg/l',
                                             'DOC' = 'mg/l',
                                             'TDN' = 'mg/l',
                                             'SiO2' = 'mg/l',
                                             'DON' = 'mg/l'))

    return(d)

}

#discharge: STATUS=READY
#. handle_errors
process_1_142 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile, colClasses = 'character')
    just_dates <- nchar(d$dateTime) == 10
    d$dateTime[just_dates] <- paste(d$dateTime[just_dates], '00:00:00')

    if(! any(grepl('^[0-9\\.]{2,}$', d$Flag))) stop('bonanza Q flag column fixed. parse it')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c(dateTime = '%Y-%m-%d %H:%M:%S'),
                         datetime_tz = 'UTC',
                         # datetime_tz = 'US/Alaska',
                         site_code_col = 'Watershed',
                         data_cols =  c('Flow' = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         sampling_type = 'I')
                         # summary_flagcols = 'Flag')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
                            # summary_flags_clean = list(c(Flow = 'G')),
                            # summary_flags_to_drop = list(c(Flow = 'sentinel')))

    return(d)

}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_159 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    raw <- read.delim(rawfile, colClasses = 'character') %>%
        filter(X0CM != -9999)

    dif_date_time <- raw[grepl(paste0(month.abb, collapse = '|'), raw$DATE),]
    og_date_time <- raw[!grepl(paste0(month.abb, collapse = '|'), raw$DATE),]

    og_date_time <- ms_read_raw_csv(preprocessed_tibble = og_date_time,
                                    datetime_cols = c(DATE = '%m/%e/%Y',
                                                      TIME = '%H:%M'),
                                    datetime_tz = 'UTC',
                                    # datetime_tz = 'US/Alaska',
                                    site_code_col = 'SITE',
                                    data_cols =  c('X0CM'= 'temp'),
                                    data_col_pattern = '#V#',
                                    is_sensor = TRUE,
                                    sampling_type = 'I')

    dif_date_time <- ms_read_raw_csv(preprocessed_tibble = dif_date_time,
                                     datetime_cols = c(DATE = '%e-%b-%y',
                                                       TIME = '%H:%M'),
                                     datetime_tz = 'UTC',
                                     # datetime_tz = 'US/Alaska',
                                     site_code_col = 'SITE',
                                     data_cols =  c('X0CM'= 'temp'),
                                     data_col_pattern = '#V#',
                                     is_sensor = TRUE,
                                     sampling_type = 'I')

    d <- rbind(og_date_time, dif_date_time) %>%
        arrange(site_code, datetime)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    return(d)

}

#precipitation: STATUS=READY
#. handle_errors
process_1_167 <- function(network, domain, prodname_ms, site_code, component){

    # There is anoth precip product (384) which uses weighing buckets that is
    # better for snow but less accurate for rain
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    raw <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(nchr = nchar(hour)) %>%
        mutate(time = ifelse(nchr == 3, paste0('0', hour), hour))

    d <- ms_read_raw_csv(preprocessed_tibble = raw,
                         datetime_cols = c(date = '%Y-%m-%d',
                                           time = '%H%M'),
                         datetime_tz = 'UTC',
                         # datetime_tz = 'US/Alaska', #causes DST parse errors
                         site_code_col = 'site_id',
                         summary_flagcols = 'flag',
                         alt_site_code = list('CRREL' = 'CRREL-Met'),
                         data_cols =  c('value'= 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         set_to_NA = c('-9999', '999.00', '-9999.000', '-7999.000',
                                       '999.000', ''),
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('flag' = 'Q'),
                            summary_flags_clean = list('flag' = c('G', 'M')),
                            summary_flags_to_drop = list('flag' = 'sentinel'),
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    if(component == '167_TIPBUCK_CPEAK_1993-2016.txt'){
        d <- filter(d, datetime >= '1998-01-01')
    }

    return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_157 <- function(network, domain, prodname_ms, site_code, component){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.txt',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    raw <- read.delim(rawfile,
                      header = TRUE,
                      colClasses = 'character',
                      skip = 3,
                      sep = ',')

    d <- ms_read_raw_csv(preprocessed_tibble = raw,
                         datetime_cols = c(Date.Off = '%m/%d/%Y'),
                         datetime_tz = 'UTC',
                         site_code_col = 'SiteID',
                         alt_site_code = list('CRREL' = 'AK01'),
                         data_cols = c('Ca', 'Mg', 'K', 'Na', 'NH4',
                                       'NO3', 'Cl', 'SO4',
                                       'pH.Lab' ='pH',
                                       'Lab.Cond' = 'spCond'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'Invalcode',
                         set_to_NA = c('-9.000', '-9.0', '-9.00'),
                         is_sensor = FALSE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('Invalcode' = c('x', 'x           ',
                                                                       'u', 'u           ',
                                                                       'f', 'f           ',
                                                                       'c', 'c           ',
                                                                       'v', 'v           ',
                                                                       'l', 'l           ',
                                                                       'p', 'p           ')),
                            summary_flags_to_drop = list('Invalcode' = 'sentinel'),
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    return(d)
}

#derive kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__152',
                                           'stream_chemistry__159'))
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms006 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms007 <- stream_gauge_from_site_data

# #precipitation: STATUS=OBSOLETE
# #. handle_errors
# process_2_ms002 <- derive_precip

# #precip_chemistry: STATUS=OBSOLETE
# #. handle_errors
# process_2_ms003 <- derive_precip_chem

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_stream_flux

# #precip_flux_inst: STATUS=OBSOLETE
# #. handle_errors
# process_2_ms005 <- derive_precip_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms005 <- derive_precip_pchem_pflux
