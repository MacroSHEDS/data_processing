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

#stream_chemistry: STATUS=PENDING
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
process_1_152 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    # Bonanza appears to set values to 0 when the sample was not recorded
    # This is likely not the best approach in case there are real values that
    # are 0 and recorded with a single 0, may be worth contacting the domain
    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = c(Sample.Date = '%m/%d/%Y'),
                         datetime_tz = 'US/Alaska',
                         site_name_col = 'Watershed',
                         alt_site_name = list('boston_creek' = 'Boston Creek'),
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
                         set_to_NA = '0')

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

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
}

#discharge: STATUS=READY
#. handle_errors
process_1_142 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    look <- read.csv(rawfile, colClasses = 'character')

    d <- ms_read_raw_csv(filepath = rawfile,
                         datetime_cols = c(Date.Time = '%m/%d/%Y %H:%M:%S'),
                         datetime_tz = 'US/Alaska',
                         site_name_col = 'Watershed',
                         data_cols =  c('Flow' = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         sampling_type = 'I')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
}

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_159 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    raw <- read.delim(rawfile, colClasses = 'character') %>%
        filter(X0CM != -9999)

    d <- ms_read_raw_csv(preprocessed_tibble = raw,
                         datetime_cols = c(DATE = '%m/%d/%Y',
                                           TIME = '%H:%M'),
                         datetime_tz = 'US/Alaska',
                         site_name_col = 'SITE',
                         data_cols =  c('X0CM'= 'temp'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    
    ggplot(d, aes(datetime, val , colour = site_name)) + geom_line()

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_167 <- function(network, domain, prodname_ms, site_name,
                          component) {

    # There is anoth precip product (384) which uses weighing buckets that is
    # better for snow but less accurate for rain
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    raw <- read.csv(rawfile, colClasses = 'character') %>%
        mutate(nchr = nchar(hour)) %>%
        mutate(time = ifelse(nchr == 3, paste0('0', hour), hour))

    d <- ms_read_raw_csv(preprocessed_tibble = raw,
                         datetime_cols = c(date = '%Y-%m-%d',
                                           time = '%H%M'),
                         datetime_tz = 'US/Alaska',
                         site_name_col = 'site_id',
                         summary_flagcols = 'flag',
                         alt_site_name = list('CRREL' = 'CRREL-Met'),
                         data_cols =  c('value'= 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE,
                         set_to_NA = c('-9999', '999.00', '-9999.000', '-7999.000',
                                       '999.000'))

    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('flag' = 'Q'),
                            summary_flags_clean = list('flag' = 'G'),
                            varflag_col_pattern = NA)

    if(component == '167_TIPBUCK_CPEAK_1993-2016.txt'){

        d <- d %>%
            filter(datetime >= '1998-01-01')
    }

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_157 <- function(network, domain, prodname_ms, site_name,
                          component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.txt',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    raw <- read.delim(rawfile, header = TRUE, colClasses = 'character', skip=3, sep=',')

    d <- ms_read_raw_csv(preprocessed_tibble = raw,
                         datetime_cols = c(Date.Off = '%m/%d/%Y'),
                         datetime_tz = 'GMT',
                         site_name_col = 'SiteID',
                         alt_site_name = list('CRREL' = 'AK01'),
                         data_cols = c('Ca',
                                           'Mg',
                                           'K',
                                           'Na',
                                           'NH4',
                                           'NO3',
                                           'Cl',
                                           'SO4',
                                           'pH.Lab' ='pH',
                                           'Lab.Cond' = 'spCond'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'Invalcode',
                         set_to_NA = c('-9.000', '-9.0', '-9.00'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list('Invalcode' = 'x'),
                            summary_flags_to_drop = list('Invalcode' = 'BAD'),
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
}

#derive kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__152',
                                           'stream_temperature__159'))
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
