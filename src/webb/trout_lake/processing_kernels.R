<<<<<<< HEAD
source('src/webb/network_helpers.R')
source('src/webb/sleeper/domain_helpers.R')
install.packages(c('dataRetrieval', 'geoknife', 'sbtools'))
library(dataRetrieval)
install.packages('tibble')
#install.packages('arrow')


=======
source('src/webb/sleepers/domain_helpers.R')
>>>>>>> 3c31a30a55d25f0dc889179e51d065fc5f0e18c6
#retrieval kernels ####
network = 'webb'
domain = 'trout_lake'

<<<<<<< HEAD
set_details <- webb_pkernel_setup(network = network, domain = domain, prodcode = "VERSIONLESS005")
prodname_ms <- set_details$prodname_ms
site_code <- set_details$site_code
component <- set_details$component
url <- set_details$url

=======
>>>>>>> 3c31a30a55d25f0dc889179e51d065fc5f0e18c6
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
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = set_details$component)
  
  # call our dataRetrieval function
  q <- readNWISdv(siteNumber="05357215", parameterCd = "00060")
  
  # download it to the raw file locatin
  write_csv(q, file = rawfile)
  
  res <- httr::HEAD(set_details$url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}

#discharge: STATUS=READY
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
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = set_details$component)
  
  # call our dataRetrieval function
  q <- readNWISdv(siteNumber="05357230", parameterCd = "00060")
  
  # download it to the raw file locatin
  write_csv(q, file = rawfile)
  
  res <- httr::HEAD(set_details$url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    # call our dataRetrieval function
     q <- readNWISdv(siteNumber="05357225", parameterCd = "00060")

    # download it to the raw file locatin
    write_csv(q, file = rawfile)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- function(set_details, network, domain) {
<<<<<<< HEAD
  
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = set_details$site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = set_details$component)
  
  # call our dataRetrieval function
  q <- readNWISdv(siteNumber="05357245", parameterCd = "00060")
  
  # download it to the raw file location
  write_csv(q, file = rawfile)
  
  res <- httr::HEAD(set_details$url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
=======

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    # call our dataRetrieval function
    q <- retrieve_usgs_sleepers_daily_q(set_details)

    # download it to the raw file locatin
    write_csv(q, file = rawfile)

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
>>>>>>> 3c31a30a55d25f0dc889179e51d065fc5f0e18c6
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS005 <- function(set_details, network, domain) {

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)
    
    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)
<<<<<<< HEAD
    print(set_details$url)
    url = "http://portal.edirepository.org/nis/dataviewer?packageid=knb-lter-ntl.276.13&entityid=9b2ec53c6adcb68db712718ee69de947"
    R.utils::downloadFile(url = url,
                          filename = rawfile,
                          skip = FALSE,
                          overwrite = TRUE,
                          )
=======

    # call our dataRetrieval function
    q <- retrieve_usgs_sleepers_daily_q(set_details)

    # download it to the raw file locatin
    write_csv(q, file = rawfile)
>>>>>>> 3c31a30a55d25f0dc889179e51d065fc5f0e18c6

    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, '(requires authentication)'),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}


#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {


    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)
    
    d <- read.delim(rawfile, sep = ',') %>%
         mutate(site = 'USGS_05357215') %>%
         as_tibble()

    
    #Conversion example 
    #d <- ms_conversions(d, 
                        #convert_units_from = c(discharge = 'ug/l'),
                        #convert_units_to = c(PO4_P = 'L/s'))
    
    d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
    
    
    d <- d %>% 
      mutate('site_no' = str_replace('allequash_creek', as.character(5357215), 'site_no'))
    d
    
   
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                    datetime_cols = list('Date' = '%Y-%m-%d'),
                    datetime_tz = 'US/Central',
                    site_code_col = 'site_no',
                    data_cols = c('X_00060_00003'= 'discharge'),
                    data_col_pattern = '#V#',
                    is_sensor = TRUE)
    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
    
    d <- qc_hdetlim_and_uncert(d, prodname_ms)
    
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
}

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

  
  
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    mutate(site = 'USGS_05357230') %>%
    as_tibble()
  
  
  #Conversion example 
  #d <- ms_conversions(d, 
  #convert_units_from = c(discharge = 'ug/l'),
  #convert_units_to = c(PO4_P = 'L/s'))
  
  d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
  
  
  d <- d %>% 
    mutate('site_no' = str_replace('north_creek', as.character(5357230), 'site_no'))
  d
  
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_no',
                       data_cols = c('X_00060_00003'= 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  
  d <- qc_hdetlim_and_uncert(d, prodname_ms)
  
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
}

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {
  
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    mutate(site = 'USGS_05357225') %>%
    as_tibble()
  
  
  #Conversion example 
  #d <- ms_conversions(d, 
  #convert_units_from = c(discharge = 'ug/l'),
  #convert_units_to = c(PO4_P = 'L/s'))
  
  d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
  
  
  d <- d %>% 
    mutate('site_no' = str_replace('stevenson_creek', as.character(5357225), 'site_no'))
  d
  
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_no',
                       data_cols = c('X_00060_00003'= 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  
  d <- qc_hdetlim_and_uncert(d, prodname_ms)
  
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
}

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {
  
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    mutate(site = 'USGS_05357215') %>%
    as_tibble()
  
  
  #Conversion example 
  #d <- ms_conversions(d, 
  #convert_units_from = c(discharge = 'ug/l'),
  #convert_units_to = c(PO4_P = 'L/s'))
  
  d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
  
  
  d <- d %>% 
    mutate('site_no' = str_replace('trout_river', as.character(5357245), 'site_no'))
  d
  
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_no',
                       data_cols = c('X_00060_00003'= 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  
  d <- qc_hdetlim_and_uncert(d, prodname_ms)
  
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
}


set_details <- webb_pkernel_setup(network = network, domain = domain, prodcode = "VERSIONLESS005")
prodname_ms <- set_details$prodname_ms
site_code <- set_details$site_code
component <- set_details$component
url <- set_details$url


#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {

  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    as_tibble()
  
  d <- d %>% mutate(
    date_time = paste(sampledate, sample_time, sep = " ")
  )
  
  
  #Strontium and Sulfur are measured in micrograms and need to be translated into milligrams.
  #strontium
  d$'sr'<-(d$'sr')/1000
  #sulfur
  d$'s'<-(d$'s')/1000
  
  #need to combine the values in both rows to get the data inputs from field and lab
  d$'no3_new' <- d$'no3' + d$'no3_2'
  d$'no2_new' <- d$'no2' + d$'no2_2'
  d$conductance_field_new <- d$conductance_field + d$conductance_lab
  d$'po4_new' <- d$'po4' + d$'po4_2'
  d$'ph_field_new' <- d$'ph_field' + d$'ph_lab'
  
  
  trout_lake_chem_var_info <- c(
    "ca" = c('mg/L', 'mg/L', 'Ca'),
    "cl" = c('mg/L', 'mg/L', 'Cl'),
    "co3" = c('mg/L', 'mg/L', 'CO3'),
    
    #specific conductance needs to involve getting data from both lab abd field columns
    "ca" = c('mg/L', 'mg/L', 'Ca'),
    
    "doc" = c('mg/L', 'mg/L', 'DOC'),
    "do" = c('mg/L', 'mg/L', 'DO'),
    "don" = c('mg/L', 'mg/L', 'DON'),
    "fe" = c('mg/L', 'mg/L', 'FE'),
    "hco3" = c('mg/L', 'mg/L', 'HCO3'),
    "k" = c('mg/L', 'mg/L', 'K'),
    "mg" = c('mg/L', 'mg/L', 'Mg'),
    "mn" = c('mg/L', 'mg/L', 'Mn'),
    "ca" = c('mg/L', 'mg/L', 'Ca'),
    "na" = c('mg/L', 'mg/L', 'Na'),
    "so4" = c('mg/L', 'mg/L', 'SO4'),
    "si" = c('mg/L', 'mg/L', 'Si02')
    "sr" = c('mg/L', 'mg/L', 'Sr'),
    "s" = c('mg/L', 'mg/L', 'S')
    "dp" = c('mg/L', 'mg/L', 'TDP')
    
  )
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date_Time' = '%Y-%m-%d %HH:%MM:%SS'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'usgs_site_id',
                       data_cols = c('X_00060_00003'= 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  
  

}


#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
<<<<<<< HEAD
process_2_ms001 <- function(network, domain, prodname_ms) {
=======
process_2_ms001 <- function(network, domain, prodname_ms){
>>>>>>> 3c31a30a55d25f0dc889179e51d065fc5f0e18c6

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
<<<<<<< HEAD
                     input_prodname_ms = c('discharge__VERSIONLESS001',
                                           'discharge__VERSIONLESS002', 
                                           'discharge__VERSIONLESS003',
                                           'discharge__VERSIONLESS004'))
    return()
=======
                     input_prodname_ms = c('stream_chemistry__VERSIONLESS003',
                                           'stream_chemistry__VERSIONLESS004',
                                           'stream_chemistry__VERSIONLESS005',
                                           'stream_chemistry__VERSIONLESS006',
                                           'stream_chemistry__VERSIONLESS007'))

>>>>>>> 3c31a30a55d25f0dc889179e51d065fc5f0e18c6
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- stream_gauge_from_site_data
<<<<<<< HEAD

=======
>>>>>>> 3c31a30a55d25f0dc889179e51d065fc5f0e18c6
