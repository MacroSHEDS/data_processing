source('src/ltreb/network_helpers.R')
install.packages(c('dataRetrieval', 'geoknife', 'sbtools'))
library(dataRetrieval)
install.packages('tibble')

#retrieval kernels ####
network = 'ltreb'
domain = 'acton_lake'


set_details <- webb_pkernel_setup(network = network, domain = domain, prodcode = "VERSIONLESS1")
prodname_ms <- set_details$prodname_ms
site_code <- set_details$site_code
component <- set_details$component
url <- set_details$url

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS1 <- function(set_details, network, domain) {
  
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
  
download.file(url = url,
                        destfile = rawfile,
  )
  
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

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS2 <- function(set_details, network, domain) {
  
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
  
download.file(url = url,
                        destfile = rawfile,
  )
  
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
process_1_VERSIONLESS1 <- function(network, domain, prodname_ms, site_code, component){
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    #Need to go back in and fix how data is being read
    mutate(site_name = '####') %>%
    as_tibble()
  
  
  #Need to convert discharge data from cubic meters to liters
  d$'DischargeHourly'<-1000*(d$'DischargeHourly')
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d %HH:%MM'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
                       data_cols = c('DischargeHourly'= 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  
  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  d <- qc_hdetlim_and_uncert(d, prodname_ms)
  d <- synchronize_timestep(d)
  
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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS2 <- function(network, domain, prodname_ms, site_code, component){
  
  #currently the data file is already in a .csv format
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    #fix site name (probably will need to use a different function here)
    mutate(site_name = '###') %>%
    as_tibble()
  
  #Need to convert units from micrograms to milligrams for all stream chemistry data points
  d <- ms_conversions(d,
                      convert_units_from = c('Ammonia', 'ug/L'),
                      convert_units_to = c('Ammonia', 'mg/L'))
  d <- ms_conversions(d,
                      convert_units_from = c('Nitrate', 'ug/L'),
                      convert_units_to = c('Nitrate', 'mg/L'))
  d <- ms_conversions(d,
                      convert_units_from = c('SolubleReactivePhosphorus', 'ug/L'),
                      convert_units_to = c('SolubleReactivePhosphorus', 'mg/L'))
  d <- ms_conversions(d,
                      convert_units_from = c('SuspendedSolids', 'ug/L'),
                      convert_units_to = c('SuspendedSolids', 'mg/L'))
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('DateTime' = '%Y-%m-%d %HH:%MM'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
                       data_cols = c('Ammonia'= 'NH3',
                                     'Nitrate' = 'NO3_NO2',
                                     'SolubleReactivePhosphorus' = 'SRP',
                                     'SuspendedSolids' = 'TSS'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  
  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  d <- qc_hdetlim_and_uncert(d, prodname_ms)
  d <- synchronize_timestep(d)
  
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

#derive: STATS=READY
process_2_ms001 <- function(network, domain, prodname_ms){
    combine_products(network = network,
                     domain = domain, 
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__VERSIONLESS001'))
}