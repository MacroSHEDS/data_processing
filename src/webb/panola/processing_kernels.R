source('src/webb/sleeper/domain_helpers.R')
source('src/webb/network_helpers.R')

# get pkernel deets
set_details <- webb_pkernel_setup(prodcode = "VERSIONLESS001", network='webb',
                                  domain='panola')

#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {
  # START OF BLOCK YOU DONT CHANGE #
  # this sets the file path of the raw data, should always be this same format
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = set_details$site_code)
  
  # this creates that directory, if it doesn't already exist
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  # END OF BLOCK YOU DONT CHANGE #
  
  # create documentation file
  rawfile <- glue('{rd}/{c}.zip',
                  rd = raw_data_dest,
                  c = set_details$component)
  
  # download the data form the download link!
  R.utils::downloadFile(url = set_details$url,
                        filename = rawfile,
                        skip = FALSE,
                        overwrite = TRUE)
  
  # this code records metadat about the date, time, and other details
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
set_details <- webb_pkernel_setup(prodcode = "VERSIONLESS002", network='webb',
                                  domain='panola')

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
  
  R.utils::downloadFile(url = set_details$url,
                        filename = rawfile,
                        skip = FALSE,
                        overwrite = TRUE)
  
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

set_details <- webb_pkernel_setup(prodcode = "VERSIONLESS001", network='webb',
                                  domain='panola')

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <-function(set_details, network, domain) {
  
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = set_details$site_code)
  
  # END OF BLOCK YOU DONT CHANGE #
  
  raw_zip <- glue('{rd}/{c}.zip',
                  rd = raw_data_dest,
                  c = set_details$component)
  raw_csv_dest <- glue('{rd}/{c}.csv',
                       rd = raw_data_dest,
                       c = set_details$component)
  
  # read and save csv from zip folder
  raw_csv <- read.csv(unz(raw_zip, "3_PMRW_Streamflow_WY86-17.csv"), header = TRUE,
           sep = ",") 
  
  # write.csv(raw_csv, raw_csv_dest)
  
  raw_csv<-raw_csv%>%
  mutate(site= 'USGS_02203970')
  copy_csv<-head(raw_csv,100)
  d <- ms_read_raw_csv(preprocessed_tibble = raw_csv,
                       datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                       datetime_tz = 'America/New_York',
                       site_code_col = 'site',
                       data_cols =  c('Streamflow' = 'discharge'),
                       data_col_pattern = '#V#',
                       summary_flagcols = 'Quality_Cd',
                       is_sensor = TRUE)
  
  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)
  
  #  1 excellent, 2 good, 3 fair, 4 poor
  # d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
  

  write_ms_file(d = d,
                  network = network,
                  domain = domain,
                  prodname_ms = prodname_ms,
                  site_code = 'USGS_02203970',
                  level = 'munged',
                  shapefile = FALSE)  
  return()
}


#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {
  
  # combine_products(network = network,
  #                  domain = domain,
  #                  prodname_ms = prodname_ms,
  #                  input_prodname_ms = c('discharge__VERSIONLESS002',
  #                                        'discharge__VERSIONLESS009'))
  return()
}
