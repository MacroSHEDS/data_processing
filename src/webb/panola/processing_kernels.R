source('src/webb/sleeper/domain_helpers.R')
source('src/webb/network_helpers.R')
source('src/global/global_helpers.R')

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

  # read and save csv from zip folder
  raw_csv <- read.csv(unz(raw_zip, "3_PMRW_Streamflow_WY86-17.csv"), header = TRUE,
           sep = ",") 
  
  raw_csv<- raw_csv%>%
    mutate(site = 'USGS_02203970')
  d <- ms_read_raw_csv(preprocessed_tibble = raw_csv,
                       datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                       datetime_tz = 'America/New_York',
                       site_code_col = 'site',
                       data_cols =  c('Streamflow' = 'discharge'),
                       data_col_pattern = '#V#',
                       summary_flagcols = c("Quality_Cd"),
                       is_sensor = TRUE)
  
  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA,
                          summary_flags_clean = list(Quality_Cd = c('1', '2')),
                          summary_flags_dirty = list(Quality_Cd = c('3', '4'))
                          )
  
  d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)
   
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
  
  return()
}

set_details <- webb_pkernel_setup(prodcode = "VERSIONLESS002", network='webb',
                                  domain='panola')

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <-function(set_details, network, domain) {
  
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = set_details$site_code)
  
  raw_zip <- glue('{rd}/{c}.zip',
                  rd = raw_data_dest,
                  c = set_details$component)

  # read and save csv from zip folder
  raw_csv <- read.csv(unz(raw_zip, "4_PMRW_StreamWaterQuality_WY86-17.csv"), header = TRUE,
                      sep = ",") 

  df<- raw_csv%>%
    mutate(site = 'USGS_02203970')

  # if NO3_Conc has "<val", threshold and use val/2
  df$NO3_Conc <- ifelse(grepl("<", df$NO3_Conc),
                        as.numeric(gsub("<", "", df$NO3_Conc))/2,
                        as.numeric(df$NO3_Conc))

  
  d <- ms_read_raw_csv(preprocessed_tibble = df,
                       datetime_cols = list('Date' = '%m/%d/%Y %H:%M:%S'),
                       datetime_tz = 'America/New_York',
                       site_code_col = 'site',
                       data_cols =  c(
                         "pH"  =  "pH",
                         "ANC_Conc" = "ANC",
                         "Ca_Conc" = "Ca",
                         "Mg_Conc" = "Mg",
                         "Na_Conc" = "Na",
                         "K_Conc" = "K",
                         "SO4_Conc" = "SO4",
                         "NO3_Conc" = "NO3",
                         "Cl_Conc" = "Cl",
                         "Si_Conc" = "Si",
                         "DOC_Conc" = "DOC"
                         ),
                       set_to_NA = ".",
                       data_col_pattern = '#V#',
                       is_sensor = FALSE)
  
  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)
  d <- ms_conversions(d,
                      convert_units_from = c('ANC' = 'ueq/L',
                                             'Ca' = 'ueq/L',
                                             'Mg' = 'ueq/l',
                                             'Na' = 'ueq/l',
                                             'K' = 'ueq/l',
                                             'SO4' = 'ueq/l',
                                             'NO3' = 'ueq/l',
                                             'Cl' = 'ueq/l',
                                             'Si' = 'umol/l',
                                             'DOC' = 'umol/l'
                      ),
                      convert_units_to = c('ANC' = 'eq/L',
                                           'Ca' = 'mg/l',
                                           'Mg' = 'mg/l',
                                           'Na' = 'mg/l',
                                           'K' = 'mg/l',
                                           'SO4' = 'mg/l',
                                           'NO3' = 'mg/l',
                                           'Cl' = 'mg/l',
                                           'Si' = 'mg/l',
                                           'DOC' = 'mg/l'
                      ))
    
  d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)


  
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

#derive kernels ####

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {
  
  combine_products(network = network,
                   domain = domain,
                   prodname_ms = prodname_ms,
                   input_prodname_ms = c('discharge__VERSIONLESS002',
                                         'discharge__VERSIONLESS009'))
  return()
}
