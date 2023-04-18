source('src/webb/network_helpers.R')

#retrieval kernels ####
## network = 'webb'
## domain = 'trout_lake'


## set_details <- webb_pkernel_setup(network = network, domain = domain, prodcode = "VERSIONLESS005")
## prodname_ms <- set_details$prodname_ms
## site_code <- set_details$site_code
## component <- set_details$component
## url <- set_details$url

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
  q <- dataRetrieval::readNWISdv(siteNumber="05357215", parameterCd = "00060")
  
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
  q <- dataRetrieval::readNWISdv(siteNumber="05357230", parameterCd = "00060")
  
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
     q <- dataRetrieval::readNWISdv(siteNumber="05357225", parameterCd = "00060")

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
  q <- dataRetrieval::readNWISdv(siteNumber="05357245", parameterCd = "00060")
  
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

    print(set_details$url)
    url = "http://portal.edirepository.org/nis/dataviewer?packageid=knb-lter-ntl.276.13&entityid=9b2ec53c6adcb68db712718ee69de947"
    R.utils::downloadFile(url = url,
                          filename = rawfile,
                          skip = FALSE,
                          overwrite = TRUE,
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
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {


    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)
    
    d <- read.delim(rawfile, sep = ',') %>%
         mutate(site_name = 'allequash_creek') %>%
         as_tibble()

    d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                    datetime_cols = list('Date' = '%Y-%m-%d'),
                    datetime_tz = 'US/Central',
                    site_code_col = 'site_name',
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
    mutate(site_name = 'north_creek') %>%
    as_tibble()

  d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
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
    mutate(site_name = 'stevenson_creek') %>%
    as_tibble()

  d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
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
    mutate(site_name = 'trout_river') %>%
    as_tibble()
  
  d$'X_00060_00003'<-28.3168*(d$'X_00060_00003')

  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
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
               date_time = paste(sampledate, sample_time, sep = " "),
               site_name = gsub(' ', '_', tolower(site_name))
             ) %>%
    relocate(date_time, .before = site_name)

  trout_lake_chem_var_info <- list(
    "ca" = c('mg/L', 'mg/L', 'Ca'),
    "cl" = c('mg/L', 'mg/L', 'Cl'),
    "co3" = c('mg/L', 'mg/L', 'CO3'),
    "ca" = c('mg/L', 'mg/L', 'Ca'),
    "doc" = c('mg/L', 'mg/L', 'DOC'),
    "do" = c('mg/L', 'mg/L', 'DO'),
    "don" = c('mg/L', 'mg/L', 'DON'),
    "fe" = c('mg/L', 'mg/L', 'Fe'),
    "hco3" = c('mg/L', 'mg/L', 'HCO3'),
    "k" = c('mg/L', 'mg/L', 'K'),
    "mg" = c('mg/L', 'mg/L', 'Mg'),
    "mn" = c('mg/L', 'mg/L', 'Mn'),
    "ca" = c('mg/L', 'mg/L', 'Ca'),
    "na" = c('mg/L', 'mg/L', 'Na'),
    "so4" = c('mg/L', 'mg/L', 'SO4'),
    "si" = c('mg/L', 'mg/L', 'SiO2'),
    "sr" = c('ug/L', 'mg/L', 'Sr'),
    "s" = c('ug/L', 'mg/L', 'S'),
    "dp" = c('mg/L', 'mg/L', 'TDP')
  )

  trout_lake_chem_var_names <- c(
    "ca"   = 'Ca',
    "cl"   = 'Cl',
    "co3"  = 'CO3',
    "doc"  = 'DOC',
    "do"   = 'DO',
    "don"  = 'DON',
    "fe"   = 'Fe',
    "hco3" = 'HCO3',
    "k"    = 'K',
    "mg"   = 'Mg',
    "mn"   = 'Mn',
    "na"   = 'Na',
    "no2"  = 'NO2',
    "no3"  = 'NO3',
    "po4"  = 'PO4',
    "so4"  = 'SO4',
    "si"   = 'SiO2',
    "sr"   = 'Sr',
    "s"    = 'S',
    "dp"   = 'TDP'
  )

  elements = names(trout_lake_chem_var_info)
  d_new <- d[,1:7]

  for(i in 1:length(elements)) {
    # coalesce element column with all columns which contain "{element}_"
    element = elements[i]
    element_rx = paste0('^', element, '_')
    d_data <- d[,7:ncol(d)]
    d_element = as.list(d_data[,grepl(element_rx, colnames(d_data))])
    d_new[[element]] = coalesce(!!! d_data[,element], !!! d_element)
  }

  d <- d_new

  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('sampledate' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
                       data_cols = trout_lake_chem_var_names,
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)

  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)

  # the chemistry name and unit data is all in a named list in domain_helpers
  # I am going to re-pack it here as var = old_units and var = new_units lists
  trout_lake_aq_chem_units_old = c()
  trout_lake_aq_chem_units_new = c()

  for(i in 1:length(trout_lake_chem_var_info)) {
    og_name <- names(trout_lake_chem_var_info[i])
    og_units <- trout_lake_chem_var_info[[i]][1]
    ms_name <- trout_lake_chem_var_info[[i]][3]
    ms_units <-trout_lake_chem_var_info[[i]][2]
    trout_lake_aq_chem_units_old[ms_name] = og_units
    trout_lake_aq_chem_units_new[ms_name] = ms_units
  }

  d <- ms_conversions(d,
                      convert_units_from = trout_lake_aq_chem_units_old,
                      convert_units_to = trout_lake_aq_chem_units_new
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
}


#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,

                     input_prodname_ms = c('stream_chemistry__VERSIONLESS003',
                                           'stream_chemistry__VERSIONLESS004',
                                           'stream_chemistry__VERSIONLESS005',
                                           'stream_chemistry__VERSIONLESS006',
                                           'stream_chemistry__VERSIONLESS007'))

}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- stream_gauge_from_site_data
