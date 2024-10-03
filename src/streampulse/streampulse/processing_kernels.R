source('src/webb/network_helpers.R')
source('src/streampulse/streampulse/domain_helpers.R')
#retrieval kernels ####
## network = 'streampulse
## domain = 'streampulse'

#Eno
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)
  
  # call our dataRetrieval function
  q <- scrape_data("Eno")
  
  # download it to the raw file locatin
  write_csv(q, file = rawfile)
  
  res <- httr::HEAD(url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}

#Mud
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)
  
  # call our dataRetrieval function
  q <- scrape_data("Mud")
    
    # download it to the raw file locatin
    write_csv(q, file = rawfile)
  
  res <- httr::HEAD(url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}

#NHC
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)
  
  # call our dataRetrieval function
  q <- scrape_data("NHC")
    
    # download it to the raw file locatin
    write_csv(q, file = rawfile)
  
  res <- httr::HEAD(url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}

#UNHC
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)
  
  # call our dataRetrieval function
  q <- scrape_data("UNHC")
    
    # download it to the raw file locatin
    write_csv(q, file = rawfile)
  
  res <- httr::HEAD(url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}

#UENO
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)
  
  # call our dataRetrieval function
  q <- scrape_data("UENO")
    
    # download it to the raw file locatin
    write_csv(q, file = rawfile)
  
  res <- httr::HEAD(url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
                    access_time = as.character(with_tz(Sys.time(),
                                                       tzone = 'UTC')),
                    last_mod_dt = last_mod_dt)
  
  return(deets_out)
  
}

#Stony
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component) {
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)
  
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)
  
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)
  
  # call our dataRetrieval function
  q <- scrape_data("Stony")
    
    # download it to the raw file locatin
    write_csv(q, file = rawfile)
  
  res <- httr::HEAD(url)
  
  last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                     start = 1,
                                     stop = 19),
                          format = '%Y-%m-%dT%H:%M:%S') %>%
    with_tz(tzone = 'UTC')
  
  deets_out <- list(url = paste(url, '(requires authentication)'),
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
                    datetime_cols = c('Date' = '%Y-%m-%d'),
                    datetime_tz = 'US/Central',
                    site_code_col = 'site_name',
                    data_cols = c('X_00060_00003'= 'discharge'),
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
                       datetime_cols = c('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
                       data_cols = c('X_00060_00003'= 'discharge'),
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
                       datetime_cols = c('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
                       data_cols = c('X_00060_00003'= 'discharge'),
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
                       datetime_cols = c('Date' = '%Y-%m-%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'site_name',
                       data_cols = c('X_00060_00003'= 'discharge'),
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
                       datetime_cols = c('sampledate' = '%Y-%m-%d'),
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

  d <- ms_conversions_(d,
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

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # creating a temporary directory to unzip the folder in
    temp_dir <- file.path(tempdir(), domain)
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)

    unzip(rawfile,
          exdir = temp_dir)

    # reading in the contents of the extracted folder
    file_names <- list.files(temp_dir, recursive = TRUE)
    file_paths <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)
    
    # read in watershed boundary
    wb <- sf::read_sf(file_paths[grepl('globalwatershed.shp', file_paths)]) %>%
      select(
        site_code = Name,
        geometry
      ) %>%
      mutate(site_code = 'trout_river')
    
      write_ms_file(d = wb,
                    network = network,
                    domain = domain,
                    prodname_ms = prodname_ms,
                    site_code = 'trout_river',
                    level = 'munged',
                    shapefile = TRUE)

    unlink(temp_dir, recursive = TRUE)
}

#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__VERSIONLESS001',
                                           'discharge__VERSIONLESS002',
                                           'discharge__VERSIONLESS003',
                                           'discharge__VERSIONLESS004'))

}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- stream_gauge_from_site_data
