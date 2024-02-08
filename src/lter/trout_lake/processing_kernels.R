#stream_chemistry: STATUS=READY
#. handle_errors
process_0_276 <- function(set_details, network, domain){

    if(grepl('Site Information', set_details$component)){
        loginfo('This component is blocklisted', logger = logger_module)
        return(generate_blocklist_indicator())
    }

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#munge kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_276 <- function(network, domain, prodname_ms, site_code, component) {

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

    # combine_products(network = network,
    #                  domain = domain,
    #                  prodname_ms = prodname_ms,
    #                  input_prodname_ms = c('discharge__VERSIONLESS001',
    #                                        'discharge__VERSIONLESS002',
    #                                        'discharge__VERSIONLESS003',
    #                                        'discharge__VERSIONLESS004'))

    site_data %>%
        filter(network == !!network,
               domain == !!domain,
               site_type == 'stream_gauge') %>%
        select(site_code, colocated_gauge_id)

    pull_usgs_discharge(network = network,
                        domain = domain,
                        prodname_ms = prodname_ms,
                        sites = c('allequash_creek' = '05357215',
                                  'north_creek' = '05357230',
                                  'stevenson_creek' = '05357225',
                                  'trout_river' = '05357245'),
                        time_step = c('daily', 'daily', 'daily', 'daily'))
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- stream_gauge_from_site_data
