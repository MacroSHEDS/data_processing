# MWO SWWD domain helpers
# list of sites needed
# (potential) shared system for pulling in SWWD based off of pattern found in download link URL
get_url_swwd_prod <- function(site_name, prodname = 'stream_chem') {
  swwd_wq_url_base <- 'http://wq.swwdmn.org/data'

  if(prodname == 'stream_chem') {
    prod_url_ext <- 'manual/table.xlsx'
  } else if (prodname == 'discharge') {
    prod_url_ext <- 'gauge/table.xlsx'
  } else {
    stop('invalid prodname')
  }

  swwd_wq_url_download <- glue::glue('{base}/{s}/{p}',
                                     base = swwd_wq_url_base,
                                     s = site_name,
                                     p = prod_url_ext
                                     )
  return(swwd_wq_url_download)
}

## swwd_variable_info <- list(
## "" = c('mg/L', 'mg/L', ''),
## "" = c('mg/L', 'mg/L', ''),
## "" = c('mg/L', 'mg/L', ''),
## "" = c('mg/l', 'mg/l', ''),
## "" = c('mg/l', 'mg/l', ''),
## "" = c('mg/L', 'mg/L', ''),
## )

mwo_pkernel_setup <- function(network = 'mwo', domain = 'swwd', prodcode = "VERSIONLESS001", site_code = 'trout-brook') {
    ## network = network
    ## domain = domain

    loginfo('Beginning retrieve (versionless products)',
            logger = logger_module)

    prod_info <- get_product_info(network = network,
                                domain = domain,
                                status_level = 'retrieve',
                                get_statuses = 'ready') %>%
        filter(prodcode == !!prodcode) %>%
        mutate(site_code = !!site_code)

    if(nrow(prod_info) == 0) return()

  return(prod_info)
}


## "" = "Date"
## "" = "Start Date"
## "" = "Depth"
## "" = "Sample Type"
## "" = "QA Type"

mwo_vars <- list(
 "Dissolved_Oxygen"                             = c("mg/L", "mg/L", "DO"),
 "Ammonia-nitrogen"                             = c("mg/L", "mg/L", "NH3_N"),
 "Chloride"                                     = c("mg/L", "mg/L", "Cl"),
 "Kjeldahl_nitrogen"                            = c("mg/L", "mg/L", "TKN"),
 "Nitrate_as_N"                                 = c("mg/L", "mg/L", "NO3_N"),
 "Nitrite_as_N"                                 = c("mg/L", "mg/L", "NO2_N"),
 "Phosphorus_as_P"                              = c("mg/L", "mg/L", "PO4_P"),
 "Total_Dissolved_Phosphorus"                   = c("mg/L", "mg/L", "TDP"),
 "Total_suspended_solids"                       = c("mg/L", "mg/L", "TSS"),
 ## "Total volatile solids"                        = c("mg/L", "mg/L", ""), # NOTE: what does a "volatile" solid even mean?
 "Cadmium"                                      = c("ug/L", "mg/L", "Cd"),
 "Chromium"                                     = c("ug/L", "mg/L", "Cr"),
 "Copper"                                       = c("ug/L", "mg/L", "Cu"),
 "Lead"                                         = c("ug/L", "mg/L", "Pb"),
 "Nickel"                                       = c("ug/L", "mg/L", "Ni"),
 "Zinc"                                         = c("ug/L", "mg/L", "Zn"),
 "pH"                                           = c("unitless", "unitless", "pH"),
 "Escherichia_coli"                             = c("#/100mL", "#/mL", "Ecoli"),
 "Specific_conductance"                         = c("uS/cm", "uS/cm", "spCond")
 ## "Transparency, tube with disk"                 = c("", "", ""),
 ## "Hardness, carbonate"                          = c("mg/L_CaCO3", "mg/L", ""),
 )
