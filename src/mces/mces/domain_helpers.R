utm_to_wsg <- function(x, y) {
  points <- cbind(x, y)
  v <- terra::vect(points, crs="+proj=utm +zone=15 +datum=WGS84  +units=m")
  y <- terra::project(v, "+proj=longlat +datum=WGS84")
  lonlat <- terra::geom(y)[, c("x", "y")]
  return(lonlat)
}

mces_site_lookup <- function(site_string) {
  tryCatch(
    expr = {
      site_code <- mces_site_codes[grepl(site_string, names(mces_site_codes))][[1]]
      return(site_code)
    },
    error = function(e) {
      return(site_string)
    }
  )
}

## ## UTM x and y from MCES data
## lat_longs <- list(
##   c(476393.07, 4980372.46),
##   c(497407.76, 4975972.75),
##   c(445990.45, 4951144.59),
##   c(457223.63, 4962236.35),
##   c(515081.66, 4991375.85),
##   c(448600.15, 4955388.52),
##   c(472806.95, 4957984.77),
##   c(469462.14, 4958105.61),
##   c(499365.96, 4971568.2),
##   c(476160.92, 4961696),
##   c(466640,    4963819.2),
##   c(462070.47, 4962837.63),
##   c(449698.33, 4946332.04),
##   c(515464.5 , 4991775.57),
##   c(516895.94, 4973624.02),
##   c(511851.33, 4952442.31)
## )

## for(i in 1:length(lat_longs)){
##   this_ll <- utm_to_wsg(x= lat_longs[[i]][1], y = lat_longs[[i]][2])
##   lat_longs[[i]][2] = this_ll['x']
##   lat_longs[[i]][1] = this_ll['y']
## }

retrieve_mces_product <- function(network,
                                 domain,
                                 prodname_ms,
                                 site_code,
                                 tracker,
                                 url){
    # creating a string which matches the names of processing kernels
    processing_func <- get(paste0('process_0_',
                                  # these names or based off of prod names in products.csv
                                  prodcode_from_prodname_ms(prodname_ms)))

    # tracking the "version" of the product
    rt <- tracker[[prodname_ms]][[site_code]]$retrieve

    held_dt <- as.POSIXct(rt$held_version,
                          tz = 'UTC')

    # "deets" is a list of all the information originally from a row in products.csv
    deets <- list(prodname_ms = prodname_ms,
                  site_code = site_code,
                  component = rt$component,
                  last_mod_dt = held_dt,
                  url = url)

    # these "deets" are fed as arguments to wwhatever processing kernel is currently being called
    # remember, this "retrieve_product" function is being called, in the retrieve.R script,
    # in a loop over the product names from products.csv -- this is why the products.csv prod names
    # must match the end of the procesing kernels which are written to retrieve that product

    # if you're working on pkernels and not actually running this func, uncomment and run these lines:
    ## set_details = deets
    ## network = network
    ## domain = domain

    result <- do.call(processing_func,
                      args = list(set_details = deets,
                                  network = network,
                                  domain = domain))


    new_status <- evaluate_result_status(result)

    if('access_time' %in% names(result) && any(! is.na(result$access_time))){
        deets$last_mod_dt <- result$access_time[! is.na(result$access_time)][1]
    }

    update_data_tracker_r(network = network,
                          domain = domain,
                          tracker_name = 'held_data',
                          set_details = deets,
                          new_status = new_status)

    source_urls <- get_source_urls(result_obj = result,
                                   processing_func = processing_func)

    write_metadata_r(murl = source_urls,
                     network = network,
                     domain = domain,
                     prodname_ms = prodname_ms)

}

# general notes:
# NOTE: "filtered" vs "unfiltered"
# NOTE: turbidity unit ocnversions NTU vs FTU vs NRTU
# NOTE: "low" "low L" ?

mces_variable_info <- list(
"Total Kjeldahl Nitrogen, Unfiltered"    = c('', 'mg/L', 'UTKN'),
"Total Phosphorus, Unfiltered"           = c('', 'mg/L', 'UTP'),
"Volatile Suspended Solids"              = c('', 'mg/L', 'VSS'),
"Ortho Phosphate as P, Filtered"         = c('', 'mg/l', 'FPO4_P'),# TODO: add to variables
"Total Phosphorus, Filtered"             = c('', 'mg/l', 'TP'),
## "Chlorophyll-a, % Pheo-Corrected"        = c('', 'mg/l', ''),
## "Chlorophyll-a/Pheophytin-a Abs. R"      = c('', 'mg/l', ''),
"Chlorophyll-a Trichromatic Uncorrected" = c('', 'mg/L', 'Chla'),
## "Chlorophyll-a, Pheo-Corrected"          = c('', '', ''),
"Suspended Solids"                       = c('', 'mg/L', 'TSS'),
"Total Nitrate/Nitrite N, Unfiltered"    = c('', 'mg/L', 'UT_NO3_NO2_N'), # TODO: add to variables
"Nitrite N, Unfiltered"                  = c('', 'mg/L', 'NO2_N'), # TODO: add to variables
"Nitrate N, Unfiltered"                  = c('', 'mg/L', 'NO3_N'),# TODO: add to variables
"E. Coli Bacteria Count"                 = c('', '#/mL', 'Ecoli'),
## "Chloride, Unfiltered"                   = c('', 'mg/L', 'Cl'),# TODO: add to variables
## "Ammonia Nitrogen, Unfiltered"           = c('', 'mg/L', 'NH3_N'),# TODO: add to variables
## "Chlorophyll-b"                          = c('', '', ''),# TODO: add to variables
## "Chlorophyll-c"                          = c('', '', ''),# TODO: add to variables
## "Pheophytin-a"                           = c('', '', ''),# TODO: add to variables
## "Conductivity"                           = c('', 'uS/cm', 'spCond'), # NOTE: conductivity vs sp. cond vs conducatance?? # TODO: add var if diferent
"Dissolved Oxygen"                       = c('', 'mg/L', 'DO'),
"pH"                                     = c('', 'unitless', 'pH'),
"Temperature"                            = c('', 'C', 'temp'),
## "Cadmium, Unfiltered"                    = c('', 'mg/L', 'Cd'),# TODO: add to variables
## "Turbidity (NTRU)"                       = c('NTRU', 'FNU', 'turbid'),# TODO: add to variables
"Total Organic Carbon, Filtered"         = c('', 'mg/L', 'TOC'),
## "Turbidity (NTU)"                        = c('NTU', 'FNU', 'turbid'), # NOTE: NTU and FNU are slightly different, but considered acceptable use same values w either, but maybe still # TODO: add variable (???)
## "COD, Unfiltered"                        = c('', '', ''),# TODO: add to variables
## "Hardness, Unfiltered"                   = c('', 'mg/L', 'CO3'), # NOTE: hardness just [CO3], carbonate# TODO: add to variables
## "Lead, Unfiltered"                       = c('', 'mg/L', 'Pb'),# TODO: add to variables
## "Sulfate, Unfiltered"                    = c('', 'mg/L', 'SO4'), # TODO: add variable, USO4
## "Ortho Phosphate as P, Unfiltered"       = c('', '', ''), # TODO: add to variables
"Silica, Filtered"                       = c('', 'mg/L', 'Si'),
## "Total Alkalinity, Unfiltered"           = c('', 'mg/L', 'alk'),# TODO: add to variables
## "Zinc, Unfiltered"                       = c('', 'mg/L', 'Zn'),# TODO: add to variables
## "Fecal Coliform Bacteria Count"          = c('', '', ''), # TODO: add to variables
## "Copper, Unfiltered"                     = c('', 'mg/L', 'Cu'),# TODO: add to variables
"Total Kjeldahl Nitrogen, Filtered"      = c('', 'mg/L', 'TKN'),
## "Chromium, Unfiltered"                   = c('', 'mg/L', 'Cr'),# TODO: add to variables
## "Magnesium, Unfiltered"                  = c('', 'mg/L', 'Mg'),# TODO: add to variables
## "COD, Filtered"                          = c('', '', ''), # TODO: add variable, chemical oxygen demand
## "Nickel, Unfiltered"                     = c('', 'mg/L', 'Ni'),# TODO: add to variables
## "CBOD 5-day, Unfiltered"                 = c('', '', ''),# TODO: add variable, chem + biological oxygen demand
## "BOD 5-day, Unfiltered"                  = c('', '', ''),# TODO: add variable, biological oxygen demand
"Sulfate, Filtered"                      = c('', 'mg/L', 'SO4'),
## "BOD K-rate, Unfiltered"                 = c('', '', ''),# TODO: add variable
## "BOD Ultimate, Filtered"                 = c('', '', ''),# TODO: add variable
## "BOD K-rate, Filtered"                   = c('', '', ''),# TODO: add variable
## "Total Phosphorus, Filtered, Low L"      = c('', '', ''),# TODO: add variable
## "Soluble Oxygen Demand, Filtered"        = c('', '', ''),# TODO: add variable
"Total Dissolved Solids"                 = c('', 'mg/L', 'TDS'),
## "Calcium, Unfiltered"                    = c('', 'mg/L', 'Ca'),
"Cadmium, Filtered"                      = c('', 'mg/L', 'Cd'),
"Total Phosphorus, Unfiltered"           = c('', 'mg/L', 'UTP'),
## "Transparency Tube"                      = c('', '', ''),# TODO: add variable (???)
"Copper, Filtered"                       = c('', 'mg/L', 'Cu'),
"Chromium, Filtered"                     = c('', 'mg/L', 'Cr'),
## "PCB: 1248, Unfiltered"                  = c('', '', ''),# TODO: add variable
"Total Kjeldahl Nitrogen, Unfiltered"    = c('', 'mg/L', 'UTKN'),
"Mercury, Filtered"                      = c('', 'mg/L', 'Hg'),
## "CBOD 5-day, Filtered"                   = c('', '', ''),# TODO: add variable
## "CBOD K-rate, Unfiltered"                = c('', '', ''),# TODO: add variable
## "CBOD K-rate, Filtered"                  = c('', '', ''),# TODO: add variable
"Ammonia Nitrogen, Filtered"             = c('', 'mg/L', 'NH3_N'),
"Zinc, Filtered"                         = c('', 'mg/L', 'Zn'),
## "Total Alkalinity, Filtered"             = c('', 'mg/L', ''),# TODO: add variable
"Sodium, Filtered"                       = c('', 'mg/L', 'Na'),
## "CBOD Ultimate, Unfiltered"              = c('', '', ''),# TODO: add variable
## "PCB: 1232, Unfiltered"                  = c('', '', ''),# TODO: add variable
## "BOD Ultimate, Unfiltered"               = c('', '', ''),# TODO: add variable
## "Mercury, Unfiltered"                    = c('', '', ''),# TODO: add variable
## "PCB: 1016, Unfiltered"                  = c('', '', ''),# TODO: add variable
"Turbidity (FNU)"                        = c('', 'FNU', 'turb'),
## "BOD 5-day, Filtered"                    = c('', '', ''),# TODO: add variable
"Chloride, Filtered"                     = c('', 'mg/L', 'Cl'),
## "CBOD Ultimate, Filtered"                = c('', '', ''),# TODO: add variable
"Total Phosphorus, Particulate"          = c('', 'mg/L', 'TPP'),
"Magnesium, Filtered"                    = c('', 'mg/L', 'Mg'),
## "PCB: 1254, Unfiltered"                  = c('', '', ''),# TODO: add variable
## "PCB: 1221, Unfiltered"                  = c('', '', ''),# TODO: add variable
## "Total Organic Carbon, Unfiltered"       = c('', 'mg/L', 'TOC'),
"Potassium, Filtered"                    = c('', 'mg/L', 'K')
)

# site translator to MCES preferred

mces_sitename_preferred <- c(
BS1_9 = "BS0019",
BA2_2 = "BA0022",
BE2_0 = "BE0020",
BL3_5 = "BL0035",
BR0_3 = "BR0003",
CA1_7 = "CA0017",
CR0_9 = "CR0009",
EA0_8 = "EA0008",
FC0_2 = "FC0002",
NM1_8 = "NM0018",
PU3_9 = "PU0039",
RI1_3 = "RI0013",
SA8_2 = "SA0082",
SI0_1 = "SI0001",
VA1_0 = "VA0010",
VR2_0 = "VR0020")
