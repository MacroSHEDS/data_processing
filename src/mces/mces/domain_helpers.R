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
"Total Kjeldahl Nitrogen, Filtered"      = c('mg/L', 'mg/L', 'TKN'),
"Total Kjeldahl Nitrogen, Unfiltered"    = c('mg/L', 'mg/L', 'UTKN'),
"Volatile Suspended Solids"              = c('mg/L', 'mg/L', 'VSS'),
"Total Phosphorus, Filtered"             = c('mg/l', 'mg/l', 'TDP'),
"Total Organic Carbon, Filtered"         = c('mg/L', 'mg/L', 'TOC'),
## "Total Organic Carbon, Unfiltered"       = c('mg/L', 'mg/L', 'TOC'),
## "Turbidity (FNU)"                        = c('FNU', 'FNU', 'turb'),
"Ortho Phosphate as P, Filtered"         = c('mg/l', 'mg/l', 'PO4_P'),
## "Ortho Phosphate as P, Unfiltered"       = c('mg/L', 'mg/L', 'PO4_P'), # NOTE: ortho vs multiphosphate
## "Chlorophyll-a, % Pheo-Corrected"        = c('', 'mg/l', ''),
## "Chlorophyll-a/Pheophytin-a Abs. R"      = c('', 'mg/l', ''),
## "Chlorophyll-a Trichromatic Uncorrected" = c('', 'mg/L', 'Chla'),
"Chlorophyll-a, Pheo-Corrected"          = c('', 'mg/L', 'Chla'),
## "Chlorophyll-b"                          = c('', '', ''),# TODO: add to variables
## "Chlorophyll-c"                          = c('', '', ''),# TODO: add to variables
## "Pheophytin-a"                           = c('', '', ''),# TODO: add to variables
"E. Coli Bacteria Count"                 = c('#/100mL', '#/mL', 'Ecoli'),
"Fecal Coliform Bacteria Count"          = c('#/100mL', '#/mL', 'fecal_coliform'),
"Total Phosphorus, Unfiltered"           = c('mg/L', 'mg/L', 'UTP'),
"Total Phosphorus, Particulate"          = c('mg/L', 'mg/L', 'TPP'),
## "Total Phosphorus, Filtered, Low L"      = c('mg/L', 'mg/L', 'TDP'),
"Ammonia Nitrogen, Filtered"             = c('mg/L', 'mg/L', 'NH3_N'),
## "Ammonia Nitrogen, Unfiltered"           = c('mg/L', 'mg/L', 'NH3_N'),
"Suspended Solids"                       = c('mg/L', 'mg/L', 'TSS'),
"Total Dissolved Solids"                 = c('mg/L', 'mg/L', 'TDS'),
"Total Nitrate/Nitrite N, Unfiltered"    = c('mg/L', 'mg/L', 'NO3_NO2_N'),
"Nitrite N, Unfiltered"                  = c('mg/L', 'mg/L', 'NO2_N'),
"Nitrate N, Unfiltered"                  = c('mg/l', 'mg/L', 'NO3_N'),
"Sulfate, Filtered"                      = c('mg/L', 'mg/L', 'SO4'),
## "Sulfate, Unfiltered"                    = c('mg/L', 'mg/L', 'SO4'),
## "Conductivity"                           = c('umho/cm', '', ''), # NOTE: conductivity vs sp. cond vs conducatance?? # TODO: add var? conversion?
"Dissolved Oxygen"                       = c('mg/L', 'mg/L', 'DO'),
"Temperature"                            = c('C', 'C', 'temp'),
"pH"                                     = c('unitless', 'unitless', 'pH'),
"Total Alkalinity, Filtered"             = c('mg/L', 'mg/L', 'alk'), # technically, mg/L_CaCO3. issue?
## "Total Alkalinity, Unfiltered"           = c('mg/l', 'mg/L', 'alk'),
## "Turbidity (NTU)"                        = c('NTU', 'FNU', 'turbid'), # NOTE: NTU and FNU are slightly different, but considered acceptable use same values w either, but maybe still # TODO: add variable (???)
## "Turbidity (NTRU)"                       = c('NTRU', 'FNU', 'turbid'), # NOTE: leaving out NTU and NTRU for now
## "COD, Unfiltered"                        = c('', '', ''),# TODO: add to variables
## "Hardness, Unfiltered"                   = c('', 'mg/L', 'CO3'), # NOTE: hardness just [CO3], carbonate# TODO: add to variables
"Cadmium, Filtered"                      = c('mg/L', 'mg/L', 'Cd'),
## "Cadmium, Unfiltered"                    = c('mg/L', 'mg/L', 'Cd'),
"Copper, Filtered"                       = c('mg/L', 'mg/L', 'Cu'),
## "Copper, Unfiltered"                     = c('mg/L', 'mg/L', 'Cu'),
"Chromium, Filtered"                     = c('mg/l', 'mg/L', 'Cr'),
## "Chromium, Unfiltered"                   = c('mg/l', 'mg/L', 'Cr'),
"Calcium, Unfiltered"                    = c('mg/L', 'mg/L', 'Ca'),
"Nickel, Unfiltered"                     = c('mg/L', 'mg/L', 'Ni'),
"Lead, Unfiltered"                       = c('mg/L', 'mg/L', 'Pb'),
"Potassium, Filtered"                    = c('mg/L', 'mg/L', 'K'),
"Silica, Filtered"                       = c('mg/l', 'mg/L', 'Si'),
"Sodium, Filtered"                       = c('mg/L', 'mg/L', 'Na'),
"Magnesium, Filtered"                    = c('mg/l', 'mg/L', 'Mg'),
## "Magnesium, Unfiltered"                  = c('mg/l', 'mg/L', 'Mg'),
"Mercury, Filtered"                      = c('mg/l', 'mg/L', 'Hg'),
## "Mercury, Unfiltred"                    = c('mg/L', 'mg/L', 'Hg'),
"Chloride, Filtered"                     = c('mg/l', 'mg/L', 'Cl'),
## "Chloride, Unfiltered"                   = c('mg/L', 'mg/L', 'Cl'),
"Zinc, Filtered"                         = c('mg/L', 'mg/L', 'Zn') # END
## "Zinc, Unfiltered"                       = c('mg/L', 'mg/L', 'Zn'),
## "COD, Filtered"                          = c('', '', ''), # TODO: add variable, chemical oxygen demand
## "CBOD 5-day, Unfiltered"                 = c('', '', ''),# TODO: add variable, chem + biological oxygen demand
## "BOD 5-day, Unfiltered"                  = c('', '', ''),# TODO: add variable, biological oxygen demand
## "BOD K-rate, Unfiltered"                 = c('', '', ''),# TODO: add variable
## "BOD Ultimate, Filtered"                 = c('', '', ''),# TODO: add variable
## "BOD K-rate, Filtered"                   = c('', '', ''),# TODO: add variable
## "Soluble Oxygen Demand, Filtered"        = c('', '', ''),# TODO: add variable
## "Transparency Tube"                      = c('', '', ''),# TODO: add variable (???)
## "PCB: 1248, Unfiltered"                  = c('', '', ''),# TODO: add variable
## "CBOD 5-day, Filtered"                   = c('', '', ''),# TODO: add variable
## "CBOD K-rate, Unfiltered"                = c('', '', ''),# TODO: add variable
## "CBOD K-rate, Filtered"                  = c('', '', ''),# TODO: add variable
## "CBOD Ultimate, Unfiltered"              = c('', '', ''),# TODO: add variable
## "PCB: 1232, Unfiltered"                  = c('', '', ''),# TODO: add variable
## "BOD Ultimate, Unfiltered"               = c('', '', ''),# TODO: add variable
## "PCB: 1016, Unfiltered"                  = c('', '', ''),# TODO: add variable
## "BOD 5-day, Filtered"                    = c('', '', ''),# TODO: add variable
## "CBOD Ultimate, Filtered"                = c('', '', ''),# TODO: add variable
## "PCB: 1254, Unfiltered"                  = c('', '', ''),# TODO: add variable
## "PCB: 1221, Unfiltered"                  = c('', '', ''),# TODO: add variable
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
