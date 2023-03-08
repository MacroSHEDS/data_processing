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

mces_variable_info <- list(
"Total Kjeldahl Nitrogen, Unfiltered" = c('', '', ''),
"Total Phosphorus, Unfiltered" = c('', '', ''),
"Volatile Suspended Solids" = c('', '', ''),
"Ortho Phosphate as P, Filtered" = c('', '', ''),
"Total Phosphorus, Filtered" = c('', '', ''),
"Chlorophyll-a, % Pheo-Corrected" = c('', '', ''),
"Chlorophyll-a/Pheophytin-a Abs. R" = c('', '', ''),
"Chlorophyll-a Trichromatic Uncorrected" = c('', '', ''),
"Chlorophyll-a, Pheo-Corrected" = c('', '', ''),
"Suspended Solids" = c('', '', ''),
"Total Nitrate/Nitrite N, Unfiltered" = c('', '', ''),
"Nitrite N, Unfiltered" = c('', '', ''),
"Nitrate N, Unfiltered" = c('', '', ''),
"E. Coli Bacteria Count" = c('', '', ''),
"Chloride, Unfiltered" = c('', '', ''),
"Ammonia Nitrogen, Unfiltered" = c('', '', ''),
"Chlorophyll-b" = c('', '', ''),
"Chlorophyll-c" = c('', '', ''),
"Pheophytin-a" = c('', '', ''),
"Conductivity" = c('', '', ''),
"Dissolved Oxygen" = c('', '', ''),
"pH" = c('', '', ''),
"Temperature" = c('', '', ''),
"Cadmium, Unfiltered" = c('', '', ''),
"Turbidity (NTRU)" = c('', '', ''),
"Total Organic Carbon, Filtered" = c('', '', ''),
"Turbidity (NTU)" = c('', '', ''),
"COD, Unfiltered" = c('', '', ''),
"Hardness, Unfiltered" = c('', '', ''),
"Lead, Unfiltered" = c('', '', ''),
"Sulfate, Unfiltered" = c('', '', ''),
"Ortho Phosphate as P, Unfiltered" = c('', '', ''),
"Silica, Filtered" = c('', '', ''),
"Total Alkalinity, Unfiltered" = c('', '', ''),
"Zinc, Unfiltered" = c('', '', ''),
"Fecal Coliform Bacteria Count" = c('', '', ''),
"Copper, Unfiltered" = c('', '', ''),
"Total Kjeldahl Nitrogen, Filtered" = c('', '', ''),
"Chromium, Unfiltered" = c('', '', ''),
"Magnesium, Unfiltered" = c('', '', ''),
"COD, Filtered" = c('', '', ''),
"Nickel, Unfiltered" = c('', '', ''),
"CBOD 5-day, Unfiltered" = c('', '', ''),
"BOD 5-day, Unfiltered" = c('', '', ''),
"Sulfate, Filtered" = c('', '', ''),
"BOD K-rate, Unfiltered" = c('', '', ''),
"BOD Ultimate, Filtered" = c('', '', ''),
"BOD K-rate, Filtered" = c('', '', ''),
"Total Phosphorus, Filtered, Low L" = c('', '', ''),
"Soluble Oxygen Demand, Filtered" = c('', '', ''),
"Total Dissolved Solids" = c('', '', ''),
"Calcium, Unfiltered" = c('', '', ''),
"Cadmium, Filtered" = c('', '', ''),
"Total Phosphorus, Unfiltered", Low = c('', '', ''),
"Transparency Tube" = c('', '', ''),
"Copper, Filtered" = c('', '', ''),
"Chromium, Filtered" = c('', '', ''),
"PCB: 1248, Unfiltered" = c('', '', ''),
"Total Kjeldahl Nitrogen, Unfiltered" = c('', '', ''),
"Mercury, Filtered" = c('', '', ''),
"CBOD 5-day, Filtered" = c('', '', ''),
"CBOD K-rate, Unfiltered" = c('', '', ''),
"CBOD K-rate, Filtered" = c('', '', ''),
"Ammonia Nitrogen, Filtered" = c('', '', ''),
"Zinc, Filtered" = c('', '', ''),
"Total Alkalinity, Filtered" = c('', '', ''),
"Sodium, Filtered" = c('', '', ''),
"CBOD Ultimate, Unfiltered" = c('', '', ''),
"PCB: 1232, Unfiltered" = c('', '', ''),
"BOD Ultimate, Unfiltered" = c('', '', ''),
"Mercury, Unfiltered" = c('', '', ''),
"PCB: 1016, Unfiltered" = c('', '', ''),
"Turbidity (FNU)" = c('', '', ''),
"BOD 5-day, Filtered" = c('', '', ''),
"Chloride, Filtered" = c('', '', ''),
"CBOD Ultimate, Filtered" = c('', '', ''),
"Total Phosphorus, Particulate" = c('', '', ''),
"Magnesium, Filtered" = c('', '', ''),
"PCB: 1254, Unfiltered" = c('', '', ''),
"PCB: 1221, Unfiltered" = c('', '', ''),
"Potassium, Filtered" = c('', '', ''),
"Total Organic Carbon, Unfiltered" = c('', '', ''))
