library(httr)
library(jsonlite)

# Set your API key
# api_key <- read_lines('~/keys/noaa_climate_api')


get_noaa_precip <- function(api_key, startdate = NULL, enddate = NULL, middate = NULL,
                            window_size = 3, lat, lon, nstations = 1){

    #get precip for nearest station to lat/lon

    #api_key: get it at https://www.ncdc.noaa.gov/cdo-web/token
    #either supply start and enddate, or middate and window_size
    #window_size: the number of days on either side of middate. more convenient
    #   to use midday and window_size when running in interactive loop.

    #returns precip in mm for the nearest `nstations` stations closest to the specified location,
    #for the specified duration, along with station id, etc.

    if(is.null(startdate)){
        startdate <- as.Date(middate) - days(as.numeric(window_size))
        enddate <- as.Date(middate) + days(as.numeric(window_size))
    }

    # Define the endpoint and parameters
    base_url <- "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    params <- list(
        datasetid = "GHCND",    # GHCN Daily dataset
        datatypeid = "PRCP",    # Precipitation data
        startdate = as.character(middate),
        enddate = as.character(middate),
        units = "metric",
        limit = as.character(nstations),
        latitude = lat,
        longitude = lon
    )

    # get nearest stations
    response <- GET(url = base_url, query = params, add_headers(token = api_key))
    params$stationid <- as.data.frame(fromJSON(rawToChar(response$content)))$results.station %>%
        paste(collapse = ',')

    params$startdate <- as.character(startdate)
    params$enddate <- as.character(enddate)
    params$limit <- '1000'
    params$lat <- params$lon <- NULL

    response <- GET(url = base_url, query = params, add_headers(token = api_key))
    data <- fromJSON(rawToChar(response$content))

    # Convert to dataframe
    precip_data <- as.data.frame(data$results) %>%
        rename(precip_mm = value) %>%
        pivot_wider(names_from = station,
                    values_from = precip_mm) %>%
        mutate(date = as.Date(ymd_hms(date))) %>%
        group_by(date) %>%
        summarize(across(contains(':'), ~mean(., na.rm = TRUE)),
                  attributes = paste(unique(strsplit(paste(gsub('[ ,]', '', c(',,EI,', ',I')), collapse = ''), '')[[1]]), collapse = ''),
                  .groups = 'drop')

    precip_data[is.na(precip_data)] <- NA #replace NaN

    return(precip_data)
}


get_nldas_precip <- function(startdate = NULL, enddate = NULL, middate = NULL,
                             window_size = 3, lat, lon){

    #see modified functions in ~/git/others_projects/NASA-NW

    if(is.null(startdate)){
        startdate <- as.Date(middate) - days(as.numeric(window_size))
        enddate <- as.Date(middate) + days(as.numeric(window_size))
    }

    download_NLDAS(tibble(NW_res = 'a', Latitude = lat, Longitude = lon),
                   start_date = startdate,
                   end_date = enddate,
                   param = 'precip'
    )
    process_NLDAS('precip')
}

# get_gridmet_precip <- function(startdate = NULL, enddate = NULL, middate = NULL,
#                                window_size = 3, lat, lon){
#
#     url <- "https://climate.northwestknowledge.net/METDATA/data"
#     params <- list(
#         lat = lat,
#         lon = lon,
#         start = startdate,
#         end = enddate,
#         vars = "pr"
#     )
#
#     response <- GET(url, query = params)
#     data <- fromJSON(content(response, "text"))
#     # data <- fromJSON(content(response, "text"), flatten = TRUE)
#
#     # Assuming data is structured with a 'data' key and precipitation data under 'pr'
#     precip_data <- sapply(data$data, function(x) x$pr)
#     dates <- seq(as.Date(start_date), as.Date(end_date), by="day")
#
#     return(data.frame(Date = dates, Precipitation_mm = precip_data))
# }


