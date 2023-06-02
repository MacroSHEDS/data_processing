#retrieval kernels ####
library(dataRetrieval)

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- function(set_details, network, domain) {
      
  prodname_ms = set_details$prodname_ms
  site_code   = set_details$site_code
  component   = set_details$component 
  last_mod_dt = set_details$held_dt
  url         = set_details$url
  
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
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)

  # download the data form the download link!
  R.utils::downloadFile(url = url,
                        filename = rawfile,
                        skip = FALSE,
                        overwrite = TRUE)

  #bigFILE <- read_csv(rawfile)     


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


#precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <-  function(set_details, network, domain) {
   
  prodname_ms = set_details$prodname_ms
  site_code   = set_details$site_code
  component   = set_details$component 
  last_mod_dt = set_details$held_dt
  url         = set_details$url
  
  # START OF BLOCK YOU DONT CHANGE #
  # this sets the file path of the raw data, should always be this same format
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)

  # this creates that directory, if it doesn't already exist
  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)

  # END OF BLOCK YOU DONT CHANGE #

  # create documentation file
  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)

  # download the data form the download link!
  R.utils::downloadFile(url = url,
                        filename = rawfile,
                        skip = FALSE,
                        overwrite = TRUE)

  ##bigFILE2 <- read_csv(rawfile)


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

#andrews_creek
#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- function(set_details, network, domain) {
  
    prodname_ms = set_details$prodname_ms
    site_code   = set_details$site_code
    component   = set_details$component 
    last_mod_dt = set_details$held_dt
    url         = set_details$url
  
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
    siteNumb <- "401723105400000"
    siteNumbs <- "USGS-401723105400000"
    parameterCD <- "00060"
    #serv = "dv"
    siteINFO <- readNWISdv(siteNumb,parameterCD)
    # create documentation file
    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component)


    test<- write_csv(siteINFO, file = rawfile)
    
    #R.utils::downloadFile(url = url,
    #                      filename = rawfile,
    #                      skip = FALSE,
    #                      overwrite = TRUE)


    # this code records metadat about the date, time, and other details
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

#icy brook
#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- function(set_details, network, domain) {
  
    prodname_ms = set_details$prodname_ms
    site_code   = set_details$site_code
    component   = set_details$component 
    last_mod_dt = set_details$held_dt
    url         = set_details$url

    # prodcode = "VERSIONLESS002"
    # network = "webb"
    # domain = "loch_vale"
    # site_code <- "sitename_NA"
    # component <- "loch_vale_discharge_icy_brook"
    # prodname_ms <- paste("discharge" ,"__", prodcode, sep = "")
    # url <- "https://waterdata.usgs.gov/co/nwis/dv?referred_module=sw&site_no=401707105395000"

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    siteNumb <- "401707105395000"
    parameterCD <- "00060"
    siteINFO <- readNWISdv(siteNumb,parameterCD)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component)

   test <-write_csv(siteINFO, file = rawfile)
    
    #R.utils::downloadFile(url = url,
    #                      filename = rawfile,
    #                      skip = FALSE,
    #                      overwrite = TRUE)
    
    res <- httr::HEAD(set_details$url)

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

#loch outlet
#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS005 <- function(set_details, network, domain) {
  
    prodname_ms = set_details$prodname_ms
    site_code   = set_details$site_code
    component   = set_details$component 
    last_mod_dt = set_details$held_dt
    url         = set_details$url

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    siteNumb <- "401733105392404"
    parameterCD <- "00060"
    #serv = "dv"
    siteINFO <- readNWISdv(siteNumb,parameterCD)


    siteAvailable <- whatNWISdata(siteNumber = siteNumb)
    parmsAvailable <- unique(siteAvailable$parm_cd)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component)

    test<- write_csv(siteINFO, file = rawfile)
    
    #R.utils::downloadFile(url = url,
    #                      filename = rawfile,
    #                      skip = FALSE,
    #                      overwrite = TRUE)

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

#andrews_creek
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS006 <- function(set_details, network, domain) {
  
    prodname_ms = set_details$prodname_ms
    site_code   = set_details$site_code
    component   = set_details$component 
    last_mod_dt = set_details$held_dt
    url         = set_details$url
  
    prodcode = "VERSIONLESS006"
    network = "webb"
    domain = "loch_vale"
    site_code <- "sitename_NA"
    component <- "loch_vale_chem_andrews_creek"
    prodname_ms <- paste("stream_chemistry" ,"__", prodcode, sep = "")
    url <- "https://nwis.waterdata.usgs.gov/co/nwis/qwdata?pm_cd_compare=Greater%20than&radio_parm_cds=all_parm_cds&site_no=401723105400000&agency_cd=USGS&format=separated_wide_rdb"

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    siteNumb <- "401723105400000"
    siteN <- "USGS-401723105400000"
    #serv = "dv"
    siteAvailable <- whatNWISdata(siteNumber = siteNumb)
    siteAvailable <- filter(siteAvailable, siteAvailable$count_nu>5)

    parmsAvailable <- unique(siteAvailable$parm_cd)
    parmsAvailable <- parmsAvailable[!is.na(parmsAvailable)]

    siteWQ <- readWQPqw(siteNumbers = c(siteN), parameterCd = parmsAvailable)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = component)

    test <-write_csv(siteWQ, file = rawfile)
    
    #R.utils::downloadFile(url = url,
    #                      filename = rawfile,
    #                      skip = FALSE,
    #                      overwrite = TRUE)

    res <- httr::HEAD(url)

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


#icy brook
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS007 <- function(set_details, network, domain) {
  
  prodname_ms = set_details$prodname_ms
  site_code   = set_details$site_code
  component   = set_details$component 
  last_mod_dt = set_details$held_dt
  url         = set_details$url
    
  raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = site_code)

  dir.create(path = raw_data_dest,
             showWarnings = FALSE,
             recursive = TRUE)

  siteNumb <- "401707105395000"
  siteN <-"USGS-401707105395000"
  #serv = "dv"
  siteAvailable <- whatNWISdata(siteNumber = siteNumb)
  siteAvailable <- filter(siteAvailable, siteAvailable$count_nu>5)

  parmsAvailable <- unique(siteAvailable$parm_cd)
  parmsAvailable <- parmsAvailable[!is.na(parmsAvailable)]

  siteWQ <- readWQPqw(siteNumbers =  c(siteN), parameterCd = parmsAvailable)

  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)

  test <- write_csv(siteWQ, file = rawfile)
  
  #R.utils::downloadFile(url = url,
  #                      filename = rawfile,
  #                      skip = FALSE,
  #                      overwrite = TRUE)

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


#loch outlet
#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS008 <- function(set_details, network, domain) {

    prodname_ms = set_details$prodname_ms
    site_code   = set_details$site_code
    component   = set_details$component 
    last_mod_dt = set_details$held_dt
    url         = set_details$url
    
    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                        n = network,
                        d = domain,
                        p = prodname_ms,
                        s = set_details$site_code)

    dir.create(path = raw_data_dest,

             showWarnings = FALSE,
             recursive = TRUE)

  siteNumb <- "401733105392404"
  siteN <- "USGS-401733105392404"

  siteAvailable <- whatNWISdata(siteNumber = siteNumb)
  siteAvailable <- filter(siteAvailable, siteAvailable$count_nu>5)
  parmsAvailable <- unique(siteAvailable$parm_cd)
  parmsAvailable <- parmsAvailable[!is.na(parmsAvailable)]

  siteWQ <- readWQPqw(siteNumbers =  c(siteN), parameterCd = parmsAvailable)

  rawfile <- glue('{rd}/{c}.csv',
                  rd = raw_data_dest,
                  c = component)

  test <- write_csv(siteWQ, rawfile)

  
  #R.utils::downloadFile(url = set_details$url,
  #                      filename = rawfile,
  #                      skip = FALSE,
  #                      overwrite = TRUE)

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

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {
    component = "loch_vale_ppt"
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)
    
    #Loch Vales siteID is CO98
    
    d <- read.delim(rawfile, sep = ',') %>%
        filter(siteID == 'CO98') %>%
        filter(!is.na(dateon)) %>%
        as_tibble()
    
    #data for precip is based on weekly sum, converting to df for conversion to daily
    #For each day (# found in days) in the range starting on date 1 and ending on day 2, change the ppt column to the value found in pptPD in that same range
    #commented out for now
    #remove unnecessary columns for efficiency
    ##d <- d[, -5:-26]
    #create as date columns
    d$dON <- as.Date(as.character(d$dateon), format = "%Y-%m-%d %H:%M")
    d$invalcode <- gsub("\\s+", "", d$invalcode)
    ##dl$dOFF <- as.Date(as.character(dl$dateoff), format = "%Y-%m-%d %H:%M")
    #maps from the ranges found in dON and dOFF into $daysMap, creating date column
    ##dl$daysMap <- Map(function(x,y) as.Date(seq(from = x, to = y, by = "day")), dl$dON, dl$dOFF)
    #unnest to create a row for each entry of daysMap
    ##dl_u <- unnest(dl, daysMap)
    #find #days in between and convert to int
    ##dl_u$diffDays <-abs(difftime(dl_u$dON, dl_u$dOFF, units = "days"))
    ##dl_u$diffDays <- as.numeric(gsub(" days", " ", dl_u$diffDays))
    #finally ppt per day
    ##dl_u$pptPD <- dl_u["subppt"] / dl_u["diffDays"]

    
  
    #DATETIME is messed up cuz of one digit thing
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('dON' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'siteID',
                         data_cols =  c('subppt' = 'precipitation'),
                         data_col_pattern = '#V#',
                         summary_flagcols = "invalcode",
                         is_sensor = FALSE,
                         set_to_NA = "-9")
    
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list(invalcode = c("u","v","c","f","vc","vf","p","uf",  "b" ,  "vpb" ,"vuc" ,"uc"  ,"e"  , "vb"  ,"vu"  ,"eu"  ,"ve"  ,"ec"  ,"np"  ,"n", "xx"  ,"pef")),
                            summary_flags_clean = list(invalid = c("")))
                            
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

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.delim(rawfile, sep = ',') %>%
      filter(siteID == 'CO98') %>%
      filter(!is.na(dateon)) %>%
      as_tibble()
    
    loch_vale_pchem_var_info <- list(
      "ph"     = c("unitless", "unitless", "pH"),
      "Conduc" = c("uS/cm", "uS/cm", "spCond"),
      "Ca"     = c("mg/L", "mg/L", "Ca"),
      "Mg"     = c("mg/L", "mg/L", "Mg"),
      "K "     = c("mg/L", "mg/L", "K"),
      "Na"     = c("mg/L", "mg/L", "Na"),
      "NH4"    = c("mg/L", "mg/L", "NH4"),
      "NO3"    = c("mg/L", "mg/L", "NO3"),
      "Cl"     = c("mg/L", "mg/L", "Cl"),
      "SO4"    = c("mg/L", "mg/L", "SO4")
      #"Br"     = c("mg/L", "mg/L", "Br")
    )
    
    loch_vale_pchem_var = c()
    
    for(i in 1:length(loch_vale_pchem_var_info)) {
      og_name <- names(loch_vale_pchem_var_info[i])
      ms_name <-loch_vale_pchem_var_info[[i]][3]
      
      loch_vale_pchem_var[og_name] = ms_name
    }
    
    d$dON <- as.Date(as.character(d$dateon), format = "%Y-%m-%d %H:%M")
    d$invalcode <- gsub("\\s+", "", d$invalcode)
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('dON' = '%Y-%m-%d'),
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'siteID',
                         data_cols =  loch_vale_pchem_var,
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = 'flag#V#',
                         summary_flagcols = "invalcode",
                         is_sensor = FALSE,
                         set_to_NA = "-9")

    d <- ms_cast_and_reflag(d,
                            variable_flags_bdl = '<')

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
      filter(site_no == '401723105400000') %>%
      filter(X_00060_00003_cd == "A") %>%
      as_tibble() %>%
      mutate(site_no = 'andrews_creek')
    
   
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                              datetime_cols = list('Date' = '%Y-%m-%d'), 
                              datetime_tz = 'US/Mountain',
                              site_code_col = 'site_no',
                              data_cols =  c(X_00060_00003 = 'discharge'),
                              data_col_pattern = '#V#',
                              is_sensor = TRUE)
    
    # d$site_code <- paste("USGS-", d$site_code, sep = "")  
    
    d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)
    
    #NOTE manually converting discharge from cubic feet per second to liter per second
    d$val <- d$val * 28.3168
               
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
    filter(site_no == '401707105395000') %>%
    filter(X_00060_00003_cd == "A") %>%
    as_tibble() %>%
    mutate(site_no = 'icy_brook')
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'), 
                       datetime_tz = 'US/Mountain',
                       site_code_col = 'site_no',
                       data_cols =  c(X_00060_00003 = 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  
  # d$site_code <- paste("USGS-", d$site_code, sep = "")  
  
  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)
  
  #NOTE manually converting discharge from cubic feet per second to liter per second
  d$val <- d$val * 28.3168
  
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
#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  
  d <- read.delim(rawfile, sep = ',') %>%
    filter(site_no == '401733105392404') %>%
    filter(X_00060_00003_cd == "A") %>%
    as_tibble() %>%
    mutate(site_no = 'the_loch_outlet')
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('Date' = '%Y-%m-%d'), 
                       datetime_tz = 'US/Mountain',
                       site_code_col = 'site_no',
                       data_cols =  c(X_00060_00003 = 'discharge'),
                       data_col_pattern = '#V#',
                       is_sensor = TRUE)
  
  # d$site_code <- paste("USGS-", d$site_code, sep = "")  
  
  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)
  
  #NOTE manually converting discharge from cubic feet per second to liter per second
  
  d$val <- d$val * 28.3168
  
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
#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component) {
  
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)
    
    d <- read.delim(rawfile, sep = ',') %>%
      as_tibble()
    
    #for UV 254, two different units reported, only using the one that matches variables sheet
    d <- d %>% 
      subset(!(ResultMeasure.MeasureUnitCode == "L/mgDOC*m" & grepl("UV 254", CharacteristicName)))
    
    #manually applying MS Detection Limit (when below dl, take half of dl)
    #for this USGS data, whenever there is NA for the measure, 
    #a detection limit is reported, if not below, no detection limit reported
    d <- d %>%
      rename(dl_col = DetectionQuantitationLimitMeasure.MeasureValue) %>%
      mutate(
        ResultMeasureValue =  ifelse(!is.na(dl_col) & is.na(ResultMeasureValue) & ResultDetectionConditionText == 'Not Detected', as.numeric(dl_col)/2, ResultMeasureValue)
        )
    
    loch_vale_andrews_creek_var_info <- list(
      "pH" = c('unitless', 'unitless', "pH"),
      "Calcium"=c("mg/l", "mg/L", "Ca"),
      "Magnesium"=c("mg/l", "mg/L", "Mg"),
      "Sodium"=c("mg/l", "mg/L", "Na"),
      "Potassium"=c("mg/l", "mg/L", "K"),
      "Chloride"=c("mg/l", "mg/L", "Cl"),
      "Sulfate"=c("mg/l", "mg/L", "SO4"),
      "Silica"=c("mg/l", "mg/L", "SiO2"),
      "Total Dissolved Solids"=c("mg/l", "mg/L", "TDS"),   
      "Specific Conductance"=c("uS/cm @25C", "uS/cm", "spCond"),
      "Strontium-87/Strontium-86, ratio"=c("unitless", "unitless","d87Sr_d86Sr"),
      "Strontium"=c("mg/l","mg/L","Sr"),
      "Temperature, water"=c("deg C", "degrees C", "temp"),
      "Mercury"=c("ng/l", "mg/L", "Hg"), # watch units 
      "Carbon dioxide"=c("mg/l","ppm","CO2"),#watch units
      "Ammonia and ammonium" = c("mg/l as N","mg/L","NH3_NH4_N"),
      "Nitrate" = c("mg/l as N", "mg/L", "NO3_N" ),
      "Nitrogen"=c("mg/l","mg/L","TPN"), #total particulate nitrogen
      "Inorganic nitrogen (nitrate and nitrite)"=c("mg/l as N","mg/L","DIN"),  #dissolved
      "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"=c("mg/l", "mg/L", "TDN"),  #dissolved
      "Deuterium/Hydrogen ratio"  =c("unitless","unitless","dH2_H"),  
      "Oxygen-18/Oxygen-16 ratio" =c("unitless", "unitless", "d18O_d16O"),
      "Nitrogen-15/14 ratio"      =c("unitless","unitless", "d15N_d14N"),
      "Sulfur-34/Sulfur-32 ratio" =c("unitless","unitless", "d34S_d32S"),
      "Organic carbon"=c("mg/l","mg/L","DOC"),  #dissolved
      "Carbon"=c("mg/l","mg/L","TPC"), #total particulate carbon
      "Phosporus"=c("mg/l as p", "mg/L", "TP"), #total
      "UV 254"=c("units/cm","AU/cm","abs254"), 
      "Oxygen"=c("mg/l","mg/L","DO") #dissolved
    )
    
    loch_vale_stream_chem_var <- list()
    
    for (i in seq_along(loch_vale_andrews_creek_var_info)) {
      og_name <- names(loch_vale_andrews_creek_var_info[i])
      ms_name <- loch_vale_andrews_creek_var_info[[i]][[3]]
      loch_vale_stream_chem_var[[og_name]] <- ms_name
    }
    
    d <-pivot_wider(d, names_from = CharacteristicName, values_from = c(ResultMeasureValue, dl_col))

    # NOTE: manually changing site code to macrosheds canonical version, i.e. from uSGS numeric code to "amdrews_creek"
    d$MonitoringLocationIdentifier = 'andrews_creek'
    
    # NOTE: must fix detection limit application below
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('ActivityStartDate' = '%Y-%m-%d'), 
                         datetime_tz = 'US/Mountain',
                         site_code_col = 'MonitoringLocationIdentifier',
                         data_cols =  loch_vale_stream_chem_var,
                         data_col_pattern = 'ResultMeasureValue_#V#',
                         is_sensor = FALSE
                         # numeric_dl_col_pattern = "DetectionQuantitationLimitMeasure.MeasureValue_#V#"
                         )
    
    # NOTE: there must be USGS edit codes which contain QC information?
    d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
    
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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS007 <- function(network, domain, prodname_ms, site_code, component) {
  
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    as_tibble()
  
  #manually applying MS Detection Limit (when below dl, take half of dl)
  #for this USGS data, whenever there is NA for the measure, 
  #a detection limit is reported, if not below, no detection limit reported
  d <- d %>%
    rename(dl_col = DetectionQuantitationLimitMeasure.MeasureValue) %>%
    mutate(
      ResultMeasureValue =  ifelse(!is.na(dl_col) & is.na(ResultMeasureValue) & ResultDetectionConditionText == 'Not Detected', as.numeric(dl_col)/2, ResultMeasureValue)
      )
  
  #for UV 254, two different units reported, only using the one that matches variables sheet
  d <- d %>% 
    subset(!(ResultMeasure.MeasureUnitCode == "L/mgDOC*m" & grepl("UV 254", CharacteristicName)))
  
  loch_vale_icy_brook_var_info <- list(
    "pH" = c('unitless', 'unitless', "pH"),
    "Calcium"=c("mg/l", "mg/L", "Ca"),
    "Magnesium"=c("mg/l", "mg/L", "Mg"),
    "Sodium"=c("mg/l", "mg/L", "Na"),
    "Potassium"=c("mg/l", "mg/L", "K"),
    "Chloride"=c("mg/l", "mg/L", "Cl"),
    "Sulfate"=c("mg/l", "mg/L", "SO4"),
    "Silica"=c("mg/l", "mg/L", "SiO2"),
    "Total Dissolved Solids"=c("mg/l", "mg/L", "TDS"),   
    "Specific Conductance"=c("uS/cm @25C", "uS/cm", "spCond"),
    "Strontium-87/Strontium-86, ratio"=c("unitless", "unitless","d87Sr_d86Sr"),
    "Strontium"=c("mg/l","mg/L","Sr"),
    "Temperature, water"=c("deg C", "degrees C", "temp"),
    "Mercury"=c("ng/l", "mg/L", "Hg"), # watch units 
    "Carbon dioxide"=c("mg/l","ppm","CO2"), #watch units
    "Ammonia and ammonium" = c("mg/l as N","mg/L" ,"NH3_NH4_N"),
    "Nitrate" = c("mg/l as N", "mg/L", "NO3_N" ),
    "Nitrogen"=c("mg/l","mg/L","TPN"), #total particulate nitrogen
    "Inorganic nitrogen (nitrate and nitrite)"=c("mg/l as N","mg/L","DIN"),  #dissolved
    "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"=c("mg/l", "mg/L", "TDN"),  #dissolved
    "Deuterium/Hydrogen ratio"  =c("unitless","unitless","dH2_H"),  
    "Oxygen-18/Oxygen-16 ratio" =c("unitless", "unitless", "d18O_d16O"),
    "Nitrogen-15/14 ratio"      =c("unitless","unitless", "d15N_d14N"),
    "Sulfur-34/Sulfur-32 ratio" =c("unitless","unitless", "d34S_d32S"),
    "Organic carbon"=c("mg/l","mg/L","DOC"),  #dissolved
    "Carbon"=c("mg/l","mg/L","TPC"), #total particulate carbon
    "Phosporus"=c("mg/l as p", "mg/L", "TP"), #total
    "UV 254"=c("units/cm","AU/cm","abs254"), 
    "Oxygen"=c("mg/l","mg/L","DO") #dissolved
  )
 
  loch_vale_stream_chem_var <- list()
  
  for (i in seq_along(loch_vale_icy_brook_var_info)) {
    og_name <- names(loch_vale_icy_brook_var_info[i])
    ms_name <- loch_vale_icy_brook_var_info[[i]][[3]]
    loch_vale_stream_chem_var[[og_name]] <- ms_name
  }
  
  # NOTE: manually changing site code to macrosheds canonical version, i.e. from uSGS numeric code to "amdrews_creek"
  d$MonitoringLocationIdentifier = 'icy_brook'
  
  d <- pivot_wider(d, names_from = CharacteristicName, values_from = c(ResultMeasureValue, dl_col))
  
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('ActivityStartDate' = '%Y-%m-%d'), 
                       datetime_tz = 'US/Mountain',
                       site_code_col = 'MonitoringLocationIdentifier',
                       data_cols =  loch_vale_stream_chem_var,
                       data_col_pattern = 'ResultMeasureValue_#V#',
                       is_sensor = FALSE
                       # numeric_dl_col_pattern = "DetectionQuantitationLimitMeasure.MeasureValue_#V#"
                       )
  
  # NOTE: there must be USGS edit codes which contain QC information?
  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  
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


#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS008 <- function(network, domain, prodname_ms, site_code, component) {
  
  rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)
  
  d <- read.delim(rawfile, sep = ',') %>%
    as_tibble()
  
  #manually applying MS Detection Limit (when below dl, take half of dl)
  #for this USGS data, whenever there is NA for the measure, 
  #a detection limit is reported, if not below, no detection limit reported
  d <- d %>%
    rename(dl_col = DetectionQuantitationLimitMeasure.MeasureValue) %>%
    mutate(
      ResultMeasureValue =  ifelse(!is.na(dl_col) & is.na(ResultMeasureValue) & ResultDetectionConditionText == 'Not Detected', as.numeric(dl_col)/2, ResultMeasureValue)
      )
  
  #for UV 254, two different units reported, only using the one that matches variables sheet
  d <- d %>% 
    subset(!(ResultMeasure.MeasureUnitCode == "L/mgDOC*m" & grepl("UV 254", CharacteristicName)))

  loch_vale_loch_outlet_var_info <- list(
    "pH" = c("unitless", "unitless", "pH"),
    "Alkalinity" = c("mg/l CaCO3", "mg/L", "alk"),
    "Calcium"=c("mg/l", "mg/L", "Ca"),
    "Magnesium"=c("mg/l", "mg/L", "Mg"),
    "Sodium"=c("mg/l", "mg/L", "Na"),
    "Potassium"=c("mg/l", "mg/L", "K"),
    "Chloride"=c("mg/l", "mg/L", "Cl"),
    "Sulfate"=c("mg/l", "mg/L", "SO4"),
    "Silica"=c("mg/l", "mg/L", "SiO2"),
    "Total Dissolved Solids"=c("mg/l", "mg/L", "TDS"),   
    "Specific Conductance"=c("uS/cm @25C", "uS/cm", "spCond"),
    "Strontium-87/Strontium-86, ratio"=c("unitless", "unitless","d87Sr_d86Sr"),
    "Strontium"=c("mg/l","mg/L","Sr"),
    "Temperature, water"=c("deg C", "degrees C", "temp"),
    "Mercury"=c("ng/l", "mg/L", "Hg"), # watch units 
    "Carbon dioxide"=c("mg/l","ppm","CO2"), #watch units
    "Ammonia and ammonium" = c("mg/l as N","mg/L" ,"NH3_NH4_N"),
    "Nitrate" = c("mg/l as N", "mg/L", "NO3_N" ),
    "Nitrogen"=c("mg/l","mg/L","TPN"), #total particulate nitrogen
    "Inorganic nitrogen (nitrate and nitrite)"=c("mg/l as N","mg/L","DIN"),  #dissolved
    "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"=c("mg/l", "mg/L", "TDN"),  #dissolved
    "Deuterium/Hydrogen ratio"  =c("unitless","unitless","dH2_H"),  
    "Oxygen-18/Oxygen-16 ratio" =c("unitless", "unitless", "d18O_d16O"),
    "Nitrogen-15/14 ratio"      =c("unitless","unitless", "d15N_d14N"),
    "Sulfur-34/Sulfur-32 ratio" =c("unitless","unitless", "d34S_d32S"),
    "Organic carbon"=c("mg/l","mg/L","DOC"),  #dissolved
    "Carbon"=c("mg/l","mg/L","TPC"), #total particulate carbon
    "Phosporus"=c("mg/l as p", "mg/L", "TP"), #total
    "UV 254"=c("units/cm","AU/cm","abs254"), 
    "Oxygen"=c("mg/l","mg/L","DO") #dissolved
  )
  
  loch_vale_stream_chem_var <- list()
  
  for (i in seq_along(loch_vale_loch_outlet_var_info)) {
    og_name <- names(loch_vale_loch_outlet_var_info[i])
    ms_name <- loch_vale_loch_outlet_var_info[[i]][[3]]
    loch_vale_stream_chem_var[[og_name]] <- ms_name
  }
  
  d <- pivot_wider(d, names_from = CharacteristicName, values_from = c(ResultMeasureValue, dl_col))
  
  # NOTE: manually changing site code to macrosheds canonical version, i.e. from uSGS numeric code to "amdrews_creek"
  d$MonitoringLocationIdentifier = 'the_loch_outlet'
    
  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('ActivityStartDate' = '%Y-%m-%d'), 
                       datetime_tz = 'US/Mountain',
                       site_code_col = 'MonitoringLocationIdentifier',
                       data_cols =  loch_vale_stream_chem_var,
                       data_col_pattern = 'ResultMeasureValue_#V#',
                       is_sensor = FALSE
                       # numeric_dl_col_pattern = "DetectionQuantitationLimitMeasure.MeasureValue_#V#"
                       )
  
  # NOTE: there must be USGS edit codes which contain QC information?
  d <- ms_cast_and_reflag(d, varflag_col_pattern = NA)
  
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

#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__VERSIONLESS003',
                                           'discharge__VERSIONLESS004',
                                           'discharge__VERSIONLESS005'))
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c(
                                           'stream_chemistry__VERSIONLESS006',
                                           'stream_chemistry__VERSIONLESS007',
                                           'stream_chemistry__VERSIONLESS008'))

}


#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms005 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms006 <- stream_gauge_from_site_data

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_precip_pchem_pflux


