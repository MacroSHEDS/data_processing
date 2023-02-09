## #retrieval kernels ####

#CUSTOMprecipitation: STATUS=READY
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
    rawfile <- glue('{rd}/{c}.csv',
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

## set_details <- webb_pkernel_setup(network = network, domain = domain, prodcode = "VERSIONLESS002")
## prodname_ms <- set_details$prodname_ms
## site_code <- set_details$site_code
## component <- set_details$component
## url <- set_details$url

#precip_chemistry: STATUS=READY
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
    q <- retrieve_usgs_sleepers_daily_q(set_details)

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

    # call our dataRetrieval function
    q <- retrieve_usgs_sleepers_daily_q(set_details)

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

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS006 <- function(set_details, network, domain) {

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

#munge kernels ####

#CUSTOMprecipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile)

    # filter to only precipitation site type
    d <- d %>%
      mutate(site_code = "R-29") %>%
      filter(!is.na(Precip..mm),
             !is.na(Date)) %>%
      select(Date, site_code, Precip..mm)


    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%y'),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'site_code',
                         data_cols =  c('Precip..mm' = 'precipitation'),
                         data_col_pattern = '#V#',
                         ## summary_flagcols = NA,
                         is_sensor = FALSE)


    d <- ms_cast_and_reflag(d,
                           varflag_col_pattern = NA
                           )

    # apply uncertainty (no detlim for water volume)
    d <- ms_check_range(d)
    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')

    # no need for conversions bc already in mm
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

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # creating a temporary directory to unzip the folder in
    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)
    unzip(rawfile,
          exdir = temp_dir)

    # reading in the contents of the extracted folder
    file_names <- list.files(temp_dir, recursive = TRUE)
    file_paths <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)

    # there are many important files in here, but we are only trying to get
    # the raw stream chemistry data
    chem_fp <- file_paths[grepl('Chemistry.csv', file_paths)]

    # the original file has encoding isssues, and connot be read directly
    # here I use a readr function which "guesses" the most likely encodings
    # which is good to know but I ultimately didnt use
    d_encoding <- readr::guess_encoding(chem_fp)[[1,1]]

    # read in the csv ignoring column name encoding issues
    d <- read.csv(chem_fp, check.names = FALSE)
    d_old_names <- names(d)

    ## NOTE: special character was microgram symbol
    ## replacing with "u"
    d_new_names <- unname(sapply(d_old_names, function(x) gsub('\\\xb5', "u", x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub('<', "", x)))

    # then rename all columns with these new names
    colnames(d) <- d_new_names

    # filter to only precipitation site type
    d <- d %>%
      filter(Sample_Type %in% c("PE", "PW"),
             # what do about W-9 big bucket?
             Sample_Name == "R-29 (PPT@W-9)") %>%
      mutate(Sample_Name = case_when(Sample_Name == "R-29 (PPT@W-9)" ~ "R-29", TRUE ~ Sample_Name))

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here to be just the old_var = new_var structure
    sleepers_aq_chem = c()

    for(i in 1:length(sleepers_stream_chem_var_info)) {
      og_name <- names(sleepers_stream_chem_var_info[i])
      ms_name <- sleepers_stream_chem_var_info[[i]][3]

      sleepers_aq_chem[og_name] = ms_name
    }

    # original data has a "summary" flag column, which lists simply a variable name -- meaning
    # that it is sctually a variable flag column of sorts. it will be easiest if we can
    # unpack this into a new column, for each variable, where the value = 1 if the original
    # flag column named it in a particular observation. one added issue is this "summary" column
    # has potential for mulitple variables at a time.

    # start by making new column for each chemistry variable
    last_col <- colnames(d)[ncol(d)]

    # re-distribute flags to corresponding variable
    d <- d %>%
      dplyr::mutate(
               across(Chemistry_Flag:all_of(!!last_col) & !ends_with("_Lab"),
                      .fns = list(
                        varflag = ~ case_when(grepl(stringr::str_match(cur_column(), "[^.]+"), Chemistry_Flag) ~ 1, TRUE ~ 0)),
                      .names = "{fn}_{col}"))

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date_Time' = "%Y-%m-%d %H:%M:%S"),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'Sample_Name',
                         data_cols =  sleepers_aq_chem,
                         data_col_pattern = '#V#',
                         # variable specific flag pattern
                         var_flagcol_pattern = 'varflag_#V#',
                         # summary (all vars) flag colun name
                         ## summary_flagcols = 'Chemistry_Flag',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            variable_flags_dirty   = c(1),

                            # will turn the *ms_status* column to 0 (e.g. clean)
                            variable_flags_clean   = c(0),
                            )

    # manual turn ms_status = 2 for all negative numbers (not in temp, ANC, or isotopes)
    no_bdl_vars = c("GN_temp", "GN_d180", "GN_NO3_d180", "GN_d87Sr_d86Sr", "GN_deuterium",
                  "GN_d13C", "GN_NO3_d15N")
    # Sleepers metadata states that all negative values are below detection limit, with the
    # value itself being the detection limit for that sample and method
    # give ms_status = 1 to all BDL observations
    d <- d %>%
      mutate(ms_status = case_when(!var %in% no_bdl_vars & val < 0 ~ 1, TRUE ~ ms_status))
    # replace all BDL observations with half DL value
    d <- d %>%
      mutate(val = case_when(!var %in% no_bdl_vars & val < 0 ~ val/2, TRUE ~ val))

    # apply uncertainty
    d <- ms_check_range(d)
    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')


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

    unlink(temp_dir, recursive = TRUE)

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

    # clean edit codes (from sleepers metadata)
    clean_codes <- c("1", "3", "4", "2", "6", "10", "11", "13", "16", "9")
    # dirty edit codes (from sleepers metadata)
    dirty_codes <- c("5", "14")
    # drop edit codes (from sleepers metadata)
    drop_codes <- c("7")

    # pre-processing
    d <- read.csv(rawfile) %>%
      mutate(site_code = "W-9",
             date = as.Date(Date.Time..EST., format = "%m/%d/%y"),
             Edit.Code = as.character(Edit.Code),
             # pre-filter edit codes, to retain filtering by original info
             # turn all "good" and "fair" edit coees to 0, and dirty to 1
             ## edit_code = case_when(Edit.Code %in% clean_codes ~ 0, Edit.Code %in% dirty_codes ~ 1, TRUE ~ Edit.Code)
             ) %>%
      filter(
        # remove all "poor" data
        !Edit.Code %in% drop_codes
        )

    # pre-filter edit codes, to retain filtering by original info
    # turn all "good" and "fair" edit coees to 0, and dirty to 1
    d$Edit.Code <- ifelse(d$Edit.Code %in% clean_codes, 0, 1)

    d <- d %>%
      # mean by date to save processing time
      group_by(site_code, date) %>%
      summarise(
        discharge = mean(Q..cfs),
        Edit.Code = max(Edit.Code)
      )

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = "%Y-%m-%d"),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'site_code',
                         data_cols =  c("discharge" = "discharge"),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'Edit.Code',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            summary_flags_dirty   = c("Edit.Code" = 1),
                            # will turn the *ms_status* column to 0 (e.g. clean)
                            summary_flags_clean   = c( "Edit.Code" = 0),
                            ## summary_flags_to_drop = "#*#",
                            varflag_col_pattern = NA
                            )


    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')
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

    d <- read.csv(rawfile) %>%
      mutate(site_code = "W-5") %>%
      rename(qflag = "X_00060_00003_cd")

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = "%Y-%m-%d"),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'site_code',
                         data_cols =  c("X_00060_00003" = "discharge"),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'qflag',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            summary_flags_dirty   = c("qflag" = c("P", "A e")),
                            # will turn the *ms_status* column to 0 (e.g. clean)
                            summary_flags_clean   = c( "qflag" = "A"),
                            ## summary_flags_to_drop = "#*#",
                            varflag_col_pattern = NA
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

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile) %>%
      mutate(site_code = "W-3") %>%
      rename(qflag = "X_00060_00003_cd")

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = "%Y-%m-%d"),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'site_code',
                         data_cols =  c("X_00060_00003" = "discharge"),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'qflag',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            summary_flags_dirty   = c("qflag" = c("P", "A e")),
                            # will turn the *ms_status* column to 0 (e.g. clean)
                            summary_flags_clean   = c( "qflag" = "A"),
                            ## summary_flags_to_drop = "#*#",
                            varflag_col_pattern = NA
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

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    # creating a temporary directory to unzip the folder in
    temp_dir <- tempdir()
    dir.create(temp_dir,
               showWarnings = FALSE,
               recursive = TRUE)
    unzip(rawfile,
          exdir = temp_dir)

    # reading in the contents of the extracted folder
    file_names <- list.files(temp_dir, recursive = TRUE)
    file_paths <- list.files(temp_dir, recursive = TRUE, full.names = TRUE)

    # there are many important files in here, but we are only trying to get
    # the raw stream chemistry data
    chem_fp <- file_paths[grepl('Chemistry.csv', file_paths)]

    # the original file has encoding isssues, and connot be read directly
    # here I use a readr function which "guesses" the most likely encodings
    # which is good to know but I ultimately didnt use
    d_encoding <- readr::guess_encoding(chem_fp)[[1,1]]

    # read in the csv ignoring column name encoding issues
    d <- read.csv(chem_fp, check.names = FALSE)
    d_old_names <- names(d)

    ## NOTE: special character was microgram symbol
    ## replacing with "u"
    d_new_names <- unname(sapply(d_old_names, function(x) gsub('\\\xb5', "u", x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub('<', "", x)))

    # then rename all columns with these new names
    colnames(d) <- d_new_names

    # filter to only stream site type
    d <- d %>%
      filter(Sample_Type == "ST")

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here to be just the old_var = new_var structure
    sleepers_aq_chem = c()

    for(i in 1:length(sleepers_stream_chem_var_info)) {
      og_name <- names(sleepers_stream_chem_var_info[i])
      ms_name <- sleepers_stream_chem_var_info[[i]][3]

      sleepers_aq_chem[og_name] = ms_name
    }

    # original data has a "summary" flag column, which lists simply a variable name -- meaning
    # that it is sctually a variable flag column of sorts. it will be easiest if we can
    # unpack this into a new column, for each variable, where the value = 1 if the original
    # flag column named it in a particular observation. one added issue is this "summary" column
    # has potential for mulitple variables at a time.

    # start by making new column for each chemistry variable
    last_col <- colnames(d)[ncol(d)]

    # re-distribute flags to corresponding variable
    d <- d %>%
      dplyr::mutate(
               across(Chemistry_Flag:all_of(!!last_col) & !ends_with("_Lab"),
                      .fns = list(
                        varflag = ~ case_when(grepl(stringr::str_match(cur_column(), "[^.]+"), Chemistry_Flag) ~ 1, TRUE ~ 0)),
                      .names = "{fn}_{col}"))

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date_Time' = "%Y-%m-%d %H:%M:%S"),
                         datetime_tz = 'US/Eastern',
                         site_code_col = 'Sample_Name',
                         data_cols =  sleepers_aq_chem,
                         data_col_pattern = '#V#',
                         # variable specific flag pattern
                         var_flagcol_pattern = 'varflag_#V#',
                         # summary (all vars) flag colun name
                         ## summary_flagcols = 'Chemistry_Flag',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            # will turn the *ms_status* column to 1 (e.g. flagged)
                            variable_flags_dirty   = c(1),

                            # will turn the *ms_status* column to 0 (e.g. clean)
                            variable_flags_clean   = c(0),
                            )

    # manual turn ms_status = 2 for all negative numbers (not in temp, ANC, or isotopes)
    no_bdl_vars = c("GN_temp", "GN_d180", "GN_NO3_d180", "GN_d87Sr_d86Sr", "GN_deuterium",
                  "GN_d13C", "GN_NO3_d15N")

    # apply uncertainty
    d <- ms_check_range(d)
    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')

    # Sleepers metadata states that all negative values are below detection limit, with the
    # value itself being the detection limit for that sample and method
    # give ms_status = 1 to all BDL observations
    d <- d %>%
      mutate(ms_status = case_when(!var %in% no_bdl_vars & val < 0 ~ 1, TRUE ~ ms_status))
    # replace all BDL observations with half DL value
    d <- d %>%
      mutate(val = case_when(!var %in% no_bdl_vars & val < 0 ~ val/2, TRUE ~ val))

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

    unlink(temp_dir, recursive = TRUE)

    return()
}
#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__VERSIONLESS001',
                                           'discharge__VERSIONLESS002',
                                           'discharge__VERSIONLESS003'
                                           ))
    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- stream_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms005 <- derive_precip_pchem_pflux

#CUSTOMprecip_flux_inst: STATUS=READY
#. handle_errors
process_2_ms006 <- function(network, domain, prodname_ms){

    chemprod <- 'precip_chemistry__ms901'
    qprod <- 'CUSTOMprecipitation__VERSIONLESS001'

    chemfiles <- ms_list_files(network = network,
                               domain = domain,
                               prodname_ms = chemprod)

    qfiles <- ms_list_files(network = network,
                            domain = domain,
                            prodname_ms = qprod)

    flux_sites <- base::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        flux <- sw(calc_inst_flux(chemprod = chemprod,
                                  qprod = qprod,
                                  site_code = s))

        if(!is.null(flux)){

            write_ms_file(d = flux,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_code = s,
                          level = 'derived',
                          shapefile = FALSE)
        }
    }
}
