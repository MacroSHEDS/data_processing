#retrieval kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS000 <- function(set_details, network, domain) {

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

    deets_out <- collect_retrieval_details(set_details$url)

    return(deets_out)
}

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

    deets_out <- collect_retrieval_details(set_details$url)

    return(deets_out)
}

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

    deets_out <- collect_retrieval_details(set_details$url)

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

    deets_out <- collect_retrieval_details(set_details$url)

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

    deets_out <- collect_retrieval_details(set_details$url)

    return(deets_out)
}

#munge kernels ####

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS000 <- function(network, domain, prodname_ms, site_code, component) {

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

    # there are many important files in here, but we are only trying to get
    # the raw stream chemistry data
    chem_fp <- file_paths[grepl('Chemistry.csv', file_paths)]

    # the original file has encoding isssues, and connot be read directly
    # here I use a readr function which "guesses" the most likely encodings
    # which is good to know but I ultimately didnt use
    # d_encoding <- readr::guess_encoding(chem_fp)[[1,1]]

    d <- read.csv(chem_fp, check.names = FALSE, fileEncoding = 'latin1')
    d_old_names <- names(d)

    d_new_names <- unname(sapply(d_old_names, function(x) gsub('µ', "u", x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub('<', "", x)))
    colnames(d) <- d_new_names

    # filter to only precipitation site type
    d <- d %>%
        filter(#Sample_Type %in% c("PW"), #xml metadata instruct about how to select a series
               # what do about W-9 big bucket?
               Sample_Name == "R-29 (PPT@W-9)",
               Precip_Type == 'USFS Durham') %>%
        arrange(Precip_Start) %>%
        mutate(Sample_Name = case_when(Sample_Name == "R-29 (PPT@W-9)" ~ "R-29",
                                       TRUE ~ Sample_Name))

    # read this "preprocssed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('Precip_Collect' = "%Y-%m-%d %H:%M:%S"),
                         datetime_tz = 'Etc/GMT-5',
                         site_code_col = 'Sample_Name',
                         data_cols =  c('Precip_Depth_mm' = 'precipitation'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE,
                         sampling_type = 'I',
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    # apply uncertainty
    d <- ms_check_range(d)
    errors(d$val) <- get_hdetlim_or_uncert(d,
                                           detlims = domain_detection_limits,
                                           prodname_ms = prodname_ms,
                                           which_ = 'uncertainty')

    d <- synchronize_timestep(d,
                              admit_NAs = TRUE,
                              paired_p_and_pchem = TRUE,
                              allow_pre_interp = TRUE)

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
                         datetime_cols = c('Date' = '%m/%d/%y'),
                         datetime_tz = 'Etc/GMT-5',
                         site_code_col = 'site_code',
                         data_cols =  c('Precip..mm' = 'precipitation'),
                         data_col_pattern = '#V#',
                         ## summary_flagcols = NA,
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

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
    temp_dir <- file.path(tempdir(), domain)
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)

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
    # d_encoding <- readr::guess_encoding(chem_fp)[[1,1]]

    d <- read.csv(chem_fp, check.names = FALSE, fileEncoding = 'latin1')
    d_old_names <- names(d)

    d_new_names <- unname(sapply(d_old_names, function(x) gsub('µ', "u", x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub('<', "", x)))
    colnames(d) <- d_new_names

    # filter to only precipitation site type
    d <- d %>%
        filter(#Sample_Type %in% c("PW"), #xml metadata instruct about how to select a series
            # what do about W-9 big bucket?
            Sample_Name == "R-29 (PPT@W-9)",
            Precip_Type == 'USFS Durham') %>%
        mutate(Sample_Name = case_when(Sample_Name == "R-29 (PPT@W-9)" ~ "R-29", TRUE ~ Sample_Name))

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here to be just the old_var = new_var structure
    sleepers_aq_chem <- c()

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

    # distribute flags from summary flagcol to variable flagcols
    d <- d %>%
        as_tibble() %>%
        select(-ends_with("_Lab")) %>%
        mutate(across(Chemistry_Flag:last_col(),
                      .fns = list(
                          varflag = ~ case_when(grepl(stringr::str_match(cur_column(), "[^.]+"),
                                                      Chemistry_Flag) ~ 1,
                                                TRUE ~ 0)),
                      .names = "{fn}_{col}"))

    # read this "preprocessed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('Precip_Collect' = "%Y-%m-%d %H:%M:%S"),
                         datetime_tz = 'Etc/GMT-5',
                         site_code_col = 'Sample_Name',
                         data_cols =  sleepers_aq_chem,
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = 'varflag_#V#',
                         is_sensor = FALSE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = '1',
                            variable_flags_clean = '0',
                            keep_empty_rows = TRUE)

    # Sleepers metadata states that all negative values are below detection limit, with the
    # value itself being the detection limit for that sample and method
    no_bdl_vars = c("GN_temp", "GN_d18O", "GN_NO3_d18O", "GN_87Sr_86Sr", "GN_dD",
                  "GN_d13C", "GN_NO3_d15N", 'GN_ANC')

    #extract and record detection limits
    update_sleepers_detlims(d, sleepers_stream_chem_var_info, no_bdl_vars)

    #designate BDL values the way ms_cast_and_reflag normally would
    d$ms_status[! d$var %in% no_bdl_vars & d$val < 0] <- 2
    d$val[! d$var %in% no_bdl_vars & d$val < 0] <- NA

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here as var = old_units and var = new_units lists
    sleepers_aq_chem_units_old = c()
    sleepers_aq_chem_units_new = c()

    for(i in 1:length(sleepers_stream_chem_var_info)) {
      og_name <- names(sleepers_stream_chem_var_info[i])
      og_units <- sleepers_stream_chem_var_info[[i]][1]

      ms_name <- sleepers_stream_chem_var_info[[i]][3]
      ms_units <-sleepers_stream_chem_var_info[[i]][2]

      sleepers_aq_chem_units_old[ms_name] = og_units
      sleepers_aq_chem_units_new[ms_name] = ms_units
    }

    sleepers_aq_chem_units_old <- sleepers_aq_chem_units_old[names(sleepers_aq_chem_units_old) %in% drop_var_prefix(d$var)]
    sleepers_aq_chem_units_new <- sleepers_aq_chem_units_new[names(sleepers_aq_chem_units_new) %in% drop_var_prefix(d$var)]

    d <- ms_conversions(d,
                        convert_units_from = sleepers_aq_chem_units_old,
                        convert_units_to = sleepers_aq_chem_units_new)

    d <- qc_hdetlim_and_uncert(d, prodname_ms)

    d <- synchronize_timestep(d,
                              admit_NAs = TRUE,
                              paired_p_and_pchem = TRUE,
                              allow_pre_interp = TRUE)

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
            !Edit.Code %in% drop_codes)

    # pre-filter edit codes, to retain filtering by original info
    # turn all "good" and "fair" edit coees to 0, and dirty to 1
    d$Edit.Code <- ifelse(d$Edit.Code %in% clean_codes, 0, 1)

    d <- d %>%
        group_by(site_code, date) %>%
        summarise(
            # convert to L/s
            discharge = mean(Q..cfs) * 28.316847,
            Edit.Code = max(Edit.Code)
        )

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('date' = "%Y-%m-%d"),
                         datetime_tz = 'Etc/GMT-5',
                         site_code_col = 'site_code',
                         data_cols =  c("discharge" = "discharge"),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'Edit.Code',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            summary_flags_dirty = list("Edit.Code" = 1),
                            summary_flags_clean = list("Edit.Code" = 0),
                            varflag_col_pattern = NA)

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
    temp_dir <- file.path(tempdir(), domain)
    dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)

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
    # d_encoding <- readr::guess_encoding(chem_fp)[[1,1]]

    d <- read.csv(chem_fp, check.names = FALSE, fileEncoding = 'latin1') %>%
        as_tibble()
    d_old_names <- names(d)

    d_new_names <- unname(sapply(d_old_names, function(x) gsub('µ', "u", x)))
    d_new_names <- unname(sapply(d_new_names, function(x) gsub('<', "", x)))

    colnames(d) <- d_new_names

    sites <- site_data %>%
        filter(network == !!network,
               domain == !!domain,
               site_type != 'rain_gauge') %>%
        pull(site_code)

    d <- filter(d, Sample_Name %in% !!sites)

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

    # distribute flags from summary flagcol to variable flagcols
    d <- d %>%
        select(-ends_with("_Lab")) %>%
        mutate(across(Chemistry_Flag:last_col(),
                      .fns = list(
                          varflag = ~ case_when(grepl(stringr::str_match(cur_column(), "[^.]+"),
                                                      Chemistry_Flag) ~ 1,
                                                TRUE ~ 0)),
                      .names = "{fn}_{col}"))

    #handle summary flag "All cations"
    cation_cols <- c("Al.ug.L", "Alm.ug.L", "Alo.ug.L", "Ba.ug.L", "Ca.ueq.L",
                     "Fe.ug.L", "K.ueq.L", "Li.ug.L", "Mg.ueq.L", "Mn.ug.L",
                     "Na.ueq.L", "NH4.ueq.L")
    d[! is.na(d$Chemistry_Flag) & d$Chemistry_Flag == 'All cations',
      paste('varflag', cation_cols, sep = '_')] <- 1

    # read this "preprocessed tibble" into MacroSheds format using ms_read_raw_csv
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = c('Date_Time' = "%Y-%m-%d %H:%M:%S"),
                         datetime_tz = 'Etc/GMT-5',
                         site_code_col = 'Sample_Name',
                         data_cols =  sleepers_aq_chem,
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = 'varflag_#V#',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_dirty = '1',
                            variable_flags_clean = '0')

    # Sleepers metadata states that all negative values are below detection limit, with the
    # value itself being the detection limit for that sample and method
    no_bdl_vars = c("GN_temp", "GN_d18O", "GN_NO3_d18O", "GN_87Sr_86Sr", "GN_dD",
                    "GN_d13C", "GN_NO3_d15N")

    #extract and record detection limits
    update_sleepers_detlims(d, sleepers_stream_chem_var_info, no_bdl_vars)

    #designate BDL values the way ms_cast_and_reflag normally would
    d$ms_status[! d$var %in% no_bdl_vars & d$val < 0] <- 2
    d$val[! d$var %in% no_bdl_vars & d$val < 0] <- NA

    # the chemistry name and unit data is all in a named list in domain_helpers
    # I am going to re-pack it here as var = old_units and var = new_units lists
    sleepers_aq_chem_units_old = c()
    sleepers_aq_chem_units_new = c()

    for(i in 1:length(sleepers_stream_chem_var_info)) {
        og_name <- names(sleepers_stream_chem_var_info[i])
        og_units <- sleepers_stream_chem_var_info[[i]][1]

        ms_name <- sleepers_stream_chem_var_info[[i]][3]
        ms_units <-sleepers_stream_chem_var_info[[i]][2]

        sleepers_aq_chem_units_old[ms_name] = og_units
        sleepers_aq_chem_units_new[ms_name] = ms_units
    }

    sleepers_aq_chem_units_old <- sleepers_aq_chem_units_old[names(sleepers_aq_chem_units_old) %in% drop_var_prefix(d$var)]
    sleepers_aq_chem_units_new <- sleepers_aq_chem_units_new[names(sleepers_aq_chem_units_new) %in% drop_var_prefix(d$var)]

    d <- ms_conversions(d,
                        convert_units_from = sleepers_aq_chem_units_old,
                        convert_units_to = sleepers_aq_chem_units_new)

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

    unlink(temp_dir, recursive = TRUE)

    return()
}

#derive kernels ####

#usgs_discharge: STATUS=READY
#. handle_errors
process_2_ms007 <- function(network, domain, prodname_ms){

    nwis_codes <- site_data %>%
        filter(network == !!network,
               domain == !!domain,
               site_type == 'stream_gauge') %>%
        select(site_code, colocated_gauge_id) %>%
        mutate(colocated_gauge_id = str_extract(colocated_gauge_id, '[0-9]+')) %>%
        tibble::deframe()

    pull_usgs_discharge(network = network,
                        domain = domain,
                        prodname_ms = prodname_ms,
                        sites = nwis_codes,
                        time_step = rep('daily', length(nwis_codes)))

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__VERSIONLESS003',
                                           'usgs_discharge__ms007'))

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

    flux_sites <- fname_from_fpath(chemfiles, include_fext = FALSE)

    for(s in flux_sites){
        q_ws <- feather::read_feather(qfiles[[1]]) %>%
            mutate(site_code = s)
        feather::write_feather(q_ws, qfiles)

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
