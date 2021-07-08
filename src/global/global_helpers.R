#functions without the "#. handle_errors" decorator have special error handling

handle_errors = function(f){

    #decorator function. takes any function as argument and executes it.
    #if error occurs within passed function, combines error message and
    #pretty callstack as single message, logs and emails this message.
    #returns custom macrosheds (ms) error class.

    #TODO: log unabridged callstack, email pretty one

    wrapper = function(...){

        thisenv = environment()

        tryCatch({
            return_val = f(...)
        }, error=function(e){

            err_cnt_new = err_cnt + 1
            assign('err_cnt', err_cnt_new, pos=.GlobalEnv)

            err_msg = as.character(e)
            if(! err_msg %in% unique_errors){
                unique_errors_new = append(unique_errors, err_msg)
                assign('unique_errors', unique_errors_new, pos=.GlobalEnv)
            }

            pretty_callstack = pprint_callstack()

            if(exists('s')) site_code = s
            if(! exists('site_code')) site_code = 'NO SITE'
            if(! exists('prodname_ms')) prodname_ms = 'NO PRODUCT'

            full_message = glue('{ec}\n\n',
                                'NETWORK: {n}\nDOMAIN: {d}\nSITE: {s}\n',
                                'PRODUCT: {p}\nERROR_MSG: {e}\nMS_CALLSTACK: {c}\n\n_',
                                ec=err_cnt, n=network, d=domain, s=site_code, p=prodname_ms,
                                e=err_msg, c=pretty_callstack)

            logerror(full_message, logger=logger_module)

            email_err_msgs = append(email_err_msgs, full_message)
            assign('email_err_msgs', email_err_msgs, pos=.GlobalEnv)

            assign('return_val', generate_ms_err(full_message), pos=thisenv)

        })

        if(is_ms_exception(return_val)){

            exception_msg = as.character(return_val)

            if(! exception_msg %in% unique_exceptions){
                unique_exceptions_new = append(unique_exceptions, exception_msg)
                assign('unique_exceptions', unique_exceptions_new,
                       pos=.GlobalEnv)
            }

            message(glue('MS Exception: ', exception_msg))
        }

        if(! is.null(return_val)) return(return_val)
    }

    return(wrapper)
}

source('src/global/function_aliases.R')
source('src/global/munge_engines.R')

if(ms_instance$use_ms_error_handling){
    source_decoratees('src/global/munge_engines.R')
}

assign('protected_environment',
       value = new.env(parent = .GlobalEnv),
       envir = .GlobalEnv)
assign('email_err_msgs',
       value = list(),
       envir = .GlobalEnv)
assign('err_cnt',
       value = 0,
       envir = .GlobalEnv)
assign('unique_errors',
       value = c(),
       envir = .GlobalEnv)
assign('unique_exceptions',
       value = c(),
       envir = .GlobalEnv)
assign('typical_derprods',
       value = c('precipitation', 'precip_chemistry', 'stream_flux_inst',
                 'precip_flux_inst', 'precip_pchem_pflux'),
       envir = .GlobalEnv)
assign('canonical_derprods',
       value = c('stream_flux_inst', 'discharge', 'precip_gauge_locations',
                 # 'precipitation', 'precip_chemistry',  'precip_flux_inst',
                 'stream_chemistry', 'stream_gauge_locations', 'ws_boundary'),
       envir = .GlobalEnv)

#exports from an attempt to use socket cluster parallelization;
# idw_pkg_export <- c('logging', 'errors', 'jsonlite', 'plyr',
#                     'tidyverse', 'lubridate', 'feather', 'glue',
#                     'emayili', 'tinsel', 'imputeTS')
# idw_var_export <- c('logger_module', 'err_cnt')

# flag systems (future use?; increasing user value and difficulty to manage):

# system1: binary status (0=chill, 1=unchill)
# system2: 0=chill, 1=unit_unknown, 2=unit_concern, 4=other_concern
# system3: 0=chill, 1=unit_unknown, 2=unit_concern,
#4=method_unknown, 8=method_concern, 16=other_concern
# system4: 0=chill, 1=unit_unknown, 2=unit_concern,
#4=method_unknown, 8=method_concern, 16=sensor_concern, 32=general_concern
flagmap = list(
    clean=c(), #0
    method_unknown=c(), #1; method not given
    method_concern=c(), #2; method known to be lame
    #--- flagsum <= 3: ok
    #--- flagsum > 3: dirty
    sensor_concern=c(), #4; keywords: sensor*, expos*, detect*
    unit_unknown=c(), #8; unit not given
    unit_concern=c(), #16; unit unclear or inconvertible
    general_concern=c() #32; keywords:
)

#ue stands for "unhandle_errors"; wrap this around any ms function
#that is called inside a processing kernel
ue <- function(o){
    if(is_ms_err(o)) stop('The previous error has been unhandled.', call. = FALSE)
    else return(o)
}

pprint_callstack = function(){

    #make callstack more informative. if we end up sourcing files rather than
    #packaging, print which files were sourced along with callstack.

    # zz = as.list(sys.calls())
    # zx[[zxc]] <<- zz
    # zxc <<- zxc + 1
    # call_list = zz
    # # call_list = zx[[1]]

    call_list = as.list(sys.calls())
    call_names = sapply(call_list, all.names, max.names=2)
    ms_entities = grep('pprint', ls(envir=.GlobalEnv), value=TRUE, invert=TRUE)
    ms_calls_bool = sapply(call_names, function(x) any(x %in% ms_entities))
    # call_vec = call_vec[call_vec != 'pprint_callstack']

    # l = length(call_list)
    # call_list_cleaned = call_list[-((l - 4):l)]
    call_list_cleaned = call_list[ms_calls_bool]
    call_vec = unlist(lapply(call_list_cleaned,
                             function(x){
                                 paste(deparse(x), collapse='\n')
                             }))

    call_string_pretty = paste(call_vec, collapse=' -->\n\t')

    return(call_string_pretty)
}

numeric_any <- function(num_vec){
    return(as.numeric(any(as.logical(num_vec))))
}

numeric_any_v <- function(...){ #attack of the ellipses

    #...: numeric vectors of equal length. should be just 0s and 1s, but
    #   integers other than 1 are also considered TRUE by as.logical()

    #the vectorized version of numeric_any. good for stuff like:
    #    mutate(ms_status = numeric_any(c(ms_status_x, ms_status_flow)))

    #returns a single vector of the same length as arguments

    #this func could be useful in global situations
    numeric_any_positional <- function(...) numeric_any(c(...))

    numeric_any_elementwise <- function(...){
        Map(function(...) numeric_any_positional(...), ...)
    }

    out <- do.call(numeric_any_elementwise,
                   args = list(...)) %>%
        unlist()

    if(is.null(out)) out <- numeric()

    return(out)
}

sd_or_0 <- function(x, na.rm = FALSE){

    #Only used to bypass the tyranny of the errors package not letting
    #me take the mean of an errors object of length 1 without setting the
    #uncertainty to 0

    x <- if(is.vector(x) || is.factor(x)) x else as.double(x)

    if(length(x) == 1) return(0)

    x <- sqrt(var(x, na.rm = na.rm))
}

gsub_v <- function(pattern, replacement_vec, x){

    #just like the first three arguments to gsub, except that
    #   replacement is now a vector of replacements.
    #return a vector of the same length as replacement_vec, where
    #   each element in replacement_vec has been used once

    subbed <- sapply(replacement_vec,
                     function(v) gsub(pattern = pattern,
                                      replacement = v,
                                      x = x),
                     USE.NAMES = FALSE)

    return(subbed)
}

identify_sampling <- function(df,
                              is_sensor,
                              date_col = 'datetime',
                              network,
                              domain,
                              prodname_ms,
                              sampling_type){

    #TODO: for hbef, identify_sampling is writing sites names as 1 not w1

    #is_sensor: named logical vector. see documention for
    #   d_raw_csv, but note that an unnamed logical vector of length one
    #   cannot be used here. also note that the original variable/flag column names
    #   from the raw file are converted to canonical macrosheds names by
    #   d_raw_csv before it passes is_sensor to identify_sampling.

    #checks
    if(any(! is.logical(is_sensor))){
        stop('all values in is_sensor must be logical.')
    }

    svh_names <- names(is_sensor)
    if(is.null(svh_names) || any(is.na(svh_names))){
        stop('all elements of is_sensor must be named.')
    }

    #parse is_sensor into a character vector of sample regimen codes
    is_sensor <- ifelse(is_sensor, 'S', 'N')

    #set up directory system to store sample regimen metadata
    sampling_dir <- glue('data/{n}/{d}',
                         n = network,
                         d = domain)

    sampling_file <- glue('data/{n}/{d}/sampling_type.json',
                          n = network,
                          d = domain)

    master <- try(jsonlite::fromJSON(readr::read_file(sampling_file)),
                  silent = TRUE)

    if('try-error' %in% class(master)){
        dir.create(sampling_dir, recursive = TRUE)
        file.create(sampling_file)
        master <- list()
    }

    #determine and record sample regimen for each variable
    col_names <- colnames(df)

    data_cols <- grep(pattern = '__[|]dat',
                      col_names,
                      value = TRUE)

    flg_cols <- grep(pattern = '__[|]flg',
                     col_names,
                     value = TRUE)

    site_codes <- unique(df$site_code)

    for(p in 1:length(data_cols)){

        # var_name <- str_split_fixed(data_cols[p], '__', 2)[1]

        # df_var <- df %>%
        #     select(datetime, !!var_name := .data[[data_cols[p]]], site_code)

        all_sites <- tibble()
        for(i in 1:length(site_codes)){

            # df_site <- df_var %>%
            df_site <- df %>%
                filter(site_code == !!site_codes[i]) %>%
                arrange(datetime)
                    # ! is.na(.data[[date_col]]), #NAs here are indicative of bugs we want to fix, so let's let them through
                    # ! is.na(.data[[var_name]])) #NAs here are indicative of bugs we want to fix, so let's let them through

            dates <- df_site[[date_col]]
            dif <- diff(dates)
            unit <- attr(dif, 'units')

            conver <- case_when(
                unit %in% c('seconds', 'secs') ~ 0.01666667,
                unit %in% c('minutes', 'mins') ~ 1,
                unit == 'hours' ~ 60,
                unit == 'days' ~ 1440,
                TRUE ~ NA_real_)

            if(is.na(conver)) stop('Weird time unit encountered. address this.')

            dif_minutes <- as.numeric(dif) * conver

            # table <- rle2(dif_minutes) %>% #table is a commonly used function
            run_table <- rle2(dif_minutes) %>%
                mutate(
                    site_code = !!site_codes[i],
                    starts := dates[starts],
                    stops := dates[stops],
                    # sum = sum(lengths, na.rm = TRUE), #superfluous
                    porportion = lengths / sum(lengths, na.rm = TRUE),
                    time = difftime(stops, starts, units = 'days'))

            # Sites with no record
            if(nrow(run_table) == 0){

                g_a <- tibble('site_code' = site_codes[i],
                              'type' = 'G',
                              'starts' = lubridate::NA_POSIXct_,
                              'interval' = NA_real_)

            } else {

                test <- filter(run_table,
                               time > 60,
                               lengths > 60)

                #Sites where there are not at least 60 consecutive records, record
                #for at least 60 consecutive days, and have an average interval of
                #more than 1 day are assumed to be grab samples
                if(nrow(test) == 0 && mean(run_table$values, na.rm = TRUE) > 1440){

                    g_a <- tibble('site_code' = site_codes[i],
                                  'type' = 'G',
                                  'starts' = min(run_table$starts,
                                                 na.rm = TRUE),
                                  'interval' = round(Mode(run_table$values,
                                                          na.rm = TRUE)))
                }

                #Sites with consecutive samples are have a consistent interval
                if(nrow(test) != 0 && nrow(run_table) <= 20){

                    g_a <- test %>%
                        select(site_code, starts, interval = values) %>%
                        group_by(site_code, interval) %>%
                        summarise(starts = min(starts,
                                               na.rm = TRUE)) %>%
                        mutate(type = 'I') %>%
                        arrange(starts)

                }

                #Sites where they do not have a consistent recording interval but
                #the average interval is less than one day are assumed to be automatic
                # (such as HBEF discharge that is automatic but lacks a consistent
                #recording interval)
                if(
                    (nrow(test) == 0 && mean(run_table$values, na.rm = TRUE) <= 1440) ||
                    (nrow(test) != 0 && nrow(run_table) > 20)
                ){ #could this be handed with else?

                    table_ <- run_table %>%
                        filter(porportion >= 0.05) %>%
                        mutate(type = 'I') %>%
                        select(starts, site_code, type, interval = values) %>%
                        mutate(interval = as.character(round(interval)))

                    table_var <- run_table %>%
                        filter(porportion <= 0.05)  %>%
                        group_by(site_code) %>%
                        summarise(starts = min(starts,
                                               na.rm = TRUE)) %>%
                        mutate(
                            type = 'I',
                            interval = 'variable') %>%
                        select(starts, site_code, type, interval)

                    g_a <- rbind(table_, table_var) %>%
                        arrange(starts)
                }
            }

            if(! is.null(sampling_type)){

                g_a <- g_a %>%
                    mutate(type = sampling_type)
            }

            var_name_base <- str_split(string = data_cols[p],
                                       pattern = '__\\|')[[1]][1]

            interval_changes <- rle2(g_a$interval)$starts

            g_a <- g_a %>%
                mutate(
                    type = paste0(type,
                                  !!is_sensor[var_name_base]),
                    var = as.character(glue('{ty}_{vb}',
                                            ty = type,
                                            vb = var_name_base))) %>%
                slice(interval_changes)

            master[[prodname_ms]][[var_name_base]][[site_codes[i]]] <-
                list('startdt' = g_a$starts,
                     'type' = g_a$type,
                     'interval' = g_a$interval)

            g_a <- g_a %>%
                mutate(interval = as.character(interval))

            all_sites <- bind_rows(all_sites, g_a)
        }

        #include new prefixes in df column names
        prefixed_varname <- all_sites$var[1]

        dat_colname <- paste0(drop_var_prefix(prefixed_varname),
                              '__|dat')
        flg_colname <- paste0(drop_var_prefix(prefixed_varname),
                              '__|flg')

        data_col_ind <- match(dat_colname,
                              colnames(df))
        flag_col_ind <- match(flg_colname,
                              colnames(df))

        colnames(df)[data_col_ind] <- paste0(prefixed_varname,
                                             '__|dat')
        colnames(df)[flag_col_ind] <- paste0(prefixed_varname,
                                             '__|flg')
    }

    readr::write_file(jsonlite::toJSON(master), sampling_file)

    return(df)
}

identify_sampling_bypass <- function(df,
                              is_sensor,
                              date_col = 'datetime',
                              network,
                              domain,
                              prodname_ms,
                              sampling_type = NULL){

    #This case is used (primarily for neon) when use of d_raw and
    # ms_cast_flag are not used because of incaomptable data structures

    #checks
    if(!is.logical(is_sensor)){
        stop('is_sensor must be logical.')
    }

    #parse is_sensor into a character vector of sample regimen codes
    is_sensor <- ifelse(is_sensor, 'S', 'N')

    #set up directory system to store sample regimen metadata
    sampling_dir <- glue('data/{n}/{d}',
                         n = network,
                         d = domain)

    sampling_file <- glue('data/{n}/{d}/sampling_type.json',
                          n = network,
                          d = domain)

    if(file.exists(sampling_file)){
        master <- jsonlite::fromJSON(readr::read_file(sampling_file))
    } else {
        dir.create(sampling_dir, recursive = TRUE)
        file.create(sampling_file)
        master <- list()
    }

    site_codes <- unique(df$site_code)

    variables <- unique(df$var)

    all_vars <- tibble()
    for(p in 1:length(variables)){
    #for(p in 1:43){

        all_sites <- tibble()
        for(i in 1:length(site_codes)){

            # df_site <- df_var %>%
            df_site <- df %>%
                filter(site_code == !!site_codes[i]) %>%
                filter(var == !!variables[p]) %>%
                arrange(datetime)
            # ! is.na(.data[[date_col]]), #NAs here are indicative of bugs we want to fix, so let's let them through
            # ! is.na(.data[[var_name]])) #NAs here are indicative of bugs we want to fix, so let's let them through

            dates <- df_site[[date_col]]
            dif <- diff(dates)
            unit <- attr(dif, 'units')

            conver <- case_when(
                unit %in% c('seconds', 'secs') ~ 0.01666667,
                unit %in% c('minutes', 'mins') ~ 1,
                unit == 'hours' ~ 60,
                unit == 'days' ~ 1440,
                TRUE ~ NA_real_)

            if(is.na(conver)) stop('Weird time unit encountered. address this.')

            dif_minutes <- as.numeric(dif) * conver

            # table <- rle2(dif_minutes) %>% #table is a commonly used function
            run_table <- rle2(dif_minutes) %>%
                mutate(
                    site_code = !!site_codes[i],
                    starts := dates[starts],
                    stops := dates[stops],
                    # sum = sum(lengths, na.rm = TRUE), #superfluous
                    porportion = lengths / sum(lengths, na.rm = TRUE),
                    time = difftime(stops, starts, units = 'days'))

            # Sites with no record
            if(nrow(run_table) == 0){

                g_a <- tibble('site_code' = site_codes[i],
                              'type' = 'G',
                              'starts' = lubridate::NA_POSIXct_,
                              'interval' = NA_real_)

            } else {

                test <- filter(run_table,
                               time > 60,
                               lengths > 60)

                #Sites where there are not at least 60 consecutive records, record
                #for at least 60 consecutive days, and have an average interval of
                #more than 1 day are assumed to be grab samples
                if(nrow(test) == 0 && mean(run_table$values, na.rm = TRUE) > 1440){

                    g_a <- tibble('site_code' = site_codes[i],
                                  'type' = 'G',
                                  'starts' = min(run_table$starts,
                                                 na.rm = TRUE),
                                  'interval' = round(Mode(run_table$values,
                                                          na.rm = TRUE)))
                }

                #Sites with consecutive samples are have a consistent interval
                if(nrow(test) != 0 && nrow(run_table) <= 20){

                    g_a <- test %>%
                        select(site_code, starts, interval = values) %>%
                        group_by(site_code, interval) %>%
                        summarise(starts = min(starts,
                                               na.rm = TRUE)) %>%
                        mutate(type = 'I') %>%
                        arrange(starts)

                }

                #Sites where they do not have a consistent recording interval but
                #the average interval is less than one day are assumed to be automatic
                # (such as HBEF discharge that is automatic but lacks a consistent
                #recording interval)
                if(
                    (nrow(test) == 0 && mean(run_table$values, na.rm = TRUE) <= 1440) ||
                    (nrow(test) != 0 && nrow(run_table) > 20)
                ){ #could this be handed with else?

                    table_ <- run_table %>%
                        filter(porportion >= 0.05) %>%
                        mutate(type = 'I') %>%
                        select(starts, site_code, type, interval = values) %>%
                        mutate(interval = as.character(round(interval)))

                    table_var <- run_table %>%
                        filter(porportion <= 0.05)  %>%
                        group_by(site_code) %>%
                        summarise(starts = min(starts,
                                               na.rm = TRUE)) %>%
                        mutate(
                            type = 'I',
                            interval = 'variable') %>%
                        select(starts, site_code, type, interval)

                    g_a <- rbind(table_, table_var) %>%
                        arrange(starts)
                }
            }

            interval_changes <- rle2(g_a$interval)$starts

            if(! is.null(sampling_type)){

                g_a <- g_a %>%
                    mutate(type = sampling_type)
            }

            g_a <- g_a %>%
                mutate(
                    type = paste0(type,
                                  !!is_sensor),
                    new_var = as.character(glue('{ty}_{vb}',
                               ty = type,
                               vb = variables[p])),
                    var = variables[p]) %>%
                slice(interval_changes)

            master[[prodname_ms]][[variables[p]]][[site_codes[i]]] <-
                list('startdt' = g_a$starts,
                     'type' = g_a$type,
                     'interval' = g_a$interval)

            all_sites <- rbind(all_sites, g_a)
        }
        all_vars <- rbind(all_sites, all_vars)# %>%
            #distinct(var, .keep_all = TRUE)
    }

    correct_names <- all_vars %>%
        select(site_code, new_var, var) %>%
        group_by(site_code, var) %>%
        summarise(new_var = first(new_var)) %>%
        ungroup()

    df <- left_join(df, correct_names, by = c("site_code", "var")) %>%
        select(datetime, site_code, var=new_var, val, ms_status)

    readr::write_file(jsonlite::toJSON(master), sampling_file)

    return(df)
}

drop_var_prefix <- function(x){

    unprefixed <- substr(x, 4, nchar(x))

    return(unprefixed)
}

extract_var_prefix <- function(x){

    prefix <- substr(x, 1, 2)

    return(prefix)
}

ms_read_raw_csv <- function(filepath,
                            preprocessed_tibble,
                            datetime_cols,
                            datetime_tz,
                            optionalize_nontoken_characters = ':',
                            site_code_col,
                            alt_site_code,
                            data_cols,
                            data_col_pattern,
                            alt_datacol_pattern,
                            is_sensor,
                            set_to_NA,
                            var_flagcol_pattern,
                            alt_varflagcol_pattern,
                            summary_flagcols,
                            sampling_type = NULL){

    #TODO:
    #add a silent = TRUE option. this would hide all warnings
    #allow a vector of possible matches for each element of datetime_cols
    #   (i.e. make it take a list of named vectors)
    #write more checks for improper specification.
    #if file to be read is stored in long format, this function will not work!
    #this could easily be adapted to read other delimited filetypes.
    #could also add a drop_empty_rows and/or drop_empty_datacols parameter.
    #   atm those things happen automatically
    #likewise, a remove_duplicates param could be nice. atm, for duplicated rows,
    #   the one with the fewest NA values is kept automatically
    #allow a third option in is_sensor for mixed sensor/nonsensor
    #   (also change param name). check for comments inside munge kernels
    #   indicating where this is needed
    #site_code_col should eventually work like datetime_cols (in case site_code is
    #   separated into multiple components)

    #filepath: string
    #preprocessed_tibble: a tibble with all character columns. Supply this
    #   argument if a dataset requires modification before it can be processed
    #   by d_raw_csv. This may be necessary if, e.g.
    #   time is stored in a format that can't be parsed by standard datetime
    #   format strings. Either filepath or preprocessed_tibble
    #   must be supplied, but not both.
    #datetime_cols: a named character vector. names are column names that
    #   contain components of a datetime. values are format strings (e.g.
    #   '%Y-%m-%d', '%H') corresponding to the datetime components in those
    #   columns.
    #datetime_tz: string specifying time zone. this specification must be
    #   among those provided by OlsonNames()
    #optionalize_nontoken_characters: character vector; used when there might be
    #   variation in date/time formatting within a column. in regex speak,
    #   optionalizing a token string means, "match this string if it exists,
    #   but move on to the next token if it doesn't." All datetime parsing tokens
    #   (like "%H") are optionalized automatically when this function converts
    #   them to regex. But other tokens like ":" and "-" that might be used in
    #   datetime strings are not. Concretely, if you wanted to read either "%H:%M:%S"
    #   or "%H:%M" in the same column, you'd set optionalize_nontoken_characters = ':',
    #   and then the parser wouldn't require there to be two colons in order to
    #   match the string. Don't use this if you don't have to, because it reduces
    #   specificity. See "optional" argument to dt_format_to_regex for more details.
    #site_code_col: name of column containing site name information
    #alt_site_code: optional list. Names of list elements are desired site_codes
    #   within MacroSheds. List elements are character vectors of alternative
    #   names that might be encountered. Used when sites are misnamed or need
    #   to be changed due to inconsistencies within and across datasets.
    #data_cols: vector of names of columns containing data. If elements of this
    #   vector are named, names are taken to be the column names as they exist
    #   in the file, and values are used to replace those names. Data columns that
    #   aren't referred to in this argument will be omitted from the output,
    #   as will their associated flag columns (if any).
    #data_col_pattern: a string containing the wildcard "#V#",
    #   which represents any number of characters. If data column names will be
    #   used as-is, this wildcard is all you need. if data columns contain
    #   recurring, superfluous characters, you can omit them with regex. for
    #   example, if data columns are named outflow_x, outflow_y, outflow_...., use
    #   data_col_pattern = 'outflow_#V#' and then you don't have to bother
    #   typing the full names in your argument to data_cols.
    #alt_datacol_pattern: optional string with same mechanics as
    #   data_col_pattern. use this if there
    #   might be a second way in which column names are generated, e.g.
    #   output_x, output_y, output_....
    #is_sensor: either a single logical value, which will be applied to all
    #   variable columns OR a named logical vector with the same length and names as
    #   data_cols. If the latter, names correspond to variable names in the file to be read.
    #   TRUE means the corresponding variable(s) was/were
    #   measured with a sensor (which may be susceptible to drift and/or fouling),
    #   FALSE means the measurement(s) was/were not recorded by a sensor. This
    #   category includes analytical measurement in a lab, visual recording, etc.
    #set_to_NA: For values such as 9999 that are proxies for NA values.
    #var_flagcol_pattern: optional string with same mechanics as the other
    #   pattern parameters. this one is for columns containing flag
    #   information that is specific to one variable. If there's only one
    #   data column, omit this argument and use summary_flagcols for all
    #   flag information.
    #alt_varflagcol_pattern: optional string with same mechanics as the other
    #   pattern parameters. just in case there are two naming conventions for
    #   variable-specific flag columns
    #summary_flagcols: optional unnamed vector of column names for flag columns
    #   that pertain to all variables
    #sampling_type: optional value to overwrite identify_sampling because in
    #   some case this function is misidentifying sampling type. This must be a
    #   single value of G or I and is applied to all variables in product

    #return value: a tibble of ordered and renamed columns, omitting any columns
    #   from the original file that do not contain data, flag/qaqc information,
    #   datetime, or site_code. All-NA data columns and their corresponding
    #   flag columns will also be omitted, as will rows where all data values
    #   are NA. Rows with NA in the datetime or site_code column are dropped.
    #   data columns are given type double. all other
    #   columns are given type character. data and flag/qaqc columns are
    #   given two-letter prefixes representing sample regimen
    #   (I = installed vs. G = grab; S = sensor vs N = non-sensor).
    #   Data and flag/qaqc columns are also given
    #   suffixes (__|flg and __|dat) that allow them to be cast into long format
    #   by ms_cast_and_reflag. d_raw_csv does not parse datetimes.

    #checks
    filepath_supplied <-  ! missing(filepath) && ! is.null(filepath)
    tibble_supplied <-  ! missing(preprocessed_tibble) && ! is.null(preprocessed_tibble)

     if(filepath_supplied && tibble_supplied){
        stop(glue('Only one of filepath and preprocessed_tibble can be supplied. ',
                  'preprocessed_tibble is for rare circumstances only.'))
    }


    if(! datetime_tz %in% OlsonNames()){
        stop('datetime_tz must be included in OlsonNames()')
    }

    if(length(data_cols) == 1 &&
       ! (missing(var_flagcol_pattern) || is.null(var_flagcol_pattern))){
        stop(paste0('Only one data column. Use summary_flagcols instead ',
                    'of var_flagcol_pattern.'))
    }

    if(any(! is.logical(is_sensor))){
        stop('all values in is_sensor must be logical.')
    }

    svh_names <- names(is_sensor)
    if(
        length(is_sensor) != 1 &&
        (is.null(svh_names) || any(is.na(svh_names)))
    ){
        stop('if is_sensor is not length 1, all elements must be named.')
    }

    if(! all(data_cols %in% ms_vars$variable_code)) {

        for(i in 1:length(data_cols)) {
            if(!data_cols[i] %in% ms_vars$variable_code) {
                logerror(msg = paste(unname(data_cols[i]), 'is not in varibles.csv; add'),
                        logger = logger_module)
            }
        }
    }

    if(! is.null(sampling_type)){
        if(! length(sampling_type) == 1){
            stop('sampling_type must be a length of 1')
        }
        if(! sampling_type %in% c('G', 'I')){
            stop('sampling_type must be either I or G')
        }
    }

    #parse args; deal with missing args
    datetime_colnames <- names(datetime_cols)
    datetime_formats <- unname(datetime_cols)

    alt_datacols <- var_flagcols <- alt_varflagcols <- NA
    alt_datacol_names <- var_flagcol_names <- alt_varflagcol_names <- NA
    if(missing(summary_flagcols)){
        summary_flagcols <- NULL
    }

    if(missing(set_to_NA)) {
        set_to_NA <- NULL
    }

    if(missing(alt_site_code)) {
        alt_site_code <- NULL
    }

    #fill in missing names in data_cols (for columns that are already
    #   canonically named)
    datacol_names0 <- names(data_cols)
    if(is.null(datacol_names0)) datacol_names0 <- rep('', length(data_cols))
    datacol_names0[datacol_names0 == ''] <-
        unname(data_cols[datacol_names0 == ''])

    #expand data columnname wildcards and rename data_cols
    datacol_names <- gsub_v(pattern = '#V#',
                            replacement_vec = datacol_names0,
                            x = data_col_pattern)
    names(data_cols) <- datacol_names

    #expand alternative data columnname wildcards and populate alt_datacols
    if(! missing(alt_datacol_pattern) && ! is.null(alt_datacol_pattern)){
        alt_datacols <- data_cols
        alt_datacol_names <- gsub_v(pattern = '#V#',
                                    replacement_vec = datacol_names0,
                                    x = alt_datacol_pattern)
        names(alt_datacols) <- alt_datacol_names
    }

    #expand varflag columnname wildcards and populate var_flagcols
    if(! missing(var_flagcol_pattern) && ! is.null(var_flagcol_pattern)){
        var_flagcols <- data_cols
        var_flagcol_names <- gsub_v(pattern = '#V#',
                                    replacement_vec = datacol_names0,
                                    x = var_flagcol_pattern)
        names(var_flagcols) <- var_flagcol_names
    }

    #expand alt varflag columnname wildcards and populate alt_varflagcols
    if(! missing(alt_varflagcol_pattern) && ! is.null(alt_varflagcol_pattern)){
        alt_varflagcols <- data_cols
        alt_varflagcol_names <- gsub_v(pattern = '#V#',
                                       replacement_vec = datacol_names0,
                                       x = alt_varflagcol_pattern)
        names(alt_varflagcols) <- alt_varflagcol_names
    }

    #combine all available column name mappings; assemble new name vector
    colnames_all <- c(data_cols, alt_datacols, var_flagcols, alt_varflagcols)
    na_inds <- is.na(colnames_all)
    colnames_all <- colnames_all[! na_inds]

    suffixes <- rep(c('__|dat', '__|dat', '__|flg', '__|flg'),
                    times = c(length(data_cols),
                              length(alt_datacols),
                              length(var_flagcols),
                              length(alt_varflagcols)))

    suffixes <- suffixes[! na_inds]
    colnames_new <- paste0(colnames_all, suffixes)

    colnames_all <- c(datetime_colnames, colnames_all)
    names(colnames_all)[1:length(datetime_cols)] <- datetime_colnames
    colnames_new <- c(datetime_colnames, colnames_new)

    if(! missing(site_code_col) && ! is.null(site_code_col)){
        colnames_all <- c('site_code', colnames_all)
        names(colnames_all)[1] <- site_code_col
        colnames_new <- c('site_code', colnames_new)
    }

    if(! is.null(summary_flagcols)){
        nsumcol <- length(summary_flagcols)
        summary_flagcols_named <- summary_flagcols
        names(summary_flagcols_named) <- summary_flagcols
        colnames_all <- c(colnames_all, summary_flagcols_named)
        colnames_new <- c(colnames_new, summary_flagcols)
    }

    #assemble colClasses argument to read.csv
    classes_d1 <- rep('numeric', length(data_cols))
    names(classes_d1) <- datacol_names

    classes_d2 <- rep('numeric', length(alt_datacols))
    names(classes_d2) <- alt_datacol_names

    classes_f1 <- rep('character', length(var_flagcols))
    names(classes_f1) <- var_flagcol_names

    classes_f2 <- rep('character', length(alt_varflagcols))
    names(classes_f2) <- alt_varflagcol_names

    classes_f3 <- rep('character', length(summary_flagcols))
    names(classes_f3) <- summary_flagcols

    class_dt <- rep('character', length(datetime_cols))
    names(class_dt) <- datetime_colnames

    if(! missing(site_code_col) && ! is.null(site_code_col)){
        class_sn <- 'character'
        names(class_sn) <- site_code_col
    }

    classes_all <- c(class_dt, class_sn, classes_d1, classes_d2, classes_f1,
                     classes_f2, classes_f3)
    classes_all <- classes_all[! is.na(names(classes_all))]

    if(filepath_supplied){
        d <- read.csv(filepath,
                      stringsAsFactors = FALSE,
                      colClasses = "character")
    } else {
        d <- mutate(preprocessed_tibble,
               across(everything(), as.character))
    }

    d <- d %>%
        as_tibble() %>%
        select(one_of(c(names(colnames_all), 'NA.'))) #for NA as in sodium
    if('NA.' %in% colnames(d)) class(d$NA.) = 'character'

    # Remove any variable flags created by pattern but do not exist in data
    # colnames_all <- colnames_all[names(colnames_all) %in% names(d)]
    # classes_all <- classes_all[names(classes_all) %in% names(d)]

    # Set values to NA if used as a flag or missing data indication
    # Not sure why %in% does not work, seem to only operate on one row
    if(! is.null(set_to_NA)){
        for(i in 1:length(set_to_NA)){
            d[d == set_to_NA[i]] <- NA
        }
    }

    #Set correct class for each column
    colnames_d <- colnames(d)

    for(i in 1:ncol(d)){

        if(colnames_d[i] == 'NA.'){
            class(d[[i]]) <- 'numeric'
            next
        }

        class(d[[i]]) <- classes_all[names(classes_all) == colnames_d[i]]
    }
    # d[] <- sw(Map(`class<-`, d, classes_all)) #sometimes classes_all is too long, which makes this fail

    #rename cols to canonical names
    for(i in 1:ncol(d)){

        if(colnames_d[i] == 'NA.'){
            colnames_d[i] <- 'Na__|dat'
            next
        }

        canonical_name_ind <- names(colnames_all) == colnames_d[i]
        if(any(canonical_name_ind)){
            colnames_d[i] <- colnames_new[canonical_name_ind]
        }
    }

    colnames(d) <- colnames_d

    #resolve datetime structure into POSIXct
    d  <- resolve_datetime(d = d,
                           datetime_colnames = datetime_colnames,
                           datetime_formats = datetime_formats,
                           datetime_tz = datetime_tz,
                           optional = optionalize_nontoken_characters)

    #remove rows with NA in datetime or site_code
    d <- filter(d,
                across(any_of(c('datetime', 'site_code')),
                       ~ ! is.na(.x)))

    #remove all-NA data columns and rows with NA in all data columns.
    #also remove flag columns for all-NA data columns.
    all_na_cols_bool <- apply(select(d, ends_with('__|dat')),
                              MARGIN = 2,
                              function(x) all(is.na(x)))
    all_na_cols <- names(all_na_cols_bool[all_na_cols_bool])
    all_na_cols <- c(all_na_cols,
                     sub(pattern = '__\\|dat',
                         replacement = '__|flg',
                         all_na_cols))

    d <- d %>%
        select(-one_of(all_na_cols)) %>%
        filter_at(vars(ends_with('__|dat')),
                  any_vars(! is.na(.)))

    #for duplicated datetime-site_code pairs, keep the row with the fewest NA
    #   values. We could instead do something more sophisticated.
    d <- d %>%
        rowwise(one_of(c('datetime', 'site_code'))) %>%
        mutate(NAsum = sum(is.na(c_across(ends_with('__|dat'))))) %>%
        ungroup() %>%
        arrange(datetime, site_code, NAsum) %>%
        select(-NAsum) %>%
        distinct(datetime, site_code, .keep_all = TRUE) %>%
        arrange(site_code, datetime)

    #convert NaNs to NAs, just in case.
    d[is.na(d)] <- NA

    #either assemble or reorder is_sensor to match names in data_cols
    if(length(is_sensor) == 1){

        is_sensor <- rep(is_sensor,
                         length(data_cols))
        names(is_sensor) <- unname(data_cols)

    } else {

        data_col_order <- match(names(is_sensor),
                                names(data_cols))
        is_sensor <- is_sensor[data_col_order]
    }

    #fix sites names if multiple names refer to the same site
    if(! is.null(alt_site_code)){

        for(z in 1:length(alt_site_code)){

            d <- mutate(d,
                        site_code = ifelse(site_code %in% !!alt_site_code[[z]],
                                           !!names(alt_site_code)[z],
                                           site_code))
        }
    }

    #prepend two-letter code to each variable representing sample regimen and
    #record sample regimen metadata
    d <- sm(identify_sampling(df = d,
                              is_sensor = is_sensor,
                              domain = domain,
                              network = network,
                              prodname_ms = prodname_ms,
                              sampling_type = sampling_type))

    #Check if all sites are in site file
    if(!all(unique(d$site_code) %in% site_data$site_code)) {

        for(i in 1:length(unique(d$site_code))) {
            if(!unique(d$site_code)[i] %in% site_data$site_code) {
                logwarn(msg = paste(unname(unique(d$site_code)[i]),
                                    'is not in site_data file; add'),
                        logger = logger_module)
            }
        }
    }

    return(d)
}

resolve_datetime <- function(d,
                             datetime_colnames,
                             datetime_formats,
                             datetime_tz,
                             optional){

    #d: a data.frame or tibble with at least one date or time column
    #   (all date and/or time columns must contain character strings,
    #   not parsed date/time/datetime objects).
    #datetime_colnames: character vector; column names that contain
    #   relevant datetime information.
    #datetime_formats: character vector; datetime parsing tokens
    #   (like '%A, %Y-%m-%d %I:%M:%S %p' or '%j') corresponding to the
    #   elements of datetime_colnames.
    #datetime_tz: character; time zone of the returned datetime column.
    #optional: character vector; see dt_format_to_regex.

    #return value: d, but with a "datetime" column containing POSIXct datetimes
    #   and without the input datetime columns

    dt_tb <- tibble(basecol = rep(NA, nrow(d)))
    for(i in 1:length(datetime_colnames)){

        dt_comps <- str_match_all(string = datetime_formats[i],
                                  pattern = '%([a-zA-Z])')[[1]][,2]
        dt_regex <- dt_format_to_regex(datetime_formats[i],
                                       optional = optional)

        dt_tb <- d %>%
            select(one_of(datetime_colnames[i])) %>%
            tidyr::extract(col = !!datetime_colnames[i],
                           into = dt_comps,
                           regex = dt_regex,
                           remove = TRUE,
                           convert = FALSE) %>%
            bind_cols(dt_tb)
    }

    dt_tb$basecol = NULL

    #fill in defaults if applicable:
    #(12 for hour, 00 for minute and second, PM for AM/PM)
    dt_tb <- dt_tb %>%
        mutate(
            across(any_of(c('H', 'I')), ~ifelse(is.na(.x), '12', .x)),
            across(any_of(c('M', 'S')), ~ifelse(is.na(.x), '00', .x)),
            across(any_of('p'), ~ifelse(is.na(.x), 'PM', .x)))

    #resolve datetime structure into POSIXct
    datetime_formats_split <- stringr::str_extract_all(datetime_formats,
                                                       '%[a-zA-Z]') %>%
        unlist()

    dt_col_order <- match(paste0('%',
                                 colnames(dt_tb)),
                          datetime_formats_split)

    if('H' %in% colnames(dt_tb)){
        dt_tb$H[dt_tb$H == ''] <- '00'
    }
    if('M' %in% colnames(dt_tb)){
        dt_tb$M[dt_tb$M == ''] <- '00'
    }
    if('S' %in% colnames(dt_tb)){
        dt_tb$S[dt_tb$S == ''] <- '00'
    }
    if('I' %in% colnames(dt_tb)){
        dt_tb$I[dt_tb$I == ''] <- '00'
    }
    if('P' %in% colnames(dt_tb)){
        dt_tb$P[dt_tb$P == ''] <- 'AM'
    }

    dt_tb <- dt_tb %>%
        tidyr::unite(col = 'datetime',
                     everything(),
                     sep = ' ',
                     remove = TRUE) %>%
        mutate(datetime = as_datetime(datetime,
                                      format = paste(datetime_formats_split[dt_col_order],
                                                     collapse = ' '),
                                      tz = datetime_tz) %>%
                   with_tz(tz = 'UTC'))

    d <- d %>%
        bind_cols(dt_tb) %>%
        select(-one_of(datetime_colnames), datetime) %>%#in case 'datetime' is in datetime_colnames
        relocate(datetime)

    return(d)
}

dt_format_to_regex <- function(fmt, optional){

    #fmt is a character vector of datetime formatting strings, such as
    #   '%A, %Y-%m-%d %I:%M:%S %p' or '%j'. each element of fmt that is a
    #   datetime token is replaced with a regex string that matches
    #   what the token represents. For example, '%Y' matches a 4-digit
    #   year and '[0-9]{4}' matches a 4-digit numeric sequence. non-token
    #   characters (anything not following a %) are not modified. Note that
    #   tokens B, b, h, A, and a are replaced by '[a-zA-Z]+', which matches
    #   any sequence of one or more alphabetic characters of either case,
    #   not just meaningful month/day names 'Weds' or 'january'. Also note
    #   that these tokens are not currently accepted: g, G, n, t, c, r, R, T.
    #optional is a vector of characters that should be made
    #   optional in the exported regex (followed by a '?'). This is useful if
    #   e.g. fmt is '%H:%M:%S' and elements to be matched may either appear in
    #   HH:MM:SS or HH:MM format. making the ":" character optional here
    #   (via optional = ':') allows the hour and minute data to be retained,
    #   whereas the regex engine would otherwise expect two ":"s, find
    #   only one, and return NA.

    dt_format_regex <- list(Y = '([0-9]{4})?',
                            y = '([0-9]{2})?',
                            B = '([a-zA-Z]+)?',
                            b = '([a-zA-Z]+)?',
                            h = '([a-zA-Z]+)?',
                            m = '([0-9]{1,2})?',
                            e = '([0-9]{1,2})?',
                            d = '([0-9]{2})?',
                            j = '([0-9]{3})?',
                            A = '([a-zA-Z]+)?',
                            a = '([a-zA-Z]+)?',
                            u = '([0-9]{1})?',
                            w = '([0-9]{1})?',
                            U = '([0-9]{2})?',
                            W = '([0-9]{2})?',
                            V = '([0-9]{2})?',
                            C = '([0-9]{2})?',
                            H = '([0-9]{1,2})?',
                            I = '([0-9]{1,2})?',
                            M = '([0-9]{1,2})?',
                            S = '([0-9]{1,2})?',
                            p = '([AP]M)?',
                            z = '([+\\-][0-9]{4})?',
                            `F` = '([0-9]{4}-[0-9]{2}-[0-9]{2})')

    for(i in 1:length(fmt)){
        fmt_components <- str_match_all(string = fmt[i],
                                        pattern = '%([a-zA-Z])')[[1]][,2]

        if(any(fmt_components %in% c('g', 'G', 'n', 't', 'c'))){
            stop(paste('Tokens g, G, n, and t are not yet accepted.',
                       'enhance dt_format_to_regex if you want to use them!'))
        }
        if(any(fmt_components %in% c('r', 'R', 'T'))){
            stop('Tokens r, R, and T are not accepted. Use a different specification.')
        }

        for(j in 1:length(fmt_components)){
            component <- fmt_components[j]
            fmt[i] <- sub(pattern = paste0('%', component),
                          replacement = dt_format_regex[[component]],
                          x = fmt[i])
        }
    }

    if(! missing(optional)){
        for(o in optional){
            fmt <- gsub(pattern = o,
                        replacement = paste0(o, '?'),
                        x = fmt)
        }
    }

    return(fmt)
}

escape_special_regex <- function(x){

    #x is a character vector. any special characters in x will be escaped with
    #   a double backslash, e.g. "air.pressure.kpa" will become
    #   "air\\.pressure\\.kpa"

    #this function currently only escapes "." and "|", because they're the
    #   special regex characters that can appear in column names.

    special_regex_colchars <- c('.', '|')
    special_regex <- paste0('([\\',
                            paste(special_regex_colchars,
                                  collapse = '\\'),
                            '])')

    escaped <- gsub(pattern = special_regex,
                    replacement = '\\\\\\1',
                    x,
                    perl = TRUE)

    return(escaped)
}

ms_cast_and_reflag <- function(d,
                               input_shape = 'wide',
                               data_col_pattern = '#V#__|dat',
                               varflag_col_pattern = '#V#__|flg',
                               variable_flags_to_drop,
                               variable_flags_clean,
                               variable_flags_dirty,
                               summary_flags_to_drop,
                               summary_flags_clean,
                               summary_flags_dirty){

    #TODO: add a silent = TRUE option. this would hide all warnings
    #allow for alternative pattern specifications.

    #d is a df/tibble with ONLY a site_code column, a datetime column,
    #   flag and/or status columns, and data columns. There must be no
    #   columns with grouping data, variable names, units, methods, etc.
    #   Data columns must be suffixed identically. Variable flag columns
    #   must be suffixed identically and differently from data columns.
    #   If d was generated by d_raw_csv, it will be good to go.
    #input_shape is the format ("wide"/"long") of d
    #   (currently only "wide" supported).
    #data_col_pattern: a string containing the wildcard "#V#",
    #   which represents any number of characters, and the suffix that pertains
    #   to data columns (currently this must be '#V#__|dat'.
    #varflag_col_pattern: a string containing the wildcard "#V#",
    #   which represents any number of characters, and the suffix that pertains
    #   to variable flag/status columns (currently this must be '#V#__|flg'.
    #   Or set this to NA if there are no variable-specific flag/status columns.
    #variable_flags_to_drop: a character vector of values that might appear in
    #   the variable flag columns. Elements of this vector are treated as
    #   bad data and are removed. Use '#*#' to refer to all values not
    #   included in variable_flags_clean. This parameter is optional,
    #   though at least 2 of variable_flags_to_drop, variable_flags_clean,
    #   and variable_flags_dirty must be supplied if varflag_col_pattern is not
    #   set to NA.
    #   If '#*#' is used, variable_flags_clean must be supplied.
    #variable_flags_clean: a character vector of values that might appear in
    #   the variable flag columns. Elements of this vector are given an
    #   ms_status of 0, meaning clean. This parameter is optional, though at least 2
    #   of variable_flags_to_drop, variable_flags_clean, and variable_flags_dirty
    #   must be supplied if varflag_col_pattern is not
    #   set to NA. This parameter does not use the '#*#' wildcard.
    #variable_flags_dirty: a character vector of values that might appear in
    #   the variable flag columns. Elements of this vector are given an
    #   ms_status of 1, meaning dirty. This parameter is optional, though at least 2
    #   of variable_flags_to_drop, variable_flags_clean, and variable_flags_dirty
    #   must be supplied if varflag_col_pattern is not
    #   set to NA. This parameter does not use the '#*#' wildcard.
    #summary_flags_to_drop: a named list. names correspond to columns in d that
    #   contain summary flag/status information. List elements must be character vectors
    #   of values that might appear in
    #   the summary flag/status columns. Elements of these vectors are treated as
    #   bad data and are removed. Use '#*#' to refer to all values not
    #   included in summary_flags_clean. This parameter is optional, though
    #   if there are summary flag columns, at least 2
    #   of summary_flags_to_drop, summary_flags_clean, and summary_flags_dirty
    #   must be supplied (omit this argument otherwise).
    #   If '#*#' is used, summary_flags_clean must be supplied.
    #   make sure list elements for summary flags are in the same order!
    #   there is currently no check for this.
    #summary_flags_clean: a named list. names correspond to columns in d that
    #   contain summary flag/status information. List elements must be character vectors
    #   of values that might appear in the summary flag/status columns.
    #   Elements of these vectors are given an ms_status of 0, meaning clean.
    #   This parameter is optional, though
    #   if there are summary flag columns, at least 2 of summary_flags_to_drop,
    #   summary_flags_clean, and summary_flags_dirty must be supplied
    #   (omit this argument otherwise).
    #   make sure list elements for summary flags are in the same order!
    #   there is currently no check for this.
    #   Note: This parameter does not use the '#*#' wildcard.
    #summary_flags_dirty: a named list. names correspond to columns in d that
    #   contain summary flag/status information. List elements must be character vectors
    #   of values that might appear in the summary flag/status columns.
    #   Elements of these vectors are given an ms_status of 1, meaning dirty.
    #   This parameter is optional, though
    #   if there are summary flag columns, at least 2 of summary_flags_to_drop,
    #   summary_flags_clean, and summary_flags_dirty must be supplied
    #   (omit this argument otherwise).
    #   make sure list elements for summary flags are in the same order!
    #   there is currently no check for this.
    #   Note: This parameter does not use the '#*#' wildcard.

    #return value: a long-format tibble with 5 columns: datetime, site_code,
    #   var, val, ms_status. Rows with NA in any column are removed.

    #arg checks
    if(! input_shape == 'wide'){
        stop('ms_cast_and_reflag only implemented for input_shape = "wide"')
    }

    sumdrop <- ! missing(summary_flags_to_drop) && ! is.null(summary_flags_to_drop)
    sumclen <- ! missing(summary_flags_clean) && ! is.null(summary_flags_clean)
    sumdirt <- ! missing(summary_flags_dirty) && ! is.null(summary_flags_dirty)
    no_sumflags <- all(c(sumdrop, sumclen, sumdirt) == FALSE)

    if(sum(c(sumdrop, sumclen, sumdirt)) == 1){
        stop(paste0('Must supply 2 (or none) of summary_flags_to_drop, ',
                    'summary_flags_clean, summary_flags_dirty'))
    }

    if(sumclen){
        if(any(sapply(summary_flags_clean, function(x) '#*#' %in% x))){
            stop(glue('the #*# wildcard may only be used in ',
                      'summary_flags_to_drop and variable_flags_to_drop'))
        }
    }
    if(sumdirt){
        if(any(sapply(summary_flags_dirty, function(x) '#*#' %in% x))){
            stop(glue('the #*# wildcard may only be used in ',
                      'summary_flags_to_drop and variable_flags_to_drop'))
        }
    }

    if(sumdrop){

        drop_wildcard_bool <- sapply(summary_flags_to_drop, function(x) '#*#' %in% x)
        if(any(drop_wildcard_bool) && ! sumclen){
            stop(glue('if #*# wildcard is used in summary_flags_to_drop, ',
                      'summary_flags_clean must be supplied'))
        }

        drop_multicode_bool <- sapply(summary_flags_to_drop, function(x) length(x) > 1)
        if(any(drop_wildcard_bool & drop_multicode_bool)){
            stop(glue('if #*# wildcard is supplied as part of summary_flags_to_drop,',
                      ' it must be in a character vector of length 1'))
        }
    }

    vardrop <- ! missing(variable_flags_to_drop) && ! is.null(variable_flags_to_drop)
    varclen <- ! missing(variable_flags_clean) && ! is.null(variable_flags_clean)
    vardirt <- ! missing(variable_flags_dirty) && ! is.null(variable_flags_dirty)
    no_varflags <- is.na(varflag_col_pattern)

    if(sum(c(vardrop, varclen, vardirt)) < 2 && ! no_varflags){
        stop(paste0('Must supply at least 2 of variable_flags_to_drop, ',
                    'variable_flags_clean, variable_flags_dirty (or set ',
                    'varflag_col_pattern = NA)'))
    }

    if(vardrop){

        if('#*#' %in% variable_flags_to_drop && length(variable_flags_to_drop) > 1){
            stop(glue('if #*# wildcard is used in variable_flags_to_drop,',
                      ' it must be the only element in its argument vector'))
        }

        if(variable_flags_to_drop == '#*#' && ! varclen){
            stop(glue('if #*# wildcard is used in variable_flags_to_drop, ',
                      'variable_flags_clean must be supplied'))
        }
    }

    if(! no_sumflags){
        if(sumdrop){
            summary_colnames <- names(summary_flags_to_drop)
        } else {
            summary_colnames <- names(summary_flags_clean)
        }
    } else {
        summary_colnames <- NULL
    }

    data_col_keyword <- gsub(pattern = '#V#',
                             replacement = '',
                             data_col_pattern)

    varflag_keyword <- gsub(pattern = '#V#',
                            replacement = '',
                            varflag_col_pattern)

    #cast to long format (would have to auto-generate names_pattern regex
    #   to allow for data_col_pattern and varflag_col_pattern to vary) if
    #   there's more than one data column. otherwise just remove data column
    #   suffix.
    ndatacols <- sum(grepl(escape_special_regex(data_col_keyword),
                           colnames(d)))
    if(ndatacols > 1){

        if(no_varflags){
            d <- pivot_longer(data = d,
                              cols = ends_with(data_col_keyword),
                              names_pattern = '^(.+?)__\\|(dat)$',
                              names_to = c('var', '.value'))
        } else {
            d <- pivot_longer(data = d,
                              cols = ends_with(c(data_col_keyword, varflag_keyword)),
                              names_pattern = '^(.+?)__\\|(dat|flg)$',
                              names_to = c('var', '.value'))
        }

    } else {

        data_ind <- grep(pattern = escape_special_regex(data_col_keyword),
             x = colnames(d))

        varname  <- gsub(pattern = escape_special_regex(data_col_keyword),
                         replacement = '',
                         x = colnames(d)[data_ind])

        colnames(d)[data_ind] <- 'dat'
        d$var <- varname
    }

    #remove rows with NA in the value column (these take up space and can be
    #reconstructed by casting to wide form
    d <- filter(d, ! is.na(dat))

    #filter rows with summary flags indicating bad data (data to drop)
    if(! no_sumflags){
        if(sumdrop){
            for(i in 1:length(summary_flags_to_drop)){

                smtd <- summary_flags_to_drop[i]

                if(length(smtd[[1]]) == 1 && smtd[[1]] == '#*#'){
                    d <- filter(d, (!!sym(names(smtd))) %in%
                                    summary_flags_clean[[i]])
                } else {
                    d <- filter(d, ! (!!sym(names(smtd))) %in% smtd)
                }
            }

        } else {

            for(i in 1:length(summary_flags_clean)){
                d <- filter(d, (!!sym(names(summary_flags_clean)[i])) %in%
                                c(summary_flags_clean[[i]],
                                  summary_flags_dirty[[i]]))
            }
        }
    }

    #filter rows with variable flags indicating bad data (data to drop)
    if(! no_varflags){
        if(vardrop){

            if(variable_flags_to_drop == '#*#'){
                d <- filter(d, flg %in% variable_flags_clean)
            } else {
                d <- filter(d, ! flg %in% variable_flags_to_drop)
            }

        } else {
            d <- filter(d, flg %in% c(variable_flags_clean, variable_flags_dirty))
        }
    }

    #binarize remaining flag information (0 = clean, 1 = dirty)
    if(! no_varflags){
        if(varclen){
            d <- mutate(d, ms_status = case_when(
                flg %in% variable_flags_clean ~ 0,
                TRUE ~ 1))
        } else {
            d <- mutate(d, ms_status = case_when(
                flg %in% variable_flags_dirty ~ 1,
                TRUE ~ 0))
        }
    } else {
        d$ms_status <- 0
    }

    if(! no_sumflags){
        if(sumclen){
            for(i in 1:length(summary_flags_clean)){
                si <- summary_flags_clean[i]
                flg_bool <- ! d[[names(si)]] %in% si[[1]]
                d$ms_status[flg_bool] <- 1
            }
        } else {
            for(i in 1:length(summary_flags_dirty)){
                si <- summary_flags_dirty[i]
                flg_bool <- d[[names(si)]] %in% si[[1]]
                d$ms_status[flg_bool] <- 1
            }
        }
    }

    #rearrange columns (this also would have to be flexified if we ever want
    #   to pass something other than the default for data_col_pattern or
    #   varflag_col_pattern
    d <- d %>%
        select(-one_of(c(summary_colnames, 'flg'))) %>%
        select(datetime, site_code, var, dat, ms_status) %>%
        rename(val = dat) %>%
        arrange(site_code, var, datetime)

    return(d)
}

ms_conversions <- function(d,
                           keep_molecular,
                           convert_units_from,
                           convert_units_to){

    #d: a macrosheds tibble that has aready been through ms_cast_and_reflag
    #keep_molecular: a character vector of molecular formulae to be
    #   left alone. Otherwise these formulae: NO3, SO4, PO4, SiO2, NH4, NH3, NO3_NO2
    #   will be converted according to the atomic masses of their main
    #   constituents. For example, NO3 should be converted to NO3-N within
    #   macrosheds, but passing 'NO3' to keep_molecular will leave it as NO3.
    #   The only time you'd want to do this is when a domain provides both
    #   forms. In that case we would process both forms separately, converting
    #   neither.
    #convert_units_from: a named character vector. Names are variable names
    #   without their sample-regimen prefixes (e.g. 'DIC', not 'GN_DIC'),
    #   and values are the units of those variables. Omit variables that don't
    #   need to be converted.
    #convert_units_to: a named character vector. Names are variable names
    #   without their sample-regimen prefixes (e.g. 'DIC', not 'GN_DIC'),
    #   and values are the units those variables should be converted to.
    #   Omit variables that don't need to be converted.

    #checks
    # cm <- ! missing(convert_molecules)
    cuF <- ! missing(convert_units_from) && ! is.null(convert_units_from)
    cuT <- ! missing(convert_units_to) && ! is.null(convert_units_to)

    if(sum(cuF, cuT) == 1){
        stop('convert_units_from and convert_units_to must be supplied together')
    }
    if(length(convert_units_from) != length(convert_units_to)){
        stop('convert_units_from and convert_units_to must have the same length')
    }
    cu_shared_names <- base::intersect(names(convert_units_from),
                                       names(convert_units_to))
    if(length(cu_shared_names) != length(convert_units_to)){
        stop('names of convert_units_from and convert_units_to must match')
    }

    vars <- drop_var_prefix(d$var)

    convert_molecules <- c('NO3', 'SO4', 'PO4', 'SiO2', 'SiO3', 'NH4', 'NH3',
                           'NO3_NO2')

    if(! missing(keep_molecular)){
        if(any(! keep_molecular %in% convert_molecules)){
            stop(glue('keep_molecular must be a subset of {cm}',
                      cm = paste(convert_molecules,
                                 collapse = ', ')))
        }
        convert_molecules <- convert_molecules[! convert_molecules %in% keep_molecular]
    }

    convert_molecules <- convert_molecules[convert_molecules %in% unique(vars)]

    molecular_conversion_map <- list(
        NH4 = 'N',
        NO3 = 'N',
        NH3 = 'N',
        SiO2 = 'Si',
        SiO3 = 'Si',
        SO4 = 'S',
        PO4 = 'P',
        NO3_NO2 = 'N')

    # if(cm){
    #     if(! all(convert_molecules %in% names(molecular_conversion_map))){
    #         miss <- convert_molecules[! convert_molecules %in%
    #                                       names(molecular_conversion_map)]
    #         stop(glue('These molecules either need to be added to ',
    #                   'molecular_conversion_map, or they should not be converted: ',
    #                   paste(miss, collapse = ', ')))
    #     }
    # }

    #handle molecular conversions, like NO3 -> NO3_N

    for(v in convert_molecules){

        d$val[vars == v] <- convert_molecule(x = d$val[vars == v],
                                             from = v,
                                             to = unname(molecular_conversion_map[v]))

        check_double <- str_split_fixed(unname(molecular_conversion_map[v]), '', n = Inf)[1,]

        if(length(check_double) > 1 && length(unique(check_double)) == 1) {
            molecular_conversion_map[v] <- unique(check_double)
        }

        new_name <- paste0(d$var[vars == v], '_', unname(molecular_conversion_map[v]))

        d$var[vars == v] <- new_name
    }

    # Converts input to grams if the final unit contains grams
    for(i in 1:length(convert_units_from)){

        unitfrom <- convert_units_from[i]
        unitto <- convert_units_to[i]
        v <- names(unitfrom)

        g_conver <- FALSE
        if(grepl('mol|eq', unitfrom) && grepl('g', unitto) ||
           v %in% convert_molecules){

            d$val[vars == v] <- convert_to_gl(x = d$val[vars == v],
                                              input_unit = unitfrom,
                                              molecule = v)

            g_conver <- TRUE
        }

        #convert prefix
        d$val[vars == v] <- convert_unit(x = d$val[vars == v],
                                         input_unit = unitfrom,
                                         output_unit = unitto)

        #Convert to mol or eq if that is the output unit
        if(grepl('mol|eq', unitto)) {

            d$val[vars == v] <- convert_from_gl(x = d$val[vars == v],
                                                input_unit = unitfrom,
                                                output_unit = unitto,
                                                molecule = v,
                                                g_conver = g_conver)
        }
    }

    return(d)
}

query_status <- function(status_code_vec, component = 'flag'){

    #TODO: investigate r packages for bitmapping in C*/FORTRAN

    #currently, status code integers have three digits. The first digit
    #represents qa/qc flags. 0 means unflagged and 1 means flagged. The
    #second digit is for datapoints that have been interpolated by macrosheds --
    #0 means original and 1 means interpolated. The third digit is for sensor (0)
    #versus grab (1) data.

    if(! component %in% c('flag', 'interp', 'regimen')){
        stop('component must be one of "flag", "interp", "regimen"')
    }

    #if we add status codes, they'll get tacked on to the right side of the
    #status code integer, and the following conditional will need to be updated.
    #just add the condition at the end, make it yield pos = 1, and increment the
    #positions yielded by the other conditions by 1.
    pos <- case_when(
        component == 'flag' ~ 3,
        component == 'interp' ~ 2,
        component == 'regimen' ~ 1)
    #component == 'new component' ~ 1

    #convert "binary" int to decimal int
    dec <- strtoi(as.character(status_code_vec), base = 2L)

    #get bit of interest as a decimal representation of a one-hot binary integer
    onehot <- bitwShiftL(1, (pos - 1))

    #if bit of interest is 0 in the status code, bitwise AND with the onehot
    #   will yield zero. if the bit of interest is 1, result will be nonzero
    bit_is_on <- bitwAnd(dec, onehot) != 0

    return(bit_is_on)
}

export_to_global <- function(from_env, exclude=NULL){

    #exclude is a character vector of names not to export.
    #unmatched names will be ignored.

    #vars could also be passed individually and handled by ...
    # vars = list(...)
    # varnames = all.vars(match.call())

    varnames = ls(name=from_env)
    varnames = varnames[! varnames %in% exclude]
    vars = mget(varnames, envir=from_env)

    for(i in 1:length(varnames)){
        assign(varnames[i], vars[[i]], .GlobalEnv)
    }

    #return()
}

get_all_local_helpers <- function(network, domain){

    #source_decoratees reads in decorator functions (tinsel package).
    #because it can only read them into the current environment, all files
    #sourced by this function are exported locally, then exported globally

    location1 = glue('src/{n}/network_helpers.R', n=network)
    if(file.exists(location1)){

        sw(source(location1, local=TRUE))

        if(ms_instance$use_ms_error_handling){
            sw(source_decoratees(location1))
        }
    }

    location2 = glue('src/{n}/{d}/domain_helpers.R', n=network, d=domain)
    if(file.exists(location2)){

        sw(source(location2, local=TRUE))

        if(ms_instance$use_ms_error_handling){
            sw(source_decoratees(location2))
        }
    }

    location3 = glue('src/{n}/processing_kernels.R', n=network)
    if(file.exists(location3)){

        sw(source(location3, local=TRUE))

        if(ms_instance$use_ms_error_handling){
            sw(source_decoratees(location3))
        }
    }

    location4 = glue('src/{n}/{d}/processing_kernels.R', n=network, d=domain)
    if(file.exists(location4)){

        sw(source(location4, local=TRUE))

        if(ms_instance$use_ms_error_handling){
            sw(source_decoratees(location4))
        }
    }

    rm(location1, location2, location3, location4)

    export_to_global(from_env=environment(),
                     exclude=c('network', 'domain', 'thisenv'))

    #return()
}

set_up_logger <- function(network = domain, domain){

    #the logging package establishes logger hierarchy based on name.
    #our root logger is named "ms", and our network-domain loggers are named
    #ms.network.domain, e.g. "ms.lter.hbef". When messages are logged, loggers
    #are referred to as name.module. A message logged to
    #logger="ms.lter.hbef.module" would be handled by loggers named
    #"ms.lter.hbef", "ms.lter", and "ms", some of which may not have established
    #handlers

    logger_name <- glue('ms.{n}.{d}',
                       n = network,
                       d = domain)

    logger_module <- glue(logger_name,
                          '.module')

    if(! dir.exists('logs')){
        dir.create('logs',
                   showWarnings = FALSE)
    }

    logging::addHandler(handler = logging::writeToFile,
                        logger = logger_name,
                        file = glue('logs/{n}_{d}.log',
                                    n = network,
                                    d = domain))

    return(logger_module)
}

extract_from_config <- function(key){
    ind = which(lapply(conf, function(x) grepl(key, x)) == TRUE)
    val = stringr::str_match(conf[ind], '.*\\"(.*)\\"')[2]
    return(val)
}

clear_from_mem <- function(..., clearlist){

    if(missing(clearlist)){
        dots = match.call(expand.dots = FALSE)$...
        clearlist = vapply(dots, as.character, '')
    }

    rm(list=clearlist, envir=.GlobalEnv)
    gc()

    #return()
}

retain_ms_globals <- function(retain_vars){

    all_globals = ls(envir=.GlobalEnv, all.names=TRUE)
    clutter = all_globals[! all_globals %in% retain_vars]

    clear_from_mem(clearlist=clutter)

    #return()
}

generate_ms_err = function(text=1){
    errobj = text
    class(errobj) = 'ms_err'
    return(errobj)
}

generate_ms_exception = function(text=1){
    excobj = text
    class(excobj) = 'ms_exception'
    return(excobj)
}

generate_blacklist_indicator = function(text=1){
    indobj = text
    class(indobj) = 'blacklist_indicator'
    return(indobj)
}

is_ms_err <- function(x){
    return('ms_err' %in% class(x))
}

is_ms_exception <- function(x){
    return('ms_exception' %in% class(x))
}

is_blacklist_indicator <- function(x){
    return('blacklist_indicator' %in% class(x))
}

evaluate_result_status <- function(r){

    if(is_ms_err(r) || is_ms_exception(r)){
        status <- 'error'
    } else if(is_blacklist_indicator(r)){
        status <- 'blacklist'
    } else {
        status <- 'ok'
    }

    return(status)
}

email_err <- function(msgs, addrs, pw){

    if(is.list(msgs)){
        msgs = Reduce(function(x, y) paste(x, y, sep='\n---\n'), msgs)
    }

    text_body = glue('Error list:\n\n', msgs, '\n\nEnd of errors')

    mailout = tryCatch({

        for(a in addrs){

            email = emayili::envelope() %>%
                envelope::from('grdouser@gmail.com') %>%
                envelope::to(a) %>%
                envelope::subject('MacroSheds error') %>%
                envelope::text(text_body)

            smtp = envelope::server(host='smtp.gmail.com',
                                    port=587, #or 465 for SMTPS
                                    username='grdouser@gmail.com',
                                    password=pw)

            smtp(email, verbose=FALSE)
        }

    }, error=function(e){

        #not sure if class "error" is always returned by tryCatch,
        #so creating custom class
        errout = 'err'
        class(errout) = 'err'
        return(errout)
    })

    if('err' %in% class(mailout)){
        msg = 'Something bogus happened in email_err'
        logging::logerror(msg, logger=logger_module)
        return('email fail')
    } else {
        return('email success')
    }

}

get_data_tracker <- function(network = domain, domain){

    #network is an optional macrosheds network name string. If omitted, it's
    #assumed to be identical to the domain string.
    #domain is a macrosheds domain string

    thisenv = environment()

    tryCatch({

        tracker_data = glue('data/{n}/{d}/data_tracker.json',
                            n=network, d=domain) %>%
            readr::read_file() %>%
            jsonlite::fromJSON()

    }, error=function(e){
        assign('tracker_data', list(), pos=thisenv)
    })

    return(tracker_data)
}

make_tracker_skeleton <- function(retrieval_chunks,
                                  versionless){

    #retrieval_chunks is a vector of identifiers for subsets (chunks) of
    #the overall dataset to be retrieved, e.g. sitemonths for NEON

    munge_derive_skeleton <- list(status = 'pending',
                                  mtime = '1500-01-01')

    tracker_skeleton <- list(
        retrieve = tibble::tibble(
            component = retrieval_chunks,
            mtime = '1500-01-01',
            held_version = ifelse(versionless, '1500-01-01', '-1'),
            status = 'pending'),
        munge = munge_derive_skeleton,
        derive = munge_derive_skeleton)

    return(tracker_skeleton)
}

insert_site_skeleton <- function(tracker,
                                 prodname_ms,
                                 site_code,
                                 site_components,
                                 versionless = FALSE){

    #if versionless is TRUE, held_version will be populated with
    #"1500-01-01" as a placeholder value,
    #because the modification date stands in for the version when
    #we're dealing with versionless products. otherwise, held_version is given
    #a placeholder of -1

    tracker[[prodname_ms]][[site_code]] <-
        make_tracker_skeleton(retrieval_chunks = site_components,
                              versionless = versionless)

    return(tracker)
}

product_is_tracked <- function(tracker, prodname_ms){
    bool = prodname_ms %in% names(tracker)
    return(bool)
}

site_is_tracked <- function(tracker, prodname_ms, site_code){
    bool = site_code %in% names(tracker[[prodname_ms]])
    return(bool)
}

track_new_product <- function(tracker, prodname_ms){

    if(prodname_ms %in% names(tracker)){
        logwarn('This product is already being tracked.', logger=logger_module)
        return(tracker)
    }

    tracker[[prodname_ms]] = list()
    return(tracker)
}

track_new_site_components <- function(tracker, prodname_ms, site_code, avail){

    retrieval_tracker <- tracker[[prodname_ms]][[site_code]]$retrieve

    new_avail <- avail %>%
        filter(! component %in% retrieval_tracker$component) %>%
        select(component) %>%
        mutate(mtime = '1900-01-01',
               held_version = '-1',
               status = 'pending')

    if(! all(retrieval_tracker$component %in% avail$component)){

        obsolete_components <- retrieval_tracker %>%
            filter(! component %in% avail$component) %>%
            pull(component)

        logwarn(msg = glue("Tracked component(s) {tc} no longer available. ",
                           "Removing from tracker.",
                           tc = paste(obsolete_components,
                                      collapse = ', ')),
                logger = logger_module)

        retrieval_tracker <- filter(retrieval_tracker,
                                    component %in% avail$component)
    }

    retrieval_tracker <- new_avail %>%
        bind_rows(retrieval_tracker) %>%
        arrange(component)

    tracker[[prodname_ms]][[site_code]]$retrieve <- retrieval_tracker

    return(tracker)
}

filter_unneeded_sets <- function(tracker_with_details){

    new_sets = tracker_with_details %>%
        filter(status != 'blacklist' | is.na(status)) %>%
        filter(needed == TRUE | is.na(needed))

    if(any(is.na(new_sets$needed))){
        msg = paste0('Must run `track_new_site_components` and ',
                     '`populate_set_details` before running `populate_set_details`')
        logerror(msg, logger=logger_module)
        stop(msg)
    }

    return(new_sets)
}

update_data_tracker_r <- function(network = domain,
                                  domain,
                                  tracker = NULL,
                                  tracker_name = NULL,
                                  set_details = NULL,
                                  new_status = NULL){

    #this updates the retrieve section of a data tracker in memory and on disk.
    #see update_data_tracker_m for the munge section and update_data_tracker_d
    #for the derive section

    #if tracker is supplied, it will be used to write/overwrite the one on disk.
    #if it is omitted or set to NULL, the appropriate tracker will be loaded
    #from disk, updated, and then written back to disk. tracker_name, set_details,
    #and new_status must be supplied if tracker is not.

    if(is.null(tracker) && (
        is.null(tracker_name) || is.null(set_details) || is.null(new_status)
    )){

        msg <- paste0('If tracker is not supplied, these args must be:',
                     'tracker_name, set_details, new_status.')

        logerror(msg,
                 logger = logger_module)
        stop(msg)
    }

    if(is.null(tracker)){

        tracker <- get_data_tracker(network = network,
                                    domain = domain)

        rt <- tracker[[set_details$prodname_ms]][[set_details$site_code]]$retrieve

        set_ind <- which(rt$component == set_details$component)

        if(new_status %in% c('pending', 'ok')){

            if('avail_version' %in% names(set_details)){
                rt$held_version[set_ind] <- as.character(set_details$avail_version)
            } else {
                rt$held_version[set_ind] <- as.character(set_details$last_mod_dt)
            }
        }

        rt$status[set_ind] <- new_status
        rt$mtime[set_ind] <- as.character(Sys.time())

        tracker[[set_details$prodname_ms]][[set_details$site_code]]$retrieve <- rt

        assign(x = tracker_name,
               value = tracker,
               pos = .GlobalEnv)
    }

    trackerdir <- glue('data/{n}/{d}',
                       n = network,
                       d = domain)

    if(! dir.exists(trackerdir)){

        dir.create(trackerdir,
                   showWarnings = FALSE,
                   recursive = TRUE)
    }

    trackerfile <- glue(trackerdir,
                        '/data_tracker.json')

    readr::write_file(x = jsonlite::toJSON(tracker),
                      file = trackerfile)
    backup_tracker(trackerfile)

    #return()
}

update_data_tracker_m <- function(network = domain,
                                  domain,
                                  tracker_name,
                                  prodname_ms,
                                  site_code,
                                  new_status){

    #this updates the munge section of a data tracker in memory and on disk.
    #see update_data_tracker_r for the retrieval section and
    #update_data_tracker_d for the derive section

    # #OBSOLETE? in addition, if new_status == 'ok', this function looks for the word
    # #"linked" in the "derive_status"
    # #column of the products.csv file for the corresponding network, domain,
    # #and linkprod row, and changes it to NA it if found.
    # #that way, any new versions of munged products will be re-linked to derive/.
    #
    # if(new_status == 'ok'){
    #
    #     prodfile <- glue('src/{n}/{d}/products.csv',
    #                      n = network,
    #                      d = domain)
    #
    #     prods <- sm(read_csv(prodfile))
    #
    #     # prodcode <- prodcode_from_prodname_ms(prodname_ms)
    #     prodname <- prodname_from_prodname_ms(prodname_ms)
    #     prodcode <- prods %>%
    #         filter(prodname == !!prodname,
    #                grepl('ms[0-9]{3}', prodcode)) %>%
    #         pull(prodcode)
    #
    #     rind <- which(prods$prodcode == prodcode & prods$prodname == prodname)
    #     sts <- prods$derive_status[rind]
    #
    #     for(i in seq_along(rind)){
    #
    #         if(! is.na(sts[i]) && sts[i] == 'linked'){
    #             prods$derive_status[rind[i]] <- NA_character_
    #         }
    #     }
    #
    #     write_csv(x = prods,
    #               file = prodfile)
    # }

    tracker <- get_data_tracker(network = network,
                                domain = domain)

    mt <- tracker[[prodname_ms]][[site_code]]$munge

    mt$status <- new_status
    mt$mtime <- as.character(Sys.time())

    tracker[[prodname_ms]][[site_code]]$munge <- mt

    assign(x = tracker_name,
           value = tracker,
           pos = .GlobalEnv)

    trackerdir <- glue('data/{n}/{d}',
                       n = network,
                       d = domain)

    if(! dir.exists(trackerdir)){
        dir.create(trackerdir,
                   showWarnings = FALSE,
                   recursive = TRUE)
    }

    trackerfile <- glue(trackerdir, '/data_tracker.json')
    readr::write_file(jsonlite::toJSON(tracker), trackerfile)
    backup_tracker(trackerfile)
}

update_data_tracker_d <- function(network = domain,
                                  domain,
                                  tracker = NULL,
                                  tracker_name = NULL,
                                  prodname_ms = NULL,
                                  site_code = NULL,
                                  new_status = NULL){

    #this updates the derive section of a data tracker in memory and on disk.
    #see update_data_tracker_r for the retrieval section and
    #update_data_tracker_m for the munge section

    #if tracker is supplied, it will be used to write/overwrite the one on disk.
    #if it is omitted or set to NULL, the appropriate tracker will be loaded
    #from disk, updated, and then written back to disk. tracker_name, set_details,
    #and new_status must be supplied if tracker is not.

    if(is.null(tracker) && (
        is.null(tracker_name) || is.null(prodname_ms) ||
        is.null(new_status) || is.null(site_code)
    )){
        msg = paste0('If tracker is not supplied, these args must be:',
                     'tracker_name, prodname_ms, new_status, new_status.')
        logerror(msg, logger=logger_module)
        stop(msg)
    }

    if(is.null(tracker)){

        tracker <- get_data_tracker(network = network,
                                    domain = domain)

        dt <- tracker[[prodname_ms]][[site_code]]$derive

        if(is.null(dt)){
            msg <- 'Derived product not yet tracked; not updating derive tracker.'
            logging::logwarn(msg)
            return(generate_ms_exception(msg))
        }

        dt$status <- new_status
        dt$mtime <- as.character(Sys.time())
        tracker[[prodname_ms]][[site_code]]$derive <- dt

        assign(x = tracker_name,
               value = tracker,
               pos = .GlobalEnv)
    }

    trackerdir <- glue('data/{n}/{d}',
                       n = network,
                       d = domain)

    if(! dir.exists(trackerdir)){
        dir.create(trackerdir,
                   showWarnings = FALSE,
                   recursive = TRUE)
    }

    trackerfile <- glue(trackerdir, '/data_tracker.json')
    readr::write_file(jsonlite::toJSON(tracker), trackerfile)
    backup_tracker(trackerfile)
}

update_data_tracker_g <- function(network = domain,
                                  domain,
                                  tracker,
                                  prodname_ms,
                                  site_code,
                                  new_status){

    #this updates the general section of a data tracker in memory and on disk.

    #see update_data_tracker_r for the retrieval section,
    #update_data_tracker_m for the munge section, and
    #update_data_tracker_d for the derive section

    dt <- tracker[[prodname_ms]][[site_code]]$general

    if(is.null(dt)){
        return(generate_ms_exception('Product not yet tracked; no action taken.'))
    }

    dt$status <- new_status
    dt$mtime <- as.character(Sys.time())

    tracker[[prodname_ms]][[site_code]]$general <- dt

    assign(x = 'held_data',
           value = tracker,
           pos = .GlobalEnv)

    trackerfile <- glue('data/{n}/{d}/data_tracker.json',
                        n = network,
                        d = domain)

    readr::write_file(jsonlite::toJSON(tracker), trackerfile)
    backup_tracker(trackerfile)
}

backup_tracker <- function(path,
                           force = FALSE){

    #force: logical. if FALSE, new tracker backup will only be written once per
    #   hour. If TRUE, it will be written regardless (up to once per second)

    mch <- stringr::str_match(path,
                              '(data/.+?/.+?)/(data_tracker.json)')[, 2:3]

    if(any(is.na(mch))){
        stop('Invalid tracker path or name')
    }

    dir.create(glue(mch[1], '/tracker_backups'),
               recursive = TRUE,
               showWarnings = FALSE)

    time_format <- ifelse(force, '%Y%m%dT%H%M%SZ', '%Y%m%dT%HZ')

    tstamp <- Sys.time() %>%
        with_tz(tzone = 'UTC') %>%
        format(time_format)

    newpath <- glue('{p}/tracker_backups/{f}_{t}',
                    p = mch[1],
                    f = mch[2],
                    t = tstamp)

    file.copy(from = path,
              to = newpath,
              overwrite = FALSE)

    #remove tracker backups older than 7 days
    system2('find', c(glue(mch[1], '/tracker_backups/*'),
                      '-mtime', '+7', '-exec', 'rm', '{}', '\\;'))
}

extract_retrieval_log <- function(tracker, prodname_ms, site_code,
                                  keep_status='ok'){

    retrieved_data = tracker[[prodname_ms]][[site_code]]$retrieve %>%
        tibble::as_tibble() %>%
        filter(status == keep_status)

    return(retrieved_data)
}

get_munge_status <- function(tracker, prodname_ms, site_code){
    munge_status = tracker[[prodname_ms]][[site_code]]$munge$status
    return(munge_status)
}

get_derive_status <- function(tracker, prodname_ms, site_code){
    derive_status = tracker[[prodname_ms]][[site_code]]$derive$status
    return(derive_status)
}

get_general_status <- function(tracker, prodname_ms, site_code){
    general_status = tracker[[prodname_ms]][[site_code]]$general$status
    return(general_status)
}

get_product_info <- function(network,
                             domain,
                             status_level,
                             get_statuses){

    #status_level: string. one of "retrieve", "munge", "derive"
    #get_statuses: character vector. any of the possible kernel statuses, including
    #   "ready", "pending", "paused"

    #if status_level is
    #"derive", output will be sorted so that canonical derive products
    #(stream_flux_inst, precipitation, precip_chem, precip_flux_inst) are last,
    #ensuring that any prerequisites, including "compiled" products, are generated
    #first. If two derive products have the same name, they will be sorted
    #numerically (e.g. ms003, ms009)

    prods <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = network,
                              d = domain)))

    status_column <- glue(status_level,
                          '_status')

    prods <- prods[prods[[status_column]] %in% get_statuses, ]

    if(status_level == 'derive'){

        custom_prods <- prods %>%
            filter(grepl('^CUSTOM', prodname))

        atypicals_sorted <- prods %>%
            filter(! prodname %in% !!typical_derprods,
                   ! grepl('^CUSTOM', prodname)) %>%
            arrange(prodcode)

        typicals_sorted <- prods %>%
            filter(prodname %in% !!typical_derprods,
                   ! grepl('^CUSTOM', prodname)) %>%
            arrange(order(match(prodname, !!typical_derprods)))

        prods <- bind_rows(atypicals_sorted,
                           typicals_sorted,
                           custom_prods)
    }

    return(prods)
}

prodcode_from_prodname_ms <- function(prodname_ms){

    #prodname_ms consists of the macrosheds official name for a data
    #category, e.g. discharge, and the source-specific code for that
    #data product, e.g. DP1.20093. These two values are concatenated,
    #separated by a double underscore. So long as we never use a double
    #underscore in a macrosheds official data category name, this function
    #will be able to split a prodname_ms into its two constituent parts.

    #accepts a vector of prodname_ms strings

    prodcode <- sapply(prodname_ms,
                       function(x){
                           namesplit <- strsplit(x, '__')[[1]]
                           name_length <- length(namesplit)
                           prodcode <- namesplit[2:name_length]
                           paste(prodcode, collapse = '__')
                       },
                       USE.NAMES = FALSE)

    # namesplit <- strsplit(prodname_ms, '__')[[1]]
    # name_length <- length(namesplit)
    # prodcode <- namesplit[2:name_length]
    # prodcode <- paste(prodcode, collapse = '__')

    return(prodcode)
}

prodname_from_prodname_ms <- function(prodname_ms){

    #prodname_ms consists of the macrosheds official name for a data
    #category, e.g. discharge, and the source-specific code for that
    #data product, e.g. DP1.20093. These two values are concatenated,
    #separated by a double underscore. So long as we never use a double
    #underscore in a macrosheds official data category name, this function
    #will be able to split a prodname_ms into its two constituent parts.

    #accepts a vector of prodname_ms strings

    prodname <- sapply(prodname_ms,
                       function(x) strsplit(x, '__')[[1]][1],
                       USE.NAMES = FALSE)
    # prodname <- strsplit(prodname_ms, '__')[[1]][1]

    return(prodname)
}

ms_retrieve <- function(network = domain,
                        domain,
                        prodname_filter = NULL){

    #execute main retrieval script for this network-domain
    norm_retrieve <- file.exists(glue('src/{n}/{d}/retrieve.R',
                                      n = network,
                                      d = domain))

    if(norm_retrieve){
        source(glue('src/{n}/{d}/retrieve.R',
                    n = network,
                    d = domain),
               local = TRUE)
    }

    #if there's a script for retrieval of versionless products, execute it too
    versionless_product_script <- glue('src/{n}/{d}/retrieve_versionless.R',
                                       n = network,
                                       d = domain)

    versionless_retrieve <- file.exists(versionless_product_script)
    if(versionless_retrieve){

        source(versionless_product_script,
               local = TRUE)
    }

    if(! norm_retrieve && ! versionless_retrieve){
        stop(glue('No retrieval script avalible for {n} {d}',
                  n = network,
                  d = domain))
    }
}

ms_munge <- function(network = domain,
                     domain,
                     prodname_filter = NULL){

    #execute main munge script for this network-domain
    norm_munge <- file.exists(glue('src/{n}/{d}/munge.R',
                                   n = network,
                                   d = domain))

    if(norm_munge){
        source(glue('src/{n}/{d}/munge.R',
                    n = network,
                    d = domain),
               local = TRUE)
    }

    #if there's a script for munging of versionless products, execute it too
    versionless_product_script <- glue('src/{n}/{d}/munge_versionless.R',
                                       n = network,
                                       d = domain)

    if(file.exists(versionless_product_script)){

        source(versionless_product_script,
               local = TRUE)
    }

    if(! norm_munge && ! file.exists(versionless_product_script)){
        stop(glue('No munge script avalible for {n} {d}',
                  n = network,
                  d = domain))
    }

    #calculate watershed areas for any provided watershed boundary files,
    #and put them in the site_data file
    munged_dir <- glue('data/{n}/{d}/munged',
                       n = network,
                       d = domain)

    if(dir.exists(munged_dir)){
        munged_subdirs <- list.dirs(munged_dir,
                                    recursive = FALSE)

        boundary_ind <- grepl(pattern = 'ws_boundary',
                              x = munged_subdirs)
    }

    if(exists('boundary_ind') && any(boundary_ind)){

        boundary_dir <- munged_subdirs[boundary_ind]

        sites <- list.dirs(boundary_dir,
                           full.names = FALSE,
                           recursive = FALSE)

        loginfo(logger = logger_module,
                msg = '(Re)calculating watershed areas for site_data')

    } else {
        sites <- character()
    }

    for(s in sites){
        catch <- ms_calc_watershed_area(network = network,
                                        domain = domain,
                                        site_code = s,
                                        level = 'munged',
                                        update_site_file = TRUE)
    }
}

ms_general <- function(network=domain, domain){
    source(glue('src/global/general.R', n=network, d=domain))
    #return()
}

ms_delineate <- function(network,
                         domain,
                         dev_machine_status,
                         verbose = FALSE){

    #dev_machine_status: either '1337', indicating that your machine has >= 16 GB
    #   RAM, or 'n00b', indicating < 16 GB RAM. DEM resolution is chosen
    #   accordingly. passed to delineate_watershed_apriori
    #verbose: logical. determines the amount of informative messaging during run

    loginfo(msg = 'Beginning watershed delineation',
            logger = logger_module)

    site_locations <- site_data %>%
        filter(
            as.logical(in_workflow),
            network == !!network,
            domain == !!domain,
            # ! is.na(latitude),
            # ! is.na(longitude),
            site_type == 'stream_gauge') %>%
        select(site_code, latitude, longitude, CRS, ws_area_ha)

    #checks
    if(any(is.na(site_locations$latitude) | is.na(site_locations$longitude))){

        missing_loc <- is.na(site_locations$latitude) |
            is.na(site_locations$longitude)

        missing_site_codes <- site_locations$site_code[missing_loc]

        stop(glue('Missing/incomplete site location for:\nnetwork: {n}\ndomain: {d}\n',
                  'site(s): {ss}\n(see site_data gsheet)',
                  n = network,
                  d = domain,
                  ss = paste(missing_site_codes,
                             collapse = ', ')))
    }

    if(any(is.na(site_locations$CRS))){

        missing_site_codes <- site_locations$site_code[is.na(site_locations$CRS)]

        stop(glue('Missing CRS for:\nnetwork: {n}\ndomain: {d}\n',
                  'site(s): {ss}\n(see site_data gsheet)',
                  n = network,
                  d = domain,
                  ss = paste(missing_site_codes,
                             collapse = ', ')))
    }

    #locate or create the directory that contains watershed boundaries
    munged_dirs <- list.dirs(glue('data/{n}/{d}/munged',
                                  n = network,
                                  d = domain),
                             recursive = FALSE,
                             full.names = FALSE)

    ws_boundary_dir <- grep(pattern = '^ws_boundary.*',
                            x = munged_dirs,
                            value = TRUE)

    if(! length(ws_boundary_dir)){
        ws_boundary_dir <- 'ws_boundary__ms000'
        level <- 'derived'
        dir.create(glue('data/{n}/{d}/derived/{w}',
                        n = network,
                        d = domain,
                        w = ws_boundary_dir),
                   recursive = TRUE,
                   showWarnings = FALSE)
    } else {
        level <- 'munged'
    }

    #for each stream gauge site, check for existing wb file. if none, delineate
    for(i in 1:nrow(site_locations)){

        site <- site_locations$site_code[i]

        if(verbose){
            print(glue('delineating {n}-{d}-{s} (site {sti} of {sl})',
                       n = network,
                       d = domain,
                       s = site,
                       sti = i,
                       sl = nrow(site_locations)))
        }

        site_dir <- glue('data/{n}/{d}/{l}/{w}/{s}',
                         n = network,
                         d = domain,
                         w = ws_boundary_dir,
                         l = level,
                         s = site)

        if(dir.exists(site_dir) && length(dir(site_dir))){
            message(glue('{s} already delineated ({d})',
                         s = site,
                         d = site_dir))
            next
        }

        dir.create(site_dir,
                   showWarnings = FALSE)

        specs <- ws_delin_specs %>%
            filter(
                network == !!network,
                domain == !!domain,
                site_code == !!site)

        if(nrow(specs) == 1){

            message('Delineating from stored specifications')

            if(specs$flat_increment == 'null'){
                flat_increment <- NULL
            } else {
                flat_increment <- as.numeric(specs$flat_increment)
            }

            # specs=list(buffer_radius_m=1000,snap_siatance_m=150,snap_method='standard',
            #            dem_resolution=10,breach_method='lc',burn_streams=FALSE)
            delineate_watershed_by_specification(
                lat = site_locations$latitude[i],
                long = site_locations$longitude[i],
                crs = site_locations$CRS[i],
                buffer_radius = specs$buffer_radius_m,
                snap_dist = specs$snap_distance_m,
                snap_method = specs$snap_method,
                dem_resolution = specs$dem_resolution,
                flat_increment = flat_increment,
                breach_method = specs$breach_method,
                burn_streams = specs$burn_streams,
                write_dir = site_dir,
                verbose = verbose) %>%
                invisible()

            loginfo(msg = glue('Delineation complete: {n}-{d}-{s}',
                               n = network,
                               d = domain,
                               s = site),
                    logger = logger_module)

            next
            #everything that follows pertains to interactive selection of an
            #appropriate delineation

        } else if(nrow(specs) == 0){

            if(ms_instance$instance_type != 'dev'){
                stop(glue('Missing delineation specs for {n}-{d}-{s}. ',
                          'Delineate locally and push changes.',
                          n = network,
                          d = domain,
                          s = site))
            }

            tmp <- tempdir()

            selection <- delineate_watershed_apriori_recurse(
                lat = site_locations$latitude[i],
                long = site_locations$longitude[i],
                crs = site_locations$CRS[i],
                site_code = site,
                dem_resolution = NULL,
                flat_increment = NULL,
                breach_method = 'basic',
                burn_streams = FALSE,
                scratch_dir = tmp,
                write_dir = site_dir,
                dev_machine_status = dev_machine_status,
                verbose = verbose)

            if(is.numeric(selection) && selection == 1) next
            if(is.numeric(selection) && selection == 2) return(invisible(NULL))

        } else {
            stop('Multiple entries for same network/domain/site in site_data')
        }

        #write the specifications of the correctly delineated watershed
        rgx <- str_match(selection,
                         paste0('^wb[0-9]+_BUF([0-9]+)(standard|jenson)',
                                'DIST([0-9]+)RES([0-9]+)INC([0-1\\.null]+)',
                                'BREACH(basic|lc)BURN(TRUE|FALSE)\\.shp$'))

        write_wb_delin_specs(network = network,
                             domain = domain,
                             site_code = site,
                             buffer_radius = as.numeric(rgx[, 2]),
                             snap_method = rgx[, 3],
                             snap_distance = as.numeric(rgx[, 4]),
                             dem_resolution = as.numeric(rgx[, 5]),
                             flat_increment = rgx[, 6],
                             breach_method = rgx[, 7],
                             burn_streams = rgx[, 8])

        #calculate watershed area and write it to site_data gsheet
        catch <- ms_calc_watershed_area(network = network,
                                        domain = domain,
                                        site_code = site,
                                        level = level,
                                        update_site_file = TRUE)
    }


    prods <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = network,
                              d = domain)))

    if(ws_boundary_dir == 'ws_boundary__ms000' &&
       ! 'ms000' %in% prods$prodcode){

        wb_successor_string <- prods %>%
            filter(
                grepl(pattern = '^ms[0-9]{3}$',
                      x = prodcode),
                prodname == 'precip_pchem_pflux') %>%
            mutate(prodname_ms = paste(prodname,
                                       prodcode,
                                       sep = '__')) %>%
            pull(prodname_ms) %>%
            paste(., collapse = '||')

        append_to_productfile(network = network,
                              domain = domain,
                              prodname = 'ws_boundary',
                              prodcode = 'ms000',
                              precursor_of = wb_successor_string,
                              notes = 'automated entry')
    }

    loginfo(msg = 'Delineations complete',
            logger = logger_module)

}

choose_dem_resolution <- function(dev_machine_status, buffer_radius){

    if(dev_machine_status == '1337'){
        dem_resolution <- case_when(
            buffer_radius <= 1e4 ~ 12,
            buffer_radius == 1e5 ~ 11,
            buffer_radius == 1e6 ~ 10,
            buffer_radius == 1e7 ~ 8,
            buffer_radius == 1e8 ~ 6,
            buffer_radius == 1e9 ~ 4,
            buffer_radius >= 1e10 ~ 2)
    } else if(dev_machine_status == 'n00b'){
        dem_resolution <- case_when(
            buffer_radius <= 1e4 ~ 10,
            buffer_radius == 1e5 ~ 8,
            buffer_radius == 1e6 ~ 6,
            buffer_radius == 1e7 ~ 4,
            buffer_radius == 1e8 ~ 2,
            buffer_radius >= 1e9 ~ 1)
    } else {
        stop('dev_machine_status must be either "1337" or "n00b"')
    }

    return(dem_resolution)
}

delineate_watershed_apriori_recurse <- function(lat,
                                                long,
                                                crs,
                                                site_code,
                                                dem_resolution = NULL,
                                                flat_increment = NULL,
                                                breach_method = 'lc',
                                                burn_streams = FALSE,
                                                buffer_radius = NULL,
                                                scratch_dir = tempdir(),
                                                write_dir,
                                                dev_machine_status = 'n00b',
                                                verbose = FALSE){

    #This function calls delineate_watershed_apriori recursively, taking
    #   user input after each call, until the user selects a delineation
    #   or aborts. For parameter documentation, see delineate_watershed_apriori.

    # tmp <- tempdir()
    scratch_dir <- stringr::str_replace_all(scratch_dir, '\\\\', '/')

    delin_out <- delineate_watershed_apriori(
        lat = lat,
        long = long,
        crs = crs,
        site_code = site_code,
        dem_resolution = dem_resolution,
        flat_increment = flat_increment,
        breach_method = breach_method,
        burn_streams = burn_streams,
        buffer_radius = buffer_radius,
        scratch_dir = scratch_dir,
        dev_machine_status = dev_machine_status,
        verbose = verbose)

    inspection_dir <- delin_out$inspection_dir

    files_to_inspect <- list.files(path = inspection_dir,
                                   pattern = '.shp')

    temp_point <- glue(scratch_dir, '/', 'POINT')

    tibble(longitude = long, latitude = lat) %>%
        sf::st_as_sf(coords = c('longitude', 'latitude'),
                     crs = crs) %>%
        sf::st_write(dsn = temp_point,
                     driver = 'ESRI Shapefile',
                     delete_dsn = TRUE,
                     quiet = TRUE)

    # #if only one delineation, write it into macrosheds storage
    # if(length(files_to_inspect) == 1){
    #
    #     selection <- files_to_inspect[1]
    #
    #     move_shapefiles(shp_files = selection,
    #                     from_dir = inspection_dir,
    #                     to_dir = write_dir)
    #
    #     message(glue('Delineation successful. Shapefile written to ',
    #                  write_dir))
    #
    #     #otherwise, technician must inspect all delineations and choose one
    # } else {

    nshapes <- length(files_to_inspect)
    numeric_selections <- paste('Accept delineation', 1:nshapes)

    wb_selections <- paste(paste0('[',
                                  c(1:nshapes, 'S', 'B', 'R', 'I', 'n', 'a'),
                                  ']'),
                           c(numeric_selections,
                             'Burn streams into the DEM (may help delineator across road-stream intersections)',
                             'Use more aggressive breaching method (temporary default, pending whitebox bugfix)',
                             'Select DEM resolution',
                             'Set flat_increment',
                             'Next (skip this one for now)',
                             'Abort delineation'),
                           sep = ': ',
                           collapse = '\n')

    helper_code <- glue('{id}.\nmapview::mapviewOptions(fgb = FALSE);',
                        'mapview::mapview(sf::st_read("{wd}/{f}")) + ',
                        'mapview::mapview(sf::st_read("{pf}"))',
                        id = 1:length(files_to_inspect),
                        wd = inspection_dir,
                        f = files_to_inspect,
                        pf = temp_point) %>%
        paste(collapse = '\n\n')

    msg <- glue('Visually inspect the watershed boundary candidate shapefiles ',
                'by pasting the mapview lines below into a separate instance of R.\n\n{hc}\n\n',
                'Enter the number corresponding to the ',
                'one that looks most legit, or select one or more tuning ',
                'options (e.g. "SBRI" without quotes). You usually won\'t ',
                'need to tune anything. If you aren\'t ',
                'sure which delineation is correct, get a site manager to verify:\n',
                'request_site_manager_verification(type=\'wb delin\', ',
                'network, domain) [function not yet built]\n\nChoices:\n{sel}\n\nEnter choice(s) here > ',
                hc = helper_code,
                sel = wb_selections)
                # td = inspection_dir)

    resp <- get_response_mchar(
        msg = msg,
        possible_resps = paste(c(1:nshapes, 'S', 'B', 'R', 'I', 'n', 'a'),
                               collapse = ''),
        allow_alphanumeric_response = FALSE)

    if('n' %in% resp){
        unlink(write_dir,
               recursive = TRUE)
        print(glue('Moving on. You haven\'t seen the last of {s}!',
                   s = site_code))
        return(1)
    }

    if('a' %in% resp){
        unlink(write_dir,
               recursive = TRUE)
        print(glue('Aborted. Any completed delineations have been saved.'))
        return(2)
    }

    if('S' %in% resp){
        burn_streams <- TRUE
    } else {
        burn_streams <- FALSE
    }

    if('B' %in% resp){
        breach_method <- 'basic'
    } else {
        breach_method <- 'basic' #TODO: undo this when whitebox is fixed
        # breach_method <- 'lc'
    }

    if('R' %in% resp){
        dem_resolution <- get_response_mchar(
            msg = paste0('Choose DEM resolution between 1 (low) and 14 (high)',
                         ' to pass to elevatr::get_elev_raster. For tiny ',
                         'watersheds, use 12-13. For giant ones, use 8-9.\n\n',
                         'Enter choice here > '),
            possible_resps = paste(1:14))
        dem_resolution <- as.numeric(dem_resolution)
    }

    if('I' %in% resp){

        bm <- ifelse(breach_method == 'basic',
                     'whitebox::wbt_breach_depressions',
                     'whitebox::wbt_breach_depressions_least_cost')

        new_options <- paste(paste0('[',
                                    c('S', 'M', 'L'),
                                    ']'),
                             c('0.001', '0.01', '0.1'),
                             sep = ': ',
                             collapse = '\n')

        resp2 <- get_response_1char(
            msg = glue('Pick the size of the elevation increment to pass to ',
                       bm, '.\n\n', new_options, '\n\nEnter choice here > '),
            possible_chars = c('S', 'M', 'L'))

        flat_increment <- switch(resp2,
                                 S = 0.001,
                                 M = 0.01,
                                 L = 0.1)
    }

    if(! grepl('[0-9]', resp)){

        selection <- delineate_watershed_apriori_recurse(
            lat = lat,
            long = long,
            crs = crs,
            site_code = site_code,
            dem_resolution = dem_resolution,
            flat_increment = flat_increment,
            breach_method = breach_method,
            burn_streams = burn_streams,
            buffer_radius = delin_out$buffer_radius,
            scratch_dir = scratch_dir,
            write_dir = write_dir,
            dev_machine_status = dev_machine_status,
            verbose = verbose)

        return(selection)
    }

    selection <- files_to_inspect[as.numeric(resp)]

    move_shapefiles(shp_files = selection,
                    from_dir = inspection_dir,
                    to_dir = write_dir,
                    new_name_vec = site_code)

    message(glue('Selection {s}:\n\t{sel}\nwas written to:\n\t{sdr}',
                 s = resp,
                 sel = selection,
                 sdr = write_dir))

    return(selection)
}

# site_row = site_data[1, ]; lat = site_row$latitude; long = site_row$longitude; crs=4326; site_code=site_row$site_code
delineate_watershed_apriori <- function(lat,
                                        long,
                                        crs,
                                        site_code,
                                        dem_resolution = NULL,
                                        flat_increment = NULL,
                                        breach_method = 'basic',
                                        burn_streams = FALSE,
                                        buffer_radius = NULL,
                                        scratch_dir = tempdir(),
                                        dev_machine_status = 'n00b',
                                        verbose = FALSE){

    #lat: numeric representing latitude in decimal degrees
    #   (negative indicates southern hemisphere)
    #long: numeric representing longitude in decimal degrees
    #   (negative indicates west of prime meridian)
    #crs: numeric representing the coordinate reference system (e.g. WSG84)
    #dem_resolution: optional integer 1-14. the granularity of the DEM that is used for
    #   delineation. this argument is passed directly to the z parameter of
    #   elevatr::get_elev_raster. 1 is low resolution; 14 is high. If NULL,
    #   this is determined automatically.
    #flat_increment: float or NULL. Passed to
    #   whitebox::wbt_breach_depressions_least_cost
    #   or whitebox::wbt_breach_depressions, depending on the value
    #   of breach_method (see next).
    #breach_method: string. Either 'basic', which invokes whitebox::wbt_breach_depressions,
    #   or 'lc', which invokes whitebox::wbt_breach_depressions_least_cost
    #burn_streams: logical. if TRUE, both whitebox::wbt_burn_streams_at_roads
    #   and whitebox::wbt_fill_burn are called on the DEM, using road and stream
    #   layers from OpenStreetMap.
    #scratch_dir: the directory where intermediate files will be dumped. This
    #   is a randomly generated temporary directory if not specified.
    #dev_machine_status: either '1337', indicating that your machine has >= 16 GB
    #   RAM, or 'n00b', indicating < 16 GB RAM. DEM resolution is chosen accordingly
    #verbose: logical. determines the amount of informative messaging during run

    #returns the location of candidate watershed boundary files

    # tmp <- tempdir()
    # tmp <- str_replace_all(tmp, '\\\\', '/')

    if(! is.null(dem_resolution) && ! is.numeric(dem_resolution)){
        stop('dem_resolution must be a numeric integer or NULL')
    }
    if(! is.null(flat_increment) && ! is.numeric(flat_increment)){
        stop('flat_increment must be numeric or NULL')
    }
    if(! breach_method %in% c('lc', 'basic')) stop('breach_method must be "basic" or "lc"')
    if(! is.logical(burn_streams)) stop('burn_streams must be logical')

    inspection_dir <- glue(scratch_dir, '/INSPECT_THESE')
    point_dir <- glue(scratch_dir, '/POINT')
    dem_f <- glue(scratch_dir, '/dem.tif')
    point_f <- glue(scratch_dir, '/point.shp')
    streams_f <- glue(scratch_dir, '/streams.shp')
    roads_f <- glue(scratch_dir, '/roads.shp')
    d8_f <- glue(scratch_dir, '/d8_pntr.tif')
    flow_f <- glue(scratch_dir, '/flow.tif')

    dir.create(path = inspection_dir,
               showWarnings = FALSE)

    #Old files were making it though to the next site and show old boundaries
    dir_clean <- list.files(inspection_dir)

    if(length(dir_clean) > 0) {
        file.remove(paste(inspection_dir, dir_clean, sep = '/'))
    }

    proj <- choose_projection(lat = lat,
                              long = long)

    site <- tibble(x = lat,
                   y = long) %>%
        sf::st_as_sf(coords = c("y", "x"),
                     crs = crs) %>%
        sf::st_transform(proj)
    # sf::st_transform(4326) #WGS 84 (would be nice to do this unprojected)

    #prepare for delineation loops
    if(is.null(buffer_radius)) buffer_radius <- 1000
    dem_coverage_insufficient <- FALSE
    while_loop_begin <- TRUE

    #snap site to flowlines 3 different ways. delineate watershed boundaries (wb)
    #for each unique snap. if the delineations get cut off, get more elevation data
    #and try again
    while(while_loop_begin || dem_coverage_insufficient){

        while_loop_begin <- FALSE

        if(is.null(dem_resolution)){
            # dem_resolution <- choose_dem_resolution(
            #     dev_machine_status = dev_machine_status,
            #     buffer_radius = buffer_radius)
            dem_resolution <- 10
        }

        if(verbose){

            if(is.null(flat_increment)){
                fi <- 'NULL (auto)'
            } else {
                fi <- as.character(flat_increment)
            }

            if(breach_method == 'lc') breach_method <- 'lc (jk, temporarily "basic")'
            print(glue('Delineation specs for this attempt:\n',
                       '\tsite_code: {st}; ',
                       'dem_resolution: {dr}; flat_increment: {fi}\n',
                       '\tbreach_method: {bm}; burn_streams: {bs}',
                       st = site_code,
                       dr = dem_resolution,
                       fi = fi,
                       bm = breach_method,
                       bs = as.character(burn_streams),
                       .trim = FALSE))
        }

        site_buf <- sf::st_buffer(x = site,
                                  dist = buffer_radius)

        dem <- expo_backoff(
            expr = {
                elevatr::get_elev_raster(locations = site_buf,
                                         z = dem_resolution,
                                         verbose = FALSE,
                                         override_size_check = TRUE)
            },
            max_attempts = 5
        )

        # terra::writeRaster(x = dem,
        raster::writeRaster(x = dem,
                            filename = dem_f,
                            overwrite = TRUE)

        #loses projection?
        sf::st_write(obj = site,
                     dsn = point_f,
                     delete_layer = TRUE,
                     quiet = TRUE)

        if(burn_streams){
            get_osm_roads(extent_raster = dem,
                          outfile = roads_f)
            get_osm_streams(extent_raster = dem,
                            outfile = streams_f)
        }

        whitebox::wbt_fill_single_cell_pits(dem = dem_f,
                                            output = dem_f) %>% invisible()

        if(breach_method == 'basic'){

            whitebox::wbt_breach_depressions(
                dem = dem_f,
                output = dem_f,
                flat_increment = flat_increment) %>% invisible()

        } else if(breach_method == 'lc'){

            whitebox::wbt_breach_depressions_least_cost(
                dem = dem_f,
                output = dem_f,
                dist = 10000, #maximum trench length
                fill = TRUE,
                flat_increment = flat_increment) %>% invisible()
        }
        #also see wbt_fill_depressions for when there are open pit mines

        if(burn_streams){

            #the secret is that BOTH of these burns can work in tandem!
            whitebox::wbt_burn_streams_at_roads(dem = dem_f,
                                                streams = streams_f,
                                                roads = roads_f,
                                                output = dem_f,
                                                width = 50) %>% invisible()
            whitebox::wbt_fill_burn(dem = dem_f,
                                    streams = streams_f,
                                    output = dem_f) %>% invisible()
        }

        whitebox::wbt_d8_pointer(dem = dem_f,
                                 output = d8_f) %>% invisible()

        whitebox::wbt_d8_flow_accumulation(input = dem_f,
                                           output = flow_f,
                                           out_type = 'catchment area') %>% invisible()

        snap1_f <- glue(scratch_dir, '/snap1_jenson_dist150.shp')
        whitebox::wbt_jenson_snap_pour_points(pour_pts = point_f,
                                              streams = flow_f,
                                              output = snap1_f,
                                              snap_dist = 150) %>% invisible()
        snap2_f <- glue(scratch_dir, '/snap2_standard_dist50.shp')
        whitebox::wbt_snap_pour_points(pour_pts = point_f,
                                       flow_accum = flow_f,
                                       output = snap2_f,
                                       snap_dist = 50) %>% invisible()
        snap3_f <- glue(scratch_dir, '/snap3_standard_dist150.shp')
        whitebox::wbt_snap_pour_points(pour_pts = point_f,
                                       flow_accum = flow_f,
                                       output = snap3_f,
                                       snap_dist = 150) %>% invisible()

        #the site has been snapped 3 different ways. identify unique snap locations.
        snap1 <- sf::st_read(snap1_f, quiet = TRUE)
        snap2 <- sf::st_read(snap2_f, quiet = TRUE)
        snap3 <- sf::st_read(snap3_f, quiet = TRUE)
        unique_snaps_f <- snap1_f
        if(! identical(snap1, snap2)) unique_snaps_f <- c(unique_snaps_f, snap2_f)
        if(! identical(snap1, snap3)) unique_snaps_f <- c(unique_snaps_f, snap3_f)

        #good for experimenting with snap specs:
        # delineate_watershed_test2(scratch_dir, point_f, flow_f,
        #                           d8_f, 'standard', 1000)

        #delineate each unique location
        for(i in 1:length(unique_snaps_f)){

            rgx <- str_match(unique_snaps_f[i],
                             '.*?_(standard|jenson)_dist([0-9]+)\\.shp$')
            snap_method <- rgx[, 2]
            snap_distance <- rgx[, 3]

            wb_f <- glue('{path}/wb{n}_buffer{b}_{typ}_dist{dst}.tif',
                         path = scratch_dir,
                         n = i,
                         b = buffer_radius,
                         typ = snap_method,
                         dst = snap_distance)

            whitebox::wbt_watershed(d8_pntr = d8_f,
                                    pour_pts = unique_snaps_f[i],
                                    output = wb_f) %>% invisible()

            wb <- raster::raster(wb_f)

            #check how many wb cells coincide with the edge of the DEM.
            #If > 0.1% or > 5, broader DEM needed
            smry <- raster_intersection_summary(wb = wb,
                                                dem = dem)

            if(verbose){
                print(glue('site buffer radius: {br}; pour point snap: {sn}/{tot}; ',
                           'n intersecting border cells: {ni}; pct intersect: {pct}',
                           br = buffer_radius,
                           sn = i,
                           tot = length(unique_snaps_f),
                           ni = round(smry$n_intersections, 2),
                           pct = round(smry$pct_wb_cells_intersect, 2)))
            }

            if(smry$pct_wb_cells_intersect > 0.1 || smry$n_intersections > 5){

                buffer_radius_new <- buffer_radius * 10
                dem_coverage_insufficient <- TRUE
                print(glue('Hit DEM edge. Incrementing buffer.'))
                break

            } else {

                dem_coverage_insufficient <- FALSE
                buffer_radius_new <- buffer_radius

                #write and record temp files for the technician to visually inspect
                wb_sf <- wb %>%
                    raster::rasterToPolygons() %>%
                    sf::st_as_sf() %>%
                    sf::st_buffer(dist = 0.1) %>%
                    sf::st_union() %>%
                    sf::st_as_sf() %>%
                    fill_sf_holes() %>%
                    sf::st_transform(4326)

                ws_area_ha <- as.numeric(sf::st_area(wb_sf)) / 10000

                wb_sf <- wb_sf %>%
                    mutate(site_code = !!site_code) %>%
                    mutate(area = !!ws_area_ha)

                if(is.null(flat_increment)){
                    flt_incrmt <- 'null'
                } else {
                    flt_incrmt <- as.character(flat_increment)
                }

                wb_sf_f <- glue('{path}/wb{n}_BUF{b}{typ}DIST{dst}RES{res}',
                                'INC{inc}BREACH{brc}BURN{brn}.shp',
                                path = inspection_dir,
                                n = i,
                                b = sprintf('%d', buffer_radius),
                                typ = snap_method,
                                dst = snap_distance,
                                res = dem_resolution,
                                inc = flt_incrmt,
                                brc = breach_method,
                                brn = as.character(burn_streams))

                sw(sf::st_write(obj = wb_sf,
                                dsn = wb_sf_f,
                                delete_dsn = TRUE,
                                quiet = TRUE))
            }
        }

        buffer_radius <- buffer_radius_new
    } #end while loop

    if(verbose){
        message(glue('Candidate delineations are in: ', inspection_dir))
    }

    delin_out <- list(inspection_dir = inspection_dir,
                      buffer_radius = buffer_radius)

    return(delin_out)
}

delineate_watershed_by_specification <- function(lat,
                                                 long,
                                                 crs,
                                                 buffer_radius,
                                                 snap_dist,
                                                 snap_method,
                                                 dem_resolution,
                                                 flat_increment,
                                                 breach_method,
                                                 burn_streams,
                                                 write_dir,
                                                 verbose = FALSE){

    #lat: numeric representing latitude in decimal degrees
    #   (negative indicates southern hemisphere)
    #long: numeric representing longitude in decimal degrees
    #   (negative indicates west of prime meridian)
    #crs: numeric representing the coordinate reference system (e.g. WSG84)
    #buffer_radius: integer. the width (m) of the buffer around the site location.
    #   a DEM will be acquired that covers at least the full area of the buffer.
    #snap_dist: integer. the distance (m) around the recorded site location
    #   to search for a flow path.
    #snap_method: character. either "standard", which snaps the site location
    #   to the cell within snap_dist that has the highest flow value, or
    #   "jenson", which snaps to the nearest flow path, regardless of flow.
    #dem_resolution: integer 1-14. the granularity of the DEM that is used for
    #   delineation. this argument is passed directly to the z parameter of
    #   elevatr::get_elev_raster. 1 is low resolution; 14 is high.
    #flat_increment: float or NULL. Passed to wbt_breach_depressions_least_cost
    #   or wbt_breach_depressions, depending on the value of breach_method (see next).
    #breach_method: string. Either 'basic', which invokes whitebox::wbt_breach_depressions,
    #   or 'lc', which invokes whitebox::wbt_breach_depressions_least_cost
    #burn_streams: logical. if TRUE, both whitebox::wbt_burn_streams_at_roads
    #   and whitebox::wbt_fill_burn are called on the DEM, using road and stream
    #   layers from OpenStreetMap.
    #write_dir: character. the directory to write shapefile watershed boundary to

    #returns the location of candidate watershed boundary files

    require(whitebox) #can't do e.g. whitebox::func in do.call

    if(! is.null(dem_resolution) && ! is.numeric(dem_resolution)){
        stop('dem_resolution must be a numeric integer or NULL')
    }
    if(! is.null(flat_increment) && ! is.numeric(flat_increment)){
        stop('flat_increment must be numeric or NULL')
    }
    if(! breach_method %in% c('lc', 'basic')) stop('breach_method must be "basic" or "lc"')
    if(! is.logical(burn_streams)) stop('burn_streams must be logical')

    tmp <- tempdir()
    inspection_dir <- glue(tmp, '/INSPECT_THESE')
    dem_f <- glue(tmp, '/dem.tif')
    point_f <- glue(tmp, '/point.shp')
    streams_f <- glue(tmp, '/streams.shp')
    roads_f <- glue(tmp, '/roads.shp')
    d8_f <- glue(tmp, '/d8_pntr.tif')
    flow_f <- glue(tmp, '/flow.tif')
    snap_f <- glue(tmp, '/snap.shp')
    wb_f <- glue(tmp, '/wb.tif')

    dir.create(path = inspection_dir,
               showWarnings = FALSE)

    proj <- choose_projection(lat = lat,
                              long = long)

    site <- tibble(x = lat,
                   y = long) %>%
        sf::st_as_sf(coords = c("y", "x"),
                     crs = crs) %>%
        sf::st_transform(proj)
    # sf::st_transform(4326) #WGS 84 (would be nice to do this unprojected)

    site_buf <- sf::st_buffer(x = site,
                              dist = buffer_radius)

    dem <- expo_backoff(
        expr = {
            elevatr::get_elev_raster(locations = site_buf,
                                     z = dem_resolution,
                                     verbose = FALSE,
                                     override_size_check = TRUE)
        },
        max_attempts = 5
    )

    raster::writeRaster(x = dem,
                        filename = dem_f,
                        overwrite = TRUE)
    # terra::rast(dem) %>%
    #     terra::writeRaster(dem_f,
    # overwrite = TRUE)
# qq <- terra::rast(dem_f)
# terra::plot(qq)

# qq <- raster::raster(dem_f)
# raster::plot(qq)
        # raster::rasterToPolygons() %>%
        # sf::st_as_sf() %>%
        # sf::st_buffer(dist = 0.1) %>%
        # sf::st_union() %>%
        # sf::st_as_sf() %>% #again? ugh.
        # sf::st_transform(4326) #EPSG for WGS84

    #loses projection
    sf::st_write(obj = site,
                 dsn = point_f,
                 delete_layer = TRUE,
                 quiet = TRUE)

    if(burn_streams){
        get_osm_roads(extent_raster = dem,
                      outfile = roads_f)
        get_osm_streams(extent_raster = dem,
                        outfile = streams_f)
    }

    whitebox::wbt_fill_single_cell_pits(dem = dem_f,
                                        output = dem_f) %>% invisible()

    if(breach_method == 'basic'){

        whitebox::wbt_breach_depressions(
            dem = dem_f,
            output = dem_f,
            flat_increment = flat_increment) %>% invisible()

    } else if(breach_method == 'lc'){

        message('lc method temporarily disabled. generates inconsistent output. using basic instead.')
        # whitebox::wbt_breach_depressions_least_cost(
        #     dem = dem_f,
        #     output = dem_f,
        #     dist = 10000, #maximum trench length
        #     fill = TRUE,
        #     flat_increment = flat_increment) %>% invisible()
        whitebox::wbt_breach_depressions(
            dem = dem_f,
            output = dem_f,
            flat_increment = flat_increment) %>% invisible()
    }
    #also see wbt_fill_depressions for when there are open pit mines

    # qq <- raster::raster(dem_f)
    # yyy=c(raster::values(qq))
    # xxx=c(raster::values(qq))

    if(burn_streams){

        #the secret is that BOTH of these burns can work in tandem!
        whitebox::wbt_burn_streams_at_roads(dem = dem_f,
                                            streams = streams_f,
                                            roads = roads_f,
                                            output = dem_f,
                                            width = 50) %>% invisible()
        whitebox::wbt_fill_burn(dem = dem_f,
                                streams = streams_f,
                                output = dem_f) %>% invisible()
    }

    whitebox::wbt_d8_pointer(dem = dem_f,
                             output = d8_f) %>% invisible()

    whitebox::wbt_d8_flow_accumulation(input = dem_f,
                                       output = flow_f,
                                       out_type = 'catchment area') %>% invisible()

    #call the appropriate snapping function from whitebox
    args <- list(pour_pts = point_f,
                 output = snap_f,
                 snap_dist = snap_dist)

    if(snap_method == 'standard'){
        args$flow_accum <- flow_f
        desired_func <- 'wbt_snap_pour_points'
    } else if(snap_method == 'jenson'){
        args$streams <- flow_f
        desired_func <- 'wbt_jenson_snap_pour_points'
    } else {
        stop('snap_method must be "standard" or "jenson"')
    }

    do.call(desired_func, args)

    #delineate
    whitebox::wbt_watershed(d8_pntr = d8_f,
                            pour_pts = snap_f,
                            output = wb_f) %>% invisible()

    wb_sf <- raster::raster(wb_f) %>%
        raster::rasterToPolygons() %>%
        sf::st_as_sf() %>%
        sf::st_buffer(dist = 0.1) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        fill_sf_holes() %>%
        sf::st_transform(crs = 4326)

    site_code <- str_match(write_dir, '.+?/([^/]+)$')[, 2]

    ws_area_ha <- as.numeric(sf::st_area(wb_sf)) / 10000

    wb_sf <- wb_sf %>%
        mutate(site_code = !!site_code) %>%
        mutate(area = !!ws_area_ha)

    dir.create(write_dir,
               showWarnings = FALSE)

    sw(sf::st_write(obj = wb_sf,
                    dsn = glue('{d}/{s}.shp',
                               d = write_dir,
                               s = site_code),
                    delete_dsn = TRUE,
                    quiet = TRUE))

    message(glue('Watershed boundary written to ',
                 write_dir))
}

get_derive_ingredient <- function(network,
                                  domain,
                                  prodname,
                                  ignore_derprod = FALSE,
                                  ignore_derprod900 = TRUE,
                                  accept_multiple = FALSE){

    #get prodname_ms's by prodname, for specifying derive kernels.

    #ignore_derprod: logical. if TRUE, don't consider any product with an
    #   msXXX prodcode. In other words, return a munged prodname_ms. If FALSE,
    #   derived products take precedence over munged products.
    #ignore_derprod900: logical. if TRUE, don't consider any product with an
    #   ms9XX prodcode.
    #accept_multiple: logical. should more than one ingredient be returned?

     prods <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = network,
                              d = domain))) %>%
         filter( (! is.na(munge_status) & munge_status == 'ready') |
                     (! is.na(derive_status) & derive_status %in% c('ready', 'linked')) |
                     (prodname == 'ws_boundary' & ! is.na(notes) & notes == 'automated entry') )

    if(ignore_derprod){

        prodname_ms <- prods %>%
            filter(
                !  grepl(pattern = '^ms[0-9]{3}$',
                         x = prodcode),
                prodname == !!prodname) %>%
            mutate(prodname_ms = paste(prodname,
                                       prodcode,
                                       sep = '__')) %>%
            pull(prodname_ms)

    } else {

        prodname_ms <- prods %>%
            filter(
                # grepl(pattern = '^ms[0-9]{3}$',
                #       x = prodcode),
                prodname == !!prodname) %>%
            mutate(prodname_ms = paste(prodname,
                                       prodcode,
                                       sep = '__')) %>%
            pull(prodname_ms)

        if(ignore_derprod900){
            prodname_ms <- prodname_ms[! grepl(pattern = '__ms9[0-9]{2}$',
                                               x = prodname_ms)]
        }

        #if there are multiple derive kernels, we're looking for the one that is
        #   a precursor to the other, so grab the one with the lower msXXX ID.
        #   UNLESS it's ws_boundary. then we want the one that's fully processed,
        #   so higher msXXX ID.
        if(length(prodname_ms) > 1){

            #ignore munge kernels
            prodname_ms <- prodname_ms[grepl(pattern = 'ms[0-9]{3}$',
                                             x = prodname_ms,
                                             perl = TRUE)]

            combinekernel_inds <- substr(prodname_ms,
                                         nchar(prodname_ms) - 2,
                                         nchar(prodname_ms)) %>%
                as.numeric()

            if(prodname == 'ws_boundary'){
                combinekernel_ind <- which.max(combinekernel_inds)
            } else {
                combinekernel_ind <- which.min(combinekernel_inds)
            }

            prodname_ms <- prodname_ms[combinekernel_ind]
        }
    }

    if(length(prodname_ms) > 1 && ! accept_multiple){
        stop('could not resolve multiple products with same prodname')
    }

    return(prodname_ms)
}

ms_derive <- function(network = domain,
                      domain,
                      prodname_filter = NULL){

    #categorize munged products. some are complete after munging, so they
    #   get hardlinked. some need to be compiled into canonical form (e.g.
    #   water_temp, spcond, gases -> stream_chemistry) by a derive
    #   kernel. and then of course there are products that consistently
    #   require actual derivation, like precip_flux_inst from precipitation
    #   and pchem (note that both of these will usually be replaced by the
    #   new precip_pchem_pflux kernels)

    if(! exists('held_data')){
        held_data <<- get_data_tracker(network = network,
                                       domain = domain)
    }

    prods <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = network,
                              d = domain)))

    #determine various categories governing how products should be handled
    has_ms_prodcode <- grepl('^ms[0-9]{3}$',
                             prods$prodcode,
                             perl = TRUE)

    is_being_munged <- ! is.na(prods$munge_status) & prods$munge_status == 'ready'

    is_self_precursor <- mapply(function(x, y){
                                    precursors <- strsplit(as.character(y),
                                                           '\\|\\|')[[1]]
                                    x %in% prodname_from_prodname_ms(precursors)
                                },
                                x = prods$prodname,
                                y = prods$precursor_of,
                                USE.NAMES = FALSE)

    is_a_link <- ! is.na(prods$derive_status) &
        prods$derive_status == 'linked'

    created_links <- prods$prodname[is_a_link]

    is_already_linked <- ! is_a_link &
        prods$prodname %in% created_links &
        prods$munge_status == 'ready'

    is_automated_entry <- ! is.na(prods$notes) &
        prods$notes == 'automated entry'

    #determine which active prods need to be linked (linkprods)
    is_linkprod <- (! is.na(prods$derive_status) &
                        prods$derive_status == 'linked') |
        (prods$prodname %in% canonical_derprods |
             grepl('CUSTOM', prods$prodname)) &
             ( ! has_ms_prodcode &
             is_being_munged &
             ! is_self_precursor)

    #this patch catches the case of precip/stream gauges being generated
    #by a derive kernel (which shouldn't be linked from munge)
    precip_gauges_derived <- any(grepl('precip_gauge_locations', prods$prodname) &
                                     has_ms_prodcode)
    stream_gauges_derived <- any(grepl('stream_gauge_locations', prods$prodname) &
                                     has_ms_prodcode)

    if(precip_gauges_derived){
       not_rly_linkprod <-  which(grepl('precip_gauge_locations',
                    prods$prodname) & ! has_ms_prodcode)
       is_linkprod[not_rly_linkprod] <- FALSE
    }

    if(stream_gauges_derived){
       not_rly_linkprod <-  which(grepl('stream_gauge_locations',
                    prods$prodname) & ! has_ms_prodcode)
       is_linkprod[not_rly_linkprod] <- FALSE
    }

    #re-link any already linked prods
    for(i in which(is_already_linked)){

        linked_prodcode <- get_derive_ingredient(network = network,
                                                 domain = domain,
                                                 prodname = prods$prodname[i]) %>%
                               prodcode_from_prodname_ms()

        prodname_ms_source <- paste(prods$prodname[i],
                                    prods$prodcode[i],
                                    sep = '__')

        tryCatch({
            create_derived_links(network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms_source,
                                 new_prodcode = linked_prodcode)
        },
        error = function(e){
            logwarn(glue('Failed to link {p} to derive/',
                         p = prodname_ms_source),
                         logger = logger_module)
            }
        )

        write_metadata_d_linkprod(network = network,
                                  domain = domain,
                                  prodname_ms_mr = prodname_ms_source,
                                  prodname_ms_d = paste0(prods$prodname[i],
                                                         '__',
                                                         linked_prodcode))
    }

    #link any new linkprods and create new product entries
    prodcodes_num <- as.numeric(substr(
        prods$prodcode[grepl('^ms[0-7][0-9]{2}$',
                             prods$prodcode)],
        start = 3,
        stop = 5))

    code_800_or_900s <- grep(pattern = '^ms[8-9][0-9]{2}$',
                             x = prods$prodcode[has_ms_prodcode &
                                                    ! is_automated_entry],
                             value = TRUE)

    if(length(code_800_or_900s)){
        stop(glue('ms8XX and ms9XX are reserved prodcodes (generated ',
                  'automatically). Please rename: ',
                  paste(code_800_or_900s, collapse = ', ')))
    }

    if(any(is_linkprod)){

        new_prodcodes_num <- seq(max(prodcodes_num) + 1,
                                 max(prodcodes_num) + sum(is_linkprod))

        new_prodcodes <- stringr::str_pad(string = new_prodcodes_num,
                                          width = 3,
                                          side = 'left',
                                          pad = 0) %>%
            {paste0('ms', .)}
    }

    new_linkprod_inds <- which(is_linkprod &
                                   ! is_already_linked &
                                   ! is_a_link)

    for(i in seq_along(new_linkprod_inds)){

        prodname <- prods$prodname[new_linkprod_inds[i]]

        prodname_ms_source <- paste(prodname,
                                    prods$prodcode[new_linkprod_inds[i]],
                                    sep = '__')

        if(prodname_ms_source == 'ws_boundary__ms000'){
            next
        }

        newcode <- new_prodcodes[i]

        thisenv = environment() #DELETE THIS CHECK WHEN FINISHED
        bypass_append = FALSE
        tryCatch({
            create_derived_links(network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms_source,
                                 new_prodcode = newcode)

        }, error = function(e){
            logwarn(glue('TEMPORARY BYPASS of normal error checking until we ',
                         'incorporate flux products from data sources'))
            #UNCOMMENT THE BELOW WHEN THE ABOVE IS NO LONGER NEEDED
            # stop(glue('{p} not found. There must be munge errors in need of fixing.',
            #           p = prodname_ms_source))
            #ALSO delete entries with #DELETE THIS
            assign('bypass_append', TRUE, envir=thisenv) #DELETE THIS
        })

        if(! bypass_append){  #MAKE THIS UNCONDITIONAL
        append_to_productfile(
            network = network,
            domain = domain,
            prodcode = newcode,
            prodname = prodname,
            derive_status = 'linked',
            notes = 'automated entry')
        }

        write_metadata_d_linkprod(network = network,
                                  domain = domain,
                                  prodname_ms_mr = prodname_ms_source,
                                  prodname_ms_d = paste0(prodname,
                                                         '__',
                                                         newcode))
    }

    waiting_retrv_kerns <- prods %>%
        filter(! is.na(retrieve_status) & retrieve_status != 'ready') %>%
        mutate(prodname_ms = paste(prodname, prodcode, sep='_')) %>%
        pull(prodname_ms)

    waiting_mng_kerns <- prods %>%
        filter(! is.na(munge_status) & munge_status != 'ready') %>%
        mutate(prodname_ms = paste(prodname, prodcode, sep='_')) %>%
        pull(prodname_ms)

    if(length(waiting_retrv_kerns)){
        logwarn(msg = glue('Some retrieve kernels are not ready: {wr}',
                           wr = paste(waiting_retrv_kerns,
                                      collapse = ', ')),
                logger = logger_module)
    }

    if(length(waiting_mng_kerns)){
        logwarn(msg = glue('Some munge kernels are not ready: {wr}',
                           wr = paste(waiting_mng_kerns,
                                      collapse = ', ')),
                logger = logger_module)
    }

    #compile any compprods and derive derprods (run all code in derive.R).
    #note: get_product_info() knows it must arrange derive products with
    #   non-canonicals first and CUSTOM products last
    source(glue('src/{n}/{d}/derive.R',
                n = network,
                d = domain),
           local = TRUE)


    # ws_bound_prod <- list.files(glue('data/{n}/{d}/derived',
    #                                  n = network,
    #                                  d = domain),
    #                             pattern = '^ws_boundary__ms',
    #                             include.dirs = TRUE) %>%
    #     sort(decreasing = TRUE) %>%
    #     {.[1]}
    #
    # if(length(ws_bound_prod) && ! is.na(ws_bound_prod)){
    #     write_metadata_d(network = network,
    #                      domain = domain,
    #                      prodname_ms = ws_bound_prod)
    # }

    #link all derived products to the data portal directory
    create_portal_links(network = network,
                        domain = domain)
}

import_ancestor_env <- function(pos = 1){

    #populates the calling environment with variables from a parent or higher
    #   ancestor environment

    #pos: integer between 1 and the depth of the callstack - 1. 1 represents
    #   the immediate parent of the function in which import_ancestor_env
    #   is called.

    import_env <- parent.frame(n = pos + 1)
    varnames <- ls(name = import_env)
    vars <- mget(varnames,
                 envir = import_env)

    for(i in 1:length(varnames)){
        assign(varnames[i],
               value = vars[[i]],
               pos = pos)
    }
}

append_to_productfile <- function(network,
                                  domain,
                                  prodcode,
                                  prodname,
                                  # type, #obsolete
                                  retrieve_status,
                                  munge_status,
                                  derive_status,
                                  precursor_of,
                                  notes,
                                  components){

    #add a line to the products.csv file for a particular network and domain.
    #any fields omitted will be populated with NA.

    #if a prodname_ms is already in products.csv, this will terminate before
    #appending.

    import_ancestor_env()
    passed_args <- as.list(match.call())
    arg_nms <- names(passed_args)
    passed_args <- passed_args[! arg_nms %in% c('', 'network', 'domain')]
    passed_args <- lapply(passed_args,
                          function(x) eval(x))

    args_legit <- sapply(passed_args,
           function(x) length(x) == 1 && is.character(x))

    if(any(! args_legit)){
        stop('all arguments must be strings')
    }

    prodfile <- glue('src/{n}/{d}/products.csv',
                     n = network,
                     d = domain)

    prods <- sm(read_csv(prodfile)) %>%
        mutate(prodcode = as.character(prodcode))

    new_row <- unlist(passed_args)

    new_row <- new_row[names(new_row) %in% colnames(prods)]

    prods <- bind_rows(prods, new_row)

    new_row_is_duplicate <-
        duplicated(select(prods, prodname, prodcode))[nrow(prods)]

    if(new_row_is_duplicate){
        logging::logwarn(glue('New row would duplicate an existing row in ',
                              'products.csv. Not appending.'))
        return()
    }

    write_csv(x = prods,
              file = prodfile)
}

move_shapefiles <- function(shp_files, from_dir, to_dir, new_name_vec = NULL){

    #shp_files is a character vector of filenames with .shp extension
    #   (.shx, .prj, .dbf are handled internally and don't need to be listed)
    #from_dir and to_dir are strings representing the source and destination
    #   directories, respectively
    #new_name_vec is an optional character vector of new names for each shape file.
    #   these can end in ".shp", but don't need to

    if(any(! grepl('\\.shp$', shp_files))){
        stop('All components of shp_files must end in ".shp"')
    }

    if(length(shp_files) != length(new_name_vec)){
        stop('new_name_vec must have the same length as shp_files')
    }

    dir.create(to_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    for(i in 1:length(shp_files)){

        shapefile_base <- strsplit(shp_files[i], '\\.shp')[[1]]

        files_to_move <- list.files(path = from_dir,
                                    pattern = shapefile_base)

        extensions <- str_match(files_to_move,
                                paste0(shapefile_base, '(\\.[a-z]{3})'))[, 2]

        if(is.null(new_name_vec)){
            new_name_base <- rep(shapefile_base, length(files_to_move))
        } else {
            new_name_base <- strsplit(new_name_vec[i], '\\.shp$')[[1]]
            new_name_base <- rep(new_name_base, length(files_to_move))
        }

        tryCatch({

            #try to move the files (may fail if they are on different partitions)
            mapply(function(x, nm, ext) file.rename(from = paste(from_dir,
                                                                 x,
                                                                 sep = '/'),
                                                    to = glue('{td}/{n}{ex}',
                                                              td = to_dir,
                                                              n = nm,
                                                              ex = ext)),
                   x = files_to_move,
                   nm = new_name_base,
                   ext = extensions)

        }, warning = function(w){

            #if that fails, copy them and then delete them
            mapply(function(x, nm, ext) file.copy(from = paste(from_dir,
                                                               x,
                                                               sep = '/'),
                                                  to = glue('{td}/{n}{ex}',
                                                            td = to_dir,
                                                            n = nm,
                                                            ex = ext),
                                                  overwrite = TRUE),
                   x = files_to_move,
                   nm = new_name_base,
                   ext = extensions)

            lapply(paste(from_dir,
                         files_to_move,
                         sep = '/'),
                   unlink)
        })
    }

    #return()
}

get_response_1char <- function(msg,
                               possible_chars,
                               subsequent_prompt = FALSE){

    #msg: character. a message that will be used to prompt the user
    #possible_chars: character vector of acceptable single-character responses
    #subsequent prompt: not to be set directly. This is handled by
    #   get_response_mchar during recursion.

    if(subsequent_prompt){
        cat(paste('Please choose one of:',
                  paste(possible_chars,
                        collapse = ', '),
                  '\n> '))
    } else {
        cat(msg)
    }

    ch <- as.character(readLines(con = stdin(), 1))

    if(length(ch) == 1 && ch %in% possible_chars){
        return(ch)
    } else {
        get_response_1char(msg = msg,
                           possible_chars = possible_chars,
                           subsequent_prompt = TRUE)
    }
}

get_response_mchar <- function(msg,
                               possible_resps,
                               allow_alphanumeric_response = TRUE,
                               subsequent_prompt = FALSE){

    #msg: character. a message that will be used to prompt the user
    #possible_resps: character vector. If length 1, each character in the response
    #   will be required to match a character in possible_resps, and the return
    #   value will be a character vector of each single-character tokens in the
    #   response. If
    #   length > 1, the response will be required to match an element of
    #   possible_resps exactly, and the response will be returned as-is.
    #allow_alphanumeric_response: logical. If FALSE, the response may not
    #   include both numerals and letters. Only applies when possible_resps
    #   has length 1.
    #subsequent prompt: not to be set directly. This is handled by
    #   get_response_mchar during recursion.

    split_by_character <- ifelse(length(possible_resps) == 1, TRUE, FALSE)

    if(subsequent_prompt){

        if(split_by_character){
            pr <- strsplit(possible_resps, split = '')[[1]]
        } else {
            pr <- possible_resps
        }

        cat(paste('Your options are:',
                  paste(pr,
                        collapse = ', '),
                  '\n> '))
    } else {
        cat(msg)
    }

    chs <- as.character(readLines(con = stdin(), 1))

    if(! allow_alphanumeric_response &&
       split_by_character &&
       grepl('[0-9]', chs) &&
       grepl('[a-zA-Z]', chs)){

        cat('Response may not include both letters and numbers.\n> ')
        resp <- get_response_mchar(
            msg = msg,
            possible_resps = possible_resps,
            allow_alphanumeric_response = allow_alphanumeric_response,
            subsequent_prompt = FALSE)

        return(resp)
    }

    if(length(chs)){
        if(split_by_character){

            if(length(possible_resps) != 1){
                stop('possible_resps must be length 1 if split_by_character is TRUE')
            }

            chs <- strsplit(chs, split = '')[[1]]
            possible_resps_split <- strsplit(possible_resps, split = '')[[1]]

            if(all(chs %in% possible_resps_split)){
                return(chs)
            }

        } else {

            if(length(possible_resps) < 2){
                stop('possible_resps must have length > 1 if split_by_character is FALSE')
            }

            if(any(possible_resps == chs)){
                return(chs)
            }
        }
    }

    resp <- get_response_mchar(
        msg = msg,
        possible_resps = possible_resps,
        allow_alphanumeric_response = allow_alphanumeric_response,
        subsequent_prompt = TRUE)

    return(resp)
}

ms_calc_watershed_area <- function(network,
                                   domain,
                                   site_code,
                                   level,
                                   update_site_file){

    #reads watershed boundary shapefile from macrosheds directory and calculates
    #   watershed area with sf::st_area

    #update_site_file: logical. if true, calculated watershed area is written
    #   to the ws_area_ha column in site_data gsheet

    #returns area in hectares

    print(glue('Computing watershed area'))

    ms_dir <- glue('data/{n}/{d}/{l}',
                   n = network,
                   d = domain,
                   l = level)

    level_dirs <- list.dirs(ms_dir,
                            recursive = FALSE,
                            full.names = FALSE)

    ws_boundary_dir <- grep(pattern = '^ws_boundary.*',
                            x = level_dirs,
                            value = TRUE)

    if(! length(ws_boundary_dir)){
        stop(glue('No ws_boundary directory found in ', munge_dir))
    }

    site_dir <- glue('data/{n}/{d}/{l}/{w}/{s}',
                     n = network,
                     d = domain,
                     l = level,
                     w = ws_boundary_dir,
                     s = site_code)

    if(! dir.exists(site_dir) || ! length(dir(site_dir))){
        stop(glue('{s} directory missing or empty (data/{n}/{d}/{l}/{w})',
                  s = site_code,
                  n = network,
                  d = domain,
                  l = level,
                  w = ws_boundary_dir))
    }

    wd_path <- glue('data/{n}/{d}/{l}/{w}/{s}/{s}.shp',
                    n = network,
                    d = domain,
                    l = level,
                    w = ws_boundary_dir,
                    s = site_code)

    wb <- sf::st_read(wd_path,
                      quiet = TRUE)

    ws_area_ha <- as.numeric(sf::st_area(wb)) / 10000

    if(update_site_file){

        site_data$ws_area_ha[site_data$domain == domain &
                                 site_data$network == network &
                                 site_data$site_code == site_code] <- ws_area_ha

        ms_write_confdata(site_data,
                          which_dataset = 'site_data',
                          to_where = ms_instance$config_data_storage,
                          overwrite = TRUE)
    }

    return(ws_area_ha)
}

write_wb_delin_specs <- function(network,
                                 domain,
                                 site_code,
                                 buffer_radius,
                                 snap_method,
                                 snap_distance,
                                 dem_resolution,
                                 flat_increment,
                                 breach_method,
                                 burn_streams){

    print(glue('Saving delineation specs'))

    new_entry <- tibble(network = network,
                        domain = domain,
                        site_code = site_code,
                        buffer_radius_m = buffer_radius,
                        snap_method = snap_method,
                        snap_distance_m = snap_distance,
                        dem_resolution = dem_resolution,
                        flat_increment = as.character(flat_increment),
                        breach_method = as.character(breach_method),
                        burn_streams = as.character(burn_streams))

    # ws_delin_specs <- bind_rows(ws_delin_specs, new_entry)

    ms_write_confdata(new_entry,
                      which_dataset = 'ws_delin_specs',
                      to_where = ms_instance$config_data_storage,
                      overwrite = FALSE)
}

serialize_list_to_dir <- function(l, dest){

    #l must be a named list
    #dest is the path to a directory that will be created if it doesn't exist

    #list element classes currently handled: data.frame, character

    elemclasses = lapply(l, class)

    handled = lapply(elemclasses,
                     function(x) any(c('character', 'data.frame') %in% x))

    if(! all(unlist(handled))){
        stop('Unhandled class encountered')
    }

    dir.create(dest, showWarnings=FALSE, recursive=TRUE)

    for(i in 1:length(l)){

        if('data.frame' %in% elemclasses[[i]]){

            fpath = paste0(dest, '/', names(l)[i], '.feather')
            write_feather(l[[i]], fpath)

        } else if('character' %in% x){

            fpath = paste0(dest, '/', names(l)[i], '.txt')
            readr::write_file(l[[i]], fpath)
        }
    }

    #return()
}

parse_molecular_formulae <- function(formulae){

    #`formulae` is a vector

    # formulae = c('C', 'C4', 'Cl', 'Cl2', 'CCl', 'C2Cl', 'C2Cl2', 'C2Cl2B2')
    # formulae = 'BCH10He10PLi2'
    # formulae='Mn'

    conc_vars = str_match(formulae, '^(?:OM|TM|DO|TD|UT|UTK|TK|TI|TO|DI)?([A-Za-z0-9]+)_?')[,2]
    two_let_symb_num = str_extract_all(conc_vars, '([A-Z][a-z][0-9]+)')
    conc_vars = str_remove_all(conc_vars, '([A-Z][a-z][0-9]+)')
    one_let_symb_num = str_extract_all(conc_vars, '([A-Z][0-9]+)')
    conc_vars = str_remove_all(conc_vars, '([A-Z][0-9]+)')
    two_let_symb = str_extract_all(conc_vars, '([A-Z][a-z])')
    conc_vars = str_remove_all(conc_vars, '([A-Z][a-z])')
    one_let_symb = str_extract_all(conc_vars, '([A-Z])')

    constituents = mapply(c, SIMPLIFY=FALSE,
                          two_let_symb_num, one_let_symb_num, two_let_symb, one_let_symb)

    return(constituents) # a list of vectors
}

combine_atomic_masses <- function(molecular_constituents){

    #`molecular_constituents` is a vector

    xmat = str_match(molecular_constituents,
                     '([A-Z][a-z]?)([0-9]+)?')[, -1, drop=FALSE]
    elems = xmat[,1]
    mults = as.numeric(xmat[,2])
    mults[is.na(mults)] = 1
    molecular_mass = sum(PeriodicTable::mass(elems) * mults)

    return(molecular_mass) #a scalar
}

calculate_molar_mass <- function(molecular_formula){

    if(length(molecular_formula) > 1){
        stop('molecular_formula must be a string of length 1')
    }

    parsed_formula = parse_molecular_formulae(molecular_formula)[[1]]

    molar_mass = combine_atomic_masses(parsed_formula)

    return(molar_mass)
}

convert_molecule <- function(x, from, to){

    #e.g. convert_molecule(1.54, 'NH4', 'N')

    molecule_real <- ms_vars %>%
        filter(variable_code == !!from) %>%
        pull(molecule)

    if(!is.na(molecule_real)) {
        from <- molecule_real
    }

    from_mass <- calculate_molar_mass(from)
    to_mass <- calculate_molar_mass(to)
    converted_mass <- x * to_mass / from_mass

    return(converted_mass)
}

update_product_file <- function(network,
                                domain,
                                level,
                                prodcode,
                                status,
                                prodname){

    prods = sm(read.csv(glue('src/{n}/{d}/products.csv',
                             n = network,
                             d = domain),
                        colClasses = 'character'))

    prodname_list <- strsplit(prodname, '; ')
    names_matched <- sapply(prodname_list, function(x) all(x %in% prods$prodname))
    if(! all(names_matched)){
        stop(glue('All prodnames in processing_kernels.R must match ',
                  'prodnames in products.csv'))
    }

    for(i in 1:length(prodcode)){
        col_name = as.character(glue(level[i], '_status'))
        row_num <- which(prods$prodcode == prodcode[i] &
                             prods$prodname %in% prodname_list[[i]])
        prods[row_num, col_name] = status[i]
    }

    # if(network == domain){
    #     write_csv(prods, glue('src/{n}/products.csv', n=network))
    # } else {
    write_csv(x = prods,
              file = glue('src/{n}/{d}/products.csv',
                          n = network,
                          d = domain))
    # }
}

update_product_statuses <- function(network, domain){

    #status_codes should maybe be defined globally, or in a file
    status_codes = c('READY', 'PENDING', 'PAUSED', 'OBSOLETE', 'TEST')
    kf = glue('src/{n}/{d}/processing_kernels.R', n=network, d=domain)
    kernel_lines = read_lines(kf)

    status_line_inds = grep(pattern = '^# ?[^#]\\w.+?STATUS=([A-Z]+)',
                            x = kernel_lines,
                            perl = TRUE)
    mch = stringr::str_match(kernel_lines[status_line_inds],
                             '#(.+?): STATUS=([A-Z]+)')[, 2:3]
    prodnames = mch[, 1, drop=TRUE]
    statuses = mch[, 2, drop=TRUE]

    if(any(! statuses %in% status_codes)){
        stop(glue('Illegal status in ', kf))
    }

    decorator_lines <- grepl(pattern = '^#\\. handle_errors$',
                             x = kernel_lines[status_line_inds + 1])

    if(any(! decorator_lines)){
        stop(glue('missing or improper decorator lines (#. handle_errors) in ',
                  kf))
    }

    funcname_lines = kernel_lines[status_line_inds + 2]

    if(any(! grep('process_[0-2]_.+?', funcname_lines))){
        stop(glue('function definition must begin exactly two lines after STATUS',
                  ' indicator. function must be named "process_<level>_<prodcode>"'))
    }

    func_codes = stringr::str_match(funcname_lines,
                                    'process_([0-2])_(.+)? <-')[, 2:3, drop=FALSE]

    func_lvls = func_codes[, 1, drop=TRUE]
    prodcodes = func_codes[, 2, drop=TRUE]

    level_names = case_when(func_lvls == 0 ~ "retrieve",
                            func_lvls == 1 ~ "munge",
                            func_lvls == 2 ~ "derive")

    status_names = tolower(statuses)

    update_product_file(network = network,
                        domain = domain,
                        level = level_names,
                        prodcode = prodcodes,
                        status = status_names,
                        prodname = prodnames)

    #return()
}

convert_to_gl <- function(x, input_unit, molecule) {

    molecule_real <- ms_vars %>%
        filter(variable_code == !!molecule) %>%
        pull(molecule)

    if(!is.na(molecule_real)) {
        formula <- molecule_real
    } else {
        formula <- molecule
    }

    if(grepl('eq', input_unit)) {
        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]

        if(length(valence) == 0) {stop('Varible is likely missing from ms_vars')}
        x = (x * calculate_molar_mass(formula)) / valence

        return(x)
    }

    if(grepl('mol', input_unit)) {
        x = x * calculate_molar_mass(formula)

        return(x)
    }

    return(x)

}

convert_from_gl <- function(x, input_unit, output_unit, molecule, g_conver) {

    molecule_real <- ms_vars %>%
        filter(variable_code == !!molecule) %>%
        pull(molecule)

    if(!is.na(molecule_real)) {
        formula <- molecule_real
    } else {
        formula <- molecule
    }

    if(grepl('eq', output_unit) && grepl('g', input_unit) ||
       grepl('eq', output_unit) && g_conver) {

        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]
        if(length(valence) == 0 | is.na(valence)) {stop('Varible is likely missing from ms_vars')}
        x = (x * valence) / calculate_molar_mass(formula)

        return(x)
    }

    if(grepl('mol', output_unit) && grepl('g', input_unit) ||
       grepl('mol', output_unit) && g_conver) {

        x = x / calculate_molar_mass(formula)

        return(x)
    }

    if(grepl('mol', output_unit) && grepl('eq', input_unit) && !g_conver) {

        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]
        if(length(valence) == 0) {stop('Varible is likely missing from ms_vars')}
        x = (x * calculate_molar_mass(formula)) / valence

        x = x / calculate_molar_mass(formula)

        return(x)
    }

    if(grepl('eq', output_unit) && grepl('mol', input_unit) && !g_conver) {

        x = x * calculate_molar_mass(formula)

        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]
        if(length(valence) == 0) {stop('Varible is likely missing from ms_vars')}
        x = (x * valence)/calculate_molar_mass(formula)

        return(x)
    }

    return(x)

}

convert_unit <- function(x, input_unit, output_unit){

    units <- tibble(prefix = c('n', "u", "m", "c", "d", "h", "k", "M"),
                    convert_factor = c(0.000000001, 0.000001, 0.001, 0.01, 0.1, 100,
                                       1000, 1000000))

    old_fraction <- as.vector(str_split_fixed(input_unit, "/", n = Inf))
    old_top <- as.vector(str_split_fixed(old_fraction[1], "", n = Inf))

    if(length(old_fraction) == 2) {
        old_bottom <- as.vector(str_split_fixed(old_fraction[2], "", n = Inf))
    }

    new_fraction <- as.vector(str_split_fixed(output_unit, "/", n = Inf))
    new_top <- as.vector(str_split_fixed(new_fraction[1], "", n = Inf))

    if(length(new_fraction == 2)) {
        new_bottom <- as.vector(str_split_fixed(new_fraction[2], "", n = Inf))
    }

    old_top_unit <- str_split_fixed(old_top, "", 2)[1]

    if(old_top_unit %in% c('g', 'e', 'q', 'l') || old_fraction[1] == 'mol') {
        old_top_conver <- 1
    } else {
        old_top_conver <- as.numeric(filter(units, prefix == old_top_unit)[,2])
    }

    old_bottom_unit <- str_split_fixed(old_bottom, "", 2)[1]

    if(old_bottom_unit %in% c('g', 'e', 'q', 'l') || old_fraction[2] == 'mol') {
        old_bottom_conver <- 1
    } else {
        old_bottom_conver <- as.numeric(filter(units, prefix == old_bottom_unit)[,2])
    }

    new_top_unit <- str_split_fixed(new_top, "", 2)[1]

    if(new_top_unit %in% c('g', 'e', 'q', 'l') || new_fraction[1] == 'mol') {
        new_top_conver <- 1
    } else {
        new_top_conver <- as.numeric(filter(units, prefix == new_top_unit)[,2])
    }

    new_bottom_unit <- str_split_fixed(new_bottom, "", 2)[1]

    if(new_bottom_unit %in% c('g', 'e', 'q', 'l') || new_fraction[2] == 'mol') {
        new_bottom_conver <- 1
    } else {
        new_bottom_conver <- as.numeric(filter(units, prefix == new_bottom_unit)[,2])
    }

    new_val <- x*old_top_conver
    new_val <- new_val/new_top_conver

    new_val <- new_val/old_bottom_conver
    new_val <- new_val*new_bottom_conver

    return(new_val)
}

write_ms_file <- function(d,
                          network,
                          domain,
                          prodname_ms,
                          site_code,
                          level = 'munged',
                          shapefile = FALSE,
                          link_to_portal = FALSE,
                          sep_errors = TRUE){

    #write an ms tibble or shapefile to its appropriate destination based on
    #network, domain, prodname_ms, site_code, and processing level. If a tibble,
    #write as a feather file (site_code.feather). Uncertainty (error) associated
    #with the val column will be extracted into a separate column called
    #val_err. Write the file to the appropriate location within the data
    #acquisition repository.

    #deprecated:
    #if link_to_portal == TRUE, create a hard link to the
    #file from the portal repository, which is assumed to be a sibling of the
    #data_acquision directory and to be named "portal".

    if(link_to_portal){
        stop("we're not linking to portal this way anymore. see create_portal_links()")
    }

    if(! level %in% c('munged', 'derived')){
        stop('level must be "munged" or "derived"')
    }

    if(shapefile){

        site_dir = glue('data/{n}/{d}/{l}/{p}/{s}',
                        n = network,
                        d = domain,
                        l = level,
                        p = prodname_ms,
                        s = site_code)

        dir.create(site_dir,
                   showWarnings = FALSE,
                   recursive = TRUE)

        sw(sf::st_write(obj = d,
                        dsn = glue(site_dir, '/', site_code, '.shp'),
                        delete_dsn = TRUE,
                        quiet = TRUE))

    } else {

        prod_dir = glue('data/{n}/{d}/{l}/{p}',
                        n = network,
                        d = domain,
                        l = level,
                        p = prodname_ms)

        dir.create(prod_dir,
                   showWarnings = FALSE,
                   recursive = TRUE)

        site_file = glue('{pd}/{s}.feather',
                         pd = prod_dir,
                         s = site_code)

        if(sep_errors) {

            #separate uncertainty into a new column.
            #remove errors attribute from val column if it exists (it always should)
            d$val_err <- errors(d$val)
            if('errors' %in% class(d$val)){
                d$val <- errors::drop_errors(d$val)
            } else {
                warning(glue('Uncertainty missing from val column ({n}-{d}-{s}-{p}). ',
                             'That means this dataset has not passed through ',
                             'carry_uncertainty yet. it should have.',
                             n = network,
                             d = domain,
                             s = site_code,
                             p = prodname_ms))
            }
        }
        #make sure write_feather will omit attrib by def (with no artifacts)
        write_feather(d, site_file)
    }

    if(link_to_portal){
        create_portal_link(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           site_code = site_code,
                           level = level,
                           dir = shapefile)
    }

    #return()
}

#deprecated (old form of this function is in helper_scrapyard.R)
create_portal_link <- function(network, domain, prodname_ms, site_code,
                               level = 'derived', dir = FALSE){

    #remove this once enough time has passed to be sure all devs are up to speed.

    stop(glue('create_portal_link has been deprecated. use create_portal_links ',
              '(plural)'))
}

create_derived_links <- function(network, domain, prodname_ms, new_prodcode){

    #for hardlinking munged products to the derive directory. this applies to all
    #munged products that require no derive-level processing.

    #new_prodcode is the derive-style prodcode (e.g. ms009) that will be
    #   given to the new links in the derive directory. this is determined
    #   programmatically by ms_derive

    new_prodname_ms <- paste(prodname_from_prodname_ms(prodname_ms),
                             new_prodcode,
                             sep = '__')

    old_loc <- ifelse(is_derived_product(prodname_ms), 'derived', 'munged')

    munge_dir <- glue('data/{n}/{d}/{l}/{p}',
                      n = network,
                      d = domain,
                      p = prodname_ms,
                      l = old_loc)

    derive_dir <- glue('data/{n}/{d}/derived/{p}',
                       n = network,
                       d = domain,
                       p = new_prodname_ms)

    dir.create(derive_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    dirs_to_build <- list.dirs(munge_dir,
                               recursive = TRUE)
    dirs_to_build <- dirs_to_build[dirs_to_build != munge_dir]

    dirs_to_build <- convert_munge_path_to_derive_path(
        paths = dirs_to_build,
        munge_prodname_ms = prodname_ms,
        derive_prodname_ms = new_prodname_ms)

    for(dr in dirs_to_build){
        dir.create(dr,
                   showWarnings = FALSE,
                   recursive = TRUE)
    }

    #"from" and "to" may seem counterintuitive here. keep in mind that files
    #as represented by the OS are actually all hardlinks to inodes in the kernel.
    #so when you make a new hardlink, you're linking *from* a new location
    #*to* an inode, as referenced by an existing hardlink. file.link uses
    #these words in a less realistic, but more intuitive way, i.e. *from*
    #an existing file *to* a new location
    files_to_link_from <- list.files(path = munge_dir,
                                     recursive = TRUE,
                                     full.names = TRUE)

    files_to_link_to <- convert_munge_path_to_derive_path(
        paths = files_to_link_from,
        munge_prodname_ms = prodname_ms,
        derive_prodname_ms = new_prodname_ms)

    for(i in 1:length(files_to_link_from)){
        unlink(files_to_link_to[i])
        invisible(sw(file.link(to = files_to_link_to[i],
                               from = files_to_link_from[i])))
    }

    #return()
}

create_portal_links <- function(network, domain){

    #for hardlinking derived products to the portal directory. this applies to all
    #derived products except cdnr_discharge, usgs_discharge, and pre-idw precipitation.
    #we will eventually let portal users view and download uninterpolated (spatially)
    #rain gauge data, and at that time we may choose to simply delete the sections
    #below (**) that filters those datasets.

    derive_dir <- glue('data/{n}/{d}/derived',
                       n = network,
                       d = domain)

    portal_dir <- glue('../portal/data/{d}',
                       d = domain)

    dir.create(portal_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    dirs_to_ignore <- c()
    dirs_to_build <- list.dirs(derive_dir,
                               recursive = TRUE)
    dirs_to_build <- dirs_to_build[dirs_to_build != derive_dir]

    dirs_to_ignore <- c(dirs_to_ignore,
                        dirs_to_build[grepl(pattern = '(?:/cdnr_discharge|/usgs_discharge)',
                                            x = dirs_to_build)])

    ppaths <- grep(pattern = '/precipitation__ms[0-9]{3}$',
                   x = dirs_to_build,
                   value = TRUE) %>%
        sort()

    if(length(ppaths) > 2){
        stop("more than 2 derived precipitation products? what's going on here?")
    }

    # ** here's the first section referenced in the docstring above ---
    if(length(ppaths) == 2){
        dirs_to_ignore <- c(dirs_to_ignore, ppaths[1])
    }
    # see the next double-asterisk for the second and final section---

    dirs_to_build <- dirs_to_build[! dirs_to_build %in% dirs_to_ignore]
    dirs_to_build <- convert_derive_path_to_portal_path(paths = dirs_to_build)

    for(dr in dirs_to_build){
        dir.create(dr,
                   showWarnings = FALSE,
                   recursive = TRUE)
    }

    #"from" and "to" may seem counterintuitive here. keep in mind that files
    #as represented by the OS are actually all hardlinks to inodes in the kernel.
    #so when you make a new hardlink, you're linking *from* a new location
    #*to* an inode, as referenced by an existing hardlink. file.link uses
    #these words in a less realistic, but more intuitive way, i.e. *from*
    #an existing file *to* a new location
    files_to_link_from <- list.files(path = derive_dir,
                                     recursive = TRUE,
                                     full.names = TRUE)

    # ** here's the second and final section referenced in the docstring above ---
    if(length(dirs_to_ignore)){
        files_to_link_from <- files_to_link_from[! grepl(
            pattern = paste0('(?:',
                             paste(paste0('^',
                                   dirs_to_ignore),
                             collapse = '|'),
                             ')'),
            x = files_to_link_from)]
    }
    # ---

    files_to_link_to <- convert_derive_path_to_portal_path(
        paths = files_to_link_from)

    for(i in 1:length(files_to_link_from)){
        unlink(files_to_link_to[i])
        invisible(sw(file.link(to = files_to_link_to[i],
                               from = files_to_link_from[i])))
    }
}

convert_munge_path_to_derive_path <- function(paths,
                                              munge_prodname_ms,
                                              derive_prodname_ms){

    #paths: strings containing filepath information. expected words are
    #   "munged" and a readable prodname_ms. something like
    #   "data/lter/hbef/munged/ws_boundary__94/w1"
    #munge_prodname_ms: the prodname_ms for this product in its munged form.
    #   e.g. "ws_boundary__94"
    #derive_prodname_ms: the prodname_ms for this product in its derived form
    #   (may be the same as munge_prodname_ms), e.g. "ws_boundary__ms005"

    paths <- gsub(pattern = 'munged',
                  replacement = 'derived',
                  x = paths)

    paths <- gsub(pattern = paste0('__',
                                   prodcode_from_prodname_ms(munge_prodname_ms)),
                  replacement = paste0('__',
                                       prodcode_from_prodname_ms(derive_prodname_ms)),
                  x = paths)

    return(paths)
}

convert_derive_path_to_portal_path <- function(paths){

    #paths: strings containing filepath information. expected words are
    #   "derived" and a readable prodname_ms. something like
    #   "data/lter/hbef/derived/discharge__ms005" or
    #   "data/lter/hbef/derived/precip_gauge_locations__ms006/RG1"

    paths <- gsub(pattern = paste0('data/', network),
                  replacement = '../portal/data',
                  x = paths)

    paths <- gsub(pattern = 'derived/',
                  replacement = '',
                  x = paths)

    paths <- gsub(pattern = '__ms[0-9]{3}',
                  replacement = '',
                  x = paths,
                  perl = TRUE)

    return(paths)
}

is_ms_prodcode <- function(prodcode){

    #always specify macrosheds "pseudo product codes" as "msXXX" where
    #X is zero-padded integer. these codes are used for derived products
    #that don't exist within the data source.

    return(grepl('ms[0-9]{3}', prodcode))
}

ms_list_files <- function(network, domain, prodname_ms){

    #level is either "munged" or "derived"
    #prodname_ms can be a single string or a vector

    level <- ifelse(is_derived_product(prodname_ms), 'derived', 'munged')

    files <- glue('data/{n}/{d}/{l}/{p}',
                  n = network,
                  d = domain,
                  l = level,
                  p = prodname_ms) %>%
        list.files(full.names = TRUE)

    return(files)
}

fname_from_fpath <- function(paths, include_fext = TRUE){

    #paths is a vector of filepaths of/this/form. final slash is used to
    #delineate file name.

    #if include_fext == FALSE, file extension will not be included

    fnames <- vapply(strsplit(paths, '/'),
                     function(x) x[length(x)],
                     FUN.VALUE = '')

    if(! include_fext){

        # This was not working for sites that have a "." in their name
        #fnames <- str_match(fnames,'(.*?)\\..*')[, 2]

        fnames <- str_split_fixed(fnames, '.feather', n = Inf)[, 1]
    }

    return(fnames)
}

#still in progress
delineate_watershed_nhd <- function(lat, long) {

    #this function delineates a watershed from a point first using NHD tools.
    #if that fails, it falls back on a more general whitebox method, which is
    #implemented in delineate_watershed. the NHD method, if implemented, should
    #correct for reach proportional distance of each site location. see
    # https://github.com/vlahm/watershed_tools/blob/master/2_batch_summary_nhd.R
    #also at that link, there's a function for retrieving COMID by lat/long, which
    #could replace discover_nhdplus_id below (which doesn't always seem to work?)

    #there's also this streamstats approach (fully packaged),
    #   but that's incomplete even for CONUS
    # x = streamstats::delineateWatershed(xlocation = long,
    #                                     ylocation = lat,
    #                                     crs = crs)
    # streamstats::leafletWatershed(x)

    site <- tibble(x = lat,
                   y = long) %>%
        sf::st_as_sf(coords = c("y", "x"), crs = 4269) %>%
        sf::st_transform(102008)

    start_comid <- nhdplusTools::discover_nhdplus_id(sf::st_sfc(
        sf::st_point(c(long, lat)), crs = 4269))

    flowline <- nhdplusTools::navigate_nldi(list(featureSource = "comid",
                                                 featureID = start_comid),
                                            mode = "upstreamTributaries",
                                            data_source = "")

    subset_file <- tempfile(fileext = ".gpkg")

    subset <- nhdplusTools::subset_nhdplus(comids = flowline$nhdplus_comid,
                                           output_file = subset_file,
                                           nhdplus_data = "download",
                                           return_data = TRUE)

    flowlines <- subset$NHDFlowline_Network %>%
        sf::st_transform(102008)

    catchments <- subset$CatchmentSP %>%
        sf::st_transform(102008)

    upstream <- nhdplusTools::get_UT(flowlines, start_comid)

    watershed <- catchments %>%
        filter(featureid %in% upstream) %>%
        sf::st_buffer(0.01) %>%
        sf::st_union() %>%
        sf::st_as_sf()

    if(as.numeric(sf::st_area(watershed)) >= 60000000) {
        return(watershed)
    }
    else {

        outline = sf::st_as_sfc(sf::st_bbox(flowlines))

        outline_buff <- outline %>%
            sf::st_buffer(5000)

        dem <- expo_backoff(
            expr = {
                elevatr::get_elev_raster(locations = as(outline_buff, 'Spatial'),
                                         z = 12,
                                         verbose = FALSE,
                                         override_size_check = TRUE)
            },
            max_attempts = 5
        )

        temp_raster <- tempfile(fileext = ".tif")

        raster::writeRaster(dem, temp_raster, overwrite = T)

        temp_point <- tempfile(fileext = ".shp")

        sf::st_write(sf::st_zm(site), temp_point, delete_layer=TRUE)

        temp_breash2 <- tempfile(fileext = ".tif")
        whitebox::wbt_fill_single_cell_pits(temp_raster, temp_breash2)

        temp_breached <- tempfile(fileext = ".tif")
        whitebox::wbt_breach_depressions(temp_breash2,temp_breached,flat_increment=.01)

        temp_d8_pntr <- tempfile(fileext = ".tif")
        whitebox::wbt_d8_pointer(temp_breached,temp_d8_pntr)

        temp_shed <- tempfile(fileext = ".tif")
        whitebox::wbt_unnest_basins(temp_d8_pntr, temp_point, temp_shed)

        # No idea why but wbt_unnest_basins() aves whatever file path with a _1 after the name so must add
        file_new <- str_split_fixed(temp_shed, "[.]", n = 2)

        file_shed <- paste0(file_new[1], "_1.", file_new[2])

        check <- raster::raster(file_shed)
        values <- raster::getValues(check)
        values[is.na(values)] <- 0


        if(sum(values, na.rm = TRUE) < 100) {

            flow <- tempfile(fileext = ".tif")
            whitebox::wbt_d8_flow_accumulation(temp_breached,flow,out_type='catchment area')

            snap <- tempfile(fileext = ".shp")
            whitebox::wbt_snap_pour_points(temp_point, flow, snap, 50)

            temp_shed <- tempfile(fileext = ".tif")
            whitebox::wbt_unnest_basins(temp_d8_pntr, snap, temp_shed)

            file_new <- str_split_fixed(temp_shed, "[.]", n = 2)

            file_shed <- paste0(file_new[1], "_1.", file_new[2])

            check <- raster::raster(file_shed)
            values <- raster::getValues(check)
            values[is.na(values)] <- 0
        }
        watershed_raster <- raster::rasterToPolygons(raster::raster(file_shed))

        #Convert shapefile to sf
        watershed_df <- sf::st_as_sf(watershed_raster)

        #buffer to join all pixles into one shape
        watershed <- sf::st_buffer(watershed_df, 0.1) %>%
            sf::st_union() %>%
            sf::st_as_sf()

        if(sum(values, na.rm = T) < 100) {
            watershed <- watershed %>%
                mutate(flag = "check")
        }

        #sf::st_write(watershed_union, dsn =
        #                glue("data/{n}/{d}/geospatial/ws_boundaries/{s}.shp",
        #                    n = sites$network, d = sites$domain, s = sites$site_code))
        return(watershed)

    }
}

calc_inst_flux <- function(chemprod, qprod, site_code, ignore_pred = FALSE){

    #chemprod is the prodname_ms for stream or precip chemistry.
    #   it can be a munged or a derived product.
    #qprod is the prodname_ms for stream discharge or precip volume over time.
    #   it can be a munged or derived product.
    #ignore_pred: logical; set to TRUE if detection limits should be retrieved
    #   from the supplied chemprod directly, rather than its precursor. passed to
    #   apply_detection_limit_t.

    if(! grepl('(precipitation|discharge)', prodname_from_prodname_ms(qprod))){
        stop('Could not determine stream/precip')
    }

    flux_vars <- ms_vars %>% #ms_vars is global
        filter(flux_convertible == 1) %>%
        pull(variable_code)

    chem <- read_combine_feathers(network = network,
                                  domain = domain,
                                  prodname_ms = chemprod) %>%
        filter(site_code == !!site_code,
               drop_var_prefix(var) %in% flux_vars)

    if(nrow(chem) == 0) return(NULL)

    # chem <- chem %>%
    #     tidyr::pivot_wider(
    #         names_from = 'var',
    #         values_from = all_of(c('val', 'ms_status', 'ms_interp'))) %>%
    #     select(datetime, starts_with(c('val', 'ms_status', 'ms_interp')))

    daterange <- range(chem$datetime)

    flow <- read_combine_feathers(network = network,
                                  domain = domain,
                                  prodname_ms = qprod) %>%
        filter(
            site_code == !!site_code,
            datetime >= !!daterange[1],
            datetime <= !!daterange[2])# %>%
        # rename(flow = val) %>% #quick and dirty way to convert to wide
        # rename(!!drop_var_prefix(.$var[1]) := val) %>%
        # select(-var, -site_code)

    if(nrow(flow) == 0) return(NULL)
    flow_is_highres <- Mode(diff(as.numeric(flow$datetime))) <= 15 * 60
    if(is.na(flow_is_highres)) flow_is_highres <- FALSE

    chem_split <- chem %>%
        group_by(var) %>%
        arrange(datetime) %>%
        dplyr::group_split() %>%
        as.list()

    #could do this in parallel (using furrr or foreach), but that might
    #incur the same OOM issues as doing it all at once in wide-format. worth trying,
    #but only use half of the available threads
    for(i in 1:length(chem_split)){

        chem_chunk <- chem_split[[i]]

        chem_is_highres <- Mode(diff(as.numeric(chem_chunk$datetime))) <= 15 * 60
        if(is.na(chem_is_highres)) chem_is_highres <- FALSE

        #if both chem and flow data are low resolution (grab samples),
        #   let approxjoin_datetime match up samples with a 12-hour gap. otherwise the
        #   gap should be 7.5 mins so that there isn't enormous duplication of
        #   timestamps where multiple high-res values can be snapped to the
        #   same low-res value
        if(! chem_is_highres && ! flow_is_highres){
            join_distance <- c('12:00:00')#, '%H:%M:%S')
        } else {
            join_distance <- c('7:30')#, '%M:%S')
        }

        chem_split[[i]] <- approxjoin_datetime(x = chem_chunk,
                                               y = flow,
                                               rollmax = join_distance,
                                               keep_datetimes_from = 'x') %>%
            mutate(site_code = site_code_x,
                   var = var_x,
                #  kg/d = mg/L *  L/s  * 86400 / 1e6
                   val = val_x * val_y * 86400 / 1e6,
                   ms_status = numeric_any_v(ms_status_x, ms_status_y),
                   ms_interp = numeric_any_v(ms_interp_x, ms_interp_y)) %>%
            select(-starts_with(c('site_code_', 'var_', 'val_',
                                  'ms_status_', 'ms_interp_'))) %>%
            filter(! is.na(val)) %>% #should be redundant
            arrange(datetime)
    }

    # #a few commented remnants from the old wide-format days have been left here,
    # #because they might be instructive in other endeavors
    # flux <- chem %>%
    #
    #     #if we ever have dependency issues with fuzzyjoin functions, we should
    #     #   implement a data.table rolling join. We'll just have to pop off
    #     #   the uncertainty in a separate tibble, do the join, noting which
    #     #   datetime series is being modified, then rejoin the uncertainty.
    #     fuzzyjoin::difference_inner_join(
    #         flow,
    #         by = 'datetime',
    #         max_dist = as.difftime(tim = '14:59',
    #                                format = '%M:%S')
    #     ) %>%
    #     select(-datetime.y) %>%
    #     rename(datetime = datetime.x) %>%
    #     # group_by(datetime) %>%
    #     # summarize(,
    #     #           .groups = 'drop') %>%
    #     select_if(~(! all(is.na(.)))) %>%
    #
    #     # rowwise(datetime) %>%
    #     # mutate(
    #     #     ms_interp = numeric_any(c_across(c(ms_interp.x, ms_interp.y))),
    #     #     ms_status = numeric_any(c_across(c(ms_status.x, ms_status.y)))) %>%
    #     # ungroup() %>%
    #     # select(-ms_status.x, -ms_status.y, -ms_interp.x, -ms_interp.y) %>%
    #     # mutate_at(vars(-datetime, -flow, -ms_status, -ms_interp),
    #     #           ~(. * flow)) %>%
    #     # pivot_longer(cols = ! c(datetime, ms_status, ms_interp),
    #     #              names_pattern = '(.*)',
    #     #              names_to = 'var') %>%
    #     # rename(val = value) %>%
    #
    #     mutate(
    #         across(.cols = matches(match = '^ms_status.+',
    #                                perl = TRUE),
    #                .fns = ~numeric_any(na.omit(c(.x, ms_status)))),
    #         across(.cols = matches(match = '^ms_interp.+',
    #                                perl = TRUE),
    #                .fns = ~numeric_any(na.omit(c(.x, ms_interp)))),
    #         across(.cols = starts_with(match = 'val_'),
    #                .fns = ~(.x * flow))) %>%
    #     select(-ms_status, -ms_interp, -flow) %>%
    #     pivot_longer(cols = ! datetime,
    #                  names_pattern = '^(val|ms_status|ms_interp)_(.*)$',
    #                  names_to = c('.value', 'var')) %>%
    #
    #     filter(! is.na(val)) %>%
    #     mutate(site_code = !!site_code) %>%
    #     arrange(site_code, var, datetime) %>%
    #     select(datetime, site_code, var, val, ms_status, ms_interp)

    flux <- chem_split %>%
        purrr::reduce(bind_rows) %>%
        arrange(site_code, var, datetime)
        # select(datetime, site_code, var, val, ms_status, ms_interp)

    if(nrow(flux) == 0) return(NULL)

    flux <- apply_detection_limit_t(X = flux,
                                    network = network,
                                    domain = domain,
                                    prodname_ms = chemprod,
                                    ignore_pred = ignore_pred)

    return(flux)
}

read_combine_shapefiles <- function(network, domain, prodname_ms){

    #TODO: sometimes multiple locations are listed for the same rain gauge.
    #   if there's a date associated with those locations, we should take that
    #   into account when performing IDW, and when plotting gauges on the map.
    #   (maybe previous locations could show up semitransparent). for now,
    #   we're just grabbing the last location listed (by order). We don't store
    #   date columns with our raingauge shapes yet, so that's the place to start.

    #TODO: also, this may be a problem for other spatial stuff.

    level <- ifelse(is_derived_product(prodname_ms),
                    'derived',
                    'munged')

    prodpaths <- list.files(glue('data/{n}/{d}/{l}/{p}',
                                 n = network,
                                 d = domain,
                                 l = level,
                                 p = prodname_ms),
                            recursive = TRUE,
                            full.names = TRUE,
                            pattern = '*.shp')

    shapes <- lapply(prodpaths,
                     function(x){
                         sf::st_read(x,
                                     stringsAsFactors = FALSE,
                                     quiet = TRUE) %>%
                             slice_tail()
                     })

    # wb <- sw(Reduce(sf::st_union, wbs)) %>%
    combined <- sw(Reduce(bind_rows, shapes))
    # sf::st_transform(projstring)

    return(combined)
}

is_derived_product <- function(prodname_ms){

    is_derived <- grepl('^ms[0-9]{3}$',
                        prodcode_from_prodname_ms(prodname_ms),
                        perl = TRUE)

    return(is_derived)
}

read_combine_feathers <- function(network,
                                  domain,
                                  prodname_ms){

    #read all data feathers associated with a network-domain-product,
    #row bind them, arrange by site_code, var, datetime. insert val_err column
    #into the val column as errors attribute and then remove val_err column
    #(error/uncertainty is handled by the errors package as an attribute,
    #so it must be written/read as a separate column).

    #the processing level is determined automatically from prodname_ms.
    #   If the product code is "msXXX" where X is a numeral, the processing
    #   level is assumed to be "derived". otherwise "munged"

    #handled in ms_list_files
    # level <- ifelse(is_derived_product(prodname_ms),
    #                 'derived',
    #                 'munged')

    prodpaths <- ms_list_files(network = network,
                               domain = domain,
                               prodname_ms = prodname_ms)

    combined <- tibble()
    for(i in 1:length(prodpaths)){
        part <- read_feather(prodpaths[i])
        combined <- bind_rows(combined, part)
    }

    combined <- combined %>%
        mutate(val = errors::set_errors(val, val_err)) %>%
        select(-val_err) %>%
        arrange(site_code, var, datetime)

    return(combined)
}

choose_projection <- function(lat = NULL,
                              long = NULL,
                              unprojected = FALSE){

    #TODO: CHOOSE PROJECTIONS MORE CAREFULLY

    if(unprojected){
        PROJ4 <- glue('+proj=longlat +datum=WGS84 +no_defs ',
                      '+ellps=WGS84 +towgs84=0,0,0')
        return(PROJ4)
    }

    if(is.null(lat) || is.null(long)){
        stop('If projecting, lat and long are required.')
    }

    abslat <- abs(lat)

    # if(abslat < 23){ #tropical
    #     PROJ4 = glue('+proj=laea +lon_0=', long)
    #              # ' +datum=WGS84 +units=m +no_defs')
    # } else { #temperate or polar
    #     PROJ4 = glue('+proj=laea +lat_0=', lat, ' +lon_0=', long)
    # }

    #this is what the makers of https://projectionwizard.org/# use to choose
    #a suitable projection: https://rdrr.io/cran/rCAT/man/simProjWiz.html
    # THIS WORKS (PROJECTS STUFF), BUT CAN'T BE READ AUTOMATICALLY BY st_read
    if(abslat < 70){ #tropical or temperate
        PROJ4 <- glue('+proj=cea +lon_0={lng} +lat_ts=0 +x_0=0 +y_0=0 ',
                      '+ellps=WGS84 +datum=WGS84 +units=m +no_defs',
                      lng = long)
    } else { #polar
        PROJ4 <- glue('+proj=laea +lat_0={lt} +lon_0={lng} +x_0=0 +y_0=0 ',
                      '+ellps=WGS84 +datum=WGS84 +units=m +no_defs',
                      lt = lat,
                      lng = long)
    }

    ## UTM/UPS would be nice for watersheds that don't fall on more than two zones
    ## (incomplete)
    # if(lat > 84 || lat < -80){ #polar; use Universal Polar Stereographic (UPS)
    #     PROJ4 <- glue('+proj=ups +lon_0=', long)
    #              # ' +datum=WGS84 +units=m +no_defs')
    # } else { #not polar; use UTM
    #     PROJ4 <- glue('+proj=utm +lat_0=', lat, ' +lon_0=', long)
    # }

    ## EXTRA CODE FOR CHOOSING PROJECTION BY LATITUDE ONLY
    # if(abslat < 23){ #tropical
    #     PROJ4 <- 9835 #Lambert cylindrical equal area (ellipsoidal; should spherical 9834 be used instead?)
    # } else if(abslat > 23 && abslat < 66){ # middle latitudes
    #     PROJ4 <- 5070 #albers equal area conic
    # } else { #polar (abslat >= 66)
    #     PROJ4 <- 9820 #lambert equal area azimuthal
    #     # PROJ4 <- 1027 #lambert equal area azimuthal (spherical)
    # }
    # PROJ4 <- 3857 #WGS 84 / Pseudo-Mercator
    # PROJ4 <- 2163

    return(PROJ4)
}

reconstitute_raster <- function(x, template){

    m = matrix(as.vector(x),
               nrow=nrow(template),
               ncol=ncol(template),
               byrow=TRUE)
    r <- raster(m,
                crs=raster::projection(template))
    extent(r) <- raster::extent(template)

    return(r)
}

shortcut_idw <- function(encompassing_dem,
                         wshd_bnd,
                         data_locations,
                         data_values,
                         durations_between_samples = NULL,
                         stream_site_code,
                         output_varname,
                         save_precip_quickref = FALSE,
                         elev_agnostic = FALSE,
                         verbose = FALSE){

    #encompassing_dem: RasterLayer must cover the area of wshd_bnd and precip_gauges
    #wshd_bnd: sf polygon with columns site_code and geometry
    #   it represents a single watershed boundary
    #data_locations:sf point(s) with columns site_code and geometry.
    #   it represents all sites (e.g. rain gauges) that will be used in
    #   the interpolation
    #data_values: data.frame with one column each for datetime and ms_status,
    #   and an additional named column of data values for each data location.
    #durations_between_samples: numeric vector representing the time differences
    #   between rows of data_values. Must be expressed in days. Only used if
    #   output_varname == 'SPECIAL CASE PRECIP' and save_precip_quickref == TRUE,
    #   so that quickref data can be expressed in mm/day, which is the unit
    #   required to calculate precip flux.
    #   NOTE: this could be calculated internally, but the duration of measurement
    #   preceding the first value can't be known. Because shortcut_idw is
    #   often run iteratively on chunks of a dataset, we require that
    #   durations_between_samples be passed as an input, so as to minimize
    #   the number of NAs generated.
    #output_varname: character; a prodname_ms, unless you're interpolating
    #   precipitation, in which case it must be "SPECIAL CASE PRECIP", because
    #   prefix information for precip is lost during the widen-by-site step
    #save_precip_quickref: logical. should interpolated precip for all DEM cells
    #   be saved for later use. Should only be true when precip chem will be
    #   interpolated too. only useable when output_varname = 'PRECIP SPECIAL CASE'
    #elev_agnostic: logical that determines whether elevation should be
    #   included as a predictor of the variable being interpolated

    # loginfo(glue('shortcut_idw: working on {ss}', ss=stream_site_code),
    #     logger = logger_module)

    if(output_varname != 'SPECIAL CASE PRECIP' && save_precip_quickref){
        stop(paste('save_precip_quickref can only be TRUE if output_varname',
                   '== "SPECIAL CASE PRECIP"'))
    }
    if(save_precip_quickref && is.null(durations_between_samples)){
        stop(paste('save_precip_quickref can only be TRUE if',
                    'durations_between_samples is supplied.'))
    }
    if(output_varname != 'SPECIAL CASE PRECIP' && ! is.null(durations_between_samples)){
        logwarn(msg = paste('In shortcut_idw: ignoring durations_between_samples because',
                            'output_varname != "SPECIAL CASE PRECIP".'),
                logger = logger_module)
    }

    if('ind' %in% colnames(data_values)){
        timestep_indices <- data_values$ind
        data_values$ind <- NULL
    }

    #matrixify input data so we can use matrix operations
    d_status <- data_values$ms_status
    d_interp <- data_values$ms_interp
    d_dt <- data_values$datetime
    data_matrix <- select(data_values,
                          -ms_status,
                          -datetime,
                          -ms_interp) %>%
        err_df_to_matrix()

    #clean dem and get elevation values
    dem_wb <- terra::crop(encompassing_dem, wshd_bnd)
    dem_wb <- terra::mask(dem_wb, wshd_bnd)
    elevs <- terra::values(dem_wb)

    #compute distances from all dem cells to all data locations
    inv_distmat <- matrix(NA, nrow = length(dem_wb), ncol = ncol(data_matrix),
                          dimnames = list(NULL, colnames(data_matrix)))
    for(k in 1:ncol(data_matrix)){
        dk <- filter(data_locations, site_code == colnames(data_matrix)[k])
        inv_dist2 <- 1 / raster::distanceFromPoints(dem_wb, dk)^2 %>%
            terra::values(.)
        inv_dist2[is.na(elevs)] <- NA #mask
        inv_distmat[, k] <- inv_dist2
    }

    # if(output_varname == 'SPECIAL CASE PRECIP'){ REMOVE
    #     precip_quickref <- data.frame(matrix(NA,
    #                                          nrow = ntimesteps,
    #                                          ncol = nrow(inv_distmat)))
    # }

    #calculate watershed mean at every timestep
    if(save_precip_quickref) precip_quickref <- list()
    ptm <- proc.time()
    ws_mean <- rep(NA, nrow(data_matrix))
    ntimesteps <- nrow(data_matrix)
    for(k in 1:ntimesteps){

        # idw_log_timestep(verbose = verbose,
        #                  site_code = stream_site_code,
        #                  v = output_varname,
        #                  k = k,
        #                  ntimesteps = ntimesteps,
        #                  time_elapsed = (proc.time() - ptm)[3] / 60)

        #assign cell weights as normalized inverse squared distances
        dk <- t(data_matrix[k, , drop = FALSE])
        inv_distmat_sub <- inv_distmat[, ! is.na(dk), drop = FALSE]
        dk <- dk[! is.na(dk), , drop = FALSE]
        weightmat <- do.call(rbind, #avoids matrix transposition
                             unlist(apply(inv_distmat_sub, #normalize by row
                                          1,
                                          function(x) list(x / sum(x))),
                                    recursive = FALSE))

        #perform vectorized idw
        dk[is.na(dk)] <- 0 #allows matrix multiplication
        d_idw <- weightmat %*% dk

        if(nrow(dk) == 0){
            ws_mean[k] <- set_errors(NA_real_, NA)
            if(save_precip_quickref){
                precip_quickref[[k]] <- matrix(NA,
                                               nrow = nrow(d_idw),
                                               ncol = ncol(d_idw))
            }
            next
        }

        #reapply uncertainty dropped by `%*%`
        errors(d_idw) <- weightmat %*% matrix(errors(dk),
                                              nrow = nrow(dk))

        #determine data-elevation relationship for interp weighting
        if(! elev_agnostic && nrow(dk) >= 3){
            d_elev <- tibble(site_code = rownames(dk),
                             d = dk[,1]) %>%
                left_join(data_locations,
                          by = 'site_code')
            mod <- lm(d ~ elevation, data = d_elev)
            ab <- as.list(mod$coefficients)

            #estimate raster values from elevation alone
            d_from_elev <- ab$elevation * elevs + ab$`(Intercept)`

            #average both approaches (this should be weighted toward idw
            #when close to any data location, and weighted half and half when far)
            # d_idw <- mapply(function(x, y) mean(c(x, y), na.rm=TRUE),
            #                 d_idw,
            #                 d_from_elev)
            d_idw <- (d_idw + d_from_elev) / 2
        }

        ws_mean[k] <- mean(d_idw, na.rm=TRUE)
        errors(ws_mean)[k] <- mean(errors(d_idw), na.rm=TRUE)

        # quickref_ind <- k %% 1000 REMOVE
        # update_quickref <- quickref_ind == 0
        # if(! update_quickref){
        # precip_quickref[[quickref_ind]] <- p_idw
        if(save_precip_quickref) precip_quickref[[k]] <- d_idw
        # } else {

        # precip_quickref[[1000]] <- p_idw

        # save_precip_quickref(precip_idw_list = precip_quickref,
        #                      network = network,
        #                      domain = domain,
        #                      site_code = stream_site_code,
        #                      # chunk_number = quickref_chunk)
        #                      timestep = k)
        # }
    }

    if(save_precip_quickref){

        #convert to mm/d
        precip_quickref <- Map(function(millimeters, days){
            return(millimeters/days)
        },
            millimeters = precip_quickref,
            days = durations_between_samples)

        names(precip_quickref) <- as.character(timestep_indices)
        write_precip_quickref(precip_idw_list = precip_quickref,
                              network = network,
                              domain = domain,
                              site_code = stream_site_code,
                              chunkdtrange = range(d_dt))
    }
    # compare_interp_methods()

    if(output_varname == 'SPECIAL CASE PRECIP'){


        ws_mean <- tibble(datetime = d_dt,
                          site_code = stream_site_code,
                          concentration = ws_mean,
                          ms_status = d_status,
                          ms_interp = d_interp)

        ws_mean <- reconstruct_var_column(d = ws_mean,
                                          network = network,
                                          domain = domain,
                                          prodname = 'precipitation')
    } else {

        ws_mean <- tibble(datetime = d_dt,
                          site_code = stream_site_code,
                          var = output_varname,
                          concentration = ws_mean,
                          ms_status = d_status,
                          ms_interp = d_interp)
    }

    return(ws_mean)
}

shortcut_idw_concflux_v2 <- function(encompassing_dem,
                                     wshd_bnd,
                                     ws_area,
                                     data_locations,
                                     precip_values,
                                     chem_values,
                                     stream_site_code,
                                     output_varname,
                                     # dump_idw_precip,
                                     verbose = FALSE){

    #This replaces shortcut_idw_concflux! shortcut_idw is still used for
    #variables that can't be flux-converted, and for precipitation

    #this function is similar to shortcut_idw_concflux.
    #if the variable represented by chem_values and output_varname is
    #flux-convertible, it multiplies precip chem
    #by precip volume to calculate flux for each cell and returns
    #the means of IDW-interpolated precipitation, precip chem, and precip flux
    #for each sample timepoint. If that variable is not flux-convertible,
    #It only returns precipitation and precip chem. All interpolated products are
    #returned as standard macrosheds timeseries tibbles in a single list.

    #encompassing_dem: RasterLayer; must cover the area of wshd_bnd and
    #   recip_gauges
    #wshd_bnd: sf polygon with columns site_code and geometry.
    #   it represents a single watershed boundary.
    #ws_area: numeric scalar representing watershed area in hectares. This is
    #   passed so that it doesn't have to be calculated repeatedly (if
    #   shortcut_idw_concflux_v2 is running iteratively).
    #data_locations: sf point(s) with columns site_code and geometry.
    #   represents all sites (e.g. rain gauges) that will be used in
    #   the interpolation.
    #precip_values: a data.frame with datetime, ms_status, ms_interp,
    #   and a column of data values for each precip location.
    #chem_values: a data.frame with datetime, ms_status, ms_interp,
    #   and a column of data values for each precip chemistry location.
    #stream_site_code: character; the name of the watershed/stream, not the
    #   name of a precip gauge
    #output_varname: character; the prodname_ms used to populate the var
    #   column in the returned tibble
    #dump_idw_precip: logical; if TRUE, IDW-interpolated precipitation will
    #   be dumped to disk (data/<network>/<domain>/precip_idw_dumps/<site_code>.rds).
    #   this file will then be read by precip_pchem_pflux_idw, in order to
    #   properly build data/<network>/<domain>/derived/<precipitation_msXXX>.
    #   the precip_idw_dumps directory is automatically removed after it's used.
    #   This should only be set to TRUE for one iteration of the calling loop,
    #   or else time will be wasted rewriting the files.
    #   REMOVED; OBSOLETE

    precip_quickref <- read_precip_quickref(network = network,
                                            domain = domain,
                                            site_code = stream_site_code,
                                            # dtrange = as.POSIXct(c('1968-01-01', '1976-06-01'), tz='UTC'))
                                            dtrange = range(chem_values$datetime))

    if(length(precip_quickref) == 1){

        just_checkin <- precip_quickref[[1]]

        if(class(just_checkin) == 'character' &&
            just_checkin == 'NO QUICKREF AVAILABLE'){

            return(tibble())
        }
    }

    precip_is_highres <- Mode(diff(as.numeric(precip_values$datetime))) <= 15 * 60
    if(is.na(precip_is_highres)) precip_is_highres <- FALSE
    chem_is_highres <- Mode(diff(as.numeric(chem_values$datetime))) <= 15 * 60
    if(is.na(chem_is_highres)) chem_is_highres <- FALSE

    #if both chem and precip data are low resolution (grab samples),
    #   let approxjoin_datetime match up samples with a 12-hour gap. otherwise the
    #   gap should be 7.5 mins so that there isn't enormous duplication of
    #   timestamps where multiple high-res values can be snapped to the
    #   same low-res value
    if(! chem_is_highres && ! precip_is_highres){
        join_distance <- c('12:00:00')
    } else {
        join_distance <- c('7:30')
    }

    dt_match_inds <- approxjoin_datetime(x = chem_values,
                                         y = precip_values,
                                         rollmax = join_distance,
                                         indices_only = TRUE)

    chem_values <- chem_values[dt_match_inds$x, ]
    common_datetimes <- chem_values$datetime

    if(length(common_datetimes) == 0){
        pchem_range <- range(chem_values$datetime)
        test <- filter(precip_values,
                       datetime > pchem_range[1],
                       datetime < pchem_range[2])
        if(nrow(test) > 0){
            logging::logerror('something is wrong with approxjoin_datetime')
        }
        return(tibble())
    }

    precip_values <- precip_values %>%
        mutate(ind = 1:n()) %>%
        slice(dt_match_inds$y) %>%
        mutate(datetime = !!common_datetimes)

    quickref_inds <- precip_values$ind
    precip_values$ind <- NULL

    #matrixify input data so we can use matrix operations
    p_status <- precip_values$ms_status
    p_interp <- precip_values$ms_interp
    p_matrix <- select(precip_values,
                       -ms_status,
                       -datetime,
                       -ms_interp) %>%
        err_df_to_matrix()

    c_status <- chem_values$ms_status
    c_interp <- chem_values$ms_interp
    c_matrix <- select(chem_values,
                       -ms_status,
                       -datetime,
                       -ms_interp) %>%
        err_df_to_matrix()

    rm(precip_values, chem_values); gc()

    d_status <- bitwOr(p_status, c_status)
    d_interp <- bitwOr(p_interp, c_interp)

    #clean dem and get elevation values
    dem_wb <- terra::crop(encompassing_dem, wshd_bnd)
    dem_wb <- terra::mask(dem_wb, wshd_bnd)
    elevs <- terra::values(dem_wb)

    #compute distances from all dem cells to all chemistry locations
    inv_distmat_c <- matrix(NA,
                            nrow = length(dem_wb),
                            ncol = ncol(c_matrix), #ngauges
                            dimnames = list(NULL,
                                            colnames(c_matrix)))

    for(k in 1:ncol(c_matrix)){
        dk <- filter(data_locations,
                     site_code == colnames(c_matrix)[k])
        inv_dist2 <- 1 / raster::distanceFromPoints(dem_wb, dk)^2 %>%
            terra::values(.)
        inv_dist2[is.na(elevs)] <- NA
        inv_distmat_c[, k] <- inv_dist2
    }

    #calculate watershed mean concentration and flux at every timestep
    ptm <- proc.time()
    ntimesteps <- nrow(c_matrix)
    ws_mean_conc <- ws_mean_flux <- rep(NA, ntimesteps)

    for(k in 1:ntimesteps){

        ## GET CHEMISTRY FOR ALL CELLS IN TIMESTEP k

        #assign cell weights as normalized inverse squared distances (c)
        ck <- t(c_matrix[k, , drop = FALSE])
        inv_distmat_c_sub <- inv_distmat_c[, ! is.na(ck), drop=FALSE]
        ck <- ck[! is.na(ck), , drop=FALSE]
        weightmat_c <- do.call(rbind,
                               unlist(apply(inv_distmat_c_sub,
                                            1,
                                            function(x) list(x / sum(x))),
                                      recursive = FALSE))

        #perform vectorized idw (c)
        ck[is.na(ck)] <- 0
        c_idw <- weightmat_c %*% ck

        #reapply uncertainty dropped by `%*%`
        errors(c_idw) <- weightmat_c %*% matrix(errors(ck),
                                                nrow = nrow(ck))

        ## GET FLUX FOR ALL CELLS; THEN AVERAGE CELLS FOR PCHEM, PFLUX, PRECIP

        #calculate flux for every cell:
        #   This is how we'd calcualate flux as kg/(ha * d):
        #       mm/d * mg/L * m/1000mm * kg/1,000,000mg * 1000L/m^(2 + 1) * 10,000m^2/ha
        #       therefore, kg/(ha * d) = mm * mg/L / d / 100 = (mm * mg) / (d * L * 100)
        #   But stream_flux_inst is not scaled by area when it's derived (that
        #   happens later), so we're going to derive precip_flux_inst
        #   as unscaled here, and then both can be scaled the same way in
        #   scale_flux_by_area. So we calculate flux in kg/(ha * d) as above,
        #   then multiply by watershed area in hectares.

        quickref_ind <- as.character(quickref_inds[k])
        #              mg/L        mm/day                          ha
        flux_interp <- c_idw * precip_quickref[[quickref_ind]] * ws_area / 100

        #calculate watershed averages (work around error drop)
        ws_mean_conc[k] <- mean(c_idw, na.rm=TRUE)
        ws_mean_flux[k] <- mean(flux_interp, na.rm=TRUE)
        errors(ws_mean_conc)[k] <- mean(errors(c_idw), na.rm=TRUE)
        errors(ws_mean_flux)[k] <- mean(errors(flux_interp), na.rm=TRUE)
    }

    # compare_interp_methods()

    ws_means <- tibble(datetime = common_datetimes,
                       site_code = stream_site_code,
                       var = output_varname,
                       concentration = ws_mean_conc,
                       flux = ws_mean_flux,
                       ms_status = d_status,
                       ms_interp = d_interp)

    return(ws_means)
}

reconstruct_var_column <- function(d,
                                   network,
                                   domain,
                                   prodname,
                                   level = 'munged'){

    #currently only used inside the precip idw interpolator, where
    #we no longer have variable prefix information. this attempts
    #to determine that information from the stored detlim file.
    #it's not yet equipped to handle the case where precipitation
    #(or any other variable) has multiple prefixes through time.

    #returns d with a new var column

    if(! level %in% c('munged', 'derived')){
        stop('level must be either "munged" or "derived"')
    }

    prods <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = network,
                              d = domain)))

    rgx <- ifelse(level == 'munged',
                  '^(?!ms[0-9]{3}).*?',
                  '^ms[0-9]{3}$')

    prodname_ms <- prods %>%
        filter(prodname == !!prodname,
               grepl(pattern = rgx,
                     x = prodcode,
                     perl = TRUE)) %>%
        mutate(prodname_ms = paste(prodname,
                                   prodcode,
                                   sep = '__')) %>%
        pull(prodname_ms)

    detlim <- tryCatch(
        {
            if(length(prodname_ms) > 1){
                knit_det_limits(network = network,
                                domain = domain,
                                prodname_ms = prodname_ms)
            } else {
                read_detection_limit(network = network,
                                     domain = domain,
                                     prodname_ms = prodname_ms)
            }
        },
        error = function(e){
            stop(glue('could not read detection limits, which are ',
                      'needed for reconstructing the var column'))
        }
    )

    if(length(detlim) == 1){
        var <- names(detlim)
    } else {
        # # d <<- d
        # vv <<- detlim
        # detlim = vv
        stop(glue('Not sure if we\'ll ever encounter this, but if ',
                  'so we need to build it now!'))
    }

    d <- d %>%
        mutate(var = !!var) %>%
        select(datetime, site_code, var,
               any_of(x = c('val', 'concentration', 'flux')),
               ms_status, ms_interp)

    return(d)
}

dump_precip_idw_tempfile <- function(ws_means,
                                     network,
                                     domain,
                                     site_code){

    #not to be confused with precip quickref, the tempfile (dumpfile) communicates
    #precipitation data used in flux calculation up the callstack from
    #shortcut_idw_concflux_v2 to precip_pchem_pflux_idw, so that the precip
    #product can be generated for free, as a byproduct

    dumpdir <- glue('data/{n}/{d}/precip_idw_dumps/',
                    n = network,
                    d = domain)

    dir.create(dumpdir,
               showWarnings = FALSE,
               recursive = TRUE)

    saveRDS(object = ws_means,
            file = glue('{dd}/{s}.rds',
                        dd = dumpdir,
                        s = site_code))
}

load_precip_idw_tempfile <- function(network,
                                     domain,
                                     site_code){

    #not to be confused with precip quickref, the tempfile (dumpfile) communicates
    #precipitation data used in flux calculation up the callstack from
    #shortcut_idw_concflux_v2 to precip_pchem_pflux_idw, so that the precip
    #product can be generated for free, as a byproduct

    dumpfile <- glue('data/{n}/{d}/precip_idw_dumps/{s}.rds',
                     n = network,
                     d = domain,
                     s = site_code)

    ws_means <- readRDS(dumpfile)

    unlink(dumpfile)

    return(ws_means)
}

write_precip_quickref <- function(precip_idw_list,
                                  network,
                                  domain,
                                  site_code,
                                  chunkdtrange){
                                 # timestep){

    #allows precip values computed by shortcut_idw for each watershed
    #   raster cell to be reused by shortcut_idw_concflux_v2

    quickref_dir <- glue('data/{n}/{d}/precip_idw_quickref/',
                         n = network,
                         d = domain)

    dir.create(path = quickref_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    chunkfile <- paste(strftime(chunkdtrange[1],
                                format = '%Y-%m-%d %H:%M:%S',
                                tz = 'UTC'),
                       strftime(chunkdtrange[2],
                                format = '%Y-%m-%d %H:%M:%S',
                                tz = 'UTC'),
                       sep = '_')

    chunkfile <- str_replace_all(chunkfile, ':', '-')

    saveRDS(object = precip_idw_list,
            file = glue('{qd}/{cf}', #omitting extension for easier parsing
                        qd = quickref_dir,
                        cf = chunkfile))

    #previous approach: when chunks have a size limit:

    # #not to be confused with the precip idw tempfile (dumpfile),
    # #the quickref file allows the same precip idw data to be used across all
    # #chemistry variables when calculating flux. it's stored in 1000-timestep
    # #chunks
    #
    # chunk_number <- floor((timestep - 1) / 1000) + 1
    # chunkID <- stringr::str_pad(string = chunk_number,
    #                             width = 3,
    #                             side = 'left',
    #                             pad = '0')
    #
    # quickref_dir <- glue('data/{n}/{d}/precip_idw_quickref/{s}',
    #                      n = network,
    #                      d = domain,
    #                      s = site_code)
    #
    # chunkfile <- glue('chunk{ch}.rds',
    #                   ch = chunkID)
    #
    # if(! file.exists(chunkfile)){ #in case another thread has already written it
    #
    #     dir.create(path = quickref_dir,
    #                showWarnings = FALSE,
    #                recursive = TRUE)
    #
    #     saveRDS(object = precip_idw_list,
    #             file = paste(quickref_dir,
    #                          chunkfile,
    #                          sep = '/'))
    # }
}

read_precip_quickref <- function(network,
                                 domain,
                                 site_code,
                                 dtrange){
                                 # timestep){

    #allows precip values computed by shortcut_idw for each watershed
    #   raster cell to be reused by shortcut_idw_concflux_v2. These values are
    #   in mm/day

    quickref_dir <- glue('data/{n}/{d}/precip_idw_quickref',
                         n = network,
                         d = domain)

    quickref_chunks <- list.files(quickref_dir)

    refranges <- lapply(quickref_chunks,
           function(x){
               as.POSIXct(strsplit(x, '_')[[1]],
                          tz = 'UTC')
           }) %>%
        plyr::ldply(function(y){
            data.frame(startdt = y[1],
                       enddt = y[2])
        }) %>%
        mutate(ref_ind = 1:n())

    refranges_sel <- refranges %>%
        filter((startdt >= dtrange[1] & enddt <= dtrange[2]) |
                   (startdt < dtrange[1] & enddt >= dtrange[1]) |
                   (enddt > dtrange[2] & startdt <= dtrange[2]))
                   #redundant?
                   # (startdt > dtrange[1] & startdt <= dtrange[2] & enddt > dtrange[2]) |
                   # (startdt < dtrange[1] & enddt < dtrange[2] & enddt >= dtrange[1]))

    if(nrow(refranges_sel) == 0){
        return(list('0' = 'NO QUICKREF AVAILABLE'))
    }

    #handle the case where an end of dtrange falls right between the start and
    #end dates of a quickref file. this is possible because precip dates can
    #be shifted (replaced with pchem dates) inside precip_pchem_pflux_idw2
    ref_ind_range <- range(refranges_sel$ref_ind)

    if(dtrange[1] < refranges_sel$startdt[1] && ref_ind_range[1] > 1){

        refranges_sel <- bind_rows(refranges[ref_ind_range[1] - 1, ],
                                   refranges_sel)
    }

    if(dtrange[2] > refranges_sel$enddt[nrow(refranges_sel)] &&
       ref_ind_range[2] < nrow(refranges)){

        refranges_sel <- bind_rows(refranges_sel,
                                   refranges[ref_ind_range[2] + 1, ])
    }

    quickref <- list()
    for(i in 1:nrow(refranges_sel)){

        fn <- paste(strftime(refranges_sel$startdt[i],
                             format = '%Y-%m-%d %H-%M-%S',
                             tz = 'UTC'),
                    strftime(refranges_sel$enddt[i],
                             format = '%Y-%m-%d %H-%M-%S',
                             tz = 'UTC'),
                    sep = '_')

        qf <- readRDS(glue('{qd}/{f}',
                           qd = quickref_dir,
                           f = fn))

        quickref <- append(quickref, qf)
    }

    #for some reason the first ref sometimes gets duplicated?
    quickref <- quickref[! duplicated(names(quickref))]

    return(quickref)
}

populate_implicit_NAs <- function(d,
                                  interval,
                                  val_fill = NA,
                                  edges_only = FALSE){

    #TODO: this would be more flexible if we could pass column names as
    #   positional args and use them in group_by and mutate

    #d: a ms tibble with at minimum datetime, site_code, and var columns
    #interval: the interval along which to populate missing values. (must be
    #   either '15 min' or '1 day'.
    #val_fill: character or NA. the token with which to populate missing
    #   elements of the `val` column. All other columns will be populated
    #   invariably with NA or 0. See details.
    #edges_only: logical. if TRUE, only two filler rows will be inserted into each
    #   gap, one just after the gap begins and the other just before the gap ends.
    #   If FALSE (the default), the gap will be fully populated according to
    #   the methods outlined in the details section.

    #this function makes implicit missing timeseries records explicit,
    #   by populating rows so that the datetime column is complete
    #   with respect to the sampling interval. In other words, if
    #   samples are taken every 15 minutes, but some samples are skipped
    #   (rows not present), this will create those rows. If ms_status or
    #   ms_interp columns are present, their new records will be populated
    #   with 0s. The val column will be populated with whatever is passed to
    #   val_fill. Any other columns will be populated with NAs.

    #returns d, complete with new rows, sorted by site_code, then var, then datetime

    if(! interval %in% c('15 min', '1 day')){
        stop('interval must be "15 min" or "1 day", unless we have decided otherwise')
    }

    complete_d <- d %>%
        mutate(fill_marker = 1) %>%
        group_by(site_code, var) %>%
        tidyr::complete(datetime = seq(min(datetime),
                                       max(datetime),
                                       by = interval)) %>%
        # mutate(site_code = .$site_code[1],
        #        var = .$var[1]) %>%
        ungroup() %>%
        arrange(site_code, var, datetime) %>%
        select(datetime, site_code, var, everything())

    if(! any(is.na(complete_d$fill_marker))) return(d)

    if(! is.na(val_fill)){
        complete_d$val[is.na(complete_d$fill_marker)] <- val_fill
    }

    if('ms_status' %in% colnames(complete_d)){
        complete_d$ms_status[is.na(complete_d$ms_status)] <- 0
    }

    if('ms_interp' %in% colnames(complete_d)){
        complete_d$ms_interp[is.na(complete_d$ms_interp)] <- 0
    }

    if(edges_only){

        midgap_rows <- rle2(is.na(complete_d$fill_marker)) %>%
            filter(values == TRUE) %>%
        # if(nrow(fill_runs) == 0) return(d)
        # midgap_rows <- fill_runs %>%
            select(starts, stops) %>%
            {purrr::map2(.x = .$starts,
                         .y = .$stops,
                         ~seq(.x, .y))} %>%
            purrr::map(~( if(length(.x) <= 2)
                {
                    return(NULL)
                } else {
                    return(.x[2:(length(.x) - 1)])
                }
            )) %>%
            unlist()
        if(! is.null(midgap_rows)){
            complete_d <- slice(complete_d,
                                -midgap_rows)
        }
    }

    complete_d$fill_marker <- NULL

    return(complete_d)
}

ms_linear_interpolate <- function(d, interval){

    #d: a ms tibble with no ms_interp column (this will be created)
    #interval: the sampling interval (either '15 min' or '1 day'). an
    #   appropriate maxgap (i.e. max number of consecutive NAs to fill) will
    #   be chosen based on this interval.

    #fills gaps up to maxgap (determined automatically), then removes missing values

    #TODO: prefer imputeTS::na_seadec when there are >=2 non-NA datapoints.
    #   There are commented sections that begin this work, but we still would
    #   need to calculate start and end when creating a ts() object. we'd
    #   also need to separate uncertainty from the val column before converting
    #   to ts. here is the line that could be added to this documentation
    #   if we ever implement na_seadec:
    #For linear interpolation with
    #   seasonal decomposition, interval will also be used to determine
    #   the fraction of the sampling period between samples.

    if(length(unique(d$site_code)) > 1){
        stop(paste('ms_linear_interpolate is not designed to handle datasets',
                   'with more than one site.'))
    }

    if(length(unique(d$var)) > 1){
        stop(paste('ms_linear_interpolate is not designed to handle datasets',
                   'with more than one variable'))
    }

    if(! interval %in% c('15 min', '1 day')){
        stop('interval must be "15 min" or "1 day", unless we have decided otherwise')
    }

    var <- drop_var_prefix(d$var[1])
    max_samples_to_impute <- ifelse(test = var %in% c('precipitation', 'discharge'),
                                    yes = 3, #is Q-ish
                                    no = 15) #is chemistry/etc

    if(interval == '15 min'){
        max_samples_to_impute <- max_samples_to_impute * 96
    }

    # ts_delta_t <- ifelse(interval == '1 day', #we might want this if we use na_seadec
    #                      1/365, #"sampling period" is 1 year; interval is 1/365 of that
    #                      1/96) #"sampling period" is 1 day; interval is 1/(24 * 4)

    d <- arrange(d, datetime)
    ms_interp_column <- is.na(d$val)

    d_interp <- d %>%
        mutate(

            #carry ms_status to any rows that have just been populated (probably
            #redundant now, but can't hurt)
            ms_status <- imputeTS::na_locf(ms_status,
                                           na_remaining = 'rev'),

            # val = if(sum(! is.na(val)) > 2){
            #
            #     #linear interp NA vals after seasonal decomposition
            #     imputeTS::na_seadec(x = as.numeric(ts(val,
            #                                start = ,
            #                                end = ,
            #                                deltat = ts_delta_t)),
            #                         maxgap = max_samples_to_impute)
            #
            # } else if(sum(! is.na(val)) > 1){
            val = if(sum(! is.na(val)) > 1){

                #linear interp NA vals
                imputeTS::na_interpolation(val,
                                           maxgap = max_samples_to_impute)

                #unless not enough data in group; then do nothing
            } else val
        ) %>%
        mutate(
            err = errors(val), #extract error from data vals
            err = case_when(
                err == 0 ~ NA_real_, #change new uncerts (0s by default) to NA
                TRUE ~ err),
            val = if(sum(! is.na(err)) > 0){
                set_errors(val, #and then carry error to interped rows
                           imputeTS::na_locf(err,
                                             na_remaining = 'rev'))
            } else {
                set_errors(val, #unless not enough error to interp
                           0)
            }) %>%
        select(any_of(c('datetime', 'site_code', 'var', 'val', 'ms_status', 'ms_interp'))) %>%
        arrange(site_code, var, datetime)

    ms_interp_column <- ms_interp_column & ! is.na(d_interp$val)
    d_interp$ms_interp <- as.numeric(ms_interp_column)
    d_interp <- filter(d_interp,
                       ! is.na(val))

    return(d_interp)
}

synchronize_timestep <- function(d){
                                 # desired_interval){
                                 # impute_limit = 30){

    #d is a df/tibble with columns: datetime (POSIXct), site_code, var, val, ms_status
    #desired_interval [HARD DEPRECATED] is a character string that can be parsed by the "by"
    #   parameter to base::seq.POSIXt, e.g. "5 mins" or "1 day". THIS IS NOW
    #   DETERMINED PROGRAMMATICALLY. WE'RE ONLY GOING TO HAVE 2 INTERVALS,
    #   ONE FOR GRAB DATA AND ONE FOR SENSOR. IF WE EVER WANT TO CHANGE THEM,
    #   IT WOULD BE BETTER TO CHANGE THEM JUST ONCE HERE, RATHER THAN IN
    #   EVERY KERNEL
    #impute_limit [HARD DEPRECATED] is the maximum number of consecutive points to
    #   inter/extrapolate. it's passed to imputeTS::na_interpolate. THIS
    #   PARAMETER WAS REMOVED BECAUSE IT SHOULD ONLY VARY WITH DESIRED INTERVAL.

    #output will include a numeric binary column called "ms_interp".
    #0 for not interpolated, 1 for interpolated

    if(nrow(d) < 2 || sum(! is.na(d$val)) < 2){
        stop('no data to synchronize. bypassing processing.')
    }

    #split dataset by site and variable. for each, determine whether we're
    #   dealing with ~daily data or ~15min data. set rounding_intervals
    #   accordingly. This approach avoids OOM errors with giant groupings.
    d_split <- d %>%
        group_by(site_code, var) %>%
        arrange(datetime) %>%
        dplyr::group_split() %>%
        as.list()

    mode_intervals_m <- vapply(
        X = d_split,
        FUN = function(x) Mode(diff(as.numeric(x$datetime)) / 60),
        FUN.VALUE = 0)

    rounding_intervals <- case_when(
        is.na(mode_intervals_m) | mode_intervals_m > 12 * 60 ~ '1 day',
        mode_intervals_m <= 12 * 60 ~ '1 day') #TODO, TEMPORARY: switch this back
        # mode_intervals_m <= 12 * 60 ~ '15 min')

    for(i in 1:length(d_split)){

        sitevar_chunk <- d_split[[i]]

        n_dupes <- sum(duplicated(sitevar_chunk$datetime) |
                       duplicated(sitevar_chunk$datetime,
                                  fromLast = TRUE))

        #average values for duplicate timestamps
        if(n_dupes > 0){

            logwarn(msg = glue('{n} duplicate datetimes found for site: {s}, var: {v}',
                               n = n_dupes,
                               s = sitevar_chunk$site_code[1],
                               v = sitevar_chunk$var[1]),
                    logger = logger_module)

            sitevar_chunk_dt <- sitevar_chunk %>%
                mutate(val_err = errors(val),
                       val = errors::drop_errors(val)) %>%
                as.data.table()

            #take the mean value for any duplicate timestamps
            sitevar_chunk <- sitevar_chunk_dt[, .(
                site_code = data.table::first(site_code),
                var = data.table::first(var),
             #data.table doesn't work with the errors package, but we're
             #determining the uncertainty of the mean by the same method here:
             #    max(SDM, mean(uncert)),
             #    where SDM is the Standard Deviation of the Mean.
             #The one difference is that we remove NAs when computing
             #the standard deviation. Rationale: 1. This will only result in
             #"incorrect" error values if there's an NA error coupled with a
             #non-NA value (if the value is NA, the error must be too).
             #I don't think this happens very often, if ever.
             #2. an error of NA just looks like 0 error, which is more misleading
             #than even a wild nonzero error.
                val_err = max(sd_or_0(val, na.rm = TRUE) / sqrt(.N),
                              mean(val_err, na.rm = TRUE)),
                val = mean(val, na.rm = TRUE),
                ms_status = numeric_any(ms_status)
             ), keyby = datetime] %>%
                as_tibble() %>%
                mutate(val = set_errors(val, val_err)) %>%
                select(-val_err)

            # sitevar_chunk <- sitevar_chunk %>%
            #     group_by(datetime) %>%
            #     summarize(site_code = first(site_code),
            #               var = first(var),
            #               val = mean(val, na.rm = TRUE),
            #               ms_status = numeric_any(ms_status)) %>%
            #     ungroup()
        }

        #round each site-variable tibble's datetime column to the desired interval.
        sitevar_chunk <- mutate(sitevar_chunk,
                                datetime = lubridate::round_date(
                                    x = datetime,
                                    unit = rounding_intervals[i]))

        #split chunk into subchunks. one has duplicate datetimes to summarize,
        #   and the other doesn't. both will be interpolated in a bit.
        to_summarize_bool <- duplicated(sitevar_chunk$datetime) |
            duplicated(sitevar_chunk$datetime,
                       fromLast = TRUE)

        summary_and_interp_chunk <- sitevar_chunk[to_summarize_bool, ]
        interp_only_chunk <- sitevar_chunk[! to_summarize_bool, ]

        if(nrow(summary_and_interp_chunk)){

            #summarize by sum for P, and mean for everything else
            # var_is_q <- drop_var_prefix(sitevar_chunk$var[1]) == 'discharge'
            var_is_p <- drop_var_prefix(sitevar_chunk$var[1]) == 'precipitation'

            summary_and_interp_chunk_dt <- summary_and_interp_chunk %>%
                mutate(val_err = errors(val),
                       val = errors::drop_errors(val)) %>%
                as.data.table()

            sitevar_chunk <- summary_and_interp_chunk_dt[, .(
                site_code = data.table::first(site_code),
                var = data.table::first(var),
                val_err = if(var_is_p)
                    {
                    #the errors package uses taylor series expansion here.
                    #maybe implement some day.
                        sum(val_err, na.rm = TRUE)
                    } else {
                        max(sd_or_0(val, na.rm = TRUE) / sqrt(.N),
                            mean(val_err, na.rm = TRUE))
                    },
                val = if(var_is_p) sum(val, na.rm = TRUE) else mean(val, na.rm = TRUE),
                ms_status = numeric_any(ms_status)
            ), by = datetime] %>%
                as_tibble() %>%
                mutate(val = set_errors(val, val_err)) %>%
                select(-val_err) %>%
                bind_rows(interp_only_chunk) %>%
                arrange(datetime)

            # sitevar_chunk <- summary_and_interp_chunk %>%
            #     group_by(datetime) %>%
            #         summarize(
            #             site_code = first(site_code),
            #             var = first(var),
            #             val = if(var_is_p)
            #                 {
            #                     sum(val, na.rm = TRUE)
            #                 # } else if(var_is_q){
            #                 #     max_ind <- which.max(val) #max() removes uncert
            #                 #     if(max_ind) val[max_ind] else NA_real_
            #                 } else {
            #                     mean(val, na.rm = TRUE)
            #                 },
            #             ms_status = numeric_any(ms_status)) %>%
            #         ungroup() %>%
            #     bind_rows(interp_only_chunk) %>%
            #     arrange(datetime)
        }

        sitevar_chunk <- populate_implicit_NAs(
            d = sitevar_chunk,
            interval = rounding_intervals[i])

        d_split[[i]] <- ms_linear_interpolate(
            d = sitevar_chunk,
            interval = rounding_intervals[i])
    }

    #recombine list of tibbles into single tibble
    d <- d_split %>%
        purrr::reduce(bind_rows) %>%
        arrange(site_code, var, datetime) %>%
        select(datetime, site_code, var, val, ms_status, ms_interp)

    return(d)
}

recursive_tracker_update <- function(l, elem_name, new_val){

    #implements depth-first tree traversal

    if(is.list(l)){

        nms <- names(l)
        for(i in 1:length(l)){

            subl <- l[[i]]

            if(nms[i] == elem_name){
                l[[i]] <- new_val
            } else if(is.list(subl)){
                l[[i]] <- recursive_tracker_update(subl, elem_name, new_val)
            }

        }
    }

    return(l)
}

ms_parallelize <- function(maxcores = Inf){

    #maxcores is the maximum number of processor cores to use for R tasks.
    #   you may want to leave a few aside for other processes.

    #value: a cluster object. you'll need this to return to serial mode and
    #   free up the cores that were employed by R. Be sure to run
    #ms_unparallelize() after the parallel tasks are complete.

    #we need to find a way to protect some cores for serving the portal
    #if we end up processing data and serving the portal on the same
    #machine/cluster. we can use taskset to assign the shiny process
    #to 1-3 cores and this process to any others.

    #NOTE: this function has been updated to work with doFuture, which
    #   supplants doParallel. doFuture allows us to dispatch jobs on a cluster
    #   via Slurm. it might also allow us to dispatch multisession jobs on
    #   Windows

    #obsolete-ish notes:
    # #variables used inside the foreach loop on the master process will
    # #be written to the global environment, in some cases overwriting needed
    # #globals. we can set them aside in a separate environment and restore them later
    # protected_vars <- c('prodname_ms', 'verbose', 'site_code')
    # assign(x = 'protected_vars',
    #        val = mget(protected_vars,
    #                   ifnotfound = list(NULL, NULL, NULL),
    #                   inherits = TRUE),
    #        envir = protected_environment)

    #then set up parallelization
    # clst <- parallel::makeCluster(ncores)
    clst <- NULL
    ncores <- min(parallel::detectCores(), maxcores)

    if(ms_instance$which_machine == 'DCC'){
        doFuture::registerDoFuture()
        # future::plan(cluster, workers = clst) #might need this instead of Slurm one day
        future::plan(future.batchtools::batchtools_slurm)
    } else if(.Platform$OS.type == 'windows'){
        #issues (found while testing on linux):
        #1. inner precip logging waits till outer loop completes, then only prints to console.
        #2. konza error that doesn't occur with FORK cluster:
        #   task 1 failed - "task 2 failed - "unused argument (datetime_x = datetime)"
        #3. not fully utilizing cores like FORK does
        doFuture::registerDoFuture()
        # clst <- parallel::makeCluster(ncores)
        future::plan(multisession, workers = ncores)
         #clst <- parallel::makeCluster(ncores, type = 'PSOCK')
    } else {
        # future::plan(multicore) #can't be done from Rstudio
        clst <- parallel::makeCluster(ncores, type = 'FORK')
        doParallel::registerDoParallel(clst)
    }

    return(clst)
}

get_env_by_variable <- function(x){

    xobj <- deparse(substitute(x))
    gobjects <- ls(envir = .GlobalEnv)
    envirs <- gobjects[sapply(gobjects, function(x) is.environment(get(x)))]
    envirs <- c('.GlobalEnv', envirs)
    xin <- sapply(envirs, function(e) xobj %in% ls(envir = get(e)))
    return(envirs[xin])
}

idw_parallel_combine <- function(d1, d2){

    #this is for use with foreach loops inside the 4 idw prep functions
    #   (precip_idw, pchem_idw, flux_idw, precip_pchem_pflux_idw)

    if(is.character(d1) && d1 == 'first iter') return(d2)

    d_comb <- bind_rows(d1, d2)

    return(d_comb)
}

idw_log_wb <- function(verbose, site_code, i, nw){

    if(! verbose) return()

    msg <- glue('site: {s} ({ii}/{w})',
                s = site_code,
                ii = i,
                w = nw)

    loginfo(msg,
            logger = logger_module)

    #return()
}

idw_log_var <- function(verbose,
                        site_code,
                        v,
                        j,
                        nvars,
                        ntimesteps,
                        is_fluxable = NA,
                        note = ''){

    if(! verbose) return()

    flux_calc_msg <- case_when(is_fluxable == FALSE ~ '[NO FLUX]',
                     is_fluxable == TRUE ~ '[yes flux]',
                     is.na(is_fluxable) ~ '')

    note <- ifelse(note == '',
                   note,
                   paste0('[', note, ']'))

    msg <- glue('site: {s}; var: {vv} ({jj}/{nv}) {f}; timesteps: {nt}; {n}',
                s = site_code,
                vv = v,
                jj = j,
                nv = nvars,
                nt = ntimesteps,
                f = flux_calc_msg,
                n = note)

    loginfo(msg,
            logger = logger_module)

    #return()
}

idw_log_timestep <- function(verbose, site_code=NULL, v, k, ntimesteps,
                             time_elapsed){

    #time elapsed must be in minutes. use something like:
    #   (proc.time() - ptm)[3] / 60)

    if(! verbose) return()

    time_elapsed <- ifelse(k == 0,
                           '?',
                           time_elapsed)

    estimated_time_remaining <- ifelse(k == 0,
                                       '?',
                                       round(time_elapsed * (ntimesteps - k) / k,
                                             1))

    if(k == 1 || k %% 1000 == 0){
        msg <- glue('site: {s}; var: {vv}; timestep: ({kk}/{nt}); ',
                    'thread elapsed (mins): {et}; thread ETA (mins): {tm}',
                    s = site_code,
                    vv = v,
                    kk = k,
                    nt = ntimesteps,
                    et = round(time_elapsed, 1),
                    tm = estimated_time_remaining)

        loginfo(msg,
                logger = logger_module)
    }

    #return()
}



get_detlim_precursors <- function(network,
                                  domain,
                                  prodname_ms){

    #this gets the prodname_ms for the direct precursor of a derived
    #product. for example, for hjandrews 'precipitation__ms001' it would return
    #'precipitation__5482'. This is necessary when applying detection limits
    #to a derived product, because those limits were defined on the direct
    #precursor

    #for precip flux products, the direct precursor is considered to be precip
    #chem. for derived products that aggregate two or more munged products of
    #the same type (e.g. discharge__9, discharge__10, etc. from
    #lter/konza), this returns all of those products as precursors.

    #for precip_pchem_pflux (the all-in-one precip derive kernel), this returns
    #the precursors for both precipitation and precip_chemistry.

    prods <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = network,
                              d = domain)))

    prodname <- prodname_from_prodname_ms(prodname_ms)

    if(prodname == 'precip_flux_inst'){
        prodname <- 'precip_chemistry'
    } else if(prodname == 'precip_pchem_pflux'){
        prodname <- c('precip_chemistry', 'precipitation')
    } else if(prodname == 'stream_flux_inst'){
        prodname <- c('stream_chemistry', 'discharge')
    }

    precursors <- prods %>%
        filter(
            prodname %in% !!prodname,
            ! grepl('^ms[0-9]{3}$', prodcode)) %>%
        mutate(prodname_ms = paste(prodname, prodcode, sep = '__')) %>%
        pull(prodname_ms)

    return(precursors)
}

datetimes_to_durations <- function(datetime_vec,
                                   variable_prefix_vec = NULL,
                                   unit,
                                   sensor_maxgap = Inf,
                                   nonsensor_maxgap = Inf,
                                   grab_maxgap = Inf,
                                   installed_maxgap = Inf){

    #datetime_vec: POSIXct. a vector of datetimes
    #variable_prefix_vec: POSIXct. a vector of macrosheds variable prefixes,
    #   e.g. 'IS'. This vector is generated by calling extract_var_prefix
    #   on the var column of a macrosheds data.frame. Only required if you
    #   supply one or more of the maxgap parameters.
    #unit: string. The desired datetime unit. Must be expressed in a form
    #   that's readable by base::difftime
    #sensor_maxgap: numeric. the largest data gap that should return
    #   a value. Durations longer than this gap will return NA. Expressed in
    #   the same units as unit (see above). This applies
    #   to sensor data only (IS and GS).
    #nonsensor_maxgap: numeric. the largest data gap that should return
    #   a value. Durations longer than this gap will return NA. Expressed in
    #   the same units as unit (see above). This applies
    #   to nonsensor data only (IN and GN).
    #grab_maxgap: numeric. the largest data gap that should return
    #   a value. Durations longer than this gap will return NA. Expressed in
    #   the same units as unit (see above). This applies
    #   to grab data only (GS and GN).
    #installed_maxgap: numeric. the largest data gap that should return
    #   a value. Durations longer than this gap will return NA. Expressed in
    #   the same units as unit (see above). This applies
    #   only to data from installed units (IS and IN).

    #an NA is prepended to the output, so that its length is the same as
    #   datetime_vec

    if(! is.null(variable_prefix_vec) &&
       ! all(variable_prefix_vec %in% c('GN', 'GS', 'IN', 'IS'))){
        stop(paste('all elements of variable_prefix_vec must be one of "GN",',
                   '"GS", "IN", "IS"'))
    }

    durs <- diff(datetime_vec)
    units(durs) <- unit
    durs <- c(NA_real_, as.numeric(durs))

    if(! is.null(variable_prefix_vec)){
        is_sensor_data <- grepl('^.S', variable_prefix_vec)
        is_installed_data <- grepl('^I', variable_prefix_vec)
    }

    if(! is.infinite(sensor_maxgap)){
        durs[is_sensor_data & durs > sensor_maxgap] <- NA_real_
    }
    if(! is.infinite(nonsensor_maxgap)){
        durs[! is_sensor_data & durs > nonsensor_maxgap] <- NA_real_
    }
    if(! is.infinite(grab_maxgap)){
        durs[! is_installed_data & durs > grab_maxgap] <- NA_real_
    }
    if(! is.infinite(installed_maxgap)){
        durs[is_installed_data & durs > installed_maxgap] <- NA_real_
    }

    return(durs)
}

precip_pchem_pflux_idw <- function(pchem_prodname,
                                   precip_prodname,
                                   wb_prodname,
                                   pgauge_prodname,
                                   prodname_ms,
                                   # flux_prodname_out,
                                   verbose = TRUE){

    #load watershed boundaries, rain gauge locations, precip and pchem data
    wb <- read_combine_shapefiles(network = network,
                                  domain = domain,
                                  prodname_ms = wb_prodname)
    rg <- read_combine_shapefiles(network = network,
                                  domain = domain,
                                  prodname_ms = pgauge_prodname)
    pchem <- try({
        read_combine_feathers(network = network,
                              domain = domain,
                              prodname_ms = pchem_prodname) %>%
            filter(site_code %in% rg$site_code)
    }, silent = TRUE)

    precip_only <- FALSE
    if('try-error' %in% class(pchem) || nrow(pchem) == 0){
        precip_only <- TRUE
        logging::logwarn('No (or empty) pchem product. IDW-Interpolating precipitation only')
    }

    precip <- try({
        read_combine_feathers(network = network,
                              domain = domain,
                              prodname_ms = precip_prodname) %>%
            filter(site_code %in% rg$site_code)
    }, silent = TRUE)

    pchem_only <- FALSE
    if('try-error' %in% class(precip) || nrow(precip) == 0){
        pchem_only <- TRUE
        if(precip_only) stop('Nothing to IDW interpolate. Is this kernel needed?')
        logging::logwarn('No (or empty) precip product. IDW-Interpolating pchem only')
    }

    #project based on average latlong of watershed boundaries
    bbox <- as.list(sf::st_bbox(wb))
    projstring <- choose_projection(lat = mean(bbox$ymin, bbox$ymax),
                                    long = mean(bbox$xmin, bbox$xmax))
    wb <- sf::st_transform(wb, projstring)
    rg <- sf::st_transform(rg, projstring)

    #get a DEM that encompasses all watersheds and gauges
    wb_rg_bbox <- sf::st_as_sf(sf::st_as_sfc(sf::st_bbox(bind_rows(wb, rg))))

    dem <- expo_backoff(
        expr = {
            elevatr::get_elev_raster(locations = wb_rg_bbox,
                                     z = 8, #res should adjust with area?,
                                     clip = 'bbox',
                                     expand = 200,
                                     verbose = FALSE,
                                     override_size_check = TRUE)
        },
        max_attempts = 5
    )

    #add elev column to rain gauges
    rg$elevation <- terra::extract(dem, rg)

    #this avoids a lot of slow summarizing
    if(! pchem_only){

        # if(length(unique(precip$var)) > 1){
        #     logwarn(paste('Multiple precip prefixes encountered.'))
        # }

        status_cols <- precip %>%
            select(datetime, ms_status, ms_interp) %>%
            group_by(datetime) %>%
            summarize(
                ms_status = numeric_any(ms_status),
                ms_interp = numeric_any(ms_interp))

        day_durations_byproduct <- datetimes_to_durations(
            datetime_vec = precip$datetime,
            variable_prefix_vec = extract_var_prefix(precip$var),
            unit = 'days',
            installed_maxgap = 2,
            grab_maxgap = 30)

        precip$val[is.na(day_durations_byproduct)] <- NA

        precip <- precip %>%
            select(-ms_status, -ms_interp, -var) %>%
            tidyr::pivot_wider(names_from = site_code,
                               values_from = val) %>%
            left_join(status_cols, #they get lumped anyway
                      by = 'datetime') %>%
            arrange(datetime)

        day_durations <- datetimes_to_durations(
            datetime_vec = precip$datetime,
            unit = 'days')
    }

    if(! precip_only){

        #determine which variables can be flux converted (prefix handling clunky here)
        flux_vars <- ms_vars$variable_code[as.logical(ms_vars$flux_convertible)]
        pchem_vars <- unique(pchem$var)
        pchem_vars_fluxable0 <- base::intersect(drop_var_prefix(pchem_vars),
                                                flux_vars)
        pchem_vars_fluxable <- pchem_vars[drop_var_prefix(pchem_vars) %in%
                                              pchem_vars_fluxable0]

        #this avoids a lot of slow summarizing
        status_cols <- pchem %>%
            select(datetime, ms_status, ms_interp) %>%
            group_by(datetime) %>%
            summarize(
                ms_status = numeric_any(ms_status),
                ms_interp = numeric_any(ms_interp))

        #clean pchem one variable at a time, matrixify it, insert it into list
        nvars <- length(pchem_vars)
        pchem_setlist <- as.list(rep(NA, nvars))
        for(i in 1:nvars){

            v <- pchem_vars[i]

            #clean data and arrange for matrixification
            pchem_setlist[[i]] <- pchem %>%
                filter(var == v) %>%
                select(-var, -ms_status, -ms_interp) %>%
                tidyr::pivot_wider(names_from = site_code,
                                   values_from = val) %>%
                left_join(status_cols,
                          by = 'datetime') %>%
                arrange(datetime)
        }
    } #end conditional pchem+pflux block (1)

    #make sure old quickref data aren't still sitting around
    unlink(glue('data/{n}/{d}/precip_idw_quickref',
                n = network,
                d = domain),
           recursive = TRUE)

    #send vars into interpolator with precip, one at a time. if var is flux-
    #convertible, interpolate precip, pchem, and pflux. otherwise, just precip
    #and pchem. combine and write outputs by site

    # # FOR TESTING (most hideous code ever written)
    # test_switch = 1 #either 1 or 2
    # if(! precip_only){
    #     if(test_switch == 1){
    #         fo <- pre_idw_filter_for_testing(pchem_setlist, precip_only, length_days = 90)
    #         if(! is.null(fo$x)){
    #             pchem_setlist <- fo$x
    #             if(length(fo$drop_these)){
    #                 pchem_vars_fluxable = pchem_vars_fluxable[-fo$drop_these]
    #             }
    #             nvars = length(pchem_setlist)
    #         }
    #         fo2 <- pre_idw_filter_for_testing(precip, precip_only,
    #                                           daterange = fo$daterange,
    #                                           length_days = 90)
    #         precip = fo2$x
    #     } else if(test_switch == 2){
    #         fo <- pre_idw_filter_for_testing(precip, precip_only, length_days = 90)
    #         precip = fo$x
    #         fo2 <- pre_idw_filter_for_testing(pchem_setlist, precip_only,
    #                                           daterange = fo$daterange,
    #                                           length_days = 90)
    #         pchem_setlist = fo2$x
    #         nvars = length(pchem_setlist)
    #     }
    #
    # } else {
    #     fo2 <- pre_idw_filter_for_testing(precip, precip_only, length_days = 90)
    #     precip = fo2$x
    # }

    # if(nrow(precip) == 0){
    #     stop('the test code removed all the precip data. change test_switch')
    # }

    for(i in 1:nrow(wb)){

        wbi <- slice(wb, i)
        site_code <- wbi$site_code
        wbi_area_ha <- as.numeric(sf::st_area(wbi)) / 10000

        idw_log_wb(verbose = verbose,
                   site_code = site_code,
                   i = i,
                   nw = nrow(wb))

        precursor_prodnames <- get_detlim_precursors(network = network,
                                                     domain = domain,
                                                     prodname_ms = prodname_ms)

        nthreads <- parallel::detectCores()

        ## IDW INTERPOLATE PRECIP FOR ALL TIMESTEPS. STORE CELL VALUES
        ## SO THEY CAN BE USED FOR PFLUX INTERP
        if(! pchem_only){

            ntimesteps_precip <- nrow(precip)
            nsuperchunks <- ceiling(ntimesteps_precip / 25000 * 2)
            nchunks_precip <- nthreads * nsuperchunks

            precip_superchunklist <- chunk_df(d = precip,
                                              nchunks = nsuperchunks,
                                              create_index_column = TRUE)

            ws_mean_precip <- tibble()
            for(s in 1:length(precip_superchunklist)){
            # ws_mean_precip <- foreach::foreach(
            #     s = 1:length(precip_superchunklist),
            #     .combine = idw_parallel_combine,
            #     .init = 'first iter') %:% {

                precip_superchunk <- precip_superchunklist[[s]]

                precip_chunklist <- chunk_df(d = precip_superchunk,
                                             nchunks = nthreads,
                                             create_index_column = FALSE)

                idw_log_var(verbose = verbose,
                            site_code = site_code,
                            v = 'precipitation',
                            j = paste('chunk', s),
                            ntimesteps = nrow(precip_superchunk),
                            nvars = nsuperchunks)

                clst <- ms_parallelize(maxcores = nthreads)
                # doFuture::registerDoFuture()
                # ncores <- min(parallel::detectCores(), maxcores)
                # clst <- parallel::makeCluster(nthreads, type='FORK')
                # future::plan(future::multicore, workers = 48)
                # future::plan(future::multisession, workers = 48)

                # parallel::stopCluster(clst)
                # fe_junk <- foreach:::.foreachGlobals
                # rm(list = ls(name = fe_junk),
                #    pos = fe_junk)

                ws_mean_precip_chunk <- foreach::foreach(
                    j = 1:min(nthreads, nrow(precip_superchunk)),
                    .combine = idw_parallel_combine,
                    .init = 'first iter') %dopar% {

                    pchunk <- precip_chunklist[[j]]

                    # idw_log_var(verbose = verbose,
                    #             site_code = site_code,
                    #             v = 'precipitation',
                    #             j = paste('chunk', j + (nthreads * (s - 1))),
                    #             ntimesteps = nrow(pchunk),
                    #             nvars = nchunks_precip)

                    foreach_return <- shortcut_idw(
                        encompassing_dem = dem,
                        wshd_bnd = wbi,
                        data_locations = rg,
                        data_values = pchunk,
                        durations_between_samples = day_durations[pchunk$ind],
                        stream_site_code = site_code,
                        output_varname = 'SPECIAL CASE PRECIP',
                        save_precip_quickref = ! precip_only,
                        elev_agnostic = FALSE,
                        verbose = verbose)

                    foreach_return
                }

                ms_unparallelize(clst)

                rm(precip_chunklist); gc()

                ws_mean_precip <- bind_rows(ws_mean_precip, ws_mean_precip_chunk)
            }

            rm(precip_superchunklist); gc()

            if(any(is.na(ws_mean_precip$datetime))){
                stop('NA datetime found in ws_mean_precip')
            }

            # #restore original varnames by site and dt
            # ws_mean_precip <- ws_mean_precip %>%
            #     arrange(datetime) %>%
            #     select(-var) %>% #just a placeholder
            #     left_join(precip_varnames,
            #               by = c('datetime', 'site_code'))

            ws_mean_precip <- ws_mean_precip %>%
                dplyr::rename_all(dplyr::recode, concentration = 'val') %>%
                # rename(val = concentration) %>%
                arrange(datetime)

            ws_mean_precip <- apply_detection_limit_t(
                X = ws_mean_precip,
                network = network,
                domain = domain,
                prodname_ms = precursor_prodnames[grepl('^precipitation',
                                                        precursor_prodnames)])

            write_ms_file(ws_mean_precip,
                          network = network,
                          domain = domain,
                          prodname_ms = 'precipitation__ms900',
                          site_code = site_code,
                          level = 'derived',
                          shapefile = FALSE,
                          link_to_portal = FALSE)

            rm(ws_mean_precip); gc()
        }

        ## NOW IDW INTERPOLATE PCHEM (IF PRECIP CHEMISTRY DATA EXIST)
        ## AND PFLUX (FOR VARIABLES THAT ARE FLUXABLE).
        if(! precip_only){

            ws_mean_chemflux <- tibble()
            for(j in 1:nvars){

                v <- pchem_vars[j]
                jd <- pchem_setlist[[j]]
                ntimesteps_chemflux <- nrow(jd)

                if(v %in% pchem_vars_fluxable && ! pchem_only){
                    is_fluxable <- TRUE
                } else {
                    is_fluxable <- FALSE
                }

                idw_log_var(verbose = verbose,
                            site_code = site_code,
                            v = v,
                            j = paste('var', j),
                            nvars = nvars,
                            ntimesteps = ntimesteps_chemflux,
                            is_fluxable = is_fluxable)

                nsuperchunks <- ceiling(ntimesteps_chemflux / 5000 * 2)
                nchunks_chemflux <- nthreads * nsuperchunks

                chemflux_superchunklist <- chunk_df(d = jd,
                                                    nchunks = nsuperchunks)
                nsuperchunks <- length(chemflux_superchunklist)

                ws_mean_chemflux_var <- tibble()
                for(s in 1:nsuperchunks){

                    chemflux_superchunk <- chemflux_superchunklist[[s]]

                    chemflux_chunklist <- chunk_df(d = chemflux_superchunk,
                                                   nchunks = nthreads)

                    idw_log_var(verbose = verbose,
                                site_code = site_code,
                                v = v,
                                j = paste('chunk', s),
                                ntimesteps = nrow(chemflux_superchunk),
                                nvars = nsuperchunks)

                    clst <- ms_parallelize(maxcores = nthreads)

                    foreach_out <- foreach::foreach(
                        l = 1:length(chemflux_chunklist),
                        # l = 1:min(nthreads, nrow(chemflux_superchunk)),
                        .combine = idw_parallel_combine,
                        .init = 'first iter') %dopar% {

                            if(is_fluxable){

                                foreach_chunk <- shortcut_idw_concflux_v2(
                                    encompassing_dem = dem,
                                    wshd_bnd = wbi,
                                    ws_area = wbi_area_ha,
                                    data_locations = rg,
                                    precip_values = precip,
                                    chem_values = chemflux_chunklist[[l]],
                                    stream_site_code = site_code,
                                    output_varname = v,
                                    verbose = verbose)

                            } else {

                                foreach_chunk <- shortcut_idw(
                                    encompassing_dem = dem,
                                    wshd_bnd = wbi,
                                    data_locations = rg,
                                    data_values = chemflux_chunklist[[l]],
                                    stream_site_code = site_code,
                                    output_varname = v,
                                    elev_agnostic = TRUE,
                                    verbose = verbose)
                            }

                            foreach_chunk
                        }

                        ms_unparallelize(clst)


                    rm(chemflux_chunklist); gc()

                    ws_mean_chemflux_var <- bind_rows(ws_mean_chemflux_var,
                                                      foreach_out)
                }

                rm(chemflux_superchunklist); gc()

                ws_mean_chemflux <- bind_rows(ws_mean_chemflux,
                                              ws_mean_chemflux_var)
            }



            # clst <- ms_parallelize()
            #
            # ws_mean_chemflux <- foreach::foreach(
            #     j = 1:nvars,
            #     # .verbose = TRUE,
            #     .combine = idw_parallel_combine,
            #     .init = 'first iter') %do% {
            #
            #     v <- pchem_vars[j]
            #     jd <- pchem_setlist[[j]]
            #     ntimesteps_chemflux <- nrow(jd)
            #
            #     if(v %in% pchem_vars_fluxable && ! pchem_only){
            #         is_fluxable <- TRUE
            #     } else {
            #         is_fluxable <- FALSE
            #     }
            #
            #     idw_log_var(verbose = verbose,
            #                 site_code = site_code,
            #                 v = v,
            #                 j = j,
            #                 nvars = nvars,
            #                 ntimesteps = ntimesteps_chemflux,
            #                 is_fluxable = is_fluxable)
            #
            #     if(ntimesteps_chemflux > 5000){
            #        # (exists('ntimesteps_precip') && ntimesteps_precip > 2e5)){
            #         nchunks <- nthreads %/% 2 #overkill?
            #     } else {
            #         nchunks <- nthreads
            #     }
            #
            #     chunklist <- chunk_df(d = jd,
            #                           nchunks = nchunks)
            #
            #     foreach_chunk_outer <- foreach::foreach(
            #         l = 1:min(nchunks, nrow(jd)),
            #         .combine = idw_parallel_combine,
            #         .init = 'first iter') %dopar% {
            #
            #         if(is_fluxable){
            #
            #             foreach_chunk_inner <- shortcut_idw_concflux_v2(
            #                 encompassing_dem = dem,
            #                 wshd_bnd = wbi,
            #                 ws_area = wbi_area_ha,
            #                 data_locations = rg,
            #                 precip_values = precip,
            #                 chem_values = chunklist[[l]],
            #                 stream_site_code = site_code,
            #                 output_varname = v,
            #                 verbose = verbose)
            #
            #         } else {
            #
            #             foreach_chunk_inner <- shortcut_idw(
            #                 encompassing_dem = dem,
            #                 wshd_bnd = wbi,
            #                 data_locations = rg,
            #                 data_values = chunklist[[l]],
            #                 stream_site_code = site_code,
            #                 output_varname = v,
            #                 elev_agnostic = TRUE,
            #                 verbose = verbose)
            #         }
            #
            #         foreach_chunk_inner
            #     }
            #
            #     foreach_chunk_outer
            # }
            #
            # rm(chunklist); gc()
            #
            # ms_unparallelize(clst)

            if(any(is.na(ws_mean_chemflux$datetime))){
                stop('NA datetime found in ws_mean_chemflux')
            }

            chemprod <- precursor_prodnames[grepl('chem', precursor_prodnames)]

            if(! pchem_only){

                ws_mean_pflux <- ws_mean_chemflux %>%
                    select(-concentration) %>%
                    rename(val = flux) %>%
                    arrange(var, datetime)

                ws_mean_pflux <- apply_detection_limit_t(ws_mean_pflux,
                                                         network = network,
                                                         domain = domain,
                                                         prodname_ms = chemprod)

                write_ms_file(ws_mean_pflux,
                              network = network,
                              domain = domain,
                              prodname_ms = 'precip_flux_inst__ms902',
                              site_code = site_code,
                              level = 'derived',
                              shapefile = FALSE,
                              link_to_portal = FALSE)

                rm(ws_mean_pflux)
            }

            ws_mean_pchem <- ws_mean_chemflux %>%
                select(-any_of('flux')) %>%
                rename(val = concentration) %>%
                arrange(var, datetime)

            ws_mean_pchem <- apply_detection_limit_t(ws_mean_pchem,
                                                     network = network,
                                                     domain = domain,
                                                     prodname_ms = chemprod)

            write_ms_file(ws_mean_pchem,
                          network = network,
                          domain = domain,
                          prodname_ms = 'precip_chemistry__ms901',
                          site_code = site_code,
                          level = 'derived',
                          shapefile = FALSE,
                          link_to_portal = FALSE)

            rm(ws_mean_pchem); gc()
        } #end conditional pchem+pflux block (2)
    }

    if(! pchem_only){
        append_to_productfile(network = network,
                              domain = domain,
                              prodcode = 'ms900',
                              prodname = 'precipitation',
                              notes = 'automated entry')
    }

    if(! precip_only){
        append_to_productfile(network = network,
                              domain = domain,
                              prodcode = 'ms901',
                              prodname = 'precip_chemistry',
                              notes = 'automated entry')

        if(! pchem_only){
            append_to_productfile(network = network,
                                  domain = domain,
                                  prodcode = 'ms902',
                                  prodname = 'precip_flux_inst',
                                  notes = 'automated entry')
        }
    }

    unlink(glue('data/{n}/{d}/precip_idw_quickref',
                n = network,
                d = domain),
           recursive = TRUE)
}

ms_unparallelize <- function(cluster_object){

    #if cluster_object is NULL, nothing will happen

    # tryCatch({print(site_code)},
    #         error=function(e) print('nope'))

    if(is.null(cluster_object)){
        future::plan(future::sequential)
        return()
    }

    parallel::stopCluster(cluster_object)

    #remove foreach clutter that might compromise the next parallel run
    fe_junk <- foreach:::.foreachGlobals

    rm(list = ls(name = fe_junk),
       pos = fe_junk)

    # #remove any unneeded globals that were created during parallelization
    # unneeded_globals <- c('pchem_vars', 'pchem_vars_fluxable',
    #                       'dem', 'wbi', 'rg', 'precip',
    #                       'pchem_setlist', 'first_fluxvar_ind', 'i', 'j')
    # sw(rm(list = unneeded_globals,
    #       envir = .GlobalEnv))

    # #restore globals that were overwritten during parallelization
    # protected_vars <- mget('protected_vars',
    #                           envir = protected_environment)
    #
    # for(i in 1:length(protected_vars)){
    #
    #     nm <- names(protected_vars)[i]
    #     val <- protected_vars[[i]]
    #
    #     if(! is.null(val)){
    #         assign(nm,
    #                value = val,
    #                envir = .GlobalEnv)
    #     } else {
    #
    #         #or remove them if they didn't exist before parallelization
    #         sw(rm(list = nm,
    #               envir = .GlobalEnv))
    #     }
    # }
}

chunk_df <- function(d, nchunks, create_index_column = FALSE){

    nr <- nrow(d)
    chunksize <- nr/nchunks

    if(nr > 0){

        if(create_index_column) d <- mutate(d, ind = 1:n())

        chunklist <- split(d,
                           0:(nr - 1) %/% chunksize)

        return(chunklist)

    } else {

        logwarn(msg = 'Trying to chunk an empty tibble. Something is probably wrong',
                logger = logger_module)

        return(d)
    }
}

invalidate_derived_products <- function(successor_string){

    if(all(is.na(successor_string)) || successor_string == ''){
        return()
    }

    successors <- strsplit(successor_string, '\\|\\|')[[1]]

    for(s in successors){

        catch <- update_data_tracker_d(network = network,
                                       domain = domain,
                                       tracker_name = 'held_data',
                                       prodname_ms = s,
                                       site_code = 'sitename_NA',
                                       new_status = 'pending')
    }

    #return()
}

write_metadata_r <- function(murl, network, domain, prodname_ms){

    #this writes the metadata file for retrieved macrosheds data
    #see write_metadata_m for munged macrosheds data and write_metadata_d
    #for derived macrosheds data

    #also see read_metadata_r and read_metadata_m

    #create raw directory if necessary

    raw_dir <- glue('data/{n}/{d}/raw/documentation',
                    n = network,
                    d = domain)
    dir.create(raw_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    #write metadata file
    data_acq_file <- glue('{rd}/documentation_{p}.txt',
                          rd = raw_dir,
                          p = prodname_ms)

    readr::write_file(murl,
                      file = data_acq_file)

    # #create portal directory if necessary
    # portal_dir <- glue('../portal/data/{d}/{p}', #portal ignores network
    #                    d = domain,
    #                    p = strsplit(prodname_ms, '__')[[1]][1])
    # dir.create(portal_dir,
    #            showWarnings = FALSE,
    #            recursive = TRUE)
    #
    # #hardlink file
    # portal_file <- glue(portal_dir, '/raw_data_documentation_url.txt')
    # unlink(portal_file)
    # invisible(sw(file.link(to = portal_file,
    #                        from = data_acq_file)))

    #return()
}

read_metadata_r <- function(network, domain, prodname_ms){

    #this reads the metadata file for retrieved macrosheds data

    #also see write_metadata_r, write_metadata_m, and write_metadata_d,
    #and read_metadata_m

    if(length(prodname_ms) != 1){
        stop('prodname_ms must be length 1')
    }

    if(prodname_ms == '<no precursors>'){
        return('N/A or Derived from lat/long (see site data table)')
    }

    murlfile <- glue('data/{n}/{d}/raw/documentation/documentation_{p}.txt',
                     n = network,
                     d = domain,
                     p = prodname_ms)

    murl <- readr::read_file(murlfile)

    return(murl)
}

read_metadata_m <- function(network, domain, prodname_ms){

    #this reads the metadata file for munged macrosheds data

    #also see write_metadata_r, write_metadata_m, and write_metadata_d,
    #and read_metadata_r

    if(length(prodname_ms) != 1){
        stop('prodname_ms must be length 1')
    }

    if(prodname_ms == '<no precursors>'){
        return('No munge kernel')
    }

    mdocf <- glue('data/{n}/{d}/munged/documentation/documentation_{p}.txt',
                  n = network,
                  d = domain,
                  p = prodname_ms)

    mdoc <- readr::read_file(mdocf)

    return(mdoc)
}

get_precursors <- function(network, domain, prodname_ms){

    #this determines which munged products were used to generate
    #a derived product

    prodfile <- glue('src/{n}/{d}/products.csv',
                     n = network,
                     d = domain)
    allprods <- sm(read_csv(prodfile))

    prodnames_ms <- paste(allprods$prodname,
                          allprods$prodcode,
                          sep='__')

    precursor_bool <- vapply(allprods$precursor_of,
                             function(x){
                                 prodname_ms %in% strsplit(as.character(x),
                                                           '\\|\\|')[[1]]
                             },
                             FUN.VALUE = logical(1),
                             USE.NAMES = FALSE)

    precursors <- prodnames_ms[precursor_bool]
    precursors <- if(length(precursors)) precursors else 'no precursors'

    return(precursors)
}

document_kernel_code <- function(network, domain, prodname_ms, level){

    #this documents (as metadata) the code used to retrieve/munge/derive
    #macrosheds data. prodname_ms == 'ws_boundary__msXXX' is handled as
    #a special case.

    #level is numeric 0, 1, or 2, corresponding to raw, munged, derived

    if(! is.numeric(level) || ! level %in% 0:2){
        stop('level must be numeric 0, 1, or 2')
    }

    if(prodname_ms == 'ws_boundary__ms000'){
        return(paste0('For ms000, see ms_delineate, defined in\n',
                      'https://github.com/MacroSHEDS/data_processing/blob/master/src/global_helpers.R'))
    }

    kernel_file <- glue('src/{n}/{d}/processing_kernels.R',
                        n = network,
                        d = domain)

    thisenv <- environment()

    sw(source(kernel_file, local = TRUE))

    prodcode <- prodcode_from_prodname_ms(prodname_ms)
    fnc <- mget(paste0('process_', level, '_', prodcode),
                envir = thisenv,
                inherits = FALSE,
                ifnotfound = list(''))[[1]] #arg only available in mget
    kernel_func <- paste(deparse(fnc), collapse = '\n')
    func_name <- glue('process_{l}_{pc}',
                      l = level,
                      pc = prodcode_from_prodname_ms(prodname_ms))

    kernel_func <- paste(func_name,
                         kernel_func,
                         sep = ' <- ')

    return(kernel_func)
}

write_metadata_m <- function(network, domain, prodname_ms, tracker){

    #this writes the metadata file for munged macrosheds data
    #see write_metadata_r for retrieved macrosheds data and write_metadata_d
    #for derived macrosheds data

    #also see read_metadata_r and read_metadata_m

    #assemble metadata
    sitelist <- names(tracker[[prodname_ms]])
    complist <- lapply(sitelist,
                       function(x){
                           comp <- tracker[[prodname_ms]][[x]]$retrieve$component
                           if(is.null(comp)) comp <- 'NULL'
                           return(comp)
                       })

    compsbysite <- mapply(function(s, c){
        glue('\tfor site: {s}\n\t\tcomp(s): {c}',
             s = s,
             c = paste(c, collapse = ', '),
             .trim = FALSE)
    }, sitelist, complist)

    display_args <- list(network = paste0("'", network, "'"),
                         domain = paste0("'", domain, "'"),
                         prodname_ms = paste0("'", prodname_ms, "'"),
                         # site_code = paste0("'", site_code, "'"),
                         site_code = glue("<separately, each of: '",
                                          paste(sitelist,
                                                collapse = "', '"),
                                          "', with corresponding component>"),
                         `component(s)` = paste0('\n',
                                                 paste(compsbysite,
                                                       collapse = '\n')))

    # metadata_r <- read_metadata_r(network = network,
    #                               domain = domain,
    #                               prodname_ms = prodname_ms)

    code_m <- document_kernel_code(network = network,
                                   domain = domain,
                                   prodname_ms = prodname_ms,
                                   level = 1)

    mdoc <- read_file('src/templates/write_metadata_m_boilerplate.txt') %>%
        glue(.,
             p = prodname_ms,
             # mr = metadata_r,
             mk = code_m,
             ma = paste(names(display_args),
                       display_args,
                       sep = ' = ',
                       collapse = '\n'))

    #create munged directory if necessary
    munged_dir <- glue('data/{n}/{d}/munged/documentation',
                       n = network,
                       d = domain)
    dir.create(munged_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    #write metadata file
    data_acq_file <- glue('{md}/documentation_{p}.txt',
                          md = munged_dir,
                          p = prodname_ms)
    readr::write_file(mdoc,
                      file = data_acq_file)

    # #create portal directory if necessary
    # portal_dir <- glue('../portal/data/{d}/documentation', #portal ignores network
    #                    d = domain)
    # dir.create(portal_dir,
    #            showWarnings = FALSE,
    #            recursive = TRUE)
    #
    # #hardlink file
    # portal_file <- glue('{pd}/documentation_{p}.txt',
    #                     pd = portal_dir,
    #                     p = prodname_ms)
    # unlink(portal_file)
    # invisible(sw(file.link(to = portal_file,
    #                        from = data_acq_file)))
}

write_metadata_d <- function(network,
                             domain,
                             prodname_ms){

    #this writes the metadata file for derived macrosheds data
    #see write_metadata_r for retrieved macrosheds data and write_metadata_m
    #for munged macrosheds data

    #also see read_metadata_r and read_metadata_m

    #assemble metadata
    display_args <- list(network = paste0("'", network, "'"),
                         domain = paste0("'", domain, "'"),
                         prodname_ms = paste0("'", prodname_ms, "'"))

    precursors <- get_precursors(network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms)

    if(length(precursors == 1) && precursors == 'no precursors'){

        if(grepl('(^cdnr_|^usgs_)', prodname_ms)){
            return()
        } else if(! grepl('(gauge_locations|ws_boundary__ms...)', prodname_ms)){
            stop(glue('really no precursors for {p}? might need to update products.csv', p = prodname_ms))
        }
    }

    precursors_m <- precursors[! is_derived_product(precursors)]
    precursors_d <- precursors[is_derived_product(precursors)]

    if(length(precursors_m) == 1 && precursors_m == 'no precursors'){
        precursors_m <- '<no precursors>'
    }

    if(length(precursors_d) & ! length(precursors_m)){

        precursors_m <- glue('<NA: all immediate precursors ({dp}) are derived products>',
                             dp = paste(precursors_d,
                                        collapse = ', '))
        urls_r <- NULL
        docs_m <- c()
        code_d <- document_kernel_code(network = network,
                                       domain = domain,
                                       prodname_ms = prodname_ms,
                                       level = 2)
    } else {

        urls_r <- sapply(precursors_m,
                         function(x){
                             read_metadata_r(network = network,
                                             domain = domain,
                                             prodname_ms = x)
                         })

        docs_m <- sapply(precursors_m,
                         function(x){
                             read_metadata_m(network = network,
                                             domain = domain,
                                             prodname_ms = x)
                         })

        code_d <- lapply(c(precursors_d, prodname_ms),
                         function(x){
                             document_kernel_code(network = network,
                                                  domain = domain,
                                                  prodname_ms = x,
                                                  level = 2)
                         })
    }

    ddoc <- read_file('src/templates/write_metadata_d_boilerplate.txt') %>%
        glue(.,
             p = prodname_ms,
             mp = paste(precursors_m,
                        collapse = '\n'),
             ru = ifelse(is.null(urls_r),
                         '<NA: See documentation for derived precursors>',
                         paste(paste0(paste0(names(urls_r),
                                             ':\n'),
                                      unname(urls_r)),
                               collapse = '\n\n')),
             flux_note = ifelse(grepl('flux', prodname_ms),
                                read_file('src/templates/flux_note.txt'),
                                ''),
             vsnless_note = ifelse(any(grepl('VERSIONLESS', precursors)),
                                   read_file('src/templates/VERSIONLESS_note.txt'),
                                   ''),
             dk = paste(code_d,
                        collapse = '\n\n'),
             da = paste(names(display_args),
                       display_args,
                       sep = ' = ',
                       collapse = '\n'),
             mb = paste(docs_m,
                        collapse = '\n\n'))

    #create derived directory if necessary
    derived_dir <- glue('data/{n}/{d}/derived/documentation',
                        n = network,
                        d = domain)
                        # p = prodname_ms)
    dir.create(derived_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    #write metadata file
    data_acq_file <- glue('{dd}/documentation_{p}.txt',
                          dd = derived_dir,
                          p = prodname_ms)
    readr::write_file(ddoc,
                      file = data_acq_file)

    # #create portal directory if necessary
    # portal_dir <- glue('../portal/data/{d}/documentation', #portal ignores network
    #                    d = domain)
    #                    # p = strsplit(prodname_ms, '__')[[1]][1])
    # dir.create(portal_dir,
    #            showWarnings = FALSE,
    #            recursive = TRUE)
    #
    # #hardlink file
    # portal_file <- glue('{pd}/documentation_{p}.txt',
    #                     pd = portal_dir,
    #                     p = prodname_ms)
    # unlink(portal_file) #this will overwrite any munged product supervened by derive
    # invisible(sw(file.link(to = portal_file,
    #                        from = data_acq_file)))
}

write_metadata_d_linkprod <- function(network,
                                      domain,
                                      prodname_ms_mr,
                                      prodname_ms_d){

    #this writes the metadata file for macrosheds linkprods.
    #see write_metadata_d for standard derived products

    #assemble metadata
    url_r <- try(read_metadata_r(network = network,
                                 domain = domain,
                                 prodname_ms = prodname_ms_mr),
                 silent = TRUE)

    if(inherits(url_r, 'try-error') || ! length(url_r)){

        if(prodname_from_prodname_ms(prodname_ms_d) == 'ws_boundary'){
            url_r <- 'NA: Derived from lat/long (see site data table)'
        } else {
            url_r <- 'NA'
        }
    }

    doc_m <- read_metadata_m(network = network,
                             domain = domain,
                             prodname_ms = prodname_ms_mr)

    if(inherits(doc_m, 'try-error') || ! length(doc_m)){

        if(prodname_from_prodname_ms(prodname_ms_d) == 'ws_boundary'){
            doc_m <- read_file('src/templates/ws_bounds_note.txt')
        } else {
            doc_m <- 'NA'
        }

    } else {

        if(prodname_from_prodname_ms(prodname_ms_d) == 'ws_boundary'){
            ws_bounds_note <- read_file('src/templates/ws_bounds_note.txt')
        } else {
            ws_bounds_note <- ''
        }
    }

    ddoc <- read_file('src/templates/write_metadata_d_linkprod_boilerplate.txt') %>%
        glue(.,
             p = prodname_ms_d,
             ru = url_r,
             flux_note = ifelse(grepl('flux', prodname_ms_d),
                                read_file('src/templates/flux_note.txt'),
                                ''),
             vsnless_note = ifelse(grepl('VERSIONLESS', prodname_ms_mr),
                                   read_file('src/templates/VERSIONLESS_note.txt'),
                                   ''),
             mb = doc_m,
             ws_bounds_note = ws_bounds_note)

    #create derived directory if necessary
    derived_dir <- glue('data/{n}/{d}/derived/documentation',
                        n = network,
                        d = domain)
    dir.create(derived_dir,
               showWarnings = FALSE,
               recursive = TRUE)

    #write metadata file
    data_acq_file <- glue('{dd}/documentation_{p}.txt',
                          dd = derived_dir,
                          p = prodname_ms_d)
    readr::write_file(ddoc,
                      file = data_acq_file)
}

identify_detection_limit_s <- function(x){

    #this is the scalar version of identify_detection_limit (_s).
    #it was the first iteration, and has been superseded by the temporally-
    #explicit version (identify_detection_limit_t).
    #that version relies on stored data, so automatically
    #writes to data/<network>/<domain>/detection_limits.json. This version
    #just returns its output. This version is still used for idw (where input
    #sites != output sites), but we should find a way to get the minimum input
    #detection limit for all sites being averaged and apply that detlim to the
    #output.)

    #if x is a 2d array-like object, the detection limit (number of
    #decimal places) of each column is returned. non-numeric columns return NA.
    #If x is a vector (or something that can be coerced to a vector),
    #the detection limit is returned as a scalar.

    #detection limit is computed as the 10th percentile of the number of characters
    #following each decimal place. NAs and zeros are ignored when computing
    #detection limit.

    identify_detection_limit_v <- function(x){

        #x is a vector, or it will be coerced to one.
        #non-numeric vectors return NA vectors of the same length

        x <- unname(unlist(x))
        if(! is.numeric(x)) return(rep(NA, length(x)))

        options(scipen = 100)
        nas <- is.na(x) | x == 0

        x <- as.character(x)
        nsigdigs <- stringr::str_split_fixed(x, '\\.', 2)[, 2] %>%
            nchar()

        nsigdigs[nas] <- NA

        options(scipen = 0)

        return(nsigdigs)
    }

    if(! is.null(dim(x))){

        detlim <- vapply(X = x,
                         FUN = function(y){
                             identify_detection_limit_v(y) %>%
                                 # Mode(na.rm = TRUE)
                                 quantile(probs = 0.1,
                                          na.rm = TRUE,
                                          names = FALSE)
                         },
                         FUN.VALUE = numeric(1))

    } else if(is.atomic(x) && length(x)){
        detlim <- identify_detection_limit_v(x) %>%
            # Mode(na.rm=TRUE)
            quantile(probs = 0.1,
                     na.rm = TRUE,
                     names = FALSE)
    } else {
        stop('x must be a vector or 2d array-like')
    }

    return(detlim)
}

apply_detection_limit_s <- function(x, digits){

    #this is the scalar version of apply_detection_limit (_s).
    #it was the first iteration, and has been superseded by the temporally-
    #explicit version (apply_detection_limit_t).
    #that version relies on stored data, so automatically
    #reads from data/<network>/<domain>/detection_limits.json. This version
    #just accepts detection limits as an argument.
    #This version is still used for idw (where input
    #sites != output sites), but we should find a way to get the minimum input
    #detection limit for all sites being averaged and apply that detlim to the
    #output.)

    #x: a 2d array-like or a numeric vector
    #digits: a numeric vector if x is a 2d array-like, or a numeric scalar if
    #digits is a vector or vector-like, containing the detection limits (in
    #digits after the decimal) to be applied to x. application of detection
    #limits is handled by round.

    #if x is a 2d array-like object, digits are applied column-wise
    #If x is a vector (or something that can be coerced to a vector),
    #digits is applied elementwise

    #if x is a 2d array-like with named columns and digits is a named vector,
    #values of digits are matched by name to columns of x. unmatched values
    #of digits are ignored. unmatched columns of x are unaffected.
    #if either x or digits is not named, all names are ignored.

    #attempting to apply detection limits to non-numerics results in error

    #NA values of digits are not used.

    if(! is.numeric(digits) && ! all(is.na(digits))){
        stop('digits must be numeric')
    }

    apply_detection_limit_v <- function(x, digits){

        x <- unname(unlist(x))
        if(is.na(digits)) return(x)
        if(! is.numeric(x)) stop('all affected columns of x must be numeric.')
        x <- round(x, digits)

        return(x)
    }

    if(! is.null(dim(x))){

        # if(length(digits) != ncol(x)){
        #     stop('length of digits must equal number of columns in x')
        # }

        if(! is.null(names(digits)) && ! is.null(colnames(x))){

            #if any columns don't have detection limits specified,
            #fill in those missing specifications with NAs
            missing_specifications <- setdiff(colnames(x), names(digits))
            more_digits <- rep(NA, length(missing_specifications))
            names(more_digits) <- missing_specifications
            digits <- c(digits, more_digits)

            #ignore detection limits whose names don't match names in x
            reorder <- match(colnames(x), names(digits))
            digits <- digits[! is.na(reorder)]
            reorder <- reorder[! is.na(reorder)]
            digits <- digits[reorder]

        } else if(length(digits) != ncol(x)){
            stop('length of digits must equal number of columns in x')
        }

        x <- mapply(FUN = function(y, z){
                    apply_detection_limit_v(y, z)
                },
                y = x,
                z = digits,
                SIMPLIFY = FALSE) %>%
            as_tibble()

    } else if(is.atomic(x) && length(x)){

        if(length(digits) != 1){
            stop('length of digits must be 1 if x is a vector')
        }

        x <- apply_detection_limit_v(x, digits)

    } else {
        stop('x must be a vector or 2d array-like')
    }

    return(x)
}

Mode <- function(x, na.rm = TRUE){

    if(na.rm){
        x <- na.omit(x)
    }

    ux <- unique(x)
    mode_out <- ux[which.max(tabulate(match(x, ux)))]
    return(mode_out)

}

get_successor <- function(network,
                          domain,
                          prodname_ms){

    successor <- sm(read_csv(glue('src/{n}/{d}/products.csv',
                              n = network,
                              d = domain))) %>%
        mutate(prodname_ms = paste(prodname,
                                   prodcode,
                                   sep = '__')) %>%
        filter(prodname_ms == !!prodname_ms) %>%
        pull(precursor_of)

    return(successor)
}

knit_det_limits <- function(network, domain, prodname_ms){

    # if(is_derived_product(prodname_ms) && ! ignore_pred){
    #
    #     #if there are multiple precursors (rare), just use the first
    #     prodname_ms <- get_detlim_precursors(network = network,
    #                                          domain = domain,
    #                                          prodname_ms = prodname_ms)
    # }

    detlim <- read_detection_limit(network, domain, prodname_ms[1])

    if(is.null(detlim)){
        prodname_ms <- get_successor(network = network,
                                     domain = domain,
                                     prodname_ms = prodname_ms[1])
    }

    for(i in 2:length(prodname_ms)) {
        detlim_ <- read_detection_limit(network, domain, prodname_ms[i])

        old_vars <- names(detlim)
        new_vars <- names(detlim_)

        common_vars <- base::intersect(old_vars, new_vars)

        if(length(common_vars) > 0) {

            for(p in 1:length(common_vars)) {

                old_sites <- names(detlim[[common_vars[p]]])
                new_sites <- names(detlim_[[common_vars[p]]])

                common_sites <- base::intersect(old_sites, new_sites)

                if(!length(common_sites) == 0) {
                    new_sites <- new_sites[!new_sites %in% common_sites]
                }

                if(length(new_sites) > 0){
                    for(z in 1:length(new_sites)) {
                        detlim[[common_vars[p]]][[new_sites[z]]] <- detlim_[[common_vars[p]]][[new_sites[z]]]
                    }
                }
            }
        }

        unique_vars <- new_vars[!new_vars %in% old_vars]
        if(length(unique_vars) > 0) {
            for(v in 1:length(unique_vars)) {
                detlim[[unique_vars[v]]] <- detlim_[[unique_vars[v]]]
            }

        }
    }

    return(detlim)
}

identify_detection_limit_t <- function(X, network, domain, prodname_ms,
                                       return_detlims = FALSE,
                                       ignore_arrange = FALSE){

    #this is the temporally explicit version of identify_detection_limit (_t).
    #it supersedes the scalar version (identify_detection_limit_s).
    #that version just returns its output. This version relies on stored data,
    #so automatically writes to data/<network>/<domain>/detection_limits.json,
    #and, if return_detlims = TRUE, returns its output as an integer vector
    #of detection limits with length equal to the number of rows in X, where each
    #value holds the detection limit of its corresponding data value in X$val

    #X is a 2d array-like object. must have datetime,
    #site_code, var, and val columns. if X was generated by ms_cast_and_reflag,
    #you're good to go.

    #the detection limit (number of decimal places)
    #of each column is written to data/<network>/<domain>/detection_limits.json
    #as a nested list:
    #prodname_ms
    #    variable
    #        startdt: datetime1, datetime2, datetimeN...
    #        lim:     limit1,    limit2,    limitN...

    #detection limit (detlim) is computed as the
    #number of characters following the decimal. NA detlims are filled
    #by locf, followed by nocb. Then, to account for false detlims arising from
    #trailing zeros, positive monotonicity is forced by carrying forward
    #cumulative maximum detlims. Each time the detlim increases,
    #a new startdt and limit are recorded.

    #X will be sorted ascendingly by site_code, var, and then datetime. If
    #   return_detlims = TRUE and you'll be using the output to establish
    #   uncertainty, be sure that X is already sorted in this way, or detlims
    #   won't line up with their corresponding data values.
    #   If X was generated by ms_cast_and_reflag, you're good to go.

    if(!isTRUE(ignore_arrange)){
        X <- as_tibble(X) %>%
            arrange(site_code, var, datetime)
    }

    identify_detection_limit_ <- function(X, v, output = 'list'){

        if(! output %in% c('vector', 'list')){
            stop('output must be "vector" or "list"')
        }

        x <- filter(X, var == v)

        if(nrow(x) == 0){
            return(NULL)
        }

        sn = x$site_code
        dt = x$datetime

        options(scipen = 100)
        nas <- is.na(x$val) | x$val == 0

        val <- as.character(x$val)
        nsigdigs <- stringr::str_split_fixed(val, '\\.', 2)[, 2] %>%
            nchar()

        nsigdigs[nas] <- NA

        #for each site, clean up the timeseries of detection limits:
        #   first, fill NAs by locf, then by nocb
        #   next, force positive monotonicity by locf
        nsigdigs_l <- tibble(nsigdigs, dt, sn) %>%
            base::split(sn) %>%
            map(~ if(all(is.na(.x$nsigdigs))) .x else
                mutate(.x,
                       nsigdigs = imputeTS::na_locf(nsigdigs,
                                                    na_remaining = 'rev') %>%
                           force_monotonic_locf()))

        if(output == 'vector'){

            #avoid the case where the first few detection lims
            #are artificially set low because their last sigdig is 0
            nsigdigs_l <- lapply(X = nsigdigs_l,
                                 FUN = function(z){

                                     #for sites with all-NA detlims, return as-is
                                     if(all(is.na(z$nsigdigs))){
                                         return(z)
                                     }

                                     if(length(z$nsigdigs) > 5 &&
                                        length(unique(z$nsigdigs[1:5]) > 1)){
                                         z$nsigdigs[1:5] <- z$nsigdigs[6]
                                     }

                                     return(z)
                                 })

            nsigdigs_df <- Reduce(bind_rows, nsigdigs_l) %>%
                arrange(sn, dt) #probably superfluous, but safe

            options(scipen = 0)

            detlims <- nsigdigs_df$nsigdigs

            return(detlims)
        }

        #build datetime-detlim pairs for each change in detlim for each variable
        detlims <- lapply(X = nsigdigs_l,
                          FUN = function(z){

                              #for sites with all-NA detlims, build the same
                              #default list as above
                              if(all(is.na(z$nsigdigs))){
                                  detlims <- list(startdt = as.character(z$dt[1]),
                                                  lim = NA)
                                  return(detlims)
                              }

                              runs <- rle2(z$nsigdigs)

                              #avoid the case where the first few detection lims
                              #are artificially set low because their last
                              #sigdig is 0
                              if(runs$lengths[1] %in% 1:5 && nrow(runs) > 1){
                                  runs <- runs[-1, ]
                                  runs$starts[1] <- 1
                              }

                              detlims <- list(startdt = as.character(z$dt[runs$starts]),
                                              lim = runs$values)
                          })

        options(scipen = 0)

        return(detlims)
    }

    variables <- unique(X$var)

    detlim <- lapply(variables,
                     function(z) identify_detection_limit_(X, z))
    detlim <- detlim[! sapply(detlim, is.null)]
    names(detlim) <- variables

    write_detection_limit(detlim,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms)

    if(return_detlims){

        detlim_v <- rep(NA, nrow(X))

        for(v in variables){
            for(s in unique(X$site_code)){
                x <- filter(X, site_code == s)
                dlv <- identify_detection_limit_(x, v, output = 'vector')
                if(is.null(dlv)) next
                detlim_v[X$site_code == s & X$var == v] <- dlv
            }
        }

        return(detlim_v)
    }

    #return()
}

apply_detection_limit_t <- function(X,
                                    network,
                                    domain,
                                    prodname_ms,
                                    ignore_pred = FALSE){

    #this is the temporally explicit version of apply_detection_limit (_t).
    #it supersedes the scalar version (apply_detection_limit_s).
    #that version just returns its output. This version relies on stored data,
    #so automatically reads from data/<network>/<domain>/detection_limits.json.

    #X is a 2d array-like object. must have datetime,
    #   site_code, var, and val columns. if X was generated by ms_cast_and_reflag,
    #   you should be good to go.
    #ignore_pred: logical; set to TRUE if detection limits should be retrieved
    #   from the supplied prodname_ms directly, rather than its precursor.

    #Attempting to apply detection
    #limits to a variable for which detection limits are not known (not present
    #in detection_limits.json) results in error. Superfluous variable entries in
    #detection_limits.json are ignored.

    X <- as_tibble(X) %>%
        arrange(site_code, var, datetime)

    if(ignore_pred &&
       (length(prodname_ms) > 1 || ! is_derived_product(prodname_ms))){
        stop('If ignoring precursors, a single derived prodname_ms must be supplied')
    }

    if(length(prodname_ms) > 1 && any(is_derived_product(prodname_ms))){
        #i can't think of a time when we'd need to supply multiple derived
        #products and knit them, but if such a case exists feel free to
        #amend this error checker
        stop('Cannot knit multiple detlims when one or more of them is derived.')
    }

    if(length(prodname_ms) == 1 &&
       is_derived_product(prodname_ms) &&
       ! ignore_pred){

        prodname_ms <- get_detlim_precursors(network = network,
                                             domain = domain,
                                             prodname_ms = prodname_ms)
    }

    if(length(prodname_ms) > 1) {
        detlim <- knit_det_limits(network = network,
                                  domain = domain,
                                  prodname_ms = prodname_ms)
    } else {
        detlim <- read_detection_limit(network = network,
                                       domain = domain,
                                       prodname_ms = prodname_ms)
    }

    if(is_ms_err(detlim)){
        stop('problem reading detection limits from file')
    }

    apply_detection_limit_ <- function(x, varnm, detlim){


        #plenty of code superfluity in this function. adapted from a previous
        #   version and there's negligible efficiency loss if any

        if(! varnm %in% names(detlim)){
            stop(glue('Missing detection limits for var: {v}', v = varnm))
        }

        detlim_var <- detlim[[varnm]]

        x <- filter(x, var == varnm)

        #nrow(x) == 1 was added because there was an error occurring if there
        #was a site with only one sample of a variable
        if(nrow(x) == 0){
            return(NULL)
        }

        sn = x$site_code
        dt = x$datetime

        site_lst <- tibble(dt, sn, val = x$val) %>%
            base::split(sn)

        Xerr <- lapply(X = site_lst,
                       FUN = function(z) errors(z$val)) %>%
            unlist() %>%
            unname()

        rounded <- lapply(X = site_lst,
                          FUN = function(z){

                              if(all(is.na(z$val))) return(z$val)

                              detlim_varsite <- detlim_var[[z$sn[1]]]
                              if(all(is.na(detlim_varsite$lim))) return(z$val)

                              cutvec <- c(as.POSIXct(detlim_varsite$startdt,
                                                     tz = 'UTC'),
                                          as.POSIXct('2900-01-01 00:00:00'))

                              roundvec <- cut(x = z$dt,
                                              breaks = cutvec,
                                              include.lowest = TRUE,
                                              labels = detlim_varsite$lim) %>%
                                              as.character() %>%
                                              as.numeric()

                              #sometimes synchronize_timestep will adjust a point
                              #to a time before the earliest startdt recorded
                              #in detection_limits.json. this handles that.
                              if(length(roundvec == 1) && is.na(roundvec)){
                                  roundvec = detlim_varsite$lim[1]
                              } else {
                                  roundvec <- imputeTS::na_locf(x = roundvec,
                                                                option = 'nocb')
                              }

                              rounded <- mapply(FUN = function(a, b){
                                                    round(a, b)
                                                },
                                                a = z$val,
                                                b = roundvec,
                                                USE.NAMES = FALSE)

                              return(rounded)
                          }) %>%
            unlist() %>%
            unname()

        errors(rounded) <- Xerr

        return(rounded)
    }

    variables <- unique(X$var)

    for(v in variables){
        for(s in unique(X$site_code)){
            x <- filter(X, site_code == s)
            dlv <- apply_detection_limit_(x, v, detlim)
            if(is.null(dlv)) next
            X$val[X$site_code == s & X$var == v] <- dlv
        }
    }

    return(X)
}

read_detection_limit <- function(network, domain, prodname_ms){

    detlims <- glue('data/{n}/{d}/detection_limits.json',
                    n = network,
                    d = domain) %>%
        readr::read_file() %>%
        jsonlite::fromJSON()

    detlims_prod <- detlims[[prodname_ms]]

    return(detlims_prod)
}

write_detection_limit <- function(detlim, network, domain, prodname_ms){

    #NOTE: this function updated 2021-02-16, near the end of rebuilding
    #   LTER (discovered issue with luquillo sites overwriting each other in
    #   the detlim file). as such, it's not thoroughly tested. some stuff
    #   that worked before might be broken now. tried to make it backward compatible though

    detlims_file <- glue('data/{n}/{d}/detection_limits.json',
                         n = network,
                         d = domain)

    detlim_new <- detlim #better name; don't want to update every call though

    if(file.exists(detlims_file)){

        detlim_stored <- jsonlite::fromJSON(readr::read_file(detlims_file))
        if(prodname_ms %in% names(detlim_stored)){

            for(v in names(detlim_new)){

                site_detlims <- detlim_new[[v]]
                for(s in names(site_detlims)){
                    detlim_stored[[prodname_ms]][[v]][[s]] <- site_detlims[[s]]
                }
            }

        } else {
            detlim_stored[[prodname_ms]] <- detlim_new
        }

    } else {
        detlim_stored <- list(placeholder = detlim_new)
        names(detlim_stored) <- prodname_ms
    }

    readr::write_file(jsonlite::toJSON(detlim_stored), detlims_file)
}

rle2 <- function(x){#, return_list = FALSE){

    r <- rle(x)
    ends <- cumsum(r$lengths)

    # if(return_list){
    #
    #     r <- list(values = r$values,
    #               starts = c(1, ends[-length(ends)] + 1),
    #               stops = ends,
    #               lengths = r$lengths)
    #
    # } else {

    r <- tibble(values = r$values,
                starts = c(1, ends[-length(ends)] + 1),
                stops = ends,
                lengths = r$lengths)
    # }

    return(r)
}

force_monotonic_locf <- function(v, ascending = TRUE){

    if(any(is.na(v))){
        stop('v may not contain NAs')
    }

    if(ascending){
        mv <- cummax(v)
        adjust <- v < mv
    } else {
        mv <- cummin(v)
        adjust <- v > mv
    }

    runs <- rle2(adjust)

    for(i in which(runs$values)){
        stt <- runs$starts[i]
        stp <- runs$stops[i]
        replc <- v[runs$stops[i - 1]]
        v[stt:stp] <- replc
    }

    return(v)
}

get_gee_imgcol <- function(gee_id, band, prodname, start, end, qaqc=FALSE,
                           qa_band = NULL, bit_mask = NULL) {

    col_name <- paste0(prodname, 'X')

    if(qaqc){

        # Mask bad pixles functions
        get_gee_QABits <- function(image) {
            # Convert binary (character) to decimal (little endian)
            qa <- sum(2^(which(rev(unlist(strsplit(as.character(bit_mask), "")) == 1))-1))
            # Return a mask band image, giving the qa value.
            image$bitwiseAnd(qa)$lt(1)
        }
        clean_gee_img <- function(img) {
            # Extract the selected band
            img_values <- img$select(band)

            # Extract the quality band
            img_qa <- img$select(qa_band)

            # Select pixels to mask
            quality_mask <- get_gee_QABits(img_qa)

            # Mask pixels with value zero.
            img_values$updateMask(quality_mask)

            return(img_values)

        }

        gee_imcol <- ee$ImageCollection(gee_id)$
            filterDate(start, end)$
            map(clean_gee_img)$
            map(function(x){
                date <- ee$Date(x$get("system:time_start"))$format('YYYY_MM_dd')
                name <- ee$String$cat(col_name, date)
                x$select(band)$rename(name)
            })


    } else{
        gee_imcol <- ee$ImageCollection(gee_id)$
            filterDate(start, end)$
            select(band)$
            map(function(x){
                date <- ee$Date(x$get("system:time_start"))$format('YYYY_MM_dd')
                name <- ee$String$cat(col_name, date)
                x$select(band)$rename(name)
            })
    }

    return(gee_imcol)

}

clean_gee_table <- function(ee_ws_table,
                            reducer) {

    col_names <- colnames(ee_ws_table)
    col_names <- col_names[!grepl('site_code', col_names)]

    table_fin <- ee_ws_table %>%
        pivot_longer(cols = !!col_names, values_to = 'val', names_to = 'var_date') %>%
        mutate(var = str_split_fixed(var_date, 'X', n = Inf)[,1],
               datetime = str_split_fixed(var_date, 'X', n = Inf)[,2]) %>%
        mutate(var = glue('{v}_{r}',
                          v = var,
                          r = reducer)) %>%
        select(site_code, datetime, var, val)

    return(table_fin)
}

get_gee_standard <- function(network,
                             domain,
                             gee_id,
                             band,
                             prodname,
                             rez,
                             site_boundary,
                             batch = FALSE,
                             qa_band = NULL,
                             bit_mask = NULL){

    qaqc <- FALSE
    if(!is.null(qa_band) || !is.null(bit_mask)){
        if(any(is.null(qa_band), is.null(bit_mask))){
            stop('qa_band and bit_mask must be fined is one is provided')
        } else{
            qaqc <- TRUE
        }
    }

    area <- sf::st_area(site_boundary)

    sheds <- site_boundary %>%
        as.data.frame() %>%
        sf::st_as_sf() %>%
        select(site_code) %>%
        sf::st_transform(4326) %>%
        sf::st_set_crs(4326)

    site <- unique(sheds$site_code)

    if(as.numeric(area) > 10528200 || batch){

        # Remove file if is in drive
        googledrive::drive_rm('GEE/rgee.csv', verbose = FALSE)

        # Mask bad pixles functions
        get_gee_QABits <- function(image) {
            # Convert binary (character) to decimal (little endian)
            qa <- sum(2^(which(rev(unlist(strsplit(as.character(bit_mask), "")) == 1))-1))
            # Return a mask band image, giving the qa value.
            image$bitwiseAnd(qa)$lt(1)
        }
        clean_gee_img <- function(img) {
            # Extract the selected band
            img_values <- img$select(band)

            # Extract the quality band
            img_qa <- img$select(qa_band)

            # Select pixels to mask
            quality_mask <- get_gee_QABits(img_qa)

            # Mask pixels with value zero.
            img_values$updateMask(quality_mask)

        }


        user_info <- rgee::ee_user_info(quiet = TRUE)
        asset_path <- paste0(user_info$asset_home, '/data_aq_sheds')

        ee_shape <- sf_as_ee(sheds,
                             via = 'getInfo_to_asset',
                             assetId = asset_path,
                             overwrite = TRUE,
                             quiet = TRUE)

        if(qaqc){
            imgcol <- ee$ImageCollection(gee_id)$map(clean_gee_img)$select(band)
        }else{
            imgcol <- ee$ImageCollection(gee_id)$select(band)
        }

        flat_img <- imgcol$map(function(image) {
            image$reduceRegions(
                collection = ee_shape,
                reducer = ee$Reducer$stdDev()$combine(
                    reducer2 = ee$Reducer$median(),
                    sharedInputs = TRUE),
                scale = rez
            )
        })$flatten()

        gee <- flat_img$select(propertySelectors = c('site_code', 'imageId',
                                                     'stdDev', 'median'),
                               retainGeometry = FALSE)

        ee_description <-  glue('{n}_{d}_{s}_{p}',
                                d = domain,
                                n = network,
                                s = site,
                                p = prodname)

        ee_task <- ee$batch$Export$table$toDrive(collection = gee,
                                              description = ee_description,
                                              fileFormat = 'CSV',
                                              folder = 'GEE',
                                              fileNamePrefix = 'rgee')

        ee_task$start()
        ee_monitoring(ee_task, quiet = TRUE)

        temp_rgee <- tempfile(fileext = '.csv')
        googledrive::drive_download(file = 'GEE/rgee.csv',
                                    temp_rgee,
                                    verbose = FALSE)
     # Seems to be broken
        # rgee::ee_drive_to_local(task = ee_task,
        #                         dsn = temp_rgee,
        #                         overwrite = TRUE,
        #                         quiet = TRUE)

        sd_name <- glue('{c}_sd', c = prodname)
        median_name <- glue('{c}_median', c = prodname)

        fin_table <- read_csv(temp_rgee) %>%
            mutate(imageId = substr(`system:index`, 1, 10))

        googledrive::drive_rm('GEE/rgee.csv', verbose = FALSE)
        rgee::ee_manage_delete(path_asset = asset_path,
                               quiet = TRUE)

        if(!'median' %in% colnames(fin_table) && !'stdDev' %in% colnames(fin_table)){
            return(NULL)
        }

        fin_table <- fin_table %>%
            select(site_code, stdDev, median, imageId) %>%
            rename(datetime = imageId,
                   !!sd_name := stdDev,
                   !!median_name := median) %>%
            pivot_longer(cols = all_of(c(sd_name, median_name)),
                         names_to = 'var',
                         values_to = 'val')

        fin <- list(table = fin_table,
                    type = 'batch')

    } else {

        imgcol <- get_gee_imgcol(gee_id,
                                 band,
                                 prodname,
                                 '1957-10-25',
                                 '2040-01-01',
                                 qaqc = qaqc)

        ext_median <- try(ee_extract(
            x = imgcol,
            y = sheds,
            scale = rez,
            fun = ee$Reducer$median(),
            sf = FALSE
        ))

        if(length(ext_median) <= 4 || class(ext_median) == 'try-error') {
            return(NULL)
        }

        ext_median <- clean_gee_table(ee_ws_table = ext_median,
                                      reducer = 'median')

        ext_sd <- try(ee_extract(
            x = imgcol,
            y = sheds,
            scale = rez,
            fun = ee$Reducer$stdDev(),
            sf = FALSE
        ))

        if(length(ext_sd) <= 4 || class(ext_sd) == 'try-error') {
            ext_sd <- tibble()
        } else {
            ext_sd <- clean_gee_table(ext_sd, reducer = 'sd')
        }

        fin_table <- rbind(ext_median, ext_sd)

        fin <- list(table = fin_table,
                    type = 'ee_extract')
    }

    return(fin)
}

detection_limit_as_uncertainty <- function(detlim){

    # uncert <- lapply(detlim,
    #                  FUN = function(x) 1 / 10^x) %>%
    #               as_tibble()

    uncert <- 1 / 10^detlim

    return(uncert)
}

carry_uncertainty <- function(d, network, domain, prodname_ms, ignore_arrange = FALSE){

    # Filter out any rows where vals are outside realistic  range, defined
    # in variables sheet
    d <- ms_check_range(d)

    u <- identify_detection_limit_t(d,
                                    network = network,
                                    domain = domain,
                                    prodname_ms = prodname_ms,
                                    return_detlims = TRUE,
                                    ignore_arrange = ignore_arrange)
    u <- detection_limit_as_uncertainty(u)
    errors(d$val) <- u
    # d <- insert_uncertainty_df(d, u)

    return(d)
}

err_df_to_matrix <- function(df){

    if(! all(sapply(df, class) %in% c('errors', 'numeric'))){
        stop('all columns of df must be of class "errors" or "numeric"')
    }

    errmat <- as.matrix(as.data.frame(lapply(df, errors)))
    M <- as.matrix(df)
    errors(M) <- errmat

    return(M)
}

get_relative_uncert <- function(x){

    if(any(class(x) %in% c('list', 'data.frame', 'array'))){
        stop(glue('this function not yet adapted for class {cl}',
                  cl = paste(class(tibble(x=1:3)),
                             collapse = ', ')))
    }

    ru <- errors(x) / abs(errors::drop_errors(x)) * 100

    return(ru)
}

get_phonology <- function(network, domain, prodname_ms, time, site_boundary,
                          site_code) {

    sheds_point <- site_boundary %>%
        sf::st_centroid() %>%
        sf::st_bbox()

    long <- as.numeric(sheds_point[1])

    place <- ifelse(long > -97.5, 'east', 'west')

    year_files <- list.files(glue('data/spatial/phenology/{u}/{p}',
                             p = place,
                             u = time))

    site_boundary <- sf::st_transform(site_boundary,
                                      '+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs')

    years <- as.numeric(str_split_fixed(year_files, '[.]', n = 2)[,1])

    final <- tibble()
    for(y in 1:length(years)) {

        path <- glue('data/spatial/phenology/{u}/{p}/{t}.tif',
                     u = time, p = place, t = years[y])

        ws_values <- try(extract_ws_mean(site_boundary = site_boundary,
                                         raster_path = path),
                         silent = TRUE)

        if(class(ws_values) == 'try-error') {
            return(generate_ms_exception(glue('No data was retrived for {s}',
                                              s = site_code)))
        }

        val <- ws_values['mean']
        val_sd <- ws_values['sd']
        percent_na <- ws_values['pctCellErr']

        col_name <- case_when(time == 'start_season' ~ 'sos',
                              time == 'end_season' ~ 'eos',
                              time == 'max_season' ~ 'mos',
                              time == 'length_season' ~ 'los')

        mean_name <- glue('{n}_mean', n = col_name)
        sd_name <- glue('{n}_sd', n = col_name)

        one_var <- tibble(!!mean_name := val,
                          !!sd_name := val_sd,
                          pctCellErr = percent_na,
                          site_code = site_code,
                          year = years[y])

        final <- rbind(final, one_var)
        }

    final <- final %>%
        mutate(!!mean_name := round(.data[[mean_name]])) %>%
        pivot_longer(cols = all_of(c(mean_name, sd_name)),
                     names_to = 'var',
                     values_to = 'val') %>%
        select(year, site_code, var, val, pctCellErr)

    dir <- glue('data/{n}/{d}/ws_traits/{p}/',
                n = network, d = domain, p = time)

    dir.create(dir, recursive = TRUE, showWarnings = FALSE)

    final_path <- glue('data/{n}/{d}/ws_traits/{p}/sum_{s}.feather',
                       n = network,
                       d = domain,
                       p = time,
                       s = site_code)

    final <- append_unprod_prefix(final, prodname_ms)
    write_feather(final, final_path)

    #return()
}

detection_limit_as_uncertainty <- function(detlim){

    # uncert <- lapply(detlim,
    #                  FUN = function(x) 1 / 10^x) %>%
    #               as_tibble()

    uncert <- 1 / 10^detlim

    return(uncert)
}

err_df_to_matrix <- function(df){

    if(! all(sapply(df, class) %in% c('errors', 'numeric'))){
        stop('all columns of df must be of class "errors" or "numeric"')
    }

    errmat <- as.matrix(as.data.frame(lapply(df, errors)))
    M <- as.matrix(df)
    errors(M) <- errmat

    return(M)
}

raster_intersection_summary <- function(wb, dem){

    #wb is a delineated watershed boundary as a rasterLayer
    #dem is a DEM rasterLayer

    summary_out <- list()

    #convert wb to sf object (there are several benign but seemingly uncatchable
    #   garbage collection errors here)
    wb <- sf::st_as_sf(raster::rasterToPolygons(wb))

    #get edge of DEM as sf object
    dem_edge <- raster::focal(x = dem, #the terra version doesn't retain NA border
                  fun = function(x, ...) return(0),
                  w = matrix(1, nrow = 3, ncol = 3)) %>%
        raster::reclassify(rcl = matrix(c(0, NA, #second, set inner cells to NA
                                          NA, 1), #first, set outer cells to 1... yup.
                                        ncol = 2)) %>%
        raster::rasterToPolygons() %>%
        sf::st_as_sf()
    # dem_edge <- raster::boundaries(dem) %>%
    #                                # classes = TRUE,
    #                                # asNA = FALSE) %>%
    #     raster::reclassify(rcl = matrix(c(0, NA), #set inner cells to NA
    #                                     ncol = 2)) %>%
    #     raster::rasterToPolygons() %>%
    #     sf::st_as_sf()

    #tally raster cells
    summary_out$n_wb_cells <- length(wb$geometry)
    summary_out$n_dem_cells <- length(dem_edge$geometry)

    #tally intersections; calc percent of wb cells that overlap
    intersections <- sf::st_intersects(wb, dem_edge) %>%
        as.matrix() %>%
        apply(MARGIN = 2,
              FUN = sum) %>%
        table()

    true_intersections <- sum(intersections[names(intersections) > 0])

    summary_out$n_intersections <- true_intersections
    summary_out$pct_wb_cells_intersect <- true_intersections /
        summary_out$n_wb_cells * 100

    return(summary_out)
}

remove_all_na_sites <- function(d){

    d_test <- d %>%
        mutate(na = ifelse(! is.na(val), 1, 0)) %>%
        group_by(site_code, var) %>%
        summarise(non_na = sum(na))

    d <- left_join(d, d_test, by = c("site_code", "var")) %>%
        filter(non_na > 1) %>%
        select(-non_na)
}

combine_products <- function(network, domain, prodname_ms,
                                    input_prodname_ms) {

    #Used to combine multiple products into one. Used when discharge, chemistry,
    #or other products are split into multiple products and we want them in one.

    files <- ms_list_files(network = network,
                           domain = domain,
                           prodname_ms = input_prodname_ms)

    dir <- glue('data/{n}/{d}/derived/{p}',
                n = network,
                d = domain,
                p = prodname_ms)

    dir.create(dir, showWarnings = FALSE)

    site_feather <- str_split_fixed(files, '/', n = Inf)[,6]
    sites <- unique(str_split_fixed(site_feather, '[.]feather', n = Inf)[,1])

    for(i in 1:length(sites)) {
        site_files <- grep(paste0(sites[i], '.feather'), files, value = TRUE)

        site_full <- map_dfr(site_files, read_feather)

        write_ms_file(d = site_full,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[i],
                      level = 'derived',
                      shapefile = FALSE)
    }
}

load_config_datasets <- function(from_where){

    #this loads our "configuration" datasets into the global environment.
    #as of 10/27/20 those datasets include site_data, variables, universal_products,
    #name_variants (which is not loaded), and
    #watershed_delineation_specs. depending on the type of instance (remote/local),
    #those datasets are either read as local CSVs or as google sheets. for ms
    #developers, this will always be "remote". for future users, it'll be a
    #configurable option.

    if(from_where == 'remote'){

        ms_vars <- sm(googlesheets4::read_sheet(
            conf$variables_gsheet,
            na = c('', 'NA'),
            col_types = 'cccccccnnccnn'
        ))

        site_data <- sm(googlesheets4::read_sheet(
            conf$site_data_gsheet,
            na = c('', 'NA'),
            col_types = 'ccccccccnnnnnccc'
        ))

        ws_delin_specs <- sm(googlesheets4::read_sheet(
            conf$delineation_gsheet,
            na = c('', 'NA'),
            col_types = 'cccncnnccl'
        ))

        univ_products <- sm(googlesheets4::read_sheet(conf$univ_prods_gsheet,
                                                      na = c('', 'NA')))

    } else if(from_where == 'local'){

        ms_vars <- sm(read_csv('data/general/variables.csv'))
        site_data <- sm(read_csv('data/general/site_data.csv'))
        univ_products <- sm(read_csv('data/general/universal_products.csv'))

        ws_delin_specs <- tryCatch(sm(read_csv('data/general/watershed_delineation_specs.csv')),
                                   error = function(e){
                                       empty_tibble <- tibble(network = 'a',
                                                              domain = 'a',
                                                              site_code = 'a',
                                                              buffer_radius_m = 1,
                                                              snap_method = 'a',
                                                              snap_distance_m = 1,
                                                              dem_resolution = 1,
                                                              flat_increment = 1,
                                                              breach_method = 'a',
                                                              burn_streams = 'a')

                                       return(empty_tibble[-1, ])
                                   })

    } else {
        stop('from_where must be either "local" or "remote"')
    }

    assign('ms_vars',
           ms_vars,
           pos = .GlobalEnv)

    assign('site_data',
           site_data,
           pos = .GlobalEnv)

    assign('ws_delin_specs',
           ws_delin_specs,
           pos = .GlobalEnv)

    assign('univ_products',
           univ_products,
           pos = .GlobalEnv)
}

write_portal_config_datasets <- function(){

    #so we don't have to read these from gdrive when running the app in
    #production. also, nice to report download sizes this way and avoid some
    #real-time calculation.

    dir.create('../portal/data/general',
               showWarnings = FALSE,
               recursive = TRUE)

    write_csv(ms_vars, '../portal/data/general/variables.csv')
    write_csv(site_data, '../portal/data/general/site_data.csv')
}

compute_download_filesizes <- function(){

    #determines approximate sizes of downloadable zipfiles for each domain.
    #doing it here saves computation time in the portal.

    dir.create('../portal/data/general/download_sizes',
               showWarnings = FALSE,
               recursive = TRUE)

    dmn_dirs <- list.files('../portal/data/')
    dmn_dirs <- dmn_dirs[! dmn_dirs == c('general', 'all_ws_bounds', 'all_ws_bounds.zip')]

    dmn_dl_size <- data.frame(domain = dmn_dirs,
                              dl_size_MB = NA_character_)

    for(i in seq_along(dmn_dirs)){

        dmnfiles <- list.files(paste0('../portal/data/', dmn_dirs[i]),
                               full.names = TRUE,
                               recursive = TRUE,
                               include.dirs = FALSE,
                               pattern = '\\.feather$')

        dmnfilesizes <- sapply(dmnfiles, file.size)

        if(length(dmnfilesizes)){

            total_MB <- dmnfilesizes %>%
                sum() %>%
                {. / 1e6 * 0.12} %>% # 0.12 is the approximate compression ratio after zipping
                round(1) %>%
                as.character()

        } else {
            total_MB <- 'pending'
        }

        dmn_dl_size$dl_size_MB[i] <- ifelse(total_MB == '0', '< 1', total_MB)
    }

    write_csv(x = dmn_dl_size,
              file = '../portal/data/general/download_sizes/timeseries.csv')
}

ms_write_confdata <- function(x,
                              which_dataset,
                              to_where,
                              overwrite = FALSE){

    #x: a tibble or data.frame
    #which_dataset: string. either "ms_vars", "site_data", "univ_products",
    #   "name_variants", or "ws_delin_specs"
    #to_where: string. either "remote", meaning write this file to a google
    #   sheets connection defined in data_acquisition/config.json and
    #   data_acquisition/googlesheet_service_accnt.json, or "local", meaning
    #   write this file locally to data_acquisition/data/general/<which_dataset>.csv
    #overwrite: logical; If FALSE, x will be appended to which_dataset

    #this writes our "configuration" datasets to their appropriate locations.
    #as of 10/27/20 those datasets include site_data, ms_vars, universal_products,
    #name_variants, and ws_delin_specs
    #depending on the type of instance (remote/local),
    #those datasets are either written to local CSVs or to google sheets. for ms
    #developers, this will always be "remote". for future users, it'll be a
    #configurable option.

    #which_dataset will also be updated in memory

    known_datasets <- c('ms_vars', 'site_data', 'ws_delin_specs',
                        'univ_products', 'name_variants')

    if(! which_dataset %in% known_datasets){
        stop(glue('which_dataset must be one of: "{kd}"',
                  kd = paste(known_datasets, collapse = '", "')))
    }

    type_string <- case_when(
        which_dataset == 'ms_vars' ~ 'cccccccnnccnn',
        which_dataset == 'site_data' ~ 'ccccccccnnnnnccc',
        which_dataset == 'ws_delin_specs' ~ 'cccncnnccl',
        TRUE ~ 'placeholder')

    if(which_dataset %in% c('univ_products', 'name_variants')){
        type_string <- NULL
    }

    if(to_where == 'remote'){

        write_loc <- case_when(
            which_dataset == 'ms_vars' ~ conf$variables_gsheet,
            which_dataset == 'site_data' ~ conf$site_data_gsheet,
            which_dataset == 'univ_products' ~ conf$univ_prods_gsheet,
            which_dataset == 'name_variants' ~ conf$name_variant_gsheet,
            which_dataset == 'ws_delin_specs' ~ conf$delineation_gsheet)

        if(overwrite){

            # sm(googlesheets4::write_sheet(data = x,
            #                               ss = write_loc,
            #                               sheet = 1))
            catch <- expo_backoff(
                expr = {
                    sm(googlesheets4::write_sheet(data = x,
                                                  ss = write_loc,
                                                  sheet = 1))
                },
                max_attempts = 4
            )

        } else {

            catch <- expo_backoff(
                expr = {
                    sm(googlesheets4::sheet_append(data = x,
                                                   ss = write_loc,
                                                   sheet = 1))
                },
                max_attempts = 4
            )

        }

        catch <- expo_backoff(
            expr = {
                dset <- sm(googlesheets4::read_sheet(ss = write_loc,
                                                     na = c('', 'NA'),
                                                     col_types = type_string))
            },
            max_attempts = 4
        )

    } else if(to_where == 'local'){

        write_loc <- case_when(
            which_dataset == 'ms_vars' ~ 'variables.csv',
            which_dataset == 'site_data' ~ 'site_data.csv',
            which_dataset == 'univ_products' ~ 'universal_products.csv',
            which_dataset == 'name_variants' ~ 'name_variants.csv',
            which_dataset == 'ws_delin_specs' ~ 'watershed_delineation_specs.csv')

        if(overwrite){

            write_csv(x,
                      path = paste0('data/general/',
                                    write_loc))
        } else {

            dset <- read_csv(path = paste0('data/general/',
                                           write_loc))
            dset <- bind_rows(dset, x)

            write_csv(x = dset,
                      path = paste0('data/general/',
                                    write_loc))
        }

        dset <- read_csv(path = paste0('data/general/',
                                       write_loc),
                         col_types = type_string)

    } else {
        stop('to_where must be either "local" or "remote"')
    }

    assign(x = which_dataset,
           value = dset,
           envir = .GlobalEnv)
}

filter_single_samp_sites <- function(df) {

    counts <- df %>%
        group_by(site_code, var) %>%
        summarise(n = n())

    df <- left_join(df, counts, by = c('site_code', 'var')) %>%
        filter(n > 1) %>%
        select(-n)

    return(df)
}

#this section is for generalized derive kernels. this file is getting pretty huge.
#we should soon separate it into several different files, each with a
#particular category of global helpers.
derive_stream_flux <- function(network, domain, prodname_ms){

    schem_prodname_ms <- get_derive_ingredient(network = network,
                                               domain = domain,
                                               prodname = 'stream_chemistry',
                                               accept_multiple = TRUE)

    disch_prodname_ms <- get_derive_ingredient(network = network,
                                               domain = domain,
                                               prodname = 'discharge',
                                               accept_multiple = TRUE)

    chemfiles <- ms_list_files(network = network,
                               domain = domain,
                               prodname_ms = schem_prodname_ms)

    qfiles <- ms_list_files(network = network,
                            domain = domain,
                            prodname_ms = disch_prodname_ms)

    flux_sites <- base::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        flux <- sw(calc_inst_flux(chemprod = schem_prodname_ms,
                                  qprod = disch_prodname_ms,
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

    return()
}

derive_precip_pchem_pflux <- function(network, domain, prodname_ms){

    #this function does the work of derive_precip, derive_precip_chem,
    #and derive_precip_flux. use it wherever possible to minimize
    #run time.

    # prodname_ms = 'precip_pchem_pflux__ms002'
    pchem_prodname_ms <- get_derive_ingredient(network = network,
                                               domain = domain,
                                               prodname = 'precip_chemistry')

    precip_prodname_ms <- get_derive_ingredient(network = network,
                                                domain = domain,
                                                prodname = 'precipitation')

    wb_prodname_ms <- get_derive_ingredient(network = network,
                                            domain = domain,
                                            prodname = 'ws_boundary')

    rg_prodname_ms <- get_derive_ingredient(network = network,
                                            domain = domain,
                                            prodname = 'precip_gauge_locations')

    # pchem_prodname = pchem_prodname_ms; precip_prodname = precip_prodname_ms
    # wb_prodname = wb_prodname_ms; pgauge_prodname = rg_prodname_ms
    precip_pchem_pflux_idw(pchem_prodname = pchem_prodname_ms,
                           precip_prodname = precip_prodname_ms,
                           wb_prodname = wb_prodname_ms,
                           pgauge_prodname = rg_prodname_ms,
                           prodname_ms = prodname_ms)
                           # flux_prodname_out = prodname_ms)

    return()
}

#end generalized derive kernel section

precip_gauge_from_site_data <- function(network, domain, prodname_ms) {

    locations <- site_data %>%
        filter(network == !!network,
               domain == !!domain,
               site_type == 'rain_gauge')

    crs <- unique(locations$CRS)

    if(length(crs) > 1) {
        stop('crs is not consistent for all sites, cannot convert location in
             site_data to precip_gauge location product')
    }

    locations <- locations %>%
        sf::st_as_sf(coords = c('longitude', 'latitude'), crs = crs) %>%
        select(site_code)

    path <- glue('data/{n}/{d}/derived/{p}',
                 n = network,
                 d = domain,
                 p = prodname_ms)

    dir.create(path, recursive = TRUE)

    for(i in 1:nrow(locations)) {

        site_code <- pull(locations[i,], site_code)

        sf::st_write(locations[i,], glue('{p}/{s}',
                                         p = path,
                                         s = site_code),
                     driver = 'ESRI Shapefile',
                     delete_dsn = TRUE,
                     quiet = TRUE)
    }
}

stream_gauge_from_site_data <- function(network, domain, prodname_ms) {

    locations <- site_data %>%
        filter(network == !!network,
               domain == !!domain,
               site_type == 'stream_gauge')

    crs <- unique(locations$CRS)

    if(length(crs) > 1) {
        stop('crs is not consistent for all sites, cannot convert location in
             site_data to stream_gauge location product')
    }

    locations <- locations %>%
        sf::st_as_sf(coords = c('longitude', 'latitude'), crs = crs) %>%
        select(site_code)

    path <- glue('data/{n}/{d}/derived/{p}',
                 n = network,
                 d = domain,
                 p = prodname_ms)

    dir.create(path, recursive = TRUE)

    for(i in 1:nrow(locations)) {

        site_code <- pull(locations[i,], site_code)

        sf::st_write(locations[i,], glue('{p}/{s}',
                                         p = path,
                                         s = site_code),
                     driver = 'ESRI Shapefile',
                     delete_dsn = TRUE,
                     quiet = TRUE)
    }
}

pull_usgs_discharge <- function(network, domain, prodname_ms, sites, time_step) {

    #This function is used in the case when a domain's discharge data is
    #associated with the USGS and is not available through the domain's portal
    #or the USGS data is preferable

    #sites: a named vector where the name is the site name we would like to be
    #    used in MacroSheds and the value is the USGS gauge ID
    #time_step: either a single input of 'daily' or 'sub_daily' depending what data
    #    is available or perfected. Or a vector the same length as sites with either
    #    'daily' and 'sub_daily'.

    if(length(time_step) == 1) {
        time_step <- rep(time_step, length(sites))
    }

    if(!all(time_step %in% c('daily', 'sub_daily'))) {
        stop('time_step can only include daily or sub_daily')
    }

    if(!length(time_step) == length(sites)) {
        stop(paste0('time_step must either be a single chracter of daily or ',
        'sub_daily, or a vector of the same length as sites'))
    }

    for(i in 1:length(sites)) {

        if(time_step[i] == 'daily') {
            discharge <- dataRetrieval::readNWISdv(sites[i], '00060') %>%
                mutate(datetime = ymd_hms(paste0(Date, ' ', '12:00:00'), tz = 'UTC')) %>%
                mutate(val = X_00060_00003)
        } else {
            discharge <- dataRetrieval::readNWISuv(sites[i], '00060') %>%
                rename(datetime = dateTime,
                       val = X_00060_00000)
        }

        discharge <- discharge %>%
            mutate(site_code =!!names(sites[i])) %>%
            mutate(var = 'discharge',
                   val = val * 28.31685,
                   ms_status = 0) %>%
            select(site_code, datetime, val, var, ms_status)

        d <- identify_sampling_bypass(discharge,
                                      is_sensor = TRUE,
                                      network = network,
                                      domain = domain,
                                      prodname_ms = prodname_ms)

        d <- carry_uncertainty(d,
                               network = network,
                               domain = domain,
                               prodname_ms = prodname_ms)

        d <- synchronize_timestep(d) #set to '15 min' when we have server

        d <- apply_detection_limit_t(d, network, domain, prodname_ms, ignore_pred=TRUE)

        if(! dir.exists(glue('data/{n}/{d}/derived/{p}',
                             n = network,
                             d = domain,
                             p = prodname_ms))) {

            dir.create(glue('data/{n}/{d}/derived/{p}',
                            n = network,
                            d = domain,
                            p = prodname_ms),
                       recursive = TRUE)
        }

         write_ms_file(d,
                       network = network,
                       domain = domain,
                       prodname_ms = prodname_ms,
                       site_code = names(sites[i]),
                       level = 'derived',
                       shapefile = FALSE,
                       link_to_portal = FALSE)
    }

    return()
}

log_with_indent <- function(msg, logger, level = 'info', indent = 1){

    #level is one of "info", "warn", 'error".
    #indent: the number of spaces to indent after the colon.

    indent_str <- paste(rep('\U2800\U2800', indent),
                        collapse = '')

    if(level == 'info'){
        loginfo(msg = paste0(enc2native(indent_str),
                             msg),
                logger = logger)
    } else if(level == 'warn'){
        logwarn(msg = paste0(enc2native(indent_str),
                             msg),
                logger = logger)
    } else if(level == 'error'){
        logerror(msg = paste0(enc2native(indent_str),
                              msg),
                logger = logger)
    }
}

postprocess_entire_dataset <- function(site_data,
                                       network_domain,
                                       dataset_version,
                                       thin_portal_data_to_interval = NA,
                                       populate_implicit_missing_values,
                                       generate_csv_for_each_product){

    #thin_portal_data_to_interval: passed to the "unit" parameter of lubridate::round_date.
    #   set to NA (the dafault) to prevent thinning.

    #for post-derive steps that save the portal some processing.

    loginfo(msg = 'Postprocessing all domains and products:',
            logger = logger_module)

    log_with_indent('scaling flux by area', logger = logger_module)
    scale_flux_by_area(network_domain = network_domain,
                       site_data = site_data)

    log_with_indent('writing config datasets to local dir', logger = logger_module)
    write_portal_config_datasets()

    log_with_indent('cataloging held data', logger = logger_module)
    catalog_held_data(site_data = site_data,
                      network_domain = network_domain)

    log_with_indent('combining watershed boundaries', logger = logger_module)
    combine_ws_boundaries()

    log_with_indent('determining which domains have Q', logger = logger_module)
    list_domains_with_discharge(site_data = site_data)

    if(! is.na(thin_portal_data_to_interval)){
        log_with_indent('thinning portal datasets to 1 day',
                        logger = logger_module)
        thin_portal_data(network_domain = network_domain,
                         thin_interval = thin_portal_data_to_interval)
    } else {
        log_with_indent('NOT thinning portal datasets',
                        logger = logger_module)
    }

    if(populate_implicit_missing_values){
        log_with_indent('Completing cases (populating implicit missing rows)',
                        logger = logger_module)
        ms_complete_all_cases(site_data = site_data)
    } else {
        log_with_indent('NOT completing cases',
                        logger = logger_module)
    }

    log_with_indent('Inserting gap-border NAs in portal dataset (so plots show gaps)',
                    logger = logger_module)
    insert_gap_border_NAs(site_data = site_data)

    if(generate_csv_for_each_product){
        log_with_indent('Generating an analysis-ready CSV for each product',
                        logger = logger_module)
        generate_product_csvs(site_data = site_data)
    } else {
        log_with_indent('NOT generating analysis-ready CSVs',
                        logger = logger_module)
    }

    log_with_indent(glue('Generating output dataset v',
                         dataset_version),
                    logger = logger_module)
    generate_output_dataset(vsn = dataset_version)

    # log_with_indent(glue('Removing unneeded files from portal dataset.',
    #                 logger = logger_module)
    # clean_portal_dataset()

    log_with_indent('Generating spatial summary data',
                    logger = logger_module)
    generate_watershed_summaries()

    log_with_indent('Generating spatial timeseries data',
                    logger = logger_module)
    generate_watershed_raw_spatial_dataset()

    log_with_indent('Generating biplot dataset',
                    logger = logger_module)
    compute_yearly_summary(filter_ms_interp = FALSE,
                           filter_ms_status = FALSE)
    compute_yearly_summary_ws()

    log_with_indent('Calculating sizes of downloadable files',
                    logger = logger_module)
    compute_download_filesizes()
}

detrmin_mean_record_length <- function(df){

    test <- df %>%
        filter(Year != year(Sys.Date())) %>%
        group_by(Year, Month, Day) %>%
        summarise(max = max(val, na.rm = TRUE)) %>%
        ungroup() %>%
        filter(!(Month == 2 & Day == 29)) %>%
        group_by(Month, Day) %>%
        summarise(n = n())

    if(mean(test$n, na.rm = TRUE) < 3){
        return(nrow(test))
    }

    if(nrow(test) < 365) {

        quart_val <- quantile(test$n, .2)

        q_check <- test %>%
            filter(n >= quart_val)

        days_in_rec <- nrow(q_check)
    } else{
        days_in_rec <- 365
    }
    return(days_in_rec)
}


clean_portal_dataset <- function(){

    #not needed yet, but soon we'll go over the 6000 file limit and need to
    #trim down. we can then host static files somewhere else for download,
    #and the portal can just hold files needed for viz. still might become a
    #problem.

    #at that time we can remove ws_boundary files and unscaled flux (will need
    #to update biplot to receive precip_flux_scaled)

    find_dirs_within_portaldata <- function(keyword){

        files <- dir(path = '../portal/data',
                     pattern = paste0(keyword, '*'),
                     recursive = TRUE,
                     full.names = TRUE,
                     include.dirs = TRUE)

        return(files)
    }

    dirs_to_delete <- c()

    #watershed boundaries are
    for(k in c('ws_boundary')){

        dirs_to_delete <- c(dirs_to_delete,
                            find_dirs_within_portaldata(keyword = k))
    }

    #drop em all from the final dataset
    for(dr in dirs_to_delete){
        unlink(x = dr,
               recursive = TRUE)
    }
}

generate_output_dataset <- function(vsn){

    tryCatch({
        system(paste0('find data -path "*derived/*.feather" -printf %P\\\\0\\\\n | ',
                  'rsync -av --files-from=- data macrosheds_dataset_v', vsn))
    }, error = function(e){
        stop(glue('generate_output_dataset can only run on a unix-like machine ',
                  'with `find` and `rsync` installed'),
             call. = FALSE)
    })

    # system2('find', c('data', '-path', '"*derived/*.feather"', '-printf',
    #                   '%P\\\\0\\\\n', '|', 'rsync', '-av', '--files-from=-',
    #                   'data', paste0('macrosheds_dataset_v', vsn)))

    find_dirs_within_outputdata <- function(keyword, vsn){

        files <- dir(path = paste0('macrosheds_dataset_v', vsn),
                     pattern = paste0(keyword, '*'),
                     recursive = TRUE,
                     full.names = TRUE,
                     include.dirs = TRUE)

        return(files)
    }

    dirs_to_delete <- c()

    #collect compprod dirs (intermediate products) that shouldn't be in the
    #final dataset
    for(k in c('cdnr_discharge__', 'usgs_discharge__')){

        dirs_to_delete <- c(dirs_to_delete,
          find_dirs_within_outputdata(keyword = k, vsn = vsn))
    }

    #collect pre-idw precip dirs (also intermediate products)
    #that shouldn't be in the final dataset
    pfpaths <- find_dirs_within_outputdata(keyword = 'precipitation__',
                                           vsn = vsn)

    ppaths <- str_match(string = pfpaths,
                        pattern = '(.*)?precipitation__ms[0-9]{3}$')[, 2] %>%
        sort()

    pfac <- factor(ppaths)
    dirs_with_p_compprods <- as.character(pfac[duplicated(pfac)])

    for(dr in dirs_with_p_compprods){

        precip_dirs <- list.files(path = dr,
                                  pattern = '^precipitation__')

        if(length(precip_dirs) != 2){
            stop('there should only be two precip dirs in consideration here')
        }

        dir_to_delete_ind <- str_match(string = precip_dirs,
                                       pattern = 'precipitation__ms([0-9]{3})')[, 2] %>%
           as.numeric() %>%
           which.min()

        dirs_to_delete <- c(dirs_to_delete,
                            paste0(dr, precip_dirs[dir_to_delete_ind]))
    }

    #drop em all from the final dataset
    for(dr in dirs_to_delete){
        unlink(x = dr,
               recursive = TRUE)
    }

    #put convenience functions in there
    file.copy(from = 'src/output_dataset_convenience_functions/load_entire_product.R',
              to = paste0('macrosheds_dataset_v', vsn, '/load_entire_product.R'))

    #add notes
    warning("Don't forget to add notes! (and eventually generate changelog automatically)")

    #zip it up
    #...
}

thin_portal_data <- function(network_domain, thin_interval){

    #thin_interval: passed to the "unit" parameter of lubridate::round_date

    domains <- network_domain$domain

    n_domains <- length(domains)
    for(i in 1:n_domains){

        dmn <- domains[i]

        log_with_indent(msg = glue('{d}: ({ii}/{n})',
                                   d = dmn,
                                   ii = i,
                                   n = n_domains),
                        logger = logger_module,
                        indent = 2)

        prod_dirs <- try(
            {
                list.files(path = glue('../portal/data/{d}/',
                                       d = dmn),
                           full.names = FALSE,
                           recursive = FALSE)
            },
            silent = TRUE
        )

        if(length(prod_dirs)){

            #filter products that never need to be thinned. keep the ones that might
            rgx <- paste0('(^precipitation|^precip_chemistry|^discharge',
                          '|^precip_flux|^stream_chemistry|^stream_flux)')
            prod_dirs <- grep(pattern = rgx,
                              x = prod_dirs,
                              value = TRUE)
        }

        for(prd in prod_dirs){

            site_files <- list.files(path = glue('../portal/data/{d}/{p}',
                                                 d = dmn,
                                                 p = prd),
                                     full.names = TRUE,
                                     recursive = FALSE)

            if(prd == 'precipitation'){
                agg_call <- quote(sum(val, na.rm = TRUE))
            } else {
                agg_call <- quote(mean(val, na.rm = TRUE))
            }

            for(stf in site_files){

                #check whether this file needs to be thinned
                dtcol <- read_feather(stf, columns = 'datetime')
                interval_min <- Mode(diff(as.numeric(dtcol$datetime)) / 60)
                needs_thin <- ! is.na(interval_min) && interval_min <= 24 * 60

                if(needs_thin){

                    d <- read_feather(stf) %>%
                        mutate(
                            datetime = lubridate::round_date(
                                x = datetime,
                                unit = thin_interval),
                            val = errors::set_errors(val, val_err)) %>%
                        select(-val_err)

                    if(length(unique(d$site_code)) > 1){
                        stop(paste('Multiple site_codes in', stf))
                    }

                    d %>%
                        group_by(datetime, var) %>%
                        summarize(
                            site_code = first(site_code),
                            val = eval(agg_call),
                            ms_status = numeric_any(ms_status),
                            ms_interp = numeric_any(ms_interp)) %>%
                        ungroup() %>%
                        mutate(val_err = errors(val),
                               val_err = ifelse(is.na(val_err), 0, val_err),
                               val = errors::drop_errors(val)) %>%
                        select(datetime, site_code, var, val, ms_status, ms_interp,
                               val_err) %>%
                        write_feather(stf)
                }
            }
        }
    }
}

list_domains_with_discharge <- function(site_data){

    #this identifies which sites have Q and which don't, so that the latter
    #   can be filtered from the sitelist in portal/global.R. that way,
    #   sites without discharge won't be selectable on the timeseries tab.

    Q_or_noQ <- site_data %>%
        filter(as.logical(in_workflow)) %>%
        select(network, domain, site_code) %>%
        distinct() %>%
        arrange(network, domain, site_code) %>%
        mutate(has_Q = NA)

    for(i in 1:nrow(Q_or_noQ)){

        ntw <- Q_or_noQ$network[i]
        dmn <- Q_or_noQ$domain[i]
        sit <- Q_or_noQ$site_code[i]

        clg <- sm(try(read_csv(glue('../portal/data/general/catalog_files/indiv_sites/',
                                    '{n}_{d}_{s}.csv',
                                    n = ntw,
                                    d = dmn,
                                    s = sit)),
                      silent = TRUE))

        if(! inherits(clg, 'try-error') && 'discharge' %in% clg$VariableCode){
            Q_or_noQ$has_Q[i] <- TRUE
        } else {
            Q_or_noQ$has_Q[i] <- FALSE
        }

    }

    if(any(is.na(Q_or_noQ$has_Q))){
        stop('NA detected in has_Q column. fix list_domains_with_discharge')
    }

    Q_or_noQ %>%
        filter(has_Q) %>%
        select(-has_Q) %>%
        write_csv(file = '../portal/data/general/sites_with_discharge.csv')
}

scale_flux_by_area <- function(network_domain, site_data){

    #this reads all flux data in data_acquisition/data and in portal/data,
    #   and scales it by watershed area. Originally this was only done for portal
    #   data, and originally each source file (*_flux_inst.feather) was retained
    #   after it was used to generate a *_flux_inst_scaled.feather. Now, the source
    #   file is removed after the scaled file is created. Thus, all our
    #   flux data is converted to kg/ha/d, and every flux file and
    #   directory gets a name change, after this function runs.

    #It would of course be more efficient to do this scaling within flux derive
    #   kernels, but this solution works fine and doesn't require major
    #   modification or rebuilding of the dataset.

    #TODO: scale flux within derive kernels eventually. it'll make for clearer
    #   documentation

    ws_areas <- site_data %>%
        filter(as.logical(in_workflow)) %>%
        select(domain, site_code, ws_area_ha) %>%
        plyr::dlply(.variables = 'domain',
                    .fun = function(x) select(x, -domain))

    domains <- names(ws_areas)

    #the original engine of this function, which still only converts portal data
    engine_for_portal <- function(flux_var, domains, ws_areas){

        for(dmn in domains){

            files <- try(
                {
                    list.files(path = glue('../portal/data/{d}/{v}',
                                           d = dmn,
                                           v = flux_var),
                               full.names = FALSE,
                               recursive = FALSE)
                },
                silent = TRUE
            )

            if('try-error' %in% class(files) || length(files) == 0) next

            dir.create(path = glue('../portal/data/{d}/{v}_scaled',
                                   d = dmn,
                                   v = flux_var),
                       recursive = TRUE,
                       showWarnings = FALSE)

            for(fil in files){

                d <- read_feather(glue('../portal/data/{d}/{v}/{f}',
                                       d = dmn,
                                       v = flux_var,
                                       f = fil))

                d <- d %>%
                    mutate(val = errors::set_errors(val, val_err)) %>%
                    select(-val_err) %>%
                    arrange(site_code, var, datetime) %>%
                    left_join(ws_areas[[dmn]],
                              by = 'site_code') %>%
                    mutate(val = sw(val / ws_area_ha)) %>%
                    select(-ws_area_ha)

                d$val_err <- errors(d$val)
                d$val <- errors::drop_errors(d$val)

                write_feather(x = d,
                              path = glue('../portal/data/{d}/{v}_scaled/{f}',
                                          d = dmn,
                                          v = flux_var,
                                          f = fil))
            }
        }
    }

    engine_for_portal(flux_var = 'stream_flux_inst',
                      domains = domains,
                      ws_areas = ws_areas)

    engine_for_portal(flux_var = 'precip_flux_inst',
                      domains = domains,
                      ws_areas = ws_areas)

    unscaled_portal_flux_dirs <- dir(path = '../portal/data',
                                     pattern = '*_flux_inst',
                                     include.dirs = TRUE,
                                     full.names = TRUE,
                                     recursive = TRUE)

    unscaled_portal_flux_dirs <-
        unscaled_portal_flux_dirs[! grepl(pattern = '(/documentation/|inst_scaled)',
                                          x = unscaled_portal_flux_dirs)]

    lapply(X = unscaled_portal_flux_dirs,
           FUN = unlink,
           recursive = TRUE)

    #the new engine, for scaling flux data within data_acquisition/data
    engine_for_data_acquis <- function(flux_var, network_domain, ws_areas){

        for(i in 1:nrow(network_domain)){

            ntw <- network_domain$network[i]
            dmn <- network_domain$domain[i]

            flux_var_dir <- try(
                {
                    ff <- list.files(path = glue('data/{n}/{d}/derived',
                                                 n = ntw,
                                                 d = dmn),
                                                 # v = flux_var),
                                     pattern = flux_var,
                                     full.names = FALSE,
                                     recursive = FALSE)

                    ff <- ff[! grepl(pattern = 'inst_scaled',
                                     x = ff)]
                },
                silent = TRUE
            )

            if('try-error' %in% class(flux_var_dir) || length(flux_var_dir) == 0) next

            prodcode <- prodcode_from_prodname_ms(flux_var_dir)

            dir.create(path = glue('data/{n}/{d}/derived/{v}_scaled__{pc}',
                                   n = ntw,
                                   d = dmn,
                                   v = flux_var,
                                   pc = prodcode),
                       recursive = TRUE,
                       showWarnings = FALSE)

            files <- list.files(path = glue('data/{n}/{d}/derived/{fvd}',
                                            n = ntw,
                                            d = dmn,
                                            fvd = flux_var_dir),
                                pattern = '*.feather',
                                full.names = TRUE,
                                recursive = TRUE)

            for(f in files){

                d <- read_feather(f)

                d <- d %>%
                    mutate(val = errors::set_errors(val, val_err)) %>%
                    select(-val_err) %>%
                    arrange(site_code, var, datetime) %>%
                    left_join(ws_areas[[dmn]],
                              by = 'site_code') %>%
                    mutate(val = sw(val / ws_area_ha)) %>%
                    select(-ws_area_ha)

                d$val_err <- errors(d$val)
                d$val <- errors::drop_errors(d$val)

                f_scaled <- sub(pattern = 'inst__',
                                replacement = 'inst_scaled__',
                                x = f)

                write_feather(x = d,
                              path = f_scaled)
            }
        }
    }

    engine_for_data_acquis(flux_var = 'stream_flux_inst',
                           network_domain = network_domain,
                           ws_areas = ws_areas)

    engine_for_data_acquis(flux_var = 'precip_flux_inst',
                           network_domain = network_domain,
                           ws_areas = ws_areas)

    unscaled_acquisition_flux_dirs <- dir(path = 'data',
                                          pattern = '*_flux_inst',
                                          include.dirs = TRUE,
                                          full.names = TRUE,
                                          recursive = TRUE)

    unscaled_acquisition_flux_dirs <-
        unscaled_acquisition_flux_dirs[! grepl(pattern = '(/documentation/|_scaled)',
                                               x = unscaled_acquisition_flux_dirs)]

    unscaled_acquisition_flux_dirs <-
        unscaled_acquisition_flux_dirs[grepl(pattern = '/derived/',
                                               x = unscaled_acquisition_flux_dirs)]

    lapply(X = unscaled_acquisition_flux_dirs,
           FUN = unlink,
           recursive = TRUE)

    return(invisible())
}

approxjoin_datetime <- function(x,
                                y,
                                rollmax = '7:30',
                                keep_datetimes_from = 'x',
                                indices_only = FALSE){
                                #direction = 'forward'){

    #x and y: macrosheds standard tibbles with only one site_code,
    #   which must be the same in x and y. Nonstandard tibbles may also work,
    #   so long as they have datetime columns, but the only case where we need
    #   this for other tibbles is inside precip_pchem_pflux_idw, in which case
    #   indices_only == TRUE, so it's not really set up for general-purpose joining
    #rollmax: the maximum snap time for matching elements of x and y.
    #   either '7:30' for continuous data or '12:00:00' for grab data
    #direction [REMOVED]: either 'forward', meaning elements of x will be rolled forward
    #   in time to match the next y, or 'backward', meaning elements of
    #   x will be rolled back in time to reach the previous y
    #keep_datetimes_from: string. either 'x' or 'y'. the datetime column from
    #   the corresponding tibble will be kept, and the other will be dropped
    #indices_only: logical. if TRUE, a join is not performed. rather,
    #   the matching indices from each tibble are returned as a named list of vectors..

    #good datasets for testing this function:
    # x <- tribble(
    #     ~datetime, ~site_code, ~var, ~val, ~ms_status, ~ms_interp,
    #     '1968-10-09 04:42:00', 'GSWS10', 'GN_alk', set_errors(27.75, 1), 0, 0,
    #     '1968-10-09 04:44:00', 'GSWS10', 'GN_alk', set_errors(21.29, 1), 0, 0,
    #     '1968-10-09 04:47:00', 'GSWS10', 'GN_alk', set_errors(21.29, 1), 0, 0,
    #     '1968-10-09 04:59:59', 'GSWS10', 'GN_alk', set_errors(16.04, 1), 0, 0,
    #     '1968-10-09 05:15:01', 'GSWS10', 'GN_alk', set_errors(17.21, 1), 1, 0,
    #     '1968-10-09 05:30:59', 'GSWS10', 'GN_alk', set_errors(16.50, 1), 0, 0) %>%
        # mutate(datetime = as.POSIXct(datetime, tz = 'UTC'))
    # y <- tribble(
    #     ~datetime, ~site_code, ~var, ~val, ~ms_status, ~ms_interp,
    #     '1968-10-09 04:00:00', 'GSWS10', 'GN_alk', set_errors(1.009, 1), 1, 0,
    #     '1968-10-09 04:15:00', 'GSWS10', 'GN_alk', set_errors(2.009, 1), 1, 1,
    #     '1968-10-09 04:30:00', 'GSWS10', 'GN_alk', set_errors(3.009, 1), 1, 1,
    #     '1968-10-09 04:45:00', 'GSWS10', 'GN_alk', set_errors(4.009, 1), 1, 1,
    #     '1968-10-09 05:00:00', 'GSWS10', 'GN_alk', set_errors(5.009, 1), 1, 1,
    #     '1968-10-09 05:15:00', 'GSWS10', 'GN_alk', set_errors(6.009, 1), 1, 1) %>%
    #     mutate(datetime = as.POSIXct(datetime, tz = 'UTC'))

    #tests
    if('site_code' %in% colnames(x) && length(unique(x$site_code)) > 1){
        stop('Only one site_code allowed in x at the moment')
    }
    if('var' %in% colnames(x) && length(unique(drop_var_prefix(x$var))) > 1){
        stop('Only one var allowed in x at the moment (not including prefix)')
    }
    if('site_code' %in% colnames(y) && length(unique(y$site_code)) > 1){
        stop('Only one site_code allowed in y at the moment')
    }
    if('var' %in% colnames(y) && length(unique(drop_var_prefix(y$var))) > 1){
        stop('Only one var allowed in y at the moment (not including prefix)')
    }
    if('site_code' %in% colnames(x) &&
       'site_code' %in% colnames(y) &&
       x$site_code[1] != y$site_code[1]) stop('x and y site_code must be the same')
    if(! rollmax %in% c('7:30', '12:00:00')) stop('rollmax must be "7:30" or "12:00:00"')
    # if(! direction %in% c('forward', 'backward')) stop('direction must be "forward" or "backward"')
    if(! keep_datetimes_from %in% c('x', 'y')) stop('keep_datetimes_from must be "x" or "y"')
    if(! 'datetime' %in% colnames(x) || ! 'datetime' %in% colnames(y)){
        stop('both x and y must have "datetime" columns containing POSIXct values')
    }
    if(! is.logical(indices_only)) stop('indices_only must be a logical')

    #deal with the case of x or y being a specialized "flow" tibble
    # x_is_flowtibble <- y_is_flowtibble <- FALSE
    # if('flow' %in% colnames(x)) x_is_flowtibble <- TRUE
    # if('flow' %in% colnames(y)) y_is_flowtibble <- TRUE
    # if(x_is_flowtibble && ! y_is_flowtibble){
    #     varname <- y$var[1]
    #     y$var = NULL
    # } else if(y_is_flowtibble && ! x_is_flowtibble){
    #     varname <- x$var[1]
    #     x$var = NULL
    # } else if(! x_is_flowtibble && ! y_is_flowtibble){
    #     varname <- x$var[1]
    #     x$var = NULL
    #     y$var = NULL
    # } else {
    #     stop('x and y are both "flow" tibbles. There should be no need for this')
    # }
    # if(x_is_flowtibble) x <- rename(x, val = flow)
    # if(y_is_flowtibble) y <- rename(y, val = flow)

    #data.table doesn't work with the errors package, so error needs
    #to be separated into its own column. also give same-name columns suffixes

    if('val' %in% colnames(x)){ #crude catch for nonstandard ms tibbles (fine for now)
        x <- x %>%
            mutate(err = errors(val),
                   val = errors::drop_errors(val)) %>%
            rename_with(.fn = ~paste0(., '_x'),
                        .cols = everything()) %>%
                        # .cols = any_of(c('site_code', 'var', 'val',
                        #                  'ms_status', 'ms_interp'))) %>%
            as.data.table()

        y <- y %>%
            mutate(err = errors(val),
                   val = errors::drop_errors(val)) %>%
            rename_with(.fn = ~paste0(., '_y'),
                        .cols = everything()) %>%
            as.data.table()
    } else {
        x <- dplyr::rename(x, datetime_x = datetime) %>% as.data.table()
        y <- dplyr::rename(y, datetime_y = datetime) %>% as.data.table()
    }

    #alternative implementation of the "on" argument in data.table joins...
    #probably more flexible, so leaving it here in case we need to do something crazy
    # data.table::setkeyv(x, 'datetime')
    # data.table::setkeyv(y, 'datetime')

    #convert the desired maximum roll distance from string to integer seconds
    rollmax <- ifelse(test = rollmax == '7:30',
                      yes = 7 * 60 + 30,
                      no = 12 * 60 * 60)

    #leaving this here in case the nearest neighbor join implemented below is too
    #slow. then we can fall back to a basic rolling join with a maximum distance
    # rollmax <- ifelse(test = direction == 'forward',
    #                   yes = -rollmax,
    #                   no = rollmax)
    #rollends will move the first/last value of x in the opposite `direction` if necessary
    # joined <- y[x, on = 'datetime', roll = rollmax, rollends = c(TRUE, TRUE)]

    #create columns in x that represent the snapping window around each datetime
    x[, `:=` (datetime_min = datetime_x - rollmax,
              datetime_max = datetime_x + rollmax)]
    y[, `:=` (datetime_y_orig = datetime_y)] #datetime col will be dropped from y

    # if(indices_only){
    #     y_indices <- y[x,
    #                    on = .(datetime_y <= datetime_max,
    #                           datetime_y >= datetime_min),
    #                    which = TRUE]
    #     return(y_indices)
    # }

    #join x rows to y if y's datetime falls within the x range
    joined <- y[x, on = .(datetime_y <= datetime_max,
                          datetime_y >= datetime_min)]
    joined <- na.omit(joined, cols = 'datetime_y_orig') #drop rows without matches

    #for any datetimes in x or y that were matched more than once, keep only
    #the nearest match
    joined[, `:=` (datetime_match_diff = abs(datetime_x - datetime_y_orig))]
    joined <- joined[, .SD[which.min(datetime_match_diff)], by = datetime_x]
    joined <- joined[, .SD[which.min(datetime_match_diff)], by = datetime_y_orig]

    if(indices_only){
        y_indices <- which(y$datetime_y %in% joined$datetime_y_orig)
        x_indices <- which(x$datetime_x %in% joined$datetime_x)
        return(list(x = x_indices, y = y_indices))
    }

    #drop and rename columns (data.table makes weird name modifications)
    if(keep_datetimes_from == 'x'){
        joined[, c('datetime_y', 'datetime_y.1', 'datetime_y_orig', 'datetime_match_diff') := NULL]
        setnames(joined, 'datetime_x', 'datetime')
    } else {
        joined[, c('datetime_x', 'datetime_y.1', 'datetime_y', 'datetime_match_diff') := NULL]
        setnames(joined, 'datetime_y_orig', 'datetime')
    }

    #restore error objects, var column, original column names (with suffixes).
    #original column order
    joined <- as_tibble(joined) %>%
        mutate(val_x = errors::set_errors(val_x, err_x),
               val_y = errors::set_errors(val_y, err_y)) %>%
        select(-err_x, -err_y)
        # mutate(var = !!varname)

    # if(x_is_flowtibble) joined <- rename(joined,
    #                                      flow = val_x,
    #                                      ms_status_flow = ms_status_x,
    #                                      ms_interp_flow = ms_interp_x)
    # if(y_is_flowtibble) joined <- rename(joined,
    #                                      flow = val_y,
    #                                      ms_status_flow = ms_status_y,
    #                                      ms_interp_flow = ms_interp_y)

    # if(! sum(grepl('^val_[xy]$', colnames(joined))) > 1){
    #     joined <- rename(joined, val = matches('^val_[xy]$'))
    # }

    joined <- select(joined,
                     datetime,
                     # matches('^val_?[xy]?$'),
                     # any_of('flow'),
                     starts_with('site_code'),
                     any_of(c(starts_with('var_'), matches('^var$'))),
                     any_of(c(starts_with('val_'), matches('^val$'))),
                     starts_with('ms_status_'),
                     starts_with('ms_interp_'))

    return(joined)
}

retrieve_versionless_product <- function(network,
                                         domain,
                                         prodname_ms,
                                         site_code,
                                         tracker,
                                         orcid_login,
                                         orcid_pass){

    #retrieves products that are served as static files.
    #IN PROGRESS: records source URIs as local metadata files

    processing_func <- get(paste0('process_0_',
                                  prodcode_from_prodname_ms(prodname_ms)))

    rt <- tracker[[prodname_ms]][[site_code]]$retrieve

    for(i in 1:nrow(rt)){

        held_dt <- as.POSIXct(rt$held_version[i],
                              tz = 'UTC')

        deets <- list(prodname_ms = prodname_ms,
                      site_code = site_code,
                      component = rt$component[i],
                      last_mod_dt = held_dt)

        result <- do.call(processing_func,
                          args = list(set_details = deets,
                                      network = network,
                                      domain = domain))

        new_status <- evaluate_result_status(result)

        if('access_time' %in% names(result) && any(! is.na(result$access_time))){
            deets$last_mod_dt <- result$access_time[! is.na(result$access_time)][1]
        } else if(is.POSIXct(result)){
            stop('update kernel to return a list with url(s) and access_time(s)')
            # deets$last_mod_dt <- as.character(result)
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
}

get_source_urls <- function(result_obj, processing_func){

    #identify URL(s) indicating data provenance, or return a
    #message explaining why there isn't one

    #find out if the processing func is an alias for download_from_googledrive()
    gd_search_string <- '\\s*download_from_googledrive_function_indicator <- TRUE'
    uses_gdrive_func <- grepl(gd_search_string, deparse(processing_func)[3])

    if(uses_gdrive_func){

        source_urls <- 'MacroSheds drive; not yet public'

    } else if('url' %in% names(result_obj)){

        source_urls <- paste(result_obj$url,
                             collapse = '\n')

    } else {
        stop('investigate this. do we need to scrape the URL from products.csv?')
    }

    return(source_urls)
}

munge_versionless_product <- function(network,
                                      domain,
                                      prodname_ms,
                                      site_code,
                                      tracker){

    #munges products that are served as static files.
    #IN PROGRESS: records source URIs as local metadata files

    processing_func <- get(paste0('process_1_',
                                  prodcode_from_prodname_ms(prodname_ms)))

    rt <- tracker[[prodname_ms]][[site_code]]$retrieve

    for(i in 1:nrow(rt)){

        held_dt <- as.POSIXct(rt$held_version[i],
                              tz = 'UTC')

        result <- do.call(processing_func,
                          args = list(network = network,
                                      domain = domain,
                                      prodname_ms = prodname_ms,
                                      site_code = site_code,
                                      component = rt$component[i]))

        new_status <- evaluate_result_status(result)

        update_data_tracker_m(network = network,
                              domain = domain,
                              tracker_name = 'held_data',
                              prodname_ms = prodname_ms,
                              site_code = site_code,
                              new_status = new_status)

        write_metadata_m(network = network,
                         domain = domain,
                         prodname_ms = prodname_ms,
                         tracker = held_data)
    }
}

catalog_held_data <- function(network_domain, site_data){

    #tabulates:
    # + total nonspatial observations for the portal landing page
    # + informational catalog for all variables
    # + informational catalog for all sites
    # + informational catalog for each individual variable
    # + informational catalog for each individual site

    nobs_nonspatial <- 0
    # site_display <- tibble()
    # site_vars <- tibble(network = character(),
    #                     domain = character(),
    #                     site = character(),
    #                     stream = character(),
    #                     lat = numeric(),
    #                     long)

    all_site_breakdown <- site_data %>%
        filter(as.logical(in_workflow)) %>%
        select(-in_workflow, -notes, -CRS, -local_time_zone)

    all_variable_breakdown <- tibble()
    for(i in 1:nrow(network_domain)){

        site_prods <- list.dirs(glue('data/{n}/{d}/derived',
                                    n = network_domain$network[i],
                                    d = network_domain$domain[i]),
                               full.names = TRUE)
        site_prods <- site_prods[! grepl('derived$', site_prods)]

        if(length(site_prods) == 0) next

        spatial_prod_inds <- grep(pattern = '(documentation|gauge|boundary)',
                                  x = site_prods)

        spatial_prods <- site_prods[spatial_prod_inds]
        nonspatial_prods <- site_prods[-spatial_prod_inds]

        for(j in seq_along(nonspatial_prods)){

            if(any(grepl('precip_pchem_pflux', nonspatial_prods))){
                logwarn(msg = 'why is there a precip_pchem_pflux directory?? fix this',
                        logger = logger_module)
                nonspatial_prods <- nonspatial_prods[! grepl('precip_pchem_pflux', nonspatial_prods)]
            }

            ## sum all observations across all sites (will be redundant when the rest is done)
            prod_nobs <- list.files(nonspatial_prods[j],
                                    full.names = TRUE,
                                    recursive = TRUE) %>%
                purrr::map(~ feather::feather_metadata(.x)$dim[1]) %>%
                purrr::reduce(sum)

            nobs_nonspatial <- nobs_nonspatial + prod_nobs

            ## catalog sites for display (one at a time is the safest way)

            product_files <- list.files(nonspatial_prods[j],
                       full.names = TRUE,
                       recursive = TRUE)

            # product_vars <- c()
            # nobs <- 0
            # first_record <- last_record <- lubridate::NA_POSIXct_

            # product_breakdown <- tibble(var = character(),
            #                               sample_regimen = character(),
            #                               n_observations = numeric(),
            #                               first_record_UTC = lubridate::POSIXct(),
            #                               last_record_UTC = lubridate::POSIXct())

            #read and combine product files; calculate some goodies
            product_breakdown <- tibble()
            for(f in product_files){

                product_breakdown <- read_feather(f) %>%
                    mutate(
                        sample_regimen = extract_var_prefix(var),
                        var = drop_var_prefix(var)) %>%
                    group_by(var, sample_regimen, site_code) %>%
                    summarize(
                        n_observations = n(),
                        first_record_UTC = min(datetime,
                                               na.rm = TRUE),
                        last_record_UTC = max(datetime,
                                              na.rm = TRUE),
                        prop_flagged = sum(ms_status) / n_observations,
                        prop_imputed = sum(ms_interp) / n_observations) %>%
                    ungroup() %>%
                    bind_rows(product_breakdown)
            }

            #summarize and enhance goodies
            product_breakdown <- product_breakdown %>%
                group_by(var, sample_regimen, site_code) %>%
                summarize(
                    n_flagged = sum(prop_flagged * n_observations,
                                    na.rm = TRUE),
                    n_imputed = sum(prop_imputed * n_observations,
                                    na.rm = TRUE),
                    n_observations = sum(n_observations,
                                         na.rm = TRUE),
                    pct_flagged = round(n_flagged / n_observations * 100,
                                        digits = 2),
                    pct_imputed = round(n_imputed / n_observations * 100,
                                        digits = 2),
                    first_record_UTC = min(first_record_UTC,
                                           na.rm = TRUE),
                    last_record_UTC = max(last_record_UTC,
                                          na.rm = TRUE)) %>%
                ungroup() %>%
                select(-n_flagged, -n_imputed) %>%
                mutate(sample_regimen = case_when(
                    sample_regimen == 'GS' ~ 'grab-sensor',
                    sample_regimen == 'IS' ~ 'installed-sensor',
                    sample_regimen == 'GN' ~ 'grab-nonsensor',
                    sample_regimen == 'IN' ~ 'installed-nonsensor')) %>%
                select(site_code, var, sample_regimen, n_observations,
                       first_record_UTC, last_record_UTC, pct_flagged,
                       pct_imputed)

            #if multiple sample regimens for a site-variable, aggregate and append them as sample_regimen "all"
            product_breakdown <- product_breakdown %>%
                group_by(site_code, var) %>%
                summarize(
                    n_observations = if(n() > 1) sum(n_observations, na.rm = TRUE) else first(n_observations),
                    pct_flagged = if(n() > 1) round(sum(pct_flagged, na.rm = TRUE), digits = 2) else first(pct_flagged),
                    pct_imputed = if(n() > 1) round(sum(pct_imputed, na.rm = TRUE), digits = 2) else first(pct_imputed),
                    first_record_UTC = if(n() > 1) min(first_record_UTC, na.rm = TRUE) else first(first_record_UTC),
                    last_record_UTC = if(n() > 1) max(last_record_UTC, na.rm = TRUE) else first(last_record_UTC),
                    sample_regimen = if(n() > 1) 'all' else 'drop'
                ) %>%
                ungroup() %>%
                filter(sample_regimen != 'drop') %>%
                bind_rows(product_breakdown)

            #merge other stuff from variables and site_data config sheets;
            #final sorting and renaming
            #(TODO: add methods once we have that worked out)
            product_breakdown <- product_breakdown %>%
                left_join(select(ms_vars,
                                 variable_code, variable_name, unit), #, method
                          by = c('var' = 'variable_code')) %>%
                left_join(select(all_site_breakdown,
                                 network, domain, site_code),
                          by = 'site_code') %>%
                select(network,
                       domain,
                       site_code,
                       VariableCode = var,
                       VariableName = variable_name,
                       SampleRegimen = sample_regimen,
                       Unit = unit,
                       Observations = n_observations,
                       FirstRecordUTC = first_record_UTC,
                       LastRecordUTC = last_record_UTC,
                       PercentFlagged = pct_flagged,
                       PercentImputed = pct_imputed) %>%
                arrange(network, domain, site_code, VariableCode, SampleRegimen) %>%
                filter(! is.na(domain)) #only needed for unresolved Arctic naming issue (1/15/21)

            #combine with other product summaries
            all_variable_breakdown <- bind_rows(all_variable_breakdown,
                                                product_breakdown)
        }
    }

    readr::write_file(x = as.character(nobs_nonspatial),
                      file = '../portal/data/general/total_nonspatial_observations.txt')

    dir.create('../portal/data/general/catalog_files',
               showWarnings = FALSE)

    #generate and write file describing all variables

    all_variable_display <- all_variable_breakdown %>%
        group_by(VariableCode) %>%
        summarize(
            Observations = sum(Observations,
                               na.rm = TRUE),
            Sites = length(unique(paste0(network, domain, site_code))),
            FirstRecordUTC = min(FirstRecordUTC,
                                 na.rm = TRUE),
            LastRecordUTC = max(LastRecordUTC,
                                na.rm = TRUE),
            VariableName = first(VariableName),
            Unit = first(Unit)) %>%
        ungroup() %>%
        mutate(MeanObsPerSite = round(Observations / Sites, 0),
               Availability = paste0("<button type='button' id='", VariableCode, "'>view</button>")) %>%
               # Availability = paste0("<a href='?", VariableCode, "'>view</a>")) %>%
        select(Availability, VariableName, VariableCode, Unit, Observations, Sites,
               MeanObsPerSite, FirstRecordUTC, LastRecordUTC)

    readr::write_csv(x = all_variable_display,
                     file = '../portal/data/general/catalog_files/all_variables.csv')


    #generate and write individual file for each variable, describing it by site

    dir.create('../portal/data/general/catalog_files/indiv_variables',
               showWarnings = FALSE)

    vars <- unique(all_variable_display$VariableCode)

    for(v in vars){

        indiv_variable_display <- all_variable_breakdown %>%
            filter(VariableCode == !!v) %>%
            group_by(network, domain, site_code) %>%
            summarize(
                Observations = sum(Observations,
                                   na.rm = TRUE),
                FirstRecordUTC = min(FirstRecordUTC,
                                     na.rm = TRUE),
                LastRecordUTC = max(LastRecordUTC,
                                    na.rm = TRUE),
                Unit = first(Unit)) %>%
            ungroup()

        ndays <- difftime(time1 = indiv_variable_display$LastRecordUTC,
                          time2 = indiv_variable_display$FirstRecordUTC,
                          units = 'days') %>%
            as.numeric()

        indiv_variable_display$MeanObsPerDay <- round(
            indiv_variable_display$Observations / ndays,
            digits = 1
        )

        indiv_variable_display <- indiv_variable_display %>%
            left_join(select(all_site_breakdown,
                             domain, pretty_domain, network, pretty_network,
                             site_code),
                      by = c('network', 'domain', 'site_code')) %>%
            select(Network = pretty_network,
                   Domain = pretty_domain,
                   SiteCode = site_code,
                   Unit, Observations, FirstRecordUTC, LastRecordUTC,
                   MeanObsPerDay)

        readr::write_csv(x = indiv_variable_display,
                         file = glue('../portal/data/general/catalog_files/indiv_variables/',
                                     v, '.csv'))
    }

    #generate and write file describing all sites
    #TODO: make sure to include a note about datum on display page
    #   also, incude url column somehow

    all_site_display <- all_variable_breakdown %>%
        group_by(network, domain, site_code) %>%
        summarize(
            Observations = sum(Observations,
                               na.rm = TRUE),
            Variables = length(unique(VariableCode)),
            FirstRecordUTC = min(FirstRecordUTC,
                                 na.rm = TRUE),
            LastRecordUTC = max(LastRecordUTC,
                                na.rm = TRUE)) %>%
        ungroup() %>%
        mutate(MeanObsPerVar = round(Observations / Variables, 0),
               ExternalLink = 'feature not yet built',
               Availability = paste0("<button type='button' id='",
                                     network, '_', domain, '_', site_code,
                                     "'>view</button>")) %>%
        left_join(all_site_breakdown,
                  by = c('network', 'domain', 'site_code')) %>%
        select(Availability,
               Network = pretty_network,
               Domain = pretty_domain,
               SiteCode = site_code,
               SiteName = full_name,
               StreamName = stream,
               Latitude = latitude,
               Longitude = longitude,
               SiteType = site_type,
               AreaHectares = ws_area_ha,
               Observations, Variables, FirstRecordUTC, LastRecordUTC,
               ExternalLink)

    readr::write_csv(x = all_site_display,
                     file = '../portal/data/general/catalog_files/all_sites.csv')

    #generate and write individual file for each site, describing it by variable

    dir.create('../portal/data/general/catalog_files/indiv_sites',
               showWarnings = FALSE)

    sites <- distinct(all_site_breakdown,
                      network, domain, site_code)

    for(i in 1:nrow(sites)){

        ntw <- sites$network[i]
        dmn <- sites$domain[i]
        sit <- sites$site_code[i]

        indiv_site_display <- all_variable_breakdown %>%
            filter(network == ntw,
                   domain == dmn,
                   site_code == sit)

        ndays <- difftime(time1 = indiv_site_display$LastRecordUTC,
                          time2 = indiv_site_display$FirstRecordUTC,
                          units = 'days') %>%
            as.numeric()

        indiv_site_display$MeanObsPerDay <- round(
            indiv_site_display$Observations / ndays,
            digits = 1
        )

        indiv_site_display <- indiv_site_display %>%
            # left_join(select(all_site_breakdown,
            #                  domain, pretty_domain, network, pretty_network,
            #                  site_code),
            #           by = c('network', 'domain', 'site_code')) %>%
            select(VariableCode, VariableName, SampleRegimen, Unit,
                   Observations, FirstRecordUTC, LastRecordUTC,
                   MeanObsPerDay, PercentFlagged, PercentImputed)

        readr::write_csv(x = indiv_site_display,
                         file = glue('../portal/data/general/catalog_files/indiv_sites/',
                                     '{n}_{d}_{s}.csv',
                                     n = ntw,
                                     d = dmn,
                                     s = sit))
    }

    #in case somebody asks for this stuff again:

    # #domains per network
    # site_data %>%
    #     filter(as.logical(in_workflow)) %>%
    #     group_by(network) %>%
    #     summarize(n_domains = length(unique(domain)))
    #
    # #sites per domain
    # site_data$stream[is.na(site_data$stream)] = 1:sum(is.na(site_data$stream))
    # site_data %>%
    #     filter(as.logical(in_workflow),
    #            site_type != 'rain_gauge') %>%
    #     group_by(network, domain) %>%
    #     summarize(n_sites = length(unique(site_code)),
    #               n_unique_streams = length(unique(stream)))
    #
    # #mean sites per domain
    # site_data %>%
    #     filter(as.logical(in_workflow),
    #            site_type != 'rain_gauge') %>%
    #     group_by(network, domain) %>%
    #     summarize(n_sites = length(unique(site_code))) %>%
    #     ungroup() %>%
    #     {mean(.$n_sites)}
    #
    # #total sites
    # site_data %>%
    #     filter(as.logical(in_workflow),
    #            site_type != 'rain_gauge') %>%
    #     group_by(network, domain) %>%
    #     summarize(n_sites = length(unique(site_code))) %>%
    #     ungroup() %>%
    #     {sum(.$n_sites)}
    #
    # #total unique streams
    # site_data %>%
    #     filter(as.logical(in_workflow),
    #            site_type != 'rain_gauge') %>%
    #     group_by(network, domain) %>%
    #     summarize(n_sites = length(unique(site_code)),
    #               n_unique_streams = length(unique(stream))) %>%
    #     ungroup() %>%
    #     {sum(.$n_unique_streams)}
}

greatest_common_divisor <- function(a, b){

    stopifnot(is.numeric(a), is.numeric(b))

    if(length(a) == 1){
        a <- rep(a, times = length(b))
    } else if(length(b) == 1){
        b <- rep(b, times = length(a))
    }

    n <- length(a)
    e <- d <- g <- numeric(n)

    for(k in 1:n){

        u <- c(1, 0, abs(a[k]))
        v <- c(0, 1, abs(b[k]))

        while(v[3] != 0){

            q <- floor(u[3]/v[3])
            t <- u - v * q
            u <- v
            v <- t
        }

        e[k] <- u[1] * sign(a[k])
        d[k] <- u[2] * sign(a[k])
        g[k] <- u[3]
    }

    return(g)
}

least_common_multiple <- function(a, b){

    stopifnot(is.numeric(a), is.numeric(b))

    if(length(a) == 1){
        a <- rep(a, times = length(b))
    } else if(length(b) == 1){
        b <- rep(b, times = length(a))
    }

    g <- greatest_common_divisor(a, b)

    return(a/g * b)
}

ms_determine_data_interval <- function(d, per_column = FALSE){

    #calculates the mode interval in each column, then returns the
    #   greatest common divisor of those modes as the interval, in minutes.

    vars <- unique(d$var)

    interval_modes <- c()
    for(v in vars){
        time_diffs <- diff(d$datetime[d$var == v])
        units(time_diffs) = 'mins'
        interval_modes <- c(interval_modes, Mode(time_diffs))
    }

    if(per_column){
        names(interval_modes) <- vars
        return(interval_modes)
    }

    interval_modes <- interval_modes[! is.na(interval_modes)]

    if(! length(interval_modes)) return(NA_real_)

    data_interval <- Reduce(greatest_common_divisor, interval_modes)

    return(data_interval)
}

ms_complete_all_cases <- function(network_domain, site_data){

    #populates implicit NAs in all feather files across all product directories.
    #   Note: this only operates on files within data_acquisition/data, NOT within
    #   portal/data (those files should stay as small as possible. see
    #   insert_gap_border_NAs).

    #also note: when we switch back to high-res mode, we'll need to see how much
    #   more space our dataset takes up after this operation. it might be
    #   several gigs, which could be a problem.

    #For special cases (currently only McMurdo), winter data gaps in discharge
    #   timeseries will be populated with 0 instead of NA. McMurdo's winter is
    #   identified as any series of NAs longer than 180 days occurring between
    #   jan 2 and dec 30.

    paths <- list.files(path = 'data',
                        pattern = '*.feather',
                        recursive = TRUE,
                        full.names = TRUE)

    paths <- paths[grepl(pattern = 'derived', x = paths)]

    for(p in paths){

        d <- read_feather(p)
        sites <- unique(d$site_code)
        vars <- unique(d$var)

        dupes_present <- FALSE
        for(s in sites){
            for(v in vars){

                dt_diff <- d %>%
                    filter(site_code == !!s,
                           var == !!v) %>%
                    arrange(datetime) %>%
                    pull(datetime) %>%
                    as.numeric() %>%
                    diff()

                if(length(dt_diff)){

                    if(any(dt_diff == 0)){
                        warning(glue('Duplicate datetime found in {pp} ({ss}-{vv} slice).',
                                     'This will be removed, but we should find out what the deal is.',
                                      pp = p,
                                      ss = s,
                                      vv = v))

                        dupes_present <- TRUE
                    }

                    if(any(dt_diff <= 15 * 60 & dt_diff != 0)){
                        stop(glue("We need to make sure it's okay to make NAs ",
                                  "explicit, now that we're in high-res mode. ",
                                  "Copy the data directory, comment this error, ",
                                  "run this function, then compare data directory ",
                                  "sizes. If completing ",
                                  "cases doesn't make it absurdly huge, then ",
                                  "remove this error and carry on."))
                    }
                }
            }
        }

        if(dupes_present){
            d <- d %>%
                distinct(site_code, var, datetime,
                         .keep_all = TRUE) %>%
                arrange(site_code, var, datetime)
        }

        interv_mins <- ms_determine_data_interval(d = d)

        if(is.na(interv_mins)){ #only one value, so no interval
            next
        } else if(interv_mins %% 1440 == 0){
            data_interval <- '1 day'
        } else if(interv_mins %% 15 == 0){
            data_interval <- '15 mins'
        } else {
            stop(glue('data interval should be either 1 day or 15 minutes. If ',
                      'this has changed, update the conditional above this error ',
                      'and check for needed updates elsewhere'))
        }

        d <- populate_implicit_NAs(d = d,
                                   interval = data_interval)

        if(grepl('/mcmurdo/', p) && grepl('/discharge__', p)){

            #for sites/products with more than one variable prefix, something more
            #sophisticated will be needed. for mcmurdo, all discharge is IS_discharge
            d <- d %>%
                group_split(site_code, var,
                            year = lubridate::year(datetime)) %>%
                purrr::map_dfr(function(x){

                    na_runlengths <- rle(is.na(x$val))
                    na_len <- na_runlengths$lengths
                    is_na <- na_runlengths$values

                    if(any(is_na[c(1, length(is_na))])) return(x)

                    winter_run <- which(is_na & na_len > 180)

                    if(length(winter_run)){

                        csums <- cumsum(na_len[1:winter_run])
                        winter_inds <- (csums[length(csums) - 1] + 1) :
                            csums[length(csums)]
                        x$val[winter_inds] <- 0
                        x$ms_interp[winter_inds] <- 1
                    }

                    return(x)
                }) %>%
                arrange(site_code, var, datetime)
        }

        write_feather(x = d,
                      path = p)
    }
}

insert_gap_border_NAs <- function(network_domain, site_data){

    #populates rows bordering missing data segments with NA data, so that
    #   gaps are properly plotted by dygraphs, etc.
    #   Note: this only operates on files within portal/data, NOT within
    #   data_acquisition/data (see ms_complete_all_cases)

    #TODO: make the following documentation true. for now, nothing is done
    #   differently for mcmurdo, meaning the portal shows gaps for mcmurdo's
    #   winter where the public export data has 0s.
    #For special cases (currently only McMurdo), rows bordering winter data gaps
    #   in discharge timeseries will be populated with 0 instead of NA.
    #   McMurdo's winter is identified as any series of NAs longer than 180 days
    #   occurring between jan 2 and dec 30.

    #notice: running this function multiple times without rebuilding the portal
    #   dataset will keep adding NA rows to the beginning and end of each data
    #   gap. Basically, if you run this n times you'll fill data gaps of up
    #   to length n/2. This isn't a problem.

    paths <- list.files(path = '../portal/data',
                        pattern = '*.feather',
                        recursive = TRUE,
                        full.names = TRUE)

    paths <- paths[! grepl(pattern = '/general/', x = paths)]

    for(p in paths){

        d <- read_feather(p)

        interv_mins <- ms_determine_data_interval(d = d)

        if(is.na(interv_mins)){
            next
        } else if(interv_mins %% 1440 == 0){
            data_interval <- '1 day'
        } else if(interv_mins %% 15 == 0){
            data_interval <- '15 mins'
        } else {
            stop(glue('data interval should be either 1 day or 15 minutes. If ',
                      'this has changed, updates the conditional above this error ',
                      'and check for needed updates elsewhere'))
        }

        d <- populate_implicit_NAs(d = d,
                                   interval = data_interval,
                                   edges_only = TRUE)

        write_feather(x = d,
                      path = p)
    }
}

generate_product_csvs <- function(network_domain, site_data){

    #in addition to the data/network/domain/product/site.feather format,
    #   we can provide analysis-ready CSVs.

    dir.create(path = 'output/all_chemQPflux_csv_wide',
               showWarnings = FALSE,
               recursive = TRUE)

    readr::write_file(x = paste('In casting these product sets from long to',
                                'wide format, ms_status (indicating data flags)',
                                'and ms_interp (indicating points interpolated',
                                'by MacroSheds) columns have been dropped. We',
                                'Can include this information in wide format,',
                                'but note that it will nearly double the size',
                                'of each CSV herein.'),
                      file = 'output/all_chemQPflux_csv_wide/README.txt')

    products <- c('stream_chemistry', 'stream_flux_inst_scaled', 'discharge',
                  'precip_chemistry', 'precip_flux_inst_scaled', 'precipitation')


    for(p in products){

        d <- load_entire_product(prodname = p,
                                 .sort = FALSE)

        if(nrow(d) != nrow(distinct(d, datetime, site_code, var))){
            print(paste('there are still duplicates in', p))
        }
        # zz = d %>%
        #     select(-ms_status, -ms_interp) %>%
        #     mutate(val = errors::drop_errors(val)) %>%
        #     tidyr::pivot_wider(names_from = var,
        #                        values_from = val,
        #                        values_fn = length)
        # zzz = apply(zz[,5:ncol(zz)], 1, function(x)any(! is.na(x) & x > 1))
        # filter(d, datetime == as.POSIXct('2003-08-20 00:00:00', tz='UTC'), site_code=='JBHH', var=='GN_PO4_P')

        d %>%
            select(-ms_status, -ms_interp) %>%
            mutate(val = errors::drop_errors(val)) %>%
            tidyr::pivot_wider(names_from = var,
                               values_from = val,
                               values_fn = mean) %>%
            arrange(network, domain, site_code, datetime) %>%
            readr::write_csv(file = glue('output/all_chemQPflux_csv_wide/{pp}.csv',
                                         pp = p))
    }
}

expo_backoff <- function(expr,
                         max_attempts = 10,
                         verbose = TRUE){

    for(attempt_i in seq_len(max_attempts)){

        results <- try(expr = expr,
                       silent = TRUE)

        if(inherits(results, 'try-error')){

            if(attempt_i == max_attempts){
                stop(attr(results, 'condition'))
            }

            backoff <- runif(n = 1,
                             min = 0,
                             max = 2^attempt_i - 1)

            if(verbose){
                print(glue("Backing off for ", round(backoff, 1), " seconds."))
            }

            Sys.sleep(backoff)

        } else {

            # if(verbose){
            #     print(paste0("Request succeeded after ", attempt_i, " attempt(s)."))
            # }

            break
        }
    }

    return(results)
}

combine_ws_boundaries <- function(){

    setwd('../portal/data/')

    ws_dirs <- dir(pattern = 'ws_boundary',
                   recursive = TRUE,
                   include.dirs = TRUE)

    ws_dirs <- grep(pattern = '^(?!.*documentation).*$',
                    x = ws_dirs,
                    value =  TRUE,
                    perl = TRUE)

    ws_list <- list()

    for(i in 1:length(ws_dirs)){

        ws_files <- list.files(ws_dirs[i],
                               pattern = '*shp',
                               recursive = TRUE,
                               full.names = TRUE)

        #read shapefiles, coerce to POLYGON, project, smooth borders
        ws_list_sub <- lapply(X = ws_files,
                              FUN = function(x){

                                  site_code <- str_match(string = x,
                                                         pattern = '^.*/(.*?)\\.shp$')[, 2]

                                  wb <- x %>%
                                      sf::st_read(quiet = TRUE) %>%
                                      sf::st_cast(to = 'POLYGON')

                                  coords <- sf::st_coordinates(wb)
                                  mean_latlong <- unname(colMeans(coords[, 1:2]))

                                  proj <- choose_projection(lat = mean_latlong[2],
                                                            long = mean_latlong[1])

                                  wb %>%
                                      sf::st_transform(crs = proj) %>%
                                      sf::st_union() %>%
                                      sf::st_as_sf() %>%
                                      mutate(site_code = !!site_code) %>%
                                      select(site_code, geometry = x) %>%
                                      sf::st_simplify(dTolerance = 30,
                                                      preserveTopology = TRUE) %>%
                                      sf::st_transform(crs = 4326) #back to WGS 84

                                  # if(length(x$geometry[[1]]) == 0) print(paste(i, site_code))
                              })

        ws_list <- append(x = ws_list,
                          values = ws_list_sub)
    }

    combined <- do.call(bind_rows, ws_list)

    dir.create(path = 'general/shed_boundary',
               showWarnings = FALSE)

    sf::st_write(obj = combined,
                 dsn = 'general/shed_boundary',
                 layer = 'shed_boundary.shp',
                 driver = 'ESRI Shapefile',
                 delete_layer = TRUE,
                 quiet = TRUE)

    if(ms_instance$op_system == 'windows'){
        setwd('../../')
        setwd('data_processing/')
    } else{
        setwd('../../data_acquisition/')
    }

}

get_osm_roads <- function(extent_raster, outfile = NULL){

    #extent_raster: either a terra spatRaster or a rasterLayer. The output
    #   roads will have the same crs, and roughly the same extent, as this raster.
    #outfile: string. If supplied, output shapefile will be written to this
    #   location. If not supplied, the output will be returned.

    message('Downloading roads layer from OpenStreetMap')

    extent_raster <- terra::rast(extent_raster)
    # rast_crs <- as.character(extent_raster@crs)
    rast_crs <- terra::crs(extent_raster,
                           proj = TRUE)

    extent_raster_wgs84 <- terra::project(extent_raster,
                                          y = 'epsg:4326')

    dem_bounds <- terra::ext(extent_raster_wgs84)[c(1, 3, 2, 4)]

    highway_types <- c('motorway', 'trunk', 'primary', 'secondary', 'tertiary')
    highway_types <- c(highway_types,
                       paste(highway_types, 'link', sep = '_'))

    roads_query <- osmdata::opq(dem_bounds) %>%
        osmdata::add_osm_feature(key = 'highway',
                                 value = highway_types)

    roads <- osmdata::osmdata_sf(roads_query)
    roads <- roads$osm_lines$geometry

    # plot(roads$osm_lines, max.plot = 1)

    roads_proj <- roads %>%
        sf::st_transform(crs = rast_crs) %>%
        sf::st_union() %>%
        # sf::st_transform(crs = WGS84) %>%
        sf::st_as_sf() %>%
        rename(geometry = x) %>%
        mutate(FID = 0:(n() - 1)) %>%
        dplyr::select(FID, geometry)

    if(! is.null(outfile)){

        sf::st_write(roads_proj,
                     dsn = outfile,
                     layer = 'roads',
                     driver = 'ESRI Shapefile',
                     delete_layer = TRUE,
                     quiet = TRUE)

        message(paste('OSM roads layer written to', outfile))

    } else {
        return(roads_proj)
    }
}

get_osm_streams <- function(extent_raster, outfile = NULL){

    #extent_raster: either a terra spatRaster or a rasterLayer. The output
    #   streams will have the same crs, and roughly the same extent, as this raster.
    #outfile: string. If supplied, output shapefile will be written to this
    #   location. If not supplied, the output will be returned.

    message('Downloading streams layer from OpenStreetMap')

    extent_raster <- terra::rast(extent_raster)
    # rast_crs <- as.character(extent_raster@crs)
    rast_crs <- terra::crs(extent_raster,
                           proj = TRUE)

    extent_raster_wgs84 <- terra::project(extent_raster,
                                          y = 'epsg:4326')

    dem_bounds <- terra::ext(extent_raster_wgs84)[c(1, 3, 2, 4)]

    streams_query <- osmdata::opq(dem_bounds) %>%
        osmdata::add_osm_feature(key = 'waterway',
                                 value = c('river', 'stream'))

    streams <- osmdata::osmdata_sf(streams_query)
    streams <- streams$osm_lines$geometry

    streams_proj <- streams %>%
        sf::st_transform(crs = rast_crs) %>%
        sf::st_union() %>%
        # sf::st_transform(crs = WGS84) %>%
        sf::st_as_sf() %>%
        rename(geometry = x) %>%
        mutate(FID = 0:(n() - 1)) %>%
        dplyr::select(FID, geometry)

    if(! is.null(outfile)){

        sf::st_write(streams_proj,
                     dsn = outfile,
                     layer = 'streams',
                     driver = 'ESRI Shapefile',
                     delete_layer = TRUE,
                     quiet = TRUE)

        message(paste('OSM streams layer written to', outfile))

    } else {
        return(streams_proj)
    }
}

fill_sf_holes <- function(x){

    #x: an sf object (probably needs to be projected)

    #if there are spaces in a shapefile polygon that are not filled in,
    #   this fills them.

    #if the first element of an sf geometry (which is a list) contains multiple
    #   elements, every element after the first is a hole. the first element
    #   is the outer geometry. so replace the geometry with a new polygon that
    #   is only the outer geometry

    wb_geom <- sf::st_geometry(x)
    # wb_geom_crs <- sf::st_crs(wb_geom)

    n_polygons <- length(wb_geom[[1]])
    if(n_polygons > 1){
        wb_geom[[1]] <- sf::st_polygon(wb_geom[[1]][1])
    }

    # if(length(wb_geom) != 1){
    #     wb_geom <- sf::st_combine(wb_geom)
    # }

    sf::st_geometry(x) <- wb_geom

    return(x)
}

get_nrcs_soils <- function(network,
                           domain,
                           nrcs_var_name,
                           site,
                           ws_boundaries){

    # Use soilDB to download  soil map unit key (mukey) calssification raster
    site_boundary <- ws_boundaries %>%
        filter(site_code == site)

    bb <- sf::st_bbox(site_boundary)

    soil <- try(sw(soilDB::mukey.wcs(aoi = bb,
                                 db = 'gssurgo',
                                 quiet = TRUE)))

    # should build a chunking method for this
    if(class(soil) == 'try-error'){

        this_var_tib <- tibble(site_code = site,
                               year = NA,
                               var =  names(nrcs_var_name),
                               val = NA)

        return(generate_ms_exception(glue('{s} is too large',
                                          s = site)))
    }

    mukey_values <- unique(soil@data@values)

    #### Grab soil vars

    # Query Soil Data Acess (SDA) to get the component key (cokey) in each mukey.
    #### each map unit made up of componets but components do not have
    #### spatial informaiton associted with them, but they do include information
    #### on the percentage of each mukey that is made up of each component.
    #### This informaiton on compositon is held in the component table in the
    #### comppct_r column, givin in a percent

    mukey_sql <- soilDB::format_SQL_in_statement(mukey_values)
    component_sql <- sprintf("SELECT cokey, mukey, compname, comppct_r, majcompflag FROM component WHERE mukey IN %s", mukey_sql)
    component <- sm(soilDB::SDA_query(component_sql))

    # Check is soil data is available
    if(length(unique(component$compname)) == 1 &&
       unique(component$compname) == 'NOTCOM'){

        this_var_tib <- tibble(site_code = site,
                               year = NA,
                               var =  names(nrcs_var_name),
                               val = NA)

        return(generate_ms_exception(glue('No data was retrived for {s}',
                                          s = site)))
    }

    cokey <- unique(component[,1])
    cokey <- data.frame(ID=cokey)


    # Query SDA for the componets to get infromation on their horizons from the
    #### chorizon table. Each componet is made up of soil horizons (layer of soil
    #### vertically) identified by a chkey.
    #### Informaiton about the depth of each horizon is needed to calculate weighted
    #### averges of any parameter for the whole soil column
    #### hzdept_r = depth to top of horizon
    #### hzdepb_r = depth to bottom of horizon
    #### om_r = percent organic matter
    cokey_sql <- soilDB::format_SQL_in_statement(cokey$ID)
    chorizon_sql <- sprintf(paste0('SELECT cokey, chkey, hzname, desgnmaster, hzdept_r, hzdepb_r, ',
                                         paste(nrcs_var_name, collapse = ', '),
                                         ' FROM chorizon WHERE cokey IN %s'), cokey_sql)

    full_soil_data <- sm(soilDB::SDA_query(chorizon_sql))

    # Calculate weighted average for the entire soil column. This involves 3 steps.
    #### First the component's weighted average of all horizones. Second, the
    #### weighted avergae of each component in a map unit. And third, the weighted
    #### average of all mukeys in the watershed (weighted by their area)

    # cokey weighted average of all horizones
    soil_data_joined <- full_join(full_soil_data, component, by = c("cokey"))

    all_soil_vars <- tibble()
    for(s in 1:length(nrcs_var_name)){

        this_var <- unname(nrcs_var_name[s])

        soil_data_one_var <- soil_data_joined %>%
            select(cokey, chkey, hzname, desgnmaster, hzdept_r, hzdepb_r,
                   !!this_var, mukey, compname, comppct_r, majcompflag)

        cokey_size <- soil_data_one_var %>%
            filter(!is.na(.data[[this_var]])) %>%
            mutate(layer_size = hzdepb_r-hzdept_r) %>%
            group_by(cokey) %>%
            summarise(cokey_size = sum(layer_size)) %>%
            ungroup()

        cokey_weighted_av <- soil_data_one_var %>%
            filter(!is.na(.data[[this_var]])) %>%
            mutate(layer_size = hzdepb_r-hzdept_r) %>%
            left_join(., cokey_size, by = 'cokey') %>%
            mutate(layer_prop = layer_size/cokey_size) %>%
            mutate(value_mat_weith = .data[[this_var]] * layer_prop)  %>%
            group_by(mukey, cokey) %>%
            summarise(value_comp = sum(value_mat_weith),
                      comppct_r = unique(comppct_r)) %>%
            ungroup()

        # mukey weighted average of all compenets
        mukey_weighted_av <- cokey_weighted_av %>%
            group_by(mukey) %>%
            mutate(comppct_r_sum = sum(comppct_r)) %>%
            summarise(value_mukey = sum(value_comp*(comppct_r/comppct_r_sum)))

        # Watershed weighted average
        site_boundary_p <- sf::st_transform(site_boundary, crs = sf::st_crs(soil))

        soil_masked <- sw(raster::mask(soil, site_boundary_p))

        watershed_mukey_values <- soil_masked@data@values %>%
            as_tibble() %>%
            filter(!is.na(value)) %>%
            group_by(value) %>%
            summarise(n = n()) %>%
            rename(mukey = value) %>%
            left_join(mukey_weighted_av, by = 'mukey')


        # Info on soil data
        # query SDA's Mapunit Aggregated Attribute table (muaggatt) by mukey.
        # table information found here: https://sdmdataaccess.sc.egov.usda.gov/documents/TableColumnDescriptionsReport.pdf
        # https://www.nrcs.usda.gov/wps/portal/nrcs/detail/soils/survey/geo/?cid=nrcs142p2_053631
        # how the tables relate using mukey, cokey, and chkey is here: https://www.nrcs.usda.gov/Internet/FSE_DOCUMENTS/nrcs142p2_050900.pdf

        if(all(is.na(watershed_mukey_values$value_mukey))){

            watershed_value <- NA
        } else{

            total_cells <- sum(watershed_mukey_values$n)
            na_cells <- watershed_mukey_values %>%
                filter(is.na(value_mukey))
            na_cells <- sum(na_cells$n)

            watershed_mukey_values_weighted <- watershed_mukey_values %>%
                filter(!is.na(value_mukey)) %>%
                mutate(sum = sum(n, na.rm = TRUE)) %>%
                mutate(prop = n/sum)

            watershed_mukey_values_weighted <- watershed_mukey_values_weighted %>%
                mutate(weighted_av = prop*value_mukey)

            watershed_value <- sum(watershed_mukey_values_weighted$weighted_av,
                                   na.rm = TRUE)

            watershed_value <- round(watershed_value, 2)
            na_prop <- round((100*(na_cells/total_cells)), 2)
        }

        this_var_tib <- tibble(year = NA,
                               site_code = site,
                               var =  names(nrcs_var_name[s]),
                               val = watershed_value,
                               pctCellErr = na_prop)

        all_soil_vars <- rbind(all_soil_vars, this_var_tib)
    }

    return(all_soil_vars)

    # #Used to visualize raster
    # for(i in 1:nrow(watershed_mukey_values_weighted)){
    #   soil_masked@data@values[soil_masked@data@values == pull(watershed_mukey_values_weighted[i,1])] <- pull(watershed_mukey_values_weighted[i,3])
    # }
    # soil_masked@data@isfactor <- FALSE
    #
    # mapview::mapview(soil_masked)
    # raster::plot(soil_masked)

    # Other table in SDA system
    # # component table
    # compnent_sql <- soilDB::format_SQL_in_statement(cokey$ID)
    # component_sql <- sprintf("SELECT cokey, runoff, compname, compkind, comppct_r, majcompflag, otherph, localphase, slope_r, hydricrating, taxorder, taxsuborder, taxsubgrp, taxpartsize FROM component WHERE cokey IN %s", compnent_sql)
    # component_return <- soilDB::SDA_query(component_sql)
    #
    # chtext
    # compnent_sql <- soilDB::format_SQL_in_statement(full_soil_data$chkey)
    # component_sql <- sprintf("SELECT texture, stratextsflag, rvindicator, texdesc, chtgkey FROM chtexturegrp WHERE chkey IN %s", compnent_sql)
    # component_return <- soilDB::SDA_query(component_sql)

    # component_sql <- sprintf("SELECT musym, brockdepmin, wtdepannmin, wtdepaprjunmin, niccdcd FROM muaggatt WHERE mukey IN %s", mukey_sql)
    # component_return <- soilDB::SDA_query(component_sql)
    #
    # # corestrictions table
    # corestrictions_sql <- sprintf("SELECT cokey, reskind, resdept_r, resdepb_r, resthk_r FROM corestrictions WHERE cokey IN %s", compnent_sql)
    # corestrictions_return <- soilDB::SDA_query(corestrictions_sql)
    #
    # # cosoilmoist table
    # cosoilmoist_sql <- sprintf("SELECT cokey,  FROM cosoilmoist WHERE cokey IN %s", compnent_sql)
    # corestrictions_return <- soilDB::SDA_query(corestrictions_sql)
    #
    # # pores
    # chkey_sql <- soilDB::format_SQL_in_statement(unique(full_soil_data$chkey))
    # cokey_to_chkey_sql_pores <- sprintf(paste0('SELECT chkey, poresize FROM chpores WHERE chkey IN %s'), chkey_sql)
    #
    # soil_pores <- soilDB::SDA_query(cokey_to_chkey_sql_pores)

}

load_spatial_data <- function(){

    spatial_files <- googledrive::drive_ls(googledrive::as_id('1EaEjkCb_U4zvLCXrULRTU-4yPC95jS__'))

    drive_files <- str_split_fixed(spatial_files$name, '\\.', n = Inf)[, 1]

    dir.create('data/spatial',
               showWarnings = FALSE)

    held_files <- list.files('data/spatial/')
    held_files[held_files == 'bfi.tif'] <- 'bfi'

    needed_files <- drive_files[! drive_files %in% held_files]

    if(length(needed_files)){

        loginfo(glue('Spatial files {sfs} needed from GDrive',
                     sfs = paste(needed_files,
                                 collapse = ', ')),
                logger = logger_module)

    } else {

        loginfo('All spatial files from GDrive are already held locally.',
                logger = logger_module)

        return(invisible())
    }

    needed_files <- paste0(needed_files, '.zip')

    needed_sets <- spatial_files %>%
        filter(name %in% needed_files)

    for(i in 1:nrow(needed_sets)){

        zip_path <- glue('data/spatial/{n}', n = needed_sets$name[i])

        print(paste0('Downloading ', needed_sets$name[i]))
        googledrive::drive_download(file = googledrive::as_id(needed_sets$id[i]),
                                    path = zip_path)

        print(paste0('Unzipping ', needed_sets$name[i]))
        unzip(zipfile = zip_path,
              exdir = 'data/spatial')

        file_check <- list.files('data/spatial')

        if('__MACOSX' %in% file_check){
            unlink('data/spatial/__MACOSX', recursive = T)
        }

        file.remove(zip_path)
    }
}

extract_ws_mean <- function(site_boundary, raster_path){

    rast_file <- terra::rast(raster_path)
    rast_crs <- terra::crs(rast_file)

    site_boundary_buf <- sw(sm(site_boundary %>%
                                   sf::st_buffer(., 0.01) %>%
                                   sf::st_transform(., rast_crs)))

    site_boundary <- site_boundary %>%
        sf::st_transform(., rast_crs)

    site_boundary_buf <- as(site_boundary_buf, "Spatial") %>%
        terra::vect()

    rast_masked <- rast_file %>%
        terra::crop(site_boundary_buf)

    weighted_results <- raster::extract(as(rast_masked, 'Raster'), site_boundary,
                                        weights = T, normalizeWeights = F)

    vals_w <- weighted_results[[1]] %>%
        as_tibble()

    ws_nas <- filter(vals_w, is.na(value))

    vals_w <- vals_w %>%
        filter(!is.na(value)) %>%
        mutate(new = value*weight)

    sd <- sd(vals_w$value)

    percent_na <- round((nrow(ws_nas)/(nrow(vals_w)+nrow(ws_nas)))*100, 2)

    val <- sum(vals_w$new)/sum(vals_w$weight)

    fin <- c(mean = val,
             sd = sd,
             pctCellErr = percent_na)

    return(fin)

}

ms_check_range <- function(d){

    d_vars <- unique(d$var)

    for(c in 1:length(d_vars)){

        var_p_frop <- drop_var_prefix(d_vars[c])

        min_val <- ms_vars %>%
            filter(variable_code == !!var_p_frop) %>%
            pull(val_min)

        if(!is.na(min_val)){
            d <- d %>%
                mutate(val = ifelse(var == !!d_vars[c] & as.numeric(val) < !!min_val, NA, val))
        }

        max_val <- ms_vars %>%
            filter(variable_code == !!var_p_frop) %>%
            pull(val_max)

        if(!is.na(max_val)){
            d <- d %>%
                mutate(val = ifelse(var == !!d_vars[c] & as.numeric(val) > !!max_val, NA, val))
        }
    }

    d <- d %>%
        filter(!is.na(val))
}

download_from_googledrive <- function(set_details, network, domain){

    #WARNING: any modification of the following line,
    #or insertion of code lines before it, will break
    #retrieve_versionless_product()
    download_from_googledrive_function_indicator <- TRUE

    prodname <- str_split_fixed(set_details$prodname_ms, '__', n = Inf)[1,1]
    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/sitename_NA',
                          n = network,
                          d = domain,
                          p = set_details$prodname_ms)

    id <- googledrive::as_id('178OOGxx1xM3C7m-Tdx6j5Dk_kxfWLJvw')
    gd_files <- googledrive::drive_ls(id, recursive = TRUE)

    network_id <- gd_files %>%
        filter(name == !!network)

    network_files <- googledrive::drive_ls(googledrive::as_id(network_id$id))

    domain_id <- network_files %>%
        filter(name == !!domain)

    domain_files <- googledrive::drive_ls(googledrive::as_id(domain_id$id))

    raw_files <- domain_files %>%
        filter(name == 'raw')

    raw_files <- googledrive::drive_ls(googledrive::as_id(raw_files$id))

    prod_folder <- raw_files %>%
        filter(name == !! set_details$prodname_ms)

    prod_files <- googledrive::drive_ls(googledrive::as_id(prod_folder$id))

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    held_files <- list.files(raw_data_dest)

    drive_files <- prod_files$name

    if(any(! drive_files %in% held_files)) {

        loginfo(glue('Retrieving {p}',
                     p = set_details$prodname_ms),
                logger = logger_module)

        needed_files <- drive_files[! drive_files %in% held_files]

        prod_files_neeed <- prod_files %>%
            filter(name %in% needed_files)

        for(i in 1:nrow(prod_files_neeed)){

            raw_file_path <- glue('{rd}/{n}',
                                  rd = raw_data_dest,
                                  n = prod_files_neeed$name[i])


            status <- googledrive::drive_download(file = googledrive::as_id(prod_files_neeed$id[i]),
                                        path = raw_file_path,
                                        overwrite = TRUE)
        }
    } else{
        loginfo(glue('Nothing to do for {p}',
                     p = set_details$prodname_ms),
                logger = logger_module)
    }
}

generate_watershed_summaries <- function(){

    fils <- list.files('../portal/data', recursive = T, full.names = T)
    fils <- fils[grepl('ws_traits', fils)]

    wide_spat_data <- site_data %>%
        filter(in_workflow == 1,
               site_type == 'stream_gauge') %>%
        select(network, domain, site_code, ws_area_ha)

    # Prism precip
    try({
        precip_files <- fils[grepl('cc_precip', fils)]
        precip_files <- precip_files[grepl('sum', precip_files)]

        precip <- map_dfr(precip_files, read_feather) %>%
            filter(year != substr(Sys.Date(), 0, 4),
                   var == 'cc_cumulative_precip') %>%
            group_by(site_code) %>%
            summarise(cc_mean_annual_precip = mean(val, na.arm = TRUE)) %>%
            filter(!is.na(cc_mean_annual_precip))
    }, silent = TRUE)

    # Prism temp
    try({
        temp_files <- fils[grepl('cc_temp', fils)]
        temp_files <- temp_files[grepl('sum', temp_files)]

        temp <- map_dfr(temp_files, read_feather) %>%
            filter(year != substr(Sys.Date(), 0, 4),
                   var == 'cc_temp_mean') %>%
            group_by(site_code) %>%
            summarise(cc_mean_annual_temp = mean(val, na.arm = TRUE)) %>%
            filter(!is.na(cc_mean_annual_temp))
    }, silent = TRUE)

    # start of season
    try({
        sos_files <- fils[grepl('start_season', fils)]

        sos <- map_dfr(sos_files, read_feather) %>%
            filter(year != substr(Sys.Date(), 0, 4),
                   var == 'vd_sos_mean') %>%
            group_by(site_code) %>%
            summarise(vd_mean_sos = mean(val, na.arm = TRUE)) %>%
            filter(!is.na(vd_mean_sos))
    }, silent = TRUE)

    # end of season
    try({
        eos_files <- fils[grepl('end_season', fils)]

        eos <- map_dfr(eos_files, read_feather) %>%
            filter(year != substr(Sys.Date(), 0, 4),
                   var == 'vd_eos_mean') %>%
            group_by(site_code) %>%
            summarise(vd_mean_eos = mean(val, na.arm = TRUE)) %>%
            filter(!is.na(vd_mean_eos))
    }, silent = TRUE)

    # length of season
    try({
        los_files <- fils[grepl('length_season', fils)]

        los <- map_dfr(los_files, read_feather) %>%
            filter(year != substr(Sys.Date(), 0, 4),
                   var == 'vd_los_mean') %>%
            group_by(site_code) %>%
            summarise(vd_mean_los = mean(val, na.arm = TRUE)) %>%
            filter(!is.na(vd_mean_los))
    }, silent = TRUE)

    # gpp
    try({
        gpp_files <- fils[grepl('gpp', fils)]
        gpp_files <- gpp_files[grepl('sum', gpp_files)]

        gpp <- map_dfr(gpp_files, read_feather) %>%
            filter(year != substr(Sys.Date(), 0, 4),
                   var == 'va_gpp_sum') %>%
            group_by(site_code) %>%
            summarise(va_mean_annual_gpp = mean(val, na.arm = TRUE)) %>%
            filter(!is.na(va_mean_annual_gpp))
    }, silent = TRUE)

    # npp
    try({
        npp_files <- fils[grepl('npp', fils)]

        npp <- map_dfr(npp_files, read_feather) %>%
            filter(year != substr(Sys.Date(), 0, 4),
                   var == 'va_npp_median') %>%
            group_by(site_code) %>%
            summarise(va_mean_annual_npp = mean(val, na.arm = TRUE)) %>%
            filter(!is.na(va_mean_annual_npp))
    }, silent = TRUE)

    # terrain
    try({
        terrain_fils <- fils[grepl('terrain', fils)]

        terrain <- map_dfr(terrain_fils, read_feather) %>%
            filter(var %in% c('te_elev_mean',
                              'te_elev_min',
                              'te_elev_max',
                              'te_aspect_mean',
                              'te_slope_mean')) %>%
            select(-year) %>%
            pivot_wider(names_from = 'var', values_from = 'val')
    }, silent = TRUE)

    # bfi
    try({
        bfi_fils <- fils[grepl('bfi', fils)]

        bfi <- map_dfr(bfi_fils, read_feather) %>%
            filter(var %in% c('hd_bfi_mean')) %>%
            filter(pctCellErr <= 15) %>%
            select(-year, -pctCellErr) %>%
            pivot_wider(names_from = 'var', values_from = 'val')
    }, silent = TRUE)

    # nlcd
    try({
        nlcd_fils <- fils[grepl('nlcd', fils)]

        nlcd <- map_dfr(nlcd_fils, read_feather) %>%
            filter(var %in% c('lg_nlcd_barren',
                              'lg_nlcd_crop',
                              'lg_nlcd_dev_hi',
                              'lg_nlcd_dev_low',
                              'lg_nlcd_dev_med',
                              'lg_nlcd_dev_open',
                              'lg_nlcd_forest_dec',
                              'lg_nlcd_forest_evr',
                              'lg_nlcd_forest_mix',
                              'lg_nlcd_grass',
                              'lg_nlcd_ice_snow',
                              'lg_nlcd_pasture',
                              'lg_nlcd_shrub',
                              'lg_nlcd_water',
                              'lg_nlcd_wetland_herb',
                              'lg_nlcd_wetland_wood',
                              'lg_nlcd_shrub_dwr',
                              'lg_nlcd_sedge',
                              'lg_lncd_lichens',
                              'lg_nlcd_moss')) %>%
            group_by(site_code) %>%
            mutate(max_year = max(year)) %>%
            filter(year == max_year) %>%
            select(-year, -max_year) %>%
            pivot_wider(names_from = 'var', values_from = 'val')
    }, silent = TRUE)

    # soil
    try({
        soil_fils <- fils[grepl('soil', fils)]

        soil <- map_dfr(soil_fils, read_feather) %>%
            filter(var %in% c('pf_soil_org',
                              'pf_soil_sand',
                              'pf_soil_silt',
                              'pf_soil_clay',
                              'pf_soil_ph')) %>%
            filter(pctCellErr <= 15) %>%
            select(-year, -pctCellErr) %>%
            pivot_wider(names_from = 'var', values_from = 'val')
    }, silent = TRUE)

    # soil thickness
    try({
        soil_thickness_fils <- fils[grepl('pelletier_soil_thickness', fils)]

        soil_thickness <- map_dfr(soil_thickness_fils, read_feather) %>%
            filter(var %in% c('pi_soil_thickness')) %>%
            filter(pctCellErr <= 15) %>%
            select(-year, -pctCellErr) %>%
            pivot_wider(names_from = 'var', values_from = 'val') %>%
            mutate(pi_soil_thickness = round(pi_soil_thickness, 2))
    }, silent = TRUE)

    # et_ref
    try({
        et_ref_fils <- fils[grepl('et_ref', fils)]
        et_ref_fils <- et_ref_fils[grepl('sum', et_ref_fils)]

        et_ref_thickness <- map_dfr(et_ref_fils, read_feather) %>%
            filter(var %in% c('ck_et_grass_ref_mean'),
                   !is.na(val)) %>%
            select(-year) %>%
            group_by(site_code) %>%
            summarise(ci_mean_annual_et = mean(val))
    }, silent = TRUE)

    # geological chem
    try({
        geochem_fils <- fils[grepl('geochemical', fils)]

        geochem <- map_dfr(geochem_fils, read_feather) %>%
            filter(var %in% c('pd_geo_Al2O3_mean',
                              'pd_geo_CaO_mean',
                              'pd_geo_CompressStrength_mean',
                              'pd_geo_Fe2O3_mean',
                              'pd_geo_HydaulicCond_mean',
                              'pd_geo_K2O_mean',
                              'pd_geo_MgO_mean',
                              'pd_geo_N_mean',
                              'pd_geo_Na2O_mean',
                              'pd_geo_P2O5_mean',
                              'pd_geo_S_mean',
                              'pd_geo_SiO2_mean'),
                   !is.na(val)) %>%
            select(-year) %>%
            group_by(site_code, var) %>%
            summarise(mean_val = mean(val)) %>%
            pivot_wider(names_from = 'var', values_from = 'mean_val')
    }, silent = TRUE)

    join_if_exists <- function(x, d){

        if(exists(x,
                  where = parent.frame(),
                  inherits = FALSE)){

            d <- full_join(d, get(x),
                           by = 'site_code')
        }

        return(d)
    }

    wide_spat_data <- join_if_exists('precip', wide_spat_data)
    wide_spat_data <- join_if_exists('temp', wide_spat_data)
    wide_spat_data <- join_if_exists('sos', wide_spat_data)
    wide_spat_data <- join_if_exists('eos', wide_spat_data)
    wide_spat_data <- join_if_exists('los', wide_spat_data)
    wide_spat_data <- join_if_exists('gpp', wide_spat_data)
    wide_spat_data <- join_if_exists('npp', wide_spat_data)
    wide_spat_data <- join_if_exists('terrain', wide_spat_data)
    wide_spat_data <- join_if_exists('bfi', wide_spat_data)
    wide_spat_data <- join_if_exists('nlcd', wide_spat_data)
    wide_spat_data <- join_if_exists('soil', wide_spat_data)
    wide_spat_data <- join_if_exists('soil_thickness', wide_spat_data)
    wide_spat_data <- join_if_exists('et_ref_thickness', wide_spat_data)
    wide_spat_data <- join_if_exists('geochem', wide_spat_data)

    dir.create('../portal/data/general/spatial_downloadables',
               recursive = TRUE,
               showWarnings = FALSE)

    write_csv(wide_spat_data,
              '../portal/data/general/spatial_downloadables/watershed_summaries.csv')
}

generate_watershed_raw_spatial_dataset <- function(){

    domains <- list.files('../portal/data/')
    domains <- domains[!grepl('general', domains)]

    ws_trait_folders <- unique(list.files(glue('../portal/data/{d}/ws_traits',
                                               d = domains)))

    all_files <- list.files('../portal/data',
                            recursive = TRUE,
                            full.names = TRUE)

    raw_spatial_dat <- tibble()
    for(i in 1:length(ws_trait_folders)){

        trait_files <- all_files[grepl(ws_trait_folders[i], all_files)]

        if(! length(trait_files)) next

        extention <- str_split_fixed(trait_files, '/', n = Inf)[,5]
        check_sum_raw <- str_split_fixed(extention, '_', n = Inf)[,1]
        sum_raw_prez <- unique(check_sum_raw)

        if(sum_raw_prez == 'sum' || ! all(c('raw', 'sum') %in% sum_raw_prez)){

            all_trait <- map_dfr(trait_files, read_feather)

            if(all(c('datetime', 'year') %in% colnames(all_trait))){

                all_trait <- sw(all_trait %>%
                    mutate(datetime = case_when(
                        is.na(datetime) ~ ymd(paste(year, 1, 1, sep = '-')),
                        ! is.na(datetime) ~ datetime)) %>%
                    select(-year))

            } else if('year' %in% colnames(all_trait)){

                all_trait <- sw(all_trait %>%
                    mutate(datetime = ymd(paste(year, 1, 1, sep = '-'))) %>%
                    select(-year))

            } else NULL
        }

        if(all(c('raw', 'sum') %in% sum_raw_prez)){
            trait_files <- trait_files[grepl('raw', trait_files)]

            all_trait <- map_dfr(trait_files, read_feather)
        }

        raw_spatial_dat <- rbind.fill(raw_spatial_dat, all_trait)
    }

    site_doms <- site_data %>%
        select(network, domain, site_code)

    raw_spatial_dat <- raw_spatial_dat %>%
        filter(!is.na(val)) %>%
        left_join(site_doms, by = 'site_code') %>%
        mutate(date = as.Date(datetime)) %>%
        select(network, domain, site_code, var, date, val, pctCellErr)

    spat_variable <- unique(raw_spatial_dat$var)

    # universal_products_meta <- universal_products %>%
    #     select(data_class, data_source, data_class_code, data_source_code)
    #
    # meta_data <- variables %>%
    #     filter(variable_code %in% !!spat_variable) %>%
    #     select(variable_code, variable_name, unit,
    #            type = variable_type,
    #            subtype = variable_subtype) %>%
    #     mutate(data_class_code = substr(variable_code, 0, 1),
    #            data_source_code = substr(variable_code, 2, 2)) %>%
    #     left_join(universal_products_meta, by = c('data_class_code', 'data_source_code')) %>%
    #     select(-data_class_code, -data_source_code)

    category_codes <- univ_products %>%
        select(variable_category_code = data_class_code,
               variable_category = data_class) %>%
        distinct() %>%
        arrange(variable_category_code)

    datasource_codes <- univ_products %>%
        select(data_source_code, data_source) %>%
        distinct() %>%
        arrange(data_source_code)

    fst::write_fst(raw_spatial_dat,
                   '../portal/data/general/spatial_downloadables/watershed_raw_spatial_timeseries.fst')
    write_csv(category_codes,
              '../portal/data/general/spatial_downloadables/variable_category_codes.csv')
    write_csv(datasource_codes,
              '../portal/data/general/spatial_downloadables/data_source_codes.csv')
}

compute_yearly_summary <- function(filter_ms_interp = FALSE,
                                   filter_ms_status = FALSE){

    # this and compute_yearly_summary_ws should probably be combined at some point, but for now,
    # compute_yearly_summary_ws() appends compute_yearly_summary with ws_traits

    #df = default sites for each domain
    df <- site_data %>%
        group_by(network, domain) %>%
        summarize(site_code = first(site_code),
                  pretty_domain = first(pretty_domain),
                  pretty_network = first(pretty_network),
                  .groups = 'drop') %>%
        select(pretty_network, network, pretty_domain, domain,
               default_site = site_code)

    all_domain <- tibble()
    for(i in 1:nrow(df)) {

        dom <- df$domain[i]

        net <- df$network[i]

        dom_path <- glue('../portal/data/{d}/stream_chemistry/', d = dom)

        site_files <- list.files(dom_path)

        sites <- str_split_fixed(site_files, pattern = '[.]', n = 2)[,1]

        stream_sites <- site_data %>%
            filter(domain == dom,
                   site_type == 'stream_gauge') %>%
            filter(site_code %in% sites) %>%
            pull(site_code)

        all_sites <- tibble()

        if(length(stream_sites) == 0) {

        } else{

            for(p in 1:length(stream_sites)) {

                path_chem <- glue("../portal/data/{d}/stream_chemistry/{s}.feather",
                                  d = dom,
                                  s = stream_sites[p])

                path_q <- glue("../portal/data/{d}/discharge/{s}.feather",
                               d = dom,
                               s = stream_sites[p])

                path_flux <- glue("../portal/data/{d}/stream_flux_inst_scaled/{s}.feather",
                                  d = dom,
                                  s = stream_sites[p])

                path_precip <- glue("../portal/data/{d}/precipitation/{s}.feather",
                                    d = dom,
                                    s = stream_sites[p])

                path_precip_chem <- glue("../portal/data/{d}/precip_chemistry/{s}.feather",
                                         d = dom,
                                         s = stream_sites[p])

                path_precip_flux <- glue("../portal/data/{d}/precip_flux_inst_scaled/{s}.feather",
                                         d = dom,
                                         s = stream_sites[p])

                #Stream discharge ####
                if(!file.exists(path_q)) {
                    site_q <- tibble()
                    q_record_length <- 365
                } else {

                    site_q <- sm(read_feather(path_q)) %>%
                        mutate(Year = year(datetime),
                               Month = month(datetime),
                               Day = day(datetime)) %>%
                        filter(Year != year(Sys.Date()))

                    q_record_length <- detrmin_mean_record_length(site_q)

                    if(filter_ms_interp){
                        site_q <- site_q %>%
                            filter(ms_interp == 0)
                    }
                    if(filter_ms_status){
                        site_q <- site_q %>%
                            filter(ms_status == 0)
                    }

                    if(nrow(site_q) == 0){
                        site_q <- tibble()
                        q_record_length <- 365
                    } else{

                        site_q <- site_q %>%
                            group_by(site_code, Year, Month, Day) %>%
                            summarise(val = mean(val, na.rm = T)) %>%
                            ungroup() %>%
                            mutate(Date = ymd(paste(Year, 1, 1, sep = '-'))) %>%
                            mutate(val = val*86400) %>%
                            group_by(site_code, Date, Year) %>%
                            summarise(val = sum(val, na.rm = TRUE),
                                      count = n()) %>%
                            mutate(val = val/1000) %>%
                            mutate(missing = (q_record_length-count)/q_record_length) %>%
                            mutate(missing = ifelse(missing < 0, 0, missing)) %>%
                            ungroup() %>%
                            select(-count) %>%
                            mutate(var = 'discharge') %>%
                            mutate(domain = dom)
                    }

                }

                all_sites <- rbind(all_sites, site_q)

                #Stream chemistry concentration ####
                if(!file.exists(path_chem)) {
                    site_chem <- tibble()
                } else {

                    #first taking a monthly mean and then taking a yearly mean from monthly
                    #mean. This is to try to limit sampling bias, where 100 samples are taken
                    #in the summer but only a few in the winter

                    site_chem <- sm(read_feather(path_chem) %>%
                                        mutate(Year = year(datetime),
                                               Month = month(datetime),
                                               Day = day(datetime)) %>%
                                        select(-datetime)) %>%
                        mutate(var = drop_var_prefix(var)) %>%
                        filter(Year != year(Sys.Date()))

                    if(filter_ms_interp){
                        site_chem <- site_chem %>%
                            filter(ms_interp == 0)
                    }
                    if(filter_ms_status){
                        site_chem <- site_chem %>%
                            filter(ms_status == 0)
                    }

                    site_chem <- site_chem %>%
                        group_by(site_code, Year, Month, Day, var) %>%
                        summarise(val = mean(val, na.rm = TRUE)) %>%
                        ungroup() %>%
                        mutate(Date = ymd(paste(Year, 1, 1, sep = '-'))) %>%
                        select(-Month) %>%
                        group_by(site_code, Date, Year, var) %>%
                        summarise(val = mean(val, na.rm = TRUE),
                                  count = n()) %>%
                        ungroup() %>%
                        mutate(missing = (q_record_length-count)/q_record_length) %>%
                        mutate(missing = ifelse(missing < 0, 0, missing)) %>%
                        select(-count) %>%
                        mutate(var = glue('{v}_conc', v = var)) %>%
                        mutate(domain = dom)
                }

                all_sites <- rbind(all_sites, site_chem)

                #Stream chemistry flux ####
                if(!file.exists(path_flux)) {
                    site_flux <- tibble()
                } else {

                    site_flux <- read_feather(path_flux)  %>%
                        mutate(Year = year(datetime),
                               Month = month(datetime),
                               Day = day(datetime)) %>%
                        select(-datetime) %>%
                        mutate(var = drop_var_prefix(var)) %>%
                        filter(Year != year(Sys.Date()))

                    if(filter_ms_interp){
                        site_flux <- site_flux %>%
                            filter(ms_interp == 0)
                    }
                    if(filter_ms_status){
                        site_flux <- site_flux %>%
                            filter(ms_status == 0)
                    }

                    site_flux <- site_flux %>%
                        group_by(site_code, Year, Month, Day, var) %>%
                        summarise(val = mean(val, na.rm = T)) %>%
                        ungroup() %>%
                        group_by(site_code, Year, var) %>%
                        mutate(Date = ymd(paste(Year, 1, 1, sep = '-'))) %>%
                        ungroup() %>%
                        group_by(site_code, Date, Year, var) %>%
                        summarise(val = sum(val, na.rm = TRUE),
                                  count = n()) %>%
                        ungroup() %>%
                        mutate(missing = (q_record_length-count)/q_record_length) %>%
                        mutate(missing = ifelse(missing < 0, 0, missing)) %>%
                        select(-count) %>%
                        mutate(var = glue('{v}_flux', v = var)) %>%
                        mutate(domain = dom)

                    site_flux[is.numeric(site_flux) & site_flux <= 0.00000001] <- NA
                }

                all_sites <- rbind(all_sites, site_flux)

                #Precipitation ####
                if(!file.exists(path_precip)) {
                    site_precip <- tibble()
                } else {

                    site_precip <- sm(read_feather(path_precip)) %>%
                        mutate(Year = year(datetime),
                               Month = month(datetime),
                               Day = day(datetime)) %>%
                        filter(Year != year(Sys.Date()))

                    if(filter_ms_interp){
                        site_precip <- site_precip %>%
                            filter(ms_interp == 0)
                    }
                    if(filter_ms_status){
                        site_precip <- site_precip %>%
                            filter(ms_status == 0)
                    }

                    site_precip <- site_precip %>%
                        group_by(site_code, Year, Month, Day) %>%
                        summarise(val = sum(val, na.rm = T)) %>%
                        ungroup() %>%
                        mutate(Date = ymd(paste(Year, 1, 1, sep = '-'))) %>%
                        group_by(site_code, Date, Year) %>%
                        summarise(val = sum(val, na.rm = TRUE),
                                  count = n()) %>%
                        ungroup() %>%
                        mutate(missing = (365-count)/365) %>%
                        mutate(missing = ifelse(missing < 0, 0, missing)) %>%
                        select(-count) %>%
                        mutate(var = 'precip') %>%
                        mutate(domain = dom)

                }

                all_sites <- rbind(all_sites, site_precip)

                #Precipitation chemistry concentration ####
                if(!file.exists(path_precip_chem)) {
                    site_precip_chem <- tibble()
                } else {

                    site_precip_chem <- sm(read_feather(path_precip_chem) %>%
                                               mutate(Year = year(datetime),
                                                      Month = month(datetime),
                                                      Day = day(datetime)) %>%
                                               select(-datetime)) %>%
                        mutate(var = drop_var_prefix(var)) %>%
                        filter(Year != year(Sys.Date()))

                    if(filter_ms_interp){
                        site_precip_chem <- site_precip_chem %>%
                            filter(ms_interp == 0)
                    }
                    if(filter_ms_status){
                        site_precip_chem <- site_precip_chem %>%
                            filter(ms_status == 0)
                    }

                    site_precip_chem <- site_precip_chem %>%
                        group_by(site_code, Year, Month, Day, var) %>%
                        summarise(val = mean(val, na.rm = TRUE)) %>%
                        ungroup() %>%
                        mutate(Date = ymd(paste(Year, 1, 1, sep = '-'))) %>%
                        select(-Month) %>%
                        group_by(site_code, Date, Year, var) %>%
                        summarise(val = mean(val, na.rm = TRUE),
                                  count = n()) %>%
                        ungroup() %>%
                        mutate(missing = (356-count)/365) %>%
                        mutate(missing = ifelse(missing < 0, 0, missing)) %>%
                        select(-count) %>%
                        mutate(var = glue('{v}_precip_conc', v = var)) %>%
                        mutate(domain = dom)

                }

                all_sites <- rbind(all_sites, site_precip_chem)

                #Precipitation chemistry flux ####
                if(!file.exists(path_precip_flux)) {
                    site_precip_flux <- tibble()
                } else {

                    site_precip_flux <- read_feather(path_precip_flux)  %>%
                        mutate(Year = year(datetime),
                               Month = month(datetime),
                               Day = day(datetime)) %>%
                        select(-datetime) %>%
                        mutate(var = drop_var_prefix(var)) %>%
                        filter(Year != year(Sys.Date()))

                    if(filter_ms_interp){
                        site_precip_flux <- site_precip_flux %>%
                            filter(ms_interp == 0)
                    }
                    if(filter_ms_status){
                        site_precip_flux <- site_precip_flux %>%
                            filter(ms_status == 0)
                    }

                    site_precip_flux <- site_precip_flux %>%
                        group_by(site_code, Year, Month, Day, var) %>%
                        summarise(val = mean(val, na.rm = T)) %>%
                        ungroup() %>%
                        group_by(site_code, Year, var) %>%
                        mutate(Date = ymd(paste(Year, 1, 1, sep = '-'))) %>%
                        ungroup() %>%
                        group_by(site_code, Date, Year, var) %>%
                        summarise(val = sum(val, na.rm = TRUE),
                                  count = n()) %>%
                        ungroup() %>%
                        mutate(missing = (356-count)/365) %>%
                        mutate(missing = ifelse(missing < 0, 0, missing)) %>%
                        mutate(var = glue('{v}_precip_flux', v = var)) %>%
                        mutate(domain = dom) %>%
                        select(-count)

                    site_precip_flux[is.numeric(site_precip_flux) & site_precip_flux <= 0.00000001] <- NA
                }

                all_sites <- rbind(all_sites, site_precip_flux)
            }
        }

        all_domain <-  rbind.fill(all_domain, all_sites)

    }

    all_domain <- all_domain %>%
        mutate(missing = missing*100) %>%
        mutate(missing = as.numeric(substr(missing, 1, 2))) %>%
        mutate(val = round(val, 4))

    dir.create('../portal/data/general/biplot')

    if(filter_ms_interp && filter_ms_status){
        write_feather(all_domain, '../portal/data/general/biplot/year_interp0_status0.feather')
    }

    if(filter_ms_interp && !filter_ms_status){
        write_feather(all_domain, '../portal/data/general/biplot/year_interp0.feather')
    }

    if(!filter_ms_interp && filter_ms_status){
        write_feather(all_domain, '../portal/data/general/biplot/year_status0.feather')
    }

    if(!filter_ms_interp && ! filter_ms_status){
        write_feather(all_domain, '../portal/data/general/biplot/year.feather')
    }
}

compute_yearly_summary_ws <- function() {

    #df = default sites for each domain
    df <- site_data %>%
        group_by(network, domain) %>%
        summarize(site_code = first(site_code),
                  pretty_domain = first(pretty_domain),
                  pretty_network = first(pretty_network),
                  .groups = 'drop') %>%
        select(pretty_network, network, pretty_domain, domain,
               default_site = site_code)

    all_domain <- tibble()
    for(i in 1:nrow(df)) {

        dom <- df$domain[i]
        net <- df$network[i]

        dom_path <- glue('../portal/data/{d}/ws_traits',
                         d = dom)

        prod_files <- list.files(dom_path,
                                 full.names = T,
                                 recursive = T)

        prod_files <- prod_files[! grepl('raw_', prod_files)]

        all_prods <- tibble()
        if(! length(prod_files) == 0){

            for(p in 1:length(prod_files)) {

                prod_tib <- read_feather(prod_files[p])
                prod_names <- names(prod_tib)

                if(! 'pctCellErr' %in% prod_names){
                    prod_tib <- prod_tib %>%
                        mutate(pctCellErr = NA)
                }

                all_prods <- rbind(all_prods, prod_tib)
            }

            all_prods <- sw(all_prods %>%
                mutate(Date = ymd(paste0(year, '-01', '-01'))) %>%
                mutate(domain = dom) %>%
                rename(Year = year))

            all_domain <- rbind(all_domain, all_prods)
        }
    }

    # Remove standard deviation
    all_ws_vars <- unique(all_domain$var)
    ws_vars_keep <- all_ws_vars[! grepl('sd', all_ws_vars)]
    ws_vars_keep <- ws_vars_keep[! grepl('1992', ws_vars_keep)]

    all_domain <- all_domain %>%
        filter(var %in% !!ws_vars_keep)

    summary_file_paths <- grep('year', list.files('../portal/data/general/biplot',
                                                  full.names = TRUE), value = T)

    for(s in 1:length(summary_file_paths)){

        conc_sum <- read_feather(summary_file_paths[s]) %>%
            mutate(pctCellErr = NA)

        all_domain <- all_domain %>%
            mutate(missing = 0)

        final <- rbind(conc_sum, all_domain)

        areas <- site_data %>%
            filter(site_type == 'stream_gauge') %>%
            select(site_code, domain, val = ws_area_ha) %>%
            mutate(Date = NA,
                   Year = NA,
                   var = 'area') %>%
            filter(!is.na(val)) %>%
            mutate(missing = 0) %>%
            mutate(pctCellErr = NA)

        #calc area normalized q
        area_q <- areas %>%
            select(site_code, domain, area = val) %>%
            full_join(., conc_sum, by = c('site_code', 'domain')) %>%
            mutate(discharge_a = ifelse(var == 'discharge', val/(area*10000), NA)) %>%
            mutate(discharge_a = discharge_a*1000) %>%
            filter(!is.na(discharge_a)) %>%
            mutate(var = 'discharge_a') %>%
            select(-val, -area) %>%
            rename(val = discharge_a)

        final <- rbind(final, areas, area_q) %>%
            filter(Year < year(Sys.Date()) | is.na(Year))

        write_feather(final, summary_file_paths[s])
    }
}

append_unprod_prefix <- function(d, prodname_ms){

    prodname_ms <- str_split_fixed(prodname_ms, '__', n = 2)[1,]
    prodname <- prodname_ms[1]
    prodcode <- prodname_ms[2]

    this_product <- univ_products %>%
        filter(prodname == !!prodname & prodcode == !!prodcode)

    data_class <- this_product %>%
        pull(data_class_code)

    data_source <- this_product %>%
        pull(data_source_code)

    d <- d %>%
        mutate(var = paste0(data_class, data_source, '_', var))


    return(d)
}
