select_if(~(! all(is.na(.)))) %>%

#undeveloped
zip_dir = function(){
    zip(glue(raw_data_dest, '.zip'), flags='-rj',
        list.files(raw_data_dest, recursive=TRUE, full.names=TRUE))
    
    unlink(raw_data_dest, recursive=TRUE)
}

#undeveloped
unzip_dir = function(){
    unzip(glue(raw_data_dest, '.zip'), exdir=raw_data_dest, overwrite=TRUE)
}

get_flag_types = function(mapping, flags) {
    
    #MUST ENHANCE THIS TO ACCOUNT FOR MULTIPLE FLAGS APPLIED TO THE SAME
    #datapoint. see DB I/O card in projects todo list
    
    flags = paste(flags) #convert special types to char
    lengths = sapply(mapping, length)
    keyvec = rep(names(mapping), times=lengths)
    valvec = paste(unlist(unname(mapping)))
    
    if(any(! flags %in% valvec)){
        unaccounted_for = flags[which(! flags %in% valvec)]
        stop(paste('Missing mapping for flags:',
            paste(unaccounted_for, collapse=', ')))
    }
    
    flag_types = keyvec[match(flags, valvec)]
    
    return(flag_types)
}

resolve_commas = function(vec, comma_standin){
    vec = gsub(',', '\\,', vec, fixed=TRUE)
    vec = gsub(comma_standin, ',', vec)
    return(vec)
}

postgres_arrayify = function(vec){
    vec = paste0('{', vec, '}')
    vec = gsub('\\{\\}', '{""}', vec)
    return(vec)
}

populate_missing_shiny_files = function(domain){

    #this is not yet working. first, shiny needs to be reconfigured to
    #pull all site files as requested. atm precip and pchem are still
    #bound into one file (i.e. precip.feather instead of site1.feather)

    list.files('data/hbef/')

    qq = read_feather('data/hbef/discharge.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/discharge.feather')

    qq = read_feather('data/hbef/flux.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/flux.feather')

    qq = read_feather('data/hbef/pchem.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/pchem.feather')

    qq = read_feather('data/hbef/precip.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/precip.feather')

}

#maybe some useful parts for a web scraping function
scrape_web <- function(){

    require(rvest)
    require(R.matlab)

    setwd('~/git/macrosheds/data_acquisition/data/lter/hjandrews')

    dset_urls = list(q=paste0('https://andrewsforest.oregonstate.edu/sites',
        '/default/files/lter/data/weather/portal/MISC/DISCHARGE/data/index.html'))

    for(i in 1:length(dset_urls)){
        read_html(dset_urls[[i]]) %>%
            html_node('td.title') %>%
            html_text()
    }

    # d = readMat('https://andrewsforest.oregonstate.edu/sites/default/files/lter/data/weather/portal/MISC/DISCHARGE/data/discharge_5min_merged.mat')
    for(i in 1:length(dset_urls)){
        d = download.file('https://andrewsforest.oregonstate.edu/sites/default/files/lter/data/weather/portal/MISC/DISCHARGE/data/discharge_5min_merged.mat',
            'provisional/q_merged.mat')
        m = readMat('provisional/q_merged.mat')
    }
}

#. handle_errors
zero_locf <- function (x, option = "locf", zero_remaining = "rev", maxgap = Inf){

    #adapted from imputeTS::na_locf

    data <- x
    if (!is.null(dim(data)[2]) && dim(data)[2] > 1) {
        for (i in 1:dim(data)[2]) {
            if (!any(data[, i] == 0)) {
                next
            }
            tryCatch(data[, i] <- zero_locf(data[, i], option,
                                          na_remaining, maxgap), error = function(cond) {
                                              warning(paste("imputeTS: No imputation performed for column",
                                                            i, "because of this", cond), call. = FALSE)
                                          })
        }
        return(data)
    }
    else {
        missindx <- data == 0
        if (!any(data == 0)) {
            return(data)
        }
        if (any(class(data) == "tbl")) {
            data <- as.vector(as.data.frame(data)[, 1])
        }
        if (all(missindx)) {
            stop("Input data has only 0s. Input data needs at least 1 nonzero data point for applying zero_locf")
        }
        if (!is.null(dim(data)[2]) && !dim(data)[2] == 1) {
            stop("Wrong input type for parameter x")
        }
        if (!is.null(dim(data)[2])) {
            data <- data[, 1]
        }
        if (!is.numeric(data)) {
            stop("Input x is not numeric")
        }
        data_vec <- as.vector(data)
        if (option == "locf") {
            imputed <- locf(data_vec, FALSE)
        }
        else if (option == "nocb") {
            imputed <- locf(data_vec, TRUE)
        }
        else {
            stop("Wrong parameter 'option' given. Value must be either 'locf' or 'nocb'.")
        }
        data[missindx] <- imputed[missindx]
        if (!any(data == 0) || na_remaining == "keep") {
        }
        else if (na_remaining == "rev") {
            if (option == "locf") {
                data <- zero_locf(data, option = "nocb")
            }
            else if (option == "nocb") {
                data <- zero_locf(data, option = "locf")
            }
        }
        else if (na_remaining == "rm") {
            data <- data[! data == 0]
        }
        else if (na_remaining == "mean") {
            data[data == 0] <- mean(data, na.rm = TRUE)
        }
        else {
            stop("Wrong parameter 'zero_remaining' given. Value must be either 'keep', 'rm', 'mean' or 'rev'.")
        }
        if (is.finite(maxgap) && maxgap >= 0) {
            rlencoding <- rle(x == 0)
            rlencoding$values[rlencoding$lengths <= maxgap] <- FALSE
            en <- inverse.rle(rlencoding)
            data[en == TRUE] <- 0
        }
        if (!is.null(dim(x)[2])) {
            x[, 1] <- data
            return(x)
        }
        return(data)
    }
}

#. handle_errors
vround <- function(x, digits){

    #just like base::round, but digits is a vector

    if(length(x) != length(digits)){
        stop('Lengths of x and digits must be equal')
    }

    for(d in unique(digits)){
        d_inds <- digits == d
        x[d_inds] <- round(x[d_inds], d)
    }

    return(x)
}

#this version of identify_detection_limit was designed when we still
#stored data in wide format.
identify_detection_limit_t <- function(X, network, domain,
                                       return_detlims = FALSE){

    #this is the temporally explicit version of identify_detection_limit (_t).
    #it supersedes the scalar version (identify_detection_limit_s).
    #that version just returns its output. This version relies on stored data,
    #so automatically writes to data/<network>/<domain>/detection_limits.json,
    #and, if return_detlims = TRUE, returns its output as an integer matrix
    #of detection limits with size equal to that of X.

    #X is a 2d array-like object with column names. must have datetime and
    #site_name columns.

    #the detection limit (number of decimal places)
    #of each column is written to data/<network>/<domain>/detection_limits.json
    #as a nested list:
    #prodname_ms
    #    variable
    #        startdt1: limit1
    #        startdt2: limit2 ...
    #non-numeric columns are not considered variables and are given detlims of
    #NA, beginning at the earliest datetime for each site. If
    #return_detlims == TRUE, these columns are populated with NAs.
    #macrosheds-canonical columns (datetime, site_name, ms_status, ms_interp)
    #are recognized as non-variables and given detection limits of NA

    #detection limit (detlim) for each site-variable-datetime is computed as the
    #number of characters following each decimal place. NA detlims are filled
    #by locf, followed by nocb. Then, to account for false detlims arising from
    #trailing zeros, positive monotonicity is forced by carrying forward
    #cumulative maximum detlims. Each time the detlim increases,
    #a new startdt and limit are recorded.

    #X will be sorted ascendingly by site_name and then datetime.

    X <- as_tibble(X) %>%
        arrange(site_name, datetime)

    # if(! 'site_name' %in% colnames(X)){
    #     sitename_present <- FALSE
    #     X$site_name = '0'
    # } else {
    #     sitename_present <- TRUE
    # }

    identify_detection_limit_v <- function(x, varnm, dt, sn, output = 'list'){

        #x is a vector
        #varnm is the name of the column that became x
        #dt is a datetime vector
        #sn is a site name vector
        #output is either 'list' or 'vector'. If 'list', this function
        #   summarizes x by site, returning a list of site names, each containing
        #   two elements, a vector of start dates, and a vector of corresponding
        #   detection limits. Detection limits are only recorded for the first
        #   value and for any change that follows. If output is 'vector', this
        #   function returns a vector of detection limits the same length as x.

        #non-numeric vectors return NA detection limits

        sites <- unique(sn)

        #for non-numerics, build a list of prodname -> site -> dt: lim
        #where dt is always the earliest datetime and lim is always NA
        if(varnm %in% ms_canonicals || ! is.numeric(x)){

            if(output == 'vector'){
                detlim <- rep(NA, length(x))
                return(detlim)
            }

            detlim <- list()
            for(i in 1:length(sites)){
                nulldt <- as.character(dt[sn == sites[i]][1])
                detlim[[i]] <- list(startdt = nulldt,
                                    lim = NA)
            }

            names(detlim) <- sites
            return(detlim)
        }

        options(scipen = 100)
        nas <- is.na(x) | x == 0

        x <- as.character(x)
        nsigdigs <- stringr::str_split_fixed(x, '\\.', 2)[, 2] %>%
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

    if(! is.null(dim(X))){

        detlim <- mapply(FUN = function(X, varnms, dt, sn){
                             identify_detection_limit_v(x = X,
                                                        varnm = varnms,
                                                        dt = dt,
                                                        sn = sn)
                         },
                         X = X,
                         varnms = colnames(X),
                         MoreArgs = list(dt = X$datetime,
                                         sn = X$site_name),
                         SIMPLIFY = FALSE)

    } else {
        stop('X must be a 2d array-like')
    }

    write_detection_limit(detlim,
                          network = network,
                          domain = domain)

    if(return_detlims){

        # detlim <- lapply(X = X,
        #                  FUN = function(y, dt, sn){
        #                      identify_detection_limit_v(y,
        #                                                 dt = dt,
        #                                                 sn = sn,
        #                                                 output = 'vector')
        #                  },
        #                  dt = X$datetime,
        #                  sn = X$site_name) %>%
        detlim <- mapply(FUN = function(X, varnms, dt, sn){
                             identify_detection_limit_v(x = X,
                                                        varnm = varnms,
                                                        dt = dt,
                                                        sn = sn,
                                                        output = 'vector')
                         },
                         X = X,
                         varnms = colnames(X),
                         MoreArgs = list(dt = X$datetime,
                                         sn = X$site_name),
                         SIMPLIFY = FALSE) %>%
            as_tibble()

        return(detlim)
    }

    return()
}
