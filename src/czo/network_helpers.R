

get_czo_product_version <- function(prodname_ms, domain, hydroshare_code, data_tracker){

    vsn_endpoint <- 'https://www.hydroshare.org/resource/'

    vsn_request <- glue(vsn_endpoint, hydroshare_code)

    if(ms_instance$op_system == 'windows'){
        newest_vsn <- xml2::xml_text(xml2::read_html(vsn_request))
    } else{
        newest_vsn <- RCurl::getURLContent(vsn_request, timeout=9)
    }

    newest_vsn <- str_match(newest_vsn, 'Last updated:\\s{1,}[A-z]{3}\\s[0-9]{2}, [0-9]{4} at \\d:[0-9]{2} [A-z][.][A-z]')

    newest_vsn <- str_split_fixed(newest_vsn, 'Last updated:\n\\s{1,}', n = Inf)[1,2]

    return(newest_vsn)
}

construct_czo_product_sets <- function(version, hydroshare_code,
                                       component, data_tracker){

    #returns: tibble with url, site_name, component (aka element_name)

    name_endpoint <- 'https://www.hydroshare.org/resource/'
    mid_names <- '/data/contents/'

    prod_url <- paste0(name_endpoint, hydroshare_code, mid_names, component)

    avail_sets <- tibble(url = prod_url,
        site_name = 'sitename_NA',
        component = component)

    return(avail_sets)
}

populate_set_details <- function(tracker, prodname_ms, site_name, avail,
    latest_vsn){
    #tracker=held_data;avail=avail_site_sets

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker_old = tracker[[prodname_ms]][[site_name]]$retrieve

    prodcode = prodcode_from_prodname_ms(prodname_ms)

    retrieval_tracker = avail %>%
        mutate(
            avail_version = latest_vsn,
            prodcode_full = NA, #no such thing for lter. could simply omit
            prodcode_id = prodcode,
            prodname_ms = prodname_ms) %>%
        full_join(retrieval_tracker_old, by='component') %>%
        mutate(needed = !avail_version == held_version)

    if(any(is.na(retrieval_tracker$needed))){
        msg = paste0('Must run `track_new_site_components` before ',
            'running `populate_set_details`')
        logerror(msg, logger=logger_module)
        stop(msg)
    }

    return(retrieval_tracker)
}

get_czo_data <- function(domain, sets, tracker, silent=TRUE){
    # sets <- new_sets; tracker <- held_data

    if(nrow(sets) == 0) return()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Processing {st}, {p}, {c}',
            st=s$site_name, p=s$prodname_ms, c=s$component)
        loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', s$prodcode_id))
        result = do.call(processing_func,
            args=list(set_details=s, network=network, domain=domain))
        # process_0_1(set_details=s, network=network, domain=domain)

        new_status <- evaluate_result_status(result)
        update_data_tracker_r(network=network, domain=domain,
            tracker_name='held_data', set_details=s, new_status=new_status)
    }
}

download_raw_file <- function(network, domain, set_details, file_type = '.csv') {
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
                         wd = getwd(),
                         n = network,
                         d = domain,
                         p = set_details$prodname_ms,
                         s = set_details$site_name)

    dir.create(raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    if(is.null(file_type)) {

        download.file(url = set_details$url,
                      destfile = glue(raw_data_dest,
                                      '/',
                                      set_details$component),
                      cacheOK = FALSE,
                      method = 'libcurl')
    } else {

        download.file(url = set_details$url,
                      destfile = glue(raw_data_dest,
                                      '/',
                                      set_details$component,
                                      file_type),
                      cacheOK = FALSE,
                      method = 'libcurl')
    }
}

pull_cdnr_discharge <- function(network, domain, prodname_ms, sites) {

    #This function is used in the case when a domain's discharge data is
    #associated with the Colorado Department of Natural Recources and is not
    #available through the domain's portal or the USGS data is preferable

    #sites: a list where the name of each list element is the macrosheds site name.
    #    Elements within each list are 'start_year' for the first year of data we
    #    want to grab and 'abbrv' is the CDNR gauge code

    for(s in 1:length(sites)) {

        comp_start <- 'https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewatertsday/?format=csv&dateFormat=spaceSepToSeconds&abbrev='
        site_abrv <- sites[[s]][['abbrv']]
        comid_mid1<- '&min-measDate=01%2F01%2F'
        comp_mid2 <- '_00%3A00&max-measDate=01%2F01%2F'
        comp_end <- '_00%3A00&apiKey=FIk5bcHm0ERUrf0db%2FvKrF8zccP35sun'

        cur_year <- as.numeric(str_split_fixed(Sys.Date(), '-', n = 3)[1,1])

        years <- seq(as.numeric(sites[[s]][['start_year']]), cur_year)

        url_requests <- paste0(comp_start, site_abrv, comid_mid1, years, comp_mid2, years+1, comp_end)

        temp_csv <- tempfile(fileext = '.csv')

        all_years <- tibble()
        for(i in 1:length(url_requests)){

            download.file(url = url_requests[i],
                          destfile = temp_csv,
                          cacheOK = FALSE,
                          method = 'curl')

            cur_year <- read_csv(temp_csv)

            if(nrow(cur_year) == 0){
                cur_year <- tibble()
            } else{

                header_pos <- grep('stationNum', pull(cur_year[1]))

                cur_year <- read_csv(temp_csv, skip = header_pos)
            }

            all_years <- rbind(all_years, cur_year)
        }

        final <- all_years %>%
            rename(val = value,
                   datetime = measDate) %>%
            mutate(var = 'discharge',
                   ms_status = ifelse(!flagA %in% c('P', 'p', 'A', 'a') | !is.na(flagB), 1, 0),
                   site_name = !!names(sites)[s],
                   val = val * 28.31685) %>%
            select(site_name, datetime, var, val, ms_status) %>%
            filter(!is.na(val)) %>%
            mutate(datetime = paste0(as.character(datetime), ' ', '12:00:00')) %>%
            mutate(datetime = as_datetime(datetime, format = '%Y-%m-%d %H:%M:%S', tz = 'America/Denver')) %>%
            mutate(datetime = with_tz(datetime, tzone = 'UTC'))

        d <- identify_sampling_bypass(final,
                                      is_sensor = TRUE,
                                      network = network,
                                      domain = domain,
                                      prodname_ms = prodname_ms)

        d <- carry_uncertainty(d,
                               network = network,
                               domain = domain,
                               prodname_ms = prodname_ms)

        d <- synchronize_timestep(d,
                                  desired_interval = '1 day', #set to '15 min' when we have server
                                  impute_limit = 30)

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
                      site_name = names(sites)[s],
                      level = 'derived',
                      shapefile = FALSE,
                      link_to_portal = FALSE)
    }

    return()
}
