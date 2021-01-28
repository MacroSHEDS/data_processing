

get_czo_product_version <- function(prodname_ms, domain, hydroshare_code, data_tracker){



    url_request_search <- paste0('https://www.hydroshare.org/hsapi/resource/',
                                 hydroshare_code,
                                 '/sysmeta/')

    search_results <- tempfile()

    download.file(url = url_request_search,
                  destfile = search_results,
                  cacheOK = FALSE,
                  method = 'curl')

    search_results_list <- read_json(search_results)
    newest_vsn <- search_results_list[["date_last_updated"]]

    return(newest_vsn)
}

construct_czo_product_sets <- function(hydroshare_code, component, data_tracker,
                                       latest_vsn){

    #returns: tibble with url, site_name, component (aka element_name)

    name_endpoint <- 'https://www.hydroshare.org/hsapi/resource/'
    mid_names <- '/files/'

    prod_url <- paste0(name_endpoint, hydroshare_code, mid_names, component)

    site_names_all <- c()
    components_all <- c()
    for(d in 1:length(component)){

        component_split <- str_split_fixed(component[d], '[/]', n = Inf)[1,]

        if(length(component_split) > 1){

            site_name <- component_split[1]
            components_single <- component_split[2]
        } else{
            site_name <- 'sitename_NA'
            components_single <- component_split
        }

        site_names_all <- append(site_names_all, site_name)
        components_all <- append(components_all, components_single)
    }


    avail_sets <- tibble(url = prod_url,
        site_name = site_names_all,
        component = components_all,
        avail_version = latest_vsn)

    return(avail_sets)
}

populate_set_details <- function(tracker, prodname_ms, site_name, avail){
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

get_czo_components <- function(search_string, hydroshare_code) {

    #This function determins the needed components in a hydroshare products.
    #The component column in the product.csv can be a single component that
    #is needed, a string of components that are needed seperated by "__|", or
    #search terms following this conventions:
    #    !!SEARCH(CONTAINS(serach_phrase|search_phrase)__|EXCLUDE(component_name|component_name))
    #
    #CONTAINS and EXCLUDE are each seperate grepl searchs where in text in the
    #   perenthesis are the pattern input in grepl. Exclude will remove the
    #   matches and contains will keep the. Multiple grepl searches can be done
    #   by seperating CONTAINS or EXCLUDE with __|
    #
    #CONTAINS will search each componet for the phrase that is specified, you can include
    #things like '.csv' to get all csv files or 'discharge&.csv' for components that
    #contain discharge and a .csv extentions.
    #
    #EXCLUDE will remove the specifed components within EXCLUDE
    #Note that hydroshare can have folder in products, the search terms will operate on
    #the name of a componet with the name in the file extention ex. 'folder/component.csv'
    #so to eclude this you need to specify 'folder/component.csv' only including
    #'component.csv' will not catch it in the EXCLUDE

    if(str_split_fixed(search_string, '[(]', n = Inf)[1,1] %in% c('!!SEARCH', '')){

        url_request_search <- paste0('https://www.hydroshare.org/hsapi/resource/',
                                     hydroshare_code,
                                     '/file_list/')

        search_results <- tempfile()

        download.file(url = url_request_search,
                      destfile = search_results,
                      cacheOK = FALSE,
                      method = 'curl')

        search_results_list <- read_json(search_results)[['results']]
        search_results <- sapply (search_results_list, '[[', 2)

        if(str_split_fixed(search_string, '[(]', n = Inf)[1,1] == '') {

            search_results_fin <-  search_results[!grepl('ReadMe.md', search_results)]

            search_results_fin <- str_split_fixed(search_results_fin, '[/]', n = Inf)[,8]

            return(search_results_fin)

        } else{

            search_results_vec <- c()
            for(t in 1:length(search_results)){
                comp_new <- str_split_fixed(search_results[t], '[/]', n = Inf)

                if(length(comp_new) > 8){
                    comp_new <- paste(comp_new[,8:length(comp_new)], collapse = '/')
                } else{
                    comp_new <- comp_new[1,8]
                }

                search_results_vec <- append(search_results_vec, comp_new)
            }

            #paste(str_split(search_results, '[/]', n = Inf)[,8:9], collapse = '/')

            search_terms <- str_split_fixed(search_string, '!!SEARCH[(]', n = Inf)[1,2]
            search_terms <- substr(search_terms, start = 1, stop = nchar(search_terms)-1)
            search_terms <-  as.vector(str_split_fixed(search_terms, '__[|]', n = Inf))

            for(p in 1:length(search_terms)){

                if(grepl('CONTAINS[(]', search_terms[p])){

                    contains <- str_split_fixed(search_terms[p], 'CONTAINS[(]', n = Inf)[1,2]
                    contains <- substr(contains, start = 1, stop = nchar(contains)-1)

                    search_results_vec <- search_results_vec[grepl(contains, search_results_vec)]
                }

                if(grepl('EXCLUDE[(]', search_terms[p])){

                    exclude <- str_split_fixed(search_terms[p], 'EXCLUDE[(]', n = Inf)[1,2]
                    exclude <- substr(exclude, start = 1, stop = nchar(exclude)-1)

                    search_results_vec <- search_results_vec[!grepl(exclude, search_results_vec)]
                }
            }

            return(search_results_vec)
        }

    } else{

        search_string <- as.vector(str_split_fixed(search_string, fixed('__|'), n = Inf))

        return(search_string)

    }
}
