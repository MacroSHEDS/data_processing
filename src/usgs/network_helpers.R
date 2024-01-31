populate_set_details <- function(tracker, prodname_ms, latest_vsn, site_code){

    #must return a tibble with a "needed" column, which indicates which new
    #datasets need to be retrieved

    retrieval_tracker = tracker[[prodname_ms]][[site_code]]$retrieve

    retrieval_tracker <- retrieval_tracker %>%
        mutate(needed = ifelse(held_version == -1, TRUE, FALSE))

    retrieval_tracker <- full_join(retrieval_tracker, latest_vsn, by = 'component')

    file_path <- glue('data/usgs/usgs/raw/{p}/{s}/{c}.feather',
                      p = prodname_ms,
                      s = site_code,
                      c = retrieval_tracker$component)

    all_com_last_records <- tibble()
    for(l in 1:length(file_path)){

        if(file.exists(file_path[l])){
            file <- read_feather(file_path[l])

            datetime_col <- grep('date', names(file), value = TRUE, ignore.case = TRUE)
            # datetime_col <- grep('dateTime|startDateTime', names(file), value = TRUE)
            if(length(datetime_col) != 1){
                stop('find a happy medium string match for date/datetime')
            }

            last_record <- max(file[[datetime_col]])

            component_records <- tibble(component = retrieval_tracker$component[l],
                                        last_record = last_record)

            all_com_last_records <- rbind(all_com_last_records, component_records)

        }
    }

    if(nrow(all_com_last_records) > 0){

        retrieval_tracker_ <- retrieval_tracker %>%
            filter(! needed) %>%
            left_join(all_com_last_records, by = 'component') %>%
            mutate(needed = ifelse(ymd(end_date) > last_record, TRUE, FALSE))

        retrieval_tracker <- retrieval_tracker %>%
            filter(needed) %>%
            rbind(retrieval_tracker_)
    }


    retrieval_tracker <- retrieval_tracker %>%
        mutate(site_code = !!site_code,
               prodname_ms = !!prodname_ms) %>%
        select(prodname_ms, site_code, site_no, component, data_type_cd, end_date, status, needed)

    return(retrieval_tracker)
}

get_usgs_data <-  function(sets, silent=TRUE){
    # sets <- new_sets; tracker <- held_data

    if(nrow(sets) == 0) return()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        s = sets[i, ]

        msg = glue('Retrieving {st}, {p}, {c}',
                   st=s$site_code, p=s$prodname_ms, c=s$component)
        loginfo(msg, logger=logger_module)

        processing_func = get(paste0('process_0_', str_split_fixed(s$prodname_ms, '__', n = Inf)[1,2]))
        result = do.call(processing_func,
                         args=list(set_details=s, network=network, domain=domain))


        new_status <- evaluate_result_status(result)

        s <- s %>%
            rename(last_mod_dt = end_date)

        update_data_tracker_r(network=network, domain=domain,
                              tracker_name='held_data',
                              set_details=s,
                              new_status=new_status)
     }
}

get_usgs_verstion <- function(prodname_ms,
                              domain,
                              usgs_code,
                              usgs_site){

    data_avail <- dataRetrieval::whatNWISdata(siteNumber = usgs_site) %>%
        filter(parm_cd %in% usgs_code)

    if(nrow(data_avail) == 0){
        return(generate_ms_exception('No data available for this site'))
    }

    parm_info <- tibble()
    for(i in 1:length(usgs_code)){

        one_parm <- data_avail %>%
            filter(parm_cd == !!usgs_code[i])

        if(nrow(one_parm) > 1){
            one_parm <- one_parm %>%
                filter(count_nu == max(one_parm$count_nu))
                # filter(data_type_cd == 'uv')
        }

        one_parm <- one_parm %>%
            select(site_no, parm_cd, end_date, data_type_cd)

        parm_info <- rbind(parm_info, one_parm)

    }

    parm_info <- parm_info %>%
        mutate(site_code = !!names(usgs_site),
               url = NA) %>%
        rename(component = parm_cd)

    return(parm_info)
}

get_usgs_codes <- function(prodname_ms){

    if(prodname_ms == 'discharge__1'){
        usgs_code <- '00060'
    }
    if(prodname_ms == 'stream_chemistry__2'){
        usgs_code <- c('00010', '00095', '00300', '00400', '63680')
    }
    if(prodname_ms == 'stream_chemistry__3'){
        usgs_code <- c('00665', '00940', '00608', '80154', '00600', '00631',
                       '00625')
    }

    return(usgs_code)
}
