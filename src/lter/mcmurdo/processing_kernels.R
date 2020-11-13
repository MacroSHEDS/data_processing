source('src/lter/mcmurdo/domain_helpers.R')

#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_9001 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9030 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9002 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9003 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9007 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9009 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9010 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9011 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9013 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9014 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9015 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9018 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9022 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9021 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9027 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9016 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9029 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9024 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9023 <- retrieve_mcmurdo

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9017 <- retrieve_mcmurdo

#dissolved_carbon: STATUS=READY
#. handle_errors
process_0_24 <- retrieve_mcmurdo

#total_nitrogen: STATUS=READY
#. handle_errors
process_0_78 <- retrieve_mcmurdo

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_20 <- retrieve_mcmurdo

#stream_nutrients: STATUS=READY
#. handle_errors
process_0_21 <- retrieve_mcmurdo

#munge kernels ####

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9030 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9002 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9003 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9007 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9009 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9010 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9011 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9013 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9014 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9015 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9018 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9022 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9021 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9027 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9016 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9029 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9024 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9023 <- munge_mcmurdo_discharge

#stream_cond_temp; discharge: STATUS=READY
#. handle_errors
process_0_9017 <- munge_mcmurdo_discharge

#dissolved_carbon: STATUS=READY
#. handle_errors
process_0_24 <- function(network, domain, prodname_ms, site_name,
                        component) {
    
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)

    
    d <- read.csv(rawfile, colClasses = 'character', skip = 26) %>%
        rename(DOC=6,
               comment = 8) %>%
        mutate(DOC = ifelse(comment == '<0.1 mg/L', 0, DOC)) %>%
        filter(STRMGAGEID != '',
               STRMGAGEID != 'garwood',
               STRMGAGEID != 'miers')
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE_TIME' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_name_col = 'STRMGAGEID',
                         data_cols =  c('DOC'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE)
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    
    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)
    
    d <- synchronize_timestep(d,
                              desired_interval = '1 day', #set to '15 min' when we have server
                              impute_limit = 30)
    
    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
    
    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_20 <- function(network, domain, prodname_ms, site_name,
                         component) {
    
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = component)
    
    
    d <- read.csv(rawfile, colClasses = 'character', skip = 26) %>%
        filter(STRMGAGEID != '',
               STRMGAGEID != 'garwood',
               STRMGAGEID != 'miers') 
    
    col_old <- colnames(d)
    col_name <- str_replace_all(col_old, '[.]', '_') 
    
    colnames(d) <- col_name
    
    d <- d %>%
        mutate(Li__mg_L_ = ifelse(Li_COMMENTS == 'not detected|ND|ND <0.001', 0, Li__mg_L_))
    
    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('DATE_TIME' = '%m/%d/%Y %H:%M'),
                         datetime_tz = 'Antarctica/McMurdo',
                         site_name_col = 'STRMGAGEID',
                         data_cols =  c('Cl__mM_' = 'Cl'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE)
    
    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)
    
    
    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)
    
    d <- synchronize_timestep(d,
                              desired_interval = '1 day', #set to '15 min' when we have server
                              impute_limit = 30)
    
    d <- apply_detection_limit_t(d, network, domain, prodname_ms)
    
    return(d)
}

#stream_nutrients: STATUS=READY
#. handle_errors
process_0_21 <- retrieve_mcmurdo


#derive kernels ####