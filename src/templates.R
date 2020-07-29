#a non_spatial munge kernel with nearly every bell and whistle ####

#product: STATUS=PENDING
#. handle_errors
process_1_XXX <- function(network, domain, prodname_ms, site_name,
                          component){
                          # components){

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
    # rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                   n = network,
                   d = domain,
                   p = prodname_ms,
                   s = site_name,
                   c = component)
                   # c = components[X])

    d = sw(read_csv(rawfile,
                    progress = FALSE,
                    col_types = readr::cols_only(
                        DATETIME = 'c',
                        WS = 'c',
                        # PRECIP_METHOD = 'c', #method information
                        # QC_LEVEL = 'c', #derived, gapfilled, etc
                        Discharge_ls = 'd')))

    d = ue(sourceflags_to_ms_status(d,
                                    flagstatus_mappings = list(
                                        PRECIP_TOT_FLAG = c('A', 'E'),
                                        EVENT_CODE = NA)))
    d <- d %>%
        # Flag = 'c'))) %>% #all flags are acceptable for this product
        rename(site_name = WS,
               datetime = DATETIME,
               discharge = Discharge_ls) %>%
        # rename_all(dplyr::recode, #essentially rename_if_exists
        #            precipCatch = 'precipitation_ns',
        #            flowGageHt = 'discharge_ns') %>%
        mutate(
            # datetime = with_tz(force_tz(as.POSIXct(datetime), 'US/Eastern'), 'UTC'), #don't think this is ever needed
            datetime = lubridate::ymd(datetime, tz = 'UTC'),
            # datetime = with_tz(as_datetime(datetime, 'US/Eastern'), 'UTC'),
            site_name = paste0('w', site_name),
            # ms_status = 0) %>% #only if you don't need sourceflags_to_ms_status
            # ms_status = ifelse(is.na(fieldCode), FALSE, TRUE), #same
            DIC = ue(convert_unit(DIC, 'uM', 'mM')),
            NH4_N = ue(convert_molecule(NH4, 'NH4', 'N')),
            NO3_N = ue(convert_molecule(NO3, 'NO3', 'N')),
            PO4_P = ue(convert_molecule(PO4, 'PO4', 'P'))) %>%
        select(-date, -timeEST, -PO4, -NH4, -NO3, -fieldCode) %>%
        filter_at(vars(-site_name, -datetime, -ms_status),
                  any_vars(! is.na(.))) %>% #probs redund with synchronize_timestep, but whatevs
        group_by(datetime, site_name) %>% #remove dupes
        summarize(
            discharge = mean(discharge, na.rm = TRUE),
            ms_status = numeric_any(ms_status)) %>%
        # summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        # mutate(ms_status = as.numeric(ms_status)) %>% #only if you had to convert it to logical
        # select(-ms_status, everything()) #old way
        relocate(ms_status, .after = last_col()) %>% #new way
        relocate(ms_interp, .after = last_col()) #sometimes gotta call twice

    d[is.na(d)] = NA #replaces NaNs. is there a clean, pipey way to do this?

    # #variable interval
    # intv <- ifelse(grepl('precip', prodname_ms),
    #                '1 day',
    #                '1 hour')
    # d <- ue(synchronize_timestep(ms_df = d,
    #                              desired_interval = intv,
    #                              impute_limit = 30))

    #constant interval
    d <- ue(synchronize_timestep(ms_df = d,
                                 desired_interval = '15 min',
                                 impute_limit = 30))

    return(d)
}
