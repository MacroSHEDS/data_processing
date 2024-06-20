retrieve_mcmurdo <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

munge_mcmurdo_discharge <- function(network, domain, prodname_ms, site_code, component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    prodcode <- prodcode_from_prodname_ms(prodname_ms)

    d <- read.csv(rawfile, colClasses = 'character') %>%
        rename_with(tolower) %>%
        rename_with(~gsub('\\.', '_', .)) %>%
        rename_with(~gsub('__', '_', .)) %>%
        tidyr::extract(date_time, into = c('month', 'day', 'year', 'time'),
                       '([0-9]{1,2})/([0-9]{1,2})/([0-9]{2,4}) (.*)') %>%
        mutate(year = case_when(nchar(year) == 4 ~ year,
                                year > 50 ~ paste0(19, year),
                                TRUE ~ paste0(20, year)),
               datetime = ymd_hm(glue('{year}-{month}-{day} {time}')))

    if(grepl('discharge', prodname_ms)){

        flagcol <- intersect(c('dischg_com', 'dis_comments', 'discharge_qlty',
                               'dschrge_qlty'),
                             colnames(d))
        datcol <- intersect(c('dschrge_rate', 'discharge_rate'),
                             colnames(d))

        d <- d %>%
            mutate(across(all_of(flagcol),
                          ~str_trim(tolower(.))),
                   across(all_of(flagcol),
                          ~case_when(grepl('good', .) ~ 'good',
                                     grepl('fair', .) ~ 'fair',
                                     grepl('poor', .) ~ 'poor',
                                     TRUE ~ .)))

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c('datetime' = '%Y-%m-%d %H:%M'),
                             # datetime_cols = c('date' = '%d-%m-%Y',
                             #                      'time' = '%H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'strmgageid',
                             data_cols =  setNames('discharge', datcol),
                             summary_flagcols = flagcol,
                             data_col_pattern = '#V#',
                             is_sensor = TRUE,
                             set_to_NA = '',
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(
            d,
            summary_flags_clean = list(
                'discharge_qlty' = c('good', 'fair', '')
            ),
            summary_flags_to_drop = list('discharge_qlty' = 'unusable'),
            varflag_col_pattern = NA
        )

    } else {

        cond_name <- intersect(c('conductivity', 'conductance'),
                               colnames(d))

        d <- d %>%
            rename_with(~sub('_quality$', '_qlty', .)) %>%
            mutate(across(ends_with('qlty'),
                          ~str_trim(tolower(.))),
                   across(ends_with('qlty'),
                          ~case_when(grepl('good', .) ~ 'good',
                                     grepl('fair', .) ~ 'fair',
                                     grepl('poor', .) ~ 'poor',
                                     TRUE ~ .)))

        d <- ms_read_raw_csv(preprocessed_tibble = d,
                             datetime_cols = c('datetime' = '%Y-%m-%d %H:%M'),
                             datetime_tz = 'Antarctica/McMurdo',
                             site_code_col = 'strmgageid',
                             data_cols =  setNames(c('temp', 'spCond'),
                                                   c('water_temp', cond_name)),
                             data_col_pattern = '#V#',
                             var_flagcol_pattern = '#V#_qlty',
                             is_sensor = TRUE,
                             set_to_NA = '',
                             sampling_type = 'I')

        d <- ms_cast_and_reflag(
            d,
            variable_flags_to_drop = 'unusable',
            variable_flags_clean = c('fair', 'good', '')
        )
    }

    return(d)
}

