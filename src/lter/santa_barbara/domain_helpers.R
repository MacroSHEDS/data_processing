retrieve_santa_barbara <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

munge_santa_barbara_precip <- function(network, domain, prodname_ms, site_code, component){

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    sbc_gauge <- grepl('SBC ', component, fixed = T)
    fc_gauge <- grepl('SBCoFC ', component, fixed = T)

    if(sbc_gauge){
        site <- str_split_fixed(component, 'Precipitation SBC', n = Inf)
    }

    if(fc_gauge){
        site <- str_split_fixed(component, 'Precipitation SBCoFC', n = Inf)
    }

    if(component == 'UCSB 200 daily precipitation, 1951-ongoing'){
        site <- 'UCSB200'
    } else {
        site <- str_split_fixed(site[,2], ',', n = Inf)
        site <- str_split_fixed(site[1,1], ' ', n = Inf)
        site <- paste(site[1,],collapse="")
    }

    d <- read.csv(rawfile1, colClasses = "character") %>%
        mutate(site_code = !!site) %>%
        rename_with(~sub('timestamp_UTC', 'timestamp_utc', .))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('timestamp_utc' = '%Y-%m-%dT%H:%M'),
                         datetime_tz = 'UTC',
                         site_code_col = 'site_code',
                         alt_site_code = list(BuelltonFS233 = 'BuelltonFireStation233'),
                         data_cols =  c('precipitation_mm' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-999', '-999.00', '-999.0'),
                         is_sensor = TRUE,
                         keep_empty_rows = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            keep_empty_rows = TRUE)

    return(d)
}

munge_santa_barbara_discharge <- function(network, domain, prodname_ms, site_code, component){

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    site <- str_split_fixed(component, ' ', n = Inf)[1,]
    site <- str_split_fixed(site[3], ',', n = Inf)[1,1]

    d <- read.csv(rawfile1, colClasses = "character") %>%
        mutate(site_code = !!site) %>%
        filter(! grepl('^NaN', timestamp_utc)) %>%
        mutate(timestamp_utc = stringr::str_trim(timestamp_utc))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('timestamp_utc' = '%Y-%m-%dT%H:%M'),
                         datetime_tz = 'UTC',
                         site_code_col = 'site_code',
                         data_cols =  c('discharge_lps' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-999', '-999.00', '-999.0'),
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    return(d)
}
