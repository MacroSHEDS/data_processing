
#retrieval kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2407 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_1000 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2400 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2465 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_1001 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.dat')

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2565 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2501 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#munge kernels ####

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_2407 <- function(network, domain, prodname_ms, site_name, components) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_name,
                    c = components)

    fils <- rawfile[!grepl('HEADER', rawfile)]

    all <- tibble()
    for(i in 1:length(fils)){

        check_lines <- read_file(fils[i])
        check_lines <- str_split_fixed(check_lines, '\\n|\\r\\r', n = Inf)
        data_line <- grep('\\\\data', check_lines)

        d <- read_csv(fils[i], skip = data_line-3, col_types = cols(.default = "c"))

        if('sitecode' %in% names(d)){

            d <- d %>%
                rename(site = sitecode)
        }

        all <- rbind.fill(all, d)
    }

    all[all == '-9999'] <- NA

    col_names <- names(all)

    all_names <- c()
    for(i in 1:length(col_names)){
        col_split <- str_split_fixed(col_names[i], '_', n = Inf)

        col_split <- paste(col_split[1:length(col_split)-1], collapse = '_')

        all_names <- append(all_names, col_split)
    }

    name_consolidate <- tibble(new_name = all_names,
                               old_name = col_names) %>%
        filter(new_name != '')

     data_cold <- name_consolidate$old_name

    if('Sodium_dissolved_SWRCDX' %in% col_names && 'Sodium_dissolved_SWRCDX2' %in% col_names){

        all <- all %>%
            mutate(sodium = case_when(!is.na(Sodium_dissolved_SWRCDX) & !is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX2,
                                      !is.na(Sodium_dissolved_SWRCDX) & is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX,
                                      is.na(Sodium_dissolved_SWRCDX) & !is.na(Sodium_dissolved_SWRCDX2) ~ Sodium_dissolved_SWRCDX2))

        data_cold <- append(data_cold, 'sodium')

        name_consolidate <- name_consolidate %>%
            add_row(new_name = 'soidum', old_name = 'sodium') %>%
            mutate(new_name_ = case_when(new_name %in% c('Sulfate_dissolved','Sulfate') ~ 'SO4',
                                        new_name == 'Specific_conductance' ~ 'spCond',
                                        new_name == 'Temperature' ~ 'temp',
                                        new_name == 'Oxygen_dissolved' ~ 'DO',
                                        new_name == 'Carbon_dissolved_organic' ~ 'DOC',
                                        new_name == 'Alkalinity_total' ~ 'alk',
                                        new_name %in% c('Calcium_dissolved','Calcium') ~ 'Ca',
                                        new_name %in% 'Magnesium_dissolved','Magnesium' ~ 'Mg',
                                        new_name == 'Sodium_dissolved|Sodium|sodium' ~ 'Na',
                                        new_name %in% 'Potassium_dissolved','Potassium' ~ 'K',
                                        new_name %in% 'Chloride_dissolved','Chloride' ~ 'Cl',
                                        new_name == 'Nitrogen_nitrate_NO3' ~ 'NO3_N', #check
                                        new_name == 'Nitrogen_NH4' ~ 'NH4',
                                        new_name == 'Nitrogen_organic' ~ 'DON', #check
                                        new_name == 'Nitrogen_tot_diss' ~ 'TDN', #check
                                        new_name == 'Nitrogen_total_kjeldahl' ~ 'TKN',
                                        new_name == 'Nitrogen_total' ~ 'TN',
                                        new_name == 'Phosphorus_phosphate' ~ 'PO4_P',
                                        new_name %in% 'Phosphorus_total','Phosphorus_tot_dissolved' ~ 'TP',
                                        new_name == 'Phosphorus_tot_diss' ~ 'TDP',
                                        new_name == 'Turbidity' ~ 'turbid', #check
                                        new_name %in% 'Aluminum_dissolved','Aluminum' ~ 'Al',
                                        new_name == 'Arsenic_dissolved' ~ 'As',
                                        new_name == 'Boron_dissolved' ~ 'B',
                                        new_name == 'Chromium_dissolved' ~ 'Cr',
                                        new_name == 'Copper_dissolved' ~ 'Cu',
                                        new_name == 'Iron_dissolved' ~ 'Fe',
                                        new_name == 'Lead_dissolved' ~ 'Pb',
                                        new_name == 'Manganese_dissolved' ~ 'Mn',
                                        new_name %in% 'Silicon_dissolved','Silicon' ~ 'Si',
                                        new_name %in% 'Silica_dissolved','Silica_SiO2' ~ 'SiO2',
                                        new_name == 'Zinc_dissolved' ~ 'Zi',
                                        new_name == 'Phosphorus_phosphate_PO4' ~ 'PO4', # check may be PO4_P
                                        new_name == 'pH' ~ 'Ph'))

        }

    all_ <- all %>%
        filter(site != '\\data') %>%
        pivot_longer(cols = data_cold, names_to = 'old_name') %>%
        left_join(name_consolidate, by = 'old_name') %>%
        filter(!is.na(value)) %>%
        select(-old_name) %>%
        rename(date = `date  time`) %>%
        group_by(site, date, new_name) %>%
        summarise(value = mean(as.numeric(value, na.tm = TRUE))) %>%
        ungroup() %>%
        mutate(time = str_split_fixed(date, ' ', n = Inf)[,2],
               date = str_split_fixed(date, ' ', n = Inf)[,1]) %>%
        mutate(time = ifelse(nchar(time) == 4, paste0('0', time), time),
               time = ifelse(time == '', '12:00', time)) %>%
        mutate(datetime = as_datetime(paste(date, time, sep = ' '), format = '%Y%m%d %H:%M', tz = 'America/New_York')) %>%
        mutate(datetime = with_tz(datetime, 'UTC')) %>%
        select(site_name = site,
               var = new_name,
               val = value,
               datetime)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('date' = '%m/%e/%Y',
                                              'time' = '%H:%M'),
                         datetime_tz = 'US/Mountain',
                         site_name_col = 'site',
                         data_cols =  c('StreamFlow' = 'discharge'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-9999.000', '-9999', '-999.9', '-999'),
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- carry_uncertainty(d,
                           network = network,
                           domain = domain,
                           prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    d <- apply_detection_limit_t(d, network, domain, prodname_ms)

    return(d)

}

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_1000 <- function(network, domain, prodname_ms, site_name, component) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_2400 <- function(network, domain, prodname_ms, site_name, component) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_2465 <- function(network, domain, prodname_ms, site_name, component) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=PENDING
#. handle_errors
process_1_1001 <- function(network, domain, prodname_ms, site_name, component) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2565 <- function(network, domain, prodname_ms, site_name, component) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=PENDING
#. handle_errors
process_1_2501 <- function(network, domain, prodname_ms, site_name, component) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}
