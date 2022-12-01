library(tidyverse)
library(macrosheds)
library(lubridate)
library(glue)

setwd('macrosheds/data_acquisition/')

# tally sites from data_acquisition/macrosheds_figshare_X/macrosheds_timeseries_data ####

vsn = 1

all_fis <- list.files(glue('macrosheds_dataset_v{vsn}/'), recursive = T, full.names = T)

q_files <- grep('discharge', all_fis, value = T)
q_files <- grep('feather', q_files, value = T)

chem_files <- grep('stream_chemistry', all_fis, value = T)
chem_files <- grep('feather', chem_files, value = T)

all_q_sites <- unique(str_split_fixed(q_files, '/', n = Inf)[,7])
all_chem_sites <- unique(str_split_fixed(chem_files, '/', n = Inf)[,7])

site_numbers <- tibble(domains = c('all'),
                       q_and_chem = length(all_q_sites[all_q_sites %in% all_chem_sites]),
                       q_only = length(all_q_sites),
                       chem_only = length(all_chem_sites),
                       chem_or_q = length(unique(c(all_q_sites, all_chem_sites))))

doms <- unique(str_split_fixed(q_files, '/', n = Inf)[,4])
for(i in 1:length(doms)){
    dom_chem <- grep(paste0('/', doms[i], '/'), chem_files, value = T)
    chem_sites <- str_split_fixed(dom_chem, '/', n = Inf)[,7]
    chem_sites <- unique(chem_sites)
    dom_q <- grep(paste0('/', doms[i], '/'), q_files, value = T)
    q_sites <- str_split_fixed(dom_q, '/', n = Inf)[,7]
    q_sites <- unique(q_sites)
    this_domain <- tibble(domains = doms[i],
                          q_and_chem = length(q_sites[q_sites %in% chem_sites]),
                          q_only = length(q_sites),
                          chem_only = length(chem_sites),
                          chem_or_q = length(unique(c(q_sites, chem_sites))))
    site_numbers <- rbind(site_numbers, this_domain)
}

write_csv(site_numbers, 'site_count.csv')

# tally sites from data_acquisition/data ####
all_fis <- list.files('data/', recursive = T, full.names = T)
all_derived_files <- grep('derived', all_fis, value = T)

q_files <- grep('discharge', all_derived_files, value = T)
q_files <- grep('feather', q_files, value = T)

chem_files <- grep('stream_chemistry', all_derived_files, value = T)
chem_files <- grep('feather', chem_files, value = T)

all_q_sites <- c(unique(str_split_fixed(q_files, '/', n = Inf)[,7]), 'como')
all_chem_sites <- c(unique(str_split_fixed(chem_files, '/', n = Inf)[,7]), 'como')

site_numbers <- tibble(domains = c('all'),
                       q_and_chem = length(all_q_sites[all_q_sites %in% all_chem_sites]),
                       q_only = length(all_q_sites),
                       chem_only = length(all_chem_sites),
                       chem_or_q = length(unique(c(all_q_sites, all_chem_sites))))

doms <- unique(str_split_fixed(q_files, '/', n = Inf)[,4])
for(i in 1:length(doms)){
    dom_chem <- grep(paste0('/', doms[i], '/'), chem_files, value = T)

    chem_sites <- str_split_fixed(dom_chem, '/', n = Inf)[,7]

    if(doms[i] == 'niwot'){
        chem_sites <- c(unique(chem_sites), 'como.feather')
    } else{
        chem_sites <- unique(chem_sites)
    }

    dom_q <- grep(paste0('/', doms[i], '/'), q_files, value = T)

    q_sites <- str_split_fixed(dom_q, '/', n = Inf)[,7]

    if(doms[i] == 'niwot'){
        q_sites <- c(unique(q_sites), 'como.feather')
    } else{
        q_sites <- unique(q_sites)
    }

    this_domain <- tibble(domains = doms[i],
                          q_and_chem = length(q_sites[q_sites %in% chem_sites]),
                          q_only = length(q_sites),
                          chem_only = length(chem_sites),
                          chem_or_q = length(unique(c(q_sites, chem_sites))))

    site_numbers <- rbind(site_numbers, this_domain)
}

write_csv(site_numbers, 'site_count.csv')

# tally sites from figshare via macrosheds r package ####
all_chem <- ms_load_product('macrosheds_figshare_v1/macrosheds_files_by_domain/',
                prodname = 'stream_chemistry',
                warn = F)

all_chem_sites <- c(unique(all_chem$site_code), 'como')

all_q <- ms_load_product('data/ms_test',
                         prodname = 'discharge',
                         warn = F)

all_q_sites <- c(unique(all_q$site_code), 'como')



length(all_q_sites[all_q_sites %in% all_chem_sites])

site_numbers <- tibble(domains = c('all'),
       q_and_chem = length(all_q_sites[all_q_sites %in% all_chem_sites]),
       q_only = length(all_q_sites),
       chem_only = length(all_chem_sites),
       chem_or_q = length(unique(c(all_q_sites, all_chem_sites))))

site_data <- ms_download_site_data()
doms <- unique(site_data$domain)
for(i in 1:length(doms)){
    dom_chem <- ms_load_product('data/ms_test',
                                prodname = 'stream_chemistry',
                                domains = doms[i],
                                warn = F)

    if(doms[i] == 'niwot'){
        chem_sites <- c(unique(dom_chem$site_code), 'como')
    } else{
        chem_sites <- unique(dom_chem$site_code)
    }

    q <- ms_load_product('data/ms_test',
                         prodname = 'discharge',
                         domains = doms[i],
                         warn = F)

    if(doms[i] == 'niwot'){
        q_sites <- c(unique(q$site_code), 'como')
    } else{
        q_sites <- unique(q$site_code)
    }


        this_domain <- tibble(domains = doms[i],
                              q_and_chem = length(q_sites[q_sites %in% chem_sites]),
                              q_only = length(q_sites),
                              chem_only = length(chem_sites),
                              chem_or_q = length(unique(c(q_sites, chem_sites))))

    site_numbers <- rbind(site_numbers, this_domain)
}

write_csv(site_numbers, 'site_count.csv')

look <- all_chem %>%
    filter(ms_interp == 0) %>%
    group_by(site_code, var) %>%
    summarise(n = n())

all_chem %>%
    filter(site_code == 'MC06',
           var == 'GN_NO3_NO2_N',
           year(datetime) == 2016) %>%
    filter(ms_interp == 0) %>%
    ggplot(aes(datetime, val)) +
    geom_point()

look <- all_chem %>%
    filter(ms_interp == 1 & ms_status == 1)


look <- all_chem %>%
    filter(ms_interp == 1)

look <- all_chem %>%
    filter(ms_status == 1)


#which sites are in each set ####

vsn = 1

#figshare set
all_fis <- list.files(glue('macrosheds_figshare_v{vsn}/macrosheds_timeseries_data/'), recursive = T, full.names = T)
q_files <- grep('discharge', all_fis, value = T)
q_files <- grep('feather', q_files, value = T)
chem_files <- grep('stream_chemistry', all_fis, value = T)
chem_files <- grep('feather', chem_files, value = T)
all_q_sites0 <- unique(str_split_fixed(q_files, '/', n = Inf)[,7])
all_chem_sites0 <- unique(str_split_fixed(chem_files, '/', n = Inf)[,7])

#acq/data
all_fis <- list.files('data/', recursive = T, full.names = T)
all_derived_files <- grep('derived', all_fis, value = T)
q_files <- grep('discharge', all_derived_files, value = T)
q_files <- grep('feather', q_files, value = T)
chem_files <- grep('stream_chemistry', all_derived_files, value = T)
chem_files <- grep('feather', chem_files, value = T)
all_q_sites <- c(unique(str_split_fixed(q_files, '/', n = Inf)[,7]), 'como')
all_chem_sites <- c(unique(str_split_fixed(chem_files, '/', n = Inf)[,7]), 'como')

setdiff(all_q_sites0, all_q_sites)
setdiff(all_q_sites, all_q_sites0)

