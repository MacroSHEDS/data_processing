all_daily <- tibble()

for(i in 1:length(sites)) {
    
        discharge <- try(dataRetrieval::readNWISdv(sites[i], '00060') %>%
            mutate(datetime = ymd_hms(paste0(Date, ' ', '12:00:00'), tz = 'UTC')) )
        
        if(inherits(discharge, 'try-error')){
            next
        }

        all_daily <- rbind(all_daily, discharge)
}


daily <- all_daily %>%
    group_by(Date, site_no) %>%
    summarise(n = n()) %>%
    mutate(year = year(Date)) %>%
    group_by(site_no) %>%
    summarise(daily_n = n())

all_subdaily <- tibble()
for(i in 1:length(sites)) {
    
    discharge <- try(dataRetrieval::readNWISuv(sites[i], '00060') %>%
        rename(datetime = dateTime,
               val = X_00060_00000))
    
    if(inherits(discharge, 'try-error')){
        next
    }
    
    all_subdaily <- rbind(all_subdaily, discharge)
}


subdaily <- all_subdaily %>%
    mutate(date = date(dateTime)) %>%
    group_by(date, site_no) %>%
    summarise(n = n()) %>%
    mutate(year = year(date)) %>%
    group_by(site_no) %>%
    summarise(subdaily_n = n())

site_codes <- tibble(site_no = sites,
                     site_code  =names(sites))

look=full_join(subdaily, daily) %>%
    # full_join(., site_codes) %>%
    mutate(pick_daily = ifelse(daily_n > subdaily_n, T, F))
