#this code is useful after cast_and_reflag (not exactly as-is)

    zz %>% 
        select(-`pCO2 µatm`) %>% 
        mutate(across(-c(Date, SiteID), as.numeric)) %>% 
        pivot_longer(cols = -c(Date, SiteID), names_to = 'var', values_to = 'val') %>% 
        ggplot()+
        geom_line(aes(Date, val, color=var))+
        facet_wrap(.~SiteID, scales = 'free')+
        theme(legend.position="none")
    d %>% 
        ggplot()+
        geom_line(aes(datetime, val, color=var))+
        facet_wrap(.~site_code, scales = 'free')+
        theme(legend.position="none")
    
    zz %>% 
        filter(SiteID == 54) %>% 
        # filter(Date > as.Date('2019-01-01'), Date < as.Date('2019-05-01')) %>% 
        select(-`pCO2 µatm`) %>% 
        select(Date, SiteID, `Ag µg/l`) %>% 
        mutate(across(-c(Date, SiteID), as.numeric)) %>% 
        pivot_longer(cols = -c(Date, SiteID), names_to = 'var', values_to = 'val') %>% 
        ggplot()+
        geom_line(aes(Date, val, color=var))+
        facet_wrap(.~SiteID, scales = 'free')+
        theme(legend.position="none")
    d %>% 
        mutate(val = drop_errors(val)) %>% 
        filter(site_code == 'Site54') %>% 
        mutate(val = val * 1000) %>% 
        filter(var=='GN_Ag') %>% 
        # filter(datetime > as.Date('2019-01-01'), datetime < as.Date('2019-05-01'), var == 'GN_Ag')
        ggplot()+
        geom_line(aes(datetime, val, color=var))+
        facet_wrap(.~site_code, scales = 'free')+
        theme(legend.position="none")
