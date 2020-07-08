#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_1 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')

}

#precip: STATUS=READY
#. handle_errors
process_0_13 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component, '.csv'),
        cacheOK=FALSE, method='curl')
}

#stream_precip_chemistry: STATUS=READY
#. handle_errors
process_0_208 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)

    if(set_details$component == 'Analytical Methods'){
        fext <- '.pdf'
    } else {
        l1 <- all(grepl('precip', c(set_details$component,  prodname_ms)))
        l2 <- all(grepl('stream', c(set_details$component,  prodname_ms)))

        if(! sum(l1, l2)){
            loginfo('Skipping redundant download', logger=logger_module)
            return(generate_blacklist_indicator())
        }

        fext <- '.csv'
    }

    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component, fext),
        cacheOK=FALSE, method='curl')
}

#rain_gauge_locations: STATUS=PENDING (not sure why this cannot be accessed, may it does not exist. also repeated with 100)
#. handle_errors
process_0_5482 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')
}

#stream_gauge_locations: STATUS=PENDING (not sure why this cannot be accessed, may it does not exist)
#. handle_errors
process_0_3239 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')
}

#rain_gauge_locations: STATUS=READY
#. handle_errors
process_0_100 <- function(set_details, network, domain){
    raw_data_dest = glue('{wd}/data/{n}/{d}/raw/{p}/{s}',
        wd=getwd(), n=network, d=domain, p=set_details$prodname_ms,
        s=set_details$site_name)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE)
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component),
        cacheOK=FALSE, method='curl')
}

#ws_boundary: STATUS=READY 
#. handle_errors 
process_0_94 <- function(set_details, network, domain) {
    raw_data_dest = glue('{wd}/data/{n}/{d}/geospatial/{p}',
                         wd=getwd(), n=network, d=domain, p=set_details$prodname_ms)
    dir.create(raw_data_dest, showWarnings=FALSE, recursive=TRUE) 
    
    dest=glue(raw_data_dest, '/', set_details$component)
    download.file(url=set_details$url,
                  destfile=dest,
                  cacheOK=FALSE, method='curl') 
    
    unzip(zipfile=dest, exdir=raw_data_dest, overwrite =TRUE)
}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_1 <- function(network, domain, prodname_ms, site_name,
    component){
    # site_name=site_name; component=in_comp

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
        n=network, d=domain, p=prodname_ms, s=site_name, c=component)

    d = sw(read_csv(rawfile, progress=FALSE,
        col_types=readr::cols_only(
            DATETIME='c', #can't parse 24:00
            WS='c',
            Discharge_ls='d'))) %>%
            # Flag='c'))) %>% #all flags are acceptable for this product
        rename(site_name = WS,
            datetime = DATETIME,
            discharge = Discharge_ls) %>%
        mutate(
            datetime = with_tz(force_tz(as.POSIXct(datetime), 'US/Eastern'), 'UTC'),
            # datetime = with_tz(as_datetime(datetime, 'US/Eastern'), 'UTC'),
            site_name = paste0('w', site_name),
            ms_status = 0) %>%
        group_by(datetime, site_name) %>%
        summarize(
            discharge = mean(discharge, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup()

    return(d)
}

#precip: STATUS=READY
#. handle_errors
process_1_13 <- function(network, domain, prodname_ms, site_name,
    component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
        n=network, d=domain, p=prodname_ms, s=site_name, c=component)

    d = sw(read_csv(rawfile, progress=FALSE,
        col_types=readr::cols_only(
            DATE='c', #can't parse 24:00
            rainGage='c',
            Precip='d'))) %>%
        # Flag='c'))) %>% #all flags are acceptable for this product
        rename(datetime = DATE,
            site_name = rainGage,
            precip = Precip) %>%
        mutate(
            datetime = with_tz(force_tz(as.POSIXct(datetime), 'US/Eastern'), 'UTC'),
            ms_status = 0) %>%
        group_by(datetime, site_name) %>%
        summarize(
            precip = mean(precip, na.rm=TRUE),
            ms_status = numeric_any(ms_status)) %>%
        ungroup()
}

#stream_chemistry; precip_chemistry: STATUS=READY
#. handle_errors
process_1_208 <- function(network, domain, prodname_ms, site_name,
    component){

    #this product includes precip chem and stream chem, which have the same
    #data structure. so for now, the component argument governs which one is
    #processed herein. once we work out rain interpolation, we might have to
    #add a conditional and some divergent processing here.

    #this product also includes its own precip (mm) and discharge (L/s), but we
    #can ignore those in favor of the more complete products 1 and 13

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
        n=network, d=domain, p=prodname_ms, s=site_name, c=component)

    d <- sw(read_csv(rawfile, col_types=readr::cols_only(
            site='c', date='c', timeEST='c', pH='n', DIC='n', spCond='n',
            temp='n', ANC960='n', ANCMet='n', #precipCatch='n',# notes='c',
            Ca='n', Mg='n', K='n', Na='n', TMAl='n', OMAl='n',
            Al_ICP='n', NH4='n', SO4='n', NO3='n', Cl='n', PO4='n',
            DOC='n', TDN='n', DON='n', SiO2='n', Mn='n', Fe='n',
            `F`='n', cationCharge='n', fieldCode='c', anionCharge='n',
            theoryCond='n', ionError='n', ionBalance='n'))) %>%
        rename(site_name = site) %>%
        mutate(site_name = ifelse(grepl('W[0-9]', site_name), #harmonize sitename conventions
            tolower(site_name), site_name)) %>%
        mutate(
            timeEST = ifelse(is.na(timeEST), '12:00', timeEST),
            datetime = lubridate::ymd_hm(paste(date, timeEST), tz = 'UTC'),
            ms_status = ifelse(is.na(fieldCode), FALSE, TRUE), #see summarize
            DIC = convert_unit(DIC, 'uM', 'mM'),
            NH4_N = convert_molecule(NH4, 'NH4', 'N'),
            NO3_N = convert_molecule(NO3, 'NO3', 'N'),
            PO4_P = convert_molecule(PO4, 'PO4', 'P')) %>%
        select(-date, -timeEST, -PO4, -NH4, -NO3, -fieldCode) %>%
        group_by(datetime, site_name) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status)) %>%
        select(-ms_status, everything())

    d[is.na(d)] = NA #replaces NaNs. is there a clean, pipey way to do this?

    return(d)
}

#ws_boundary: STATUS=READY 
#. handle_errors 
process_1_94 <- function(network, domain, prodname_ms, site_name,
                         component) {
    rawfile = glue('data/{n}/{d}/geospatial/{p}',
                   n=network, d=domain, p=prodname_ms) 
    
    ws <- sf::st_read(rawfile) %>%
        filter(!is.na(WS)) 
    
    for(i in 1:nrow(ws)) {
            new_ws <- ws[i,]
            
            site <- as_tibble(ws[i,]) %>%
                select(WS) %>%
                as.character()
            
            ws_dir <- glue(rawfile, "/", "ws", site)
            
            dir.create(ws_dir, recursive = TRUE)
            
            sf::st_write(new_ws, glue(ws_dir, "/", "ws", site, ".shp"), 
                         delete_dsn=TRUE)
        } 
}

#derive kernels####

#precip: STATUS=READY
#. handle_errors
process_2_13 <- function(network, domain, prodname_ms){

    #this logic is temporary, and just gets precip into the format that works
    #with the portal. but once interp is working, we'll perform that here
    #instead

    mfiles <- list_munged_files(network = network,
                                domain = domain,
                                prodname_ms = prodname_ms)

    combined <- tibble()
    for(f in mfiles){
        d = read_feather(f)
        combined <- bind_rows(combined, d)
    }

    #this chunk will be replaced by create_portal_link when interp is ready
    site_file <- glue('data/{n}/{d}/derived/{p}/precip.feather',
         n = network,
         d = domain,
         p = prodname_ms)
    write_feather(combined, site_file)
    portal_site_file <- glue('../portal/data/{d}/precip.feather', d = domain)
    unlink(portal_site_file)
    invisible(sw(file.link(to = portal_site_file, from = site_file)))

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_2_208 <- function(network, domain, prodname_ms){

    #this logic is temporary, and just gets pchem into the format that works
    #with the portal. but once interp is working, we'll perform that here
    #instead

    mfiles <- list_munged_files(network = network,
                                domain = domain,
                                prodname_ms = prodname_ms)

    combined <- tibble()
    for(f in mfiles){
        d = read_feather(f)
        combined <- bind_rows(combined, d)
    }

    #this chunk will be replaced by create_portal_link when interp is ready
    site_file <- glue('data/{n}/{d}/derived/{p}/precip.feather',
                      n = network,
                      d = domain,
                      p = prodname_ms)
    write_feather(combined, site_file)
    portal_site_file <- glue('../portal/data/{d}/precip.feather', d = domain)
    unlink(portal_site_file)
    invisible(sw(file.link(to = portal_site_file, from = site_file)))

    return()
}

#stream_flux: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms){

    dt_round_interv <- 'hours' #passed to lubridate::round_date

    chemfiles <- list_munged_files(network = network,
                                   domain = domain,
                                   prodname_ms = 'stream_chemistry__208')
    qfiles <- list_munged_files(network = network,
                                domain = domain,
                                prodname_ms = 'discharge__1')

    flux_vars <- ms_vars %>%
        filter(flux_convertible == 1) %>%
        pull(variable_code)

    flux_sites <- generics::intersect(
        fname_from_fpath(qfiles, include_fext = FALSE),
        fname_from_fpath(chemfiles, include_fext = FALSE))

    for(s in flux_sites){

        chem <- read_feather(glue(
                    'data/{n}/{d}/munged/stream_chemistry__208/{s}.feather',
                        n = network,
                        d = domain,
                        s = s)) %>%
            select(one_of(flux_vars), 'datetime', 'ms_status') %>%
            mutate(datetime = lubridate::round_date(datetime, dt_round_interv)) %>%
            group_by(datetime) %>%
            summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
            ungroup()

        daterange <- range(chem$datetime)
        fulldt <- tibble(datetime = seq(daterange[1], daterange[2],
                                        by=dt_round_interv))

        discharge <- read_feather(glue(
                        'data/{n}/{d}/munged/discharge__1/{s}.feather',
                            n = network,
                            d = domain,
                            s = s)) %>%
            select(-site_name) %>%
            filter(datetime >= daterange[1], datetime <= daterange[2]) %>%
            mutate(datetime = lubridate::round_date(datetime, dt_round_interv)) %>%
            group_by(datetime) %>%
            summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
            ungroup()

        flux <- chem %>%
            full_join(discharge,
                      by = 'datetime') %>%
            mutate(ms_status = numeric_any(c(ms_status.x, ms_status.y))) %>%
            select(-ms_status.x, -ms_status.y) %>%
            full_join(fulldt,
                      by='datetime') %>%
            arrange(datetime) %>%
            select_if(~(! all(is.na(.)))) %>%
            mutate_at(vars(-datetime, -ms_status),
                      imputeTS::na_interpolation,
                      maxgap = 30) %>%
            mutate_at(vars(-datetime, -discharge, -ms_status),
                      ~(. * discharge)) %>%
            mutate(site_name = s)

        write_munged_file(d = flux,
                          network = network,
                          domain = domain,
                          prodname_ms = prodname_ms,
                          site_name = s)

        create_portal_link(network = network,
                           domain = domain,
                           prodname_ms = prodname_ms,
                           site_name = s)
    }

    return()
}

#precip_flux: STATUS=PENDING
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms){

    #precip interp should be a munge step?
        #so fluc calc can always be performed after
        #precip is localized to stream sites

    #or devise some other way for the correct order of events to be ensured
    #without requiring additional thought on the part of the developer

}
