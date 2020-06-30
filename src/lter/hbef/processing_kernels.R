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
    download.file(url=set_details$url,
        destfile=glue(raw_data_dest, '/', set_details$component, '.csv'),
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

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_1 <- function(network, domain, prodname_ms, site_name,
    component){
    # site_name=site; component=in_comp

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

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
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

#stream_precip_chemistry: STATUS=READY
#. handle_errors
process_1_208 <- function(network, domain, prodname_ms, site_name,
    component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
        n=network, d=domain, p=prodname_ms, s=site_name, c=component)

    if(component == "precipitation_chemistry.csv") {

    d <- sw(read_csv(rawfile, col_types=readr::cols_only(
            site='c', date='D', timeEST='t', pH='n', DIC='n', spCond='n',
            temp='n', ANC960='n', ANCMet='n', precipCatch='n',# notes='c',
            Ca='n', Mg='n', K='n', Na='n', TMAl='n', OMAl='n',
            Al_ICP='n', NH4='n', SO4='n', NO3='n', Cl='n', PO4='n',
            DOC='n', TDN='n', DON='n', SiO2='n', Mn='n', Fe='n',
            `F`='n', cationCharge='n', fieldCode='c', anionCharge='n',
            theoryCond='n', ionError='n', ionBalance='n'))) %>%
        mutate(
            timeEST = ifelse(is.na(timeEST), '12:00:00', timeEST),
            datetime = ymd_hms(paste(date, timeEST), tz = 'UTC'),
            ms_status = ifelse(is.na(fieldCode), FALSE, TRUE)) %>% #see summarize
        # can't find metadata on DIC, spCond, ANC960, DON, cationCharge, anionCharge, theoryCond, ionBalance
        #spCond is in ms/m but variables tables says S/cm which seems less common
        rename(site_name = site) %>%
        #convert to N, P
        select(-date, -timeEST, -PO4, -NH4, -NO3, -fieldCode) %>%
        group_by(datetime, site_name) %>%
        summarize_all(~ if(is.numeric(.)) mean(., na.rm=TRUE) else any(.)) %>%
        # summarize_if(is.numeric, mean, na.rm=TRUE) %>%
        ungroup() %>%
        mutate(ms_status = as.numeric(ms_status))
    }
    if(component == "stream_chemistry.csv") {
            d = sw(read_csv(rawfile, col_types=readr::cols_only(
                site='c',
                date='D',
                timeEST='t',
                pH = "n",
                DIC = "n",
                spCond = "n",
                temp = "n",
                ANC960 = "n",
                ANCMet = "n",
                notes = "c",
                Ca = "n",
                Mh = "n",
                K = "n",
                Na = "n",
                TMAl = "n",
                OMAl = "n",
                Al_ICP = "n",
                NH4 = "n",
                SO4 = "n",
                NO3 = "n",
                Cl = "n",
                PO4 = "n",
                DOC = "n",
                TDN = "n",
                DON = "n",
                SiO2 = "n",
                Mn = "n",
                Fe = "n",
                F = "n",
                cationCharge = "n",
                feildCode = "c",
                anionCharge = "n",
                theoryCond = "n",
                ionError = "n",
                ionBalance = "n"))) %>%
                mutate(datetime = ifelse(is.na(timeEST), paste(date, "00:00:00", " "),
                    paste(date, timeEST, " "))) %>%
                mutate(datetime = force_tz(ymd_hms(datetime), 'US/Eastern'),
                    site = ifelse(site %in% c("W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9"),
                        tolower(site), site)) %>%
                # can't find metadata on DIC, spCond, ANC960, DON, cationCharge, anionCharge, theoryCond, ionBalance
                #spCond is in ms/m but variables tables says S/cm which seems less common
                rename(PO4_P = PO4,
                    site_name = site) %>%
                select(-date, timeEST, -ionError) %>%
                # notes of 1, 2 bad. (not posative these are correct)
                group_by(datetime, site_name) %>%
                summarize(
                    pH = mean(pH, na.rm = TRUE),
                    DIC = mean(DIC, na.rm = TRUE),
                    spCond = mean(spCond, na.rm = TRUE),
                    temp = mean(temp, na.rm = TRUE),
                    ANC960 = mean(ANC960, na.rm = T),
                    ANCMet = mean(ANCMet, na.rm = TRUE),
                    Ca = mean(Ca, na.rm = TRUE),
                    K = mean(K, na.rm = TRUE),
                    Na = mean(Na, na.rm = TRUE),
                    TMAl = mean(TMAl, na.rm = TRUE),
                    OMAl = mean(OMAl, na.rm = TRUE),
                    Al_ICP = mean(Al_ICP, na.rm = TRUE),
                    NH4 = mean(NH4, na.rm = TRUE),
                    SO4 = mean(SO4, na.rm = TRUE),
                    NO3 = mean(NO3, na.rm = TRUE),
                    Cl = mean(Cl, na.rm = TRUE),
                    PO4_P = mean(PO4_P, na.rm = TRUE),
                    DOC = mean(DOC, na.rm = TRUE),
                    TDN = mean(TDN, na.rm = TRUE),
                    DON = mean(DON, na.rm = TRUE),
                    SiO2 = mean(SiO2, na.rm = TRUE),
                    Mn = mean(Mn, na.rm = TRUE),
                    Fe = mean(Fe, na.rm = TRUE),
                    cationCharge = mean(cationCharge, na.rm = TRUE),
                    anionCharge = mean(anionCharge, na.rm = TRUE),
                    theoryCond = mean(theoryCond, na.rm = TRUE),
                    ionBalance = mean(ionBalance, na.rm = TRUE)) %>%
                ungroup()
    }

    return(d)
}

process_1_208 <- function(network, domain, prodname_ms, site_name,
    component){

    rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}',
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

#derive kernels####

