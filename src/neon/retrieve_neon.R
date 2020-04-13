library(httr)
library(jsonlite)
library(tidyr)
library(plyr)
library(data.table)
library(dtplyr)
library(tidyverse)
# library(lubridate)
library(feather)
library(glue)
library(logging)
library(emayili)

#neon data product codes are of this form: DPx.yyyyy.zzz,
#where x is data level (1=qaqc'd), y is product id, z is revision

setwd('/home/mike/git/macrosheds/')
source('data_acquisition/src/helpers.R')
source('data_acquisition/src/neon/neon_helpers.R')

neonprods = readr::read_csv('data_acquisition/data/neon/neon_products.csv')
neonprods = filter(neonprods, status == 'pending')

conf = jsonlite::fromJSON('data_acquisition/config.json')
held_data = try(jsonlite::fromJSON(
    readr::read_file('data_acquisition/data/neon/held_data.json')), silent=TRUE)
if('try-error' %in% class(held_data)) held_data = list()

logging::basicConfig()
logging::addHandler(logging::writeToFile, logger='neon',
    file='data_acquisition/logs/neon.log')
# logReset()

# d_=d; data_inds_=data_inds; site_=site
# out_sub_ = out_sub; replacements = c('specificCond'='specificConductance',
#         'dissolvedOxygenSat'='dissolvedOxygenSaturation')
# d=deets
process_DP1.20093.001_api = function(d, loginfo){

    data1_ind = intersect(grep("expanded", d$data$files$name),
        grep("fieldSuperParent", d$data$files$name))
    data2_ind = intersect(grep("expanded", d$data$files$name),
        grep("externalLabData", d$data$files$name))
    data3_ind = intersect(grep("expanded", d$data$files$name),
        grep("domainLabData", d$data$files$name))

    data1 = tryCatch({
        read.delim(d$data$files$url[data1_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data2 = tryCatch({
        read.delim(d$data$files$url[data2_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })
    data3 = tryCatch({
        read.delim(d$data$files$url[data3_ind], sep=",",
            stringsAsFactors=FALSE)
    }, error=function(e){
        data.frame()
    })

    if(nrow(data1)){
        data1 = select(data1, siteID, collectDate, dissolvedOxygen,
            dissolvedOxygenSaturation, specificConductance, waterTemp,
            maxDepth)
    }
    if(nrow(data2)){
        data2 = select(data2, siteID, collectDate, pH, externalConductance,
            externalANC, starts_with('water'), starts_with('total'),
            starts_with('dissolved'), uvAbsorbance250, uvAbsorbance284,
            shipmentWarmQF, externalLabDataQF)
    }
    if(nrow(data3)){
        data3 = select(data3, siteID, collectDate, starts_with('alk'),
            starts_with('anc'))
    }

    out_sub = plyr::join_all(list(data1, data2, data3), type='full') %>%
        group_by(collectDate) %>%
        summarise_each(list(~ if(is.numeric(.)){
            mean(., na.rm = TRUE)
        } else {
            first(.)
        })) %>%
        ungroup() %>%
        mutate(datetime=as.POSIXct(collectDate, tz='UTC',
            format='%Y-%m-%dT%H:%MZ')) %>%
        select(-collectDate)

    return(out_sub)
}
process_DP1.20093.001 = function(d, loginfo){

    data_pile = neonUtilities::loadByProduct(loginfo$prodcode, site=loginfo$site,
        startdate=loginfo$date, enddate=loginfo$date, package='basic',
        check.size=FALSE)

    datasets = names(data_pile)

    if('swc_externalLabDataByAnalyte' %in% datasets){
        data_pile$swc_externalLabDataByAnalyte %>%
            filter(qa filtering here) %>%
            convert_units_here() %>%
            select(collectDate, analyte, analyteConcentration, analyteUnits,
                shipmentWarmQF, externalLabDataQF, sampleCondition) %>%
            spread()
    }
    if('swc_externalLabData' %in% datasets){
        data_pile$swc_externalLabData %>%
            filter(qa filtering here) %>%
            convert_units_here() %>%
            select(collectDate, pH, externalConductance,
                externalANC, starts_with('water'), starts_with('total'),
                starts_with('dissolved'), uvAbsorbance250, uvAbsorbance284,
                shipmentWarmQF, externalLabDataQF)
    }
    # write_lines(data_pile$readme_20093$X1, '/tmp/chili.txt')

    identify which dsets is/are present and mget them into the following list
    out_sub = plyr::join_all(list(data1, data2, data3), type='full') %>%
        group_by(collectDate) %>%
        summarise_each(list(~ if(is.numeric(.)){
            mean(., na.rm = TRUE)
        } else {
            first(.)
        })) %>%
        ungroup() %>%
        mutate(datetime=as.POSIXct(collectDate, tz='UTC',
            format='%Y-%m-%dT%H:%MZ')) %>%
        select(-collectDate)

    return(out_sub)
}
    # neonUtilities::getAvg(loginfo$prodcode)
process_DP1.20033.001 = function(d, loginfo){

    data_pile = neonUtilities::loadByProduct(loginfo$prodcode, site=loginfo$site,
        startdate=loginfo$date, enddate=loginfo$date, package='basic',
        check.size=FALSE)

    # out_sub = resolve_neon_naming_conflicts(data_pile$NSW_15_minute,
    #     replacements=NULL, from_api=FALSE, loginfo_=loginfo)

        # filter(qa filtering here) %>%
        # convert_units_here() %>%
    out_sub = select(data_pile$NSW_15_minute,
            startDateTime, surfWaterNitrateMean, finalQF)

    return(out_sub)
}
process_DP1.20042.001 = function(d, loginfo){

    data_pile = neonUtilities::loadByProduct(loginfo$prodcode, site=loginfo$site,
        startdate=loginfo$date, enddate=loginfo$date, package='basic',
        check.size=FALSE)

    # datasets = names(data_pile)
    names(data_pile$NSW_15_minute)
    View(data_pile$NSW_15_minute)

    # out_sub = resolve_neon_naming_conflicts(data_pile$NSW_15_minute,
    #     replacements=NULL, from_api=FALSE, loginfo_=loginfo)

        # filter(qa filtering here) %>%
        # convert_units_here() %>%
    out_sub = select(data_pile$NSW_15_minute,
            startDateTime, surfWaterNitrateMean, finalQF)
    # write_lines(data_pile$readme_20033$X1, '/tmp/nitrate.txt')

    return(out_sub)
}
process_DP1.20288.001 = function(d, loginfo){

    data_inds = intersect(grep("expanded", d$data$files$name),
        grep("instantaneous", d$data$files$name))

    site_with_suffixes = determine_upstream_downstream(d, data_inds, loginfo)
    if(is_ms_err(site_with_suffixes)) return(site_with_suffixes)

    #process upstream and downstream sites independently
    for(j in 1:length(data_inds)){

        site_with_suffix = site_with_suffixes[j]

        #download data
        out_sub = read.delim(d$data$files$url[data_inds[j]], sep=",",
            stringsAsFactors=FALSE)

        out_sub = resolve_neon_naming_conflicts(out_sub, loginfo_=loginfo,
            replacements=c('specificCond'='specificConductance',
                'dissolvedOxygenSat'='dissolvedOxygenSaturation'))
        if(is_ms_err(out_sub)) return(out_sub)

        out_sub = mutate(out_sub,
            datetime=as.POSIXct(startDateTime,
                tz='UTC', format='%Y-%m-%dT%H:%M:%SZ'),
            site=site_with_suffix) %>%
            select(-startDateTime)
    }

    return(out_sub)
}
# sets = site_sets; held=held_data; i=nrow(sets)
get_neon_data = function(sets, prodcode, silent=TRUE){

    processing_func = get(paste0('process_', prodcode))

    out = tibble()
    successes = c()
    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        url = sets[i, 1]
        site = sets[i, 2]
        date = sets[i, 3]

        loginfo = list(site=site, date=date, prodcode=prodcode, url=url)

        msg = glue('Processing site ',
            '{site} ({prod}, {date}).', site=site, prod=prodcode, date=date)
        logging::loginfo(msg, logger='neon.module')

        deets = download_sitemonth_details(url)
        if(is_ms_err(deets) || 'error' %in% names(deets)){
            assign('email_err_msg', TRUE, pos=.GlobalEnv)
            next
        }

        out_sub = do.call(processing_func,
            args=list(d=deets, loginfo=loginfo))
        if(is_ms_err(out_sub)){
            assign('email_err_msg', TRUE, pos=.GlobalEnv)
            next
        }

        out = bind_rows(out, out_sub)
        successes = append(successes, date)
        update_held_data(new_dates=successes, loginfo)
    }

    return(out)
}

# i=2; j=1; sets=site_sets
email_err_msg = FALSE
# for(i in 1:nrow(neonprods)){
for(i in 3){

    outer_loop_err = FALSE

    #get available datasets for this data product
    prodcode = neonprods$prodID[i]
    prodID = strsplit(prodcode, '\\.')[[1]][2]
    if(! prodcode %in% names(held_data)) held_data[[prodcode]] = list()

    tryCatch({
        req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
            prodcode))
        txt = httr::content(req, as="text")
        neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
    }, error=function(e){
        logging::logerror(e, logger='neon.module')
        email_err_msg <<- outer_loop_err <<- TRUE
    })
    if(outer_loop_err) next

    #get available urls, sites, and dates
    urls = unlist(neondata$data$siteCodes$availableDataUrls)
    avail_sets = stringr::str_match(urls, '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})')

    #retrieve data by site; log acquisitions (and revisions, once neon actually has them)
    avail_sites = unique(avail_sets[, 2])
    for(j in 1:length(avail_sites)){

        curr_site = avail_sites[j]
        site_sets = avail_sets[avail_sets[, 2] == curr_site, ]

        if(! curr_site %in% names(held_data[[prodcode]])){
            held_data[[prodcode]][[curr_site]] = vector(mode='character')
        }

        #filter already held sitemonths from site_sets ####
        # site_sets = site_sets[1:1, , drop=FALSE]

        tryCatch({
            site_dset = get_neon_data(site_sets, prodcode)
        }, error=function(e){
            logging::logerror(e, logger='neon.module')
            email_err_msg <<- outer_loop_err <<- TRUE
        })
        if(outer_loop_err) next

        write_feather(site_dset,
            glue('data_acquisition/data/neon/raw/{p}_{id}_{site}.feather',
            p=neonprods$prod[i], id=prodID, site=curr_site))

    }

}

if(email_err_msg){
    email_err('neon data acquisition error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}

# zip('data/neon.zip', list.files('data/neon', recursive=TRUE, full.names=TRUE))
# out = drive_upload("data/neon.zip",
#     as_id('https://drive.google.com/drive/folders/0ABfF-JkuRvL5Uk9PVA'))
