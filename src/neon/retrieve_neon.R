library(httr)
library(jsonlite)
# library(stringr)
library(tidyr)
library(data.table)
library(dtplyr)
# library(plyr)
library(tidyverse)
# library(dplyr, warn.conflicts = FALSE)
# library(lubridate)
library(feather)
library(glue)
library(logging)

#neon data product codes are of this form: DPx.yyyyy.zzz,
#where x is data level (1=qaqc'd), y is product id, z is revision

setwd('/home/mike/git/macrosheds/')
logging::basicConfig()
# logReset()
logging::addHandler(logging::writeToFile, logger='neon',
    file='data_acquisition/logs/neon.log')

#store these elsewhere
# d_=d; data_inds_=data_inds; site_=site
determine_upstream_downstream = function(d_, data_inds_, site_){

    #determine which dataset is upstream/downstream if necessary
    updown_suffixes = c('-up', '-down')
    if(length(data_inds_) == 2){
        position = str_split(d_$data$files$name[data_inds_[1]], '\\.')[[1]][7]
        updown_order = if(position == '101') 1:2 else 2:1
    } else if(length(data_inds_) == 1){
        updown_order = 1
    } else {
        msg = glue('Problem with data file number (up/downstream) for site',
            '{site} ({prod}, {month})', site=site_, prod=prodcode, month=date)
        logwarn(msg, logger='neon.module')
        next
    }

    site_with_suffixes = paste0(site_, updown_suffixes[updown_order])

    return(site_with_suffixes)
}
# out_sub_ = out_sub
resolve_neon_naming_conflicts = function(out_sub_, replacements){

    #replacements is a named vector. name=find, value=replace;
    #failed match does nothing

    out_cols = colnames(out_sub_)

    #get list of variables included
    varind = grep('SciRvw', out_cols)
    rgx = str_match(out_cols[varind], '^(\\w*)(?:FinalQFSciRvw|SciRvwQF)$')
    # varlist = flagprefixlist = rgx[,2]
    varlist = rgx[,2]

    #harmonize redundant variable names
    for(i in 1:length(replacements)){
        r = replacements[i]
        varlist = replace(varlist, which(varlist == names(r)), r)
    }

    if('startDate' %in% out_cols){
        colnames(out_sub_) = replace(out_cols, which(out_cols == 'startDate'),
            'startDateTime')
    } else if('endDate' %in% out_cols){
        colnames(out_sub_) = replace(out_cols, which(out_cols == 'endDate'),
            'startDateTime') #not a mistake
    } else {
        # LOG HERE####
        # write(paste('Datetime column not found for:', current_var,
        #     site_with_suffix, date),
        #     '../../logs_etc/NEON/NEON_ingest.log', append=TRUE)
        next
    }

    #subset relevant columns
    flagcols = grepl('.+?FinalQF(?!SciRvw)', out_cols,
        perl=TRUE)
    datacols = ! grepl('QF', out_cols, perl=TRUE)
    relevant_cols = flagcols | datacols
    out_sub_ = out_sub_[, relevant_cols]

    return(out_sub_)
}
get_DP1.20093.001 = function(sets, silent=TRUE){

    grab = tibble()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=',i, '/', nrow(sets)))

        url = sets[i,1]
        site = sets[i,2]
        date = sets[i,3]

        # write(paste('Processing:', site, date),
        #     '../../logs_etc/NEON/NEON_ingest.log', append=TRUE)

        #download a dataset for one site and month
        d = httr::GET(url)
        d = jsonlite::fromJSON(httr::content(d, as="text"))

        # data1_ind = intersect(grep("expanded", d$data$files$name),
        #     grep("fieldData", d$data$files$name))
        data2_ind = intersect(grep("expanded", d$data$files$name),
            grep("fieldSuperParent", d$data$files$name))
        data3_ind = intersect(grep("expanded", d$data$files$name),
            grep("externalLabData", d$data$files$name))
        data4_ind = intersect(grep("expanded", d$data$files$name),
            grep("domainLabData", d$data$files$name))

        # data1 = read.delim(d$data$files$url[data1_ind], sep=",",
        #     stringsAsFactors=FALSE)
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
        data4 = tryCatch({
            read.delim(d$data$files$url[data4_ind], sep=",",
                stringsAsFactors=FALSE)
        }, error=function(e){
            data.frame()
        })

        if(nrow(data2)){
            data2 = select(data2, siteID, collectDate, dissolvedOxygen,
                dissolvedOxygenSaturation, specificConductance, waterTemp,
                maxDepth)
        }
        if(nrow(data3)){
            data3 = select(data3, siteID, collectDate, pH, externalConductance,
                externalANC, starts_with('water'), starts_with('total'),
                starts_with('dissolved'), uvAbsorbance250, uvAbsorbance284,
                shipmentWarmQF, externalLabDataQF)
        }
        if(nrow(data4)){
            data4 = select(data4, siteID, collectDate, starts_with('alk'),
                starts_with('anc'))
        }

        grab_sub = plyr::join_all(list(data2, data3, data4), type='full') %>%
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

        grab = bind_rows(grab, grab_sub)
    }

    return(grab)
}
get_DP1.20288.001 = function(sets, silent=TRUE){

    out = tibble()

    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=',i, '/', nrow(sets)))

        url = sets[i,1]
        site = sets[i,2]
        date = sets[i,3]

        # write(paste('Processing:', site, date),
        #     '../../logs_etc/NEON/NEON_ingest.log', append=TRUE)

        #download a dataset for one site and month
        d = httr::GET(url)
        d = jsonlite::fromJSON(httr::content(d, as="text"))

        data_inds = intersect(grep("expanded", d$data$files$name),
            grep("instantaneous", d$data$files$name))

        site_with_suffixes = determine_upstream_downstream(d, data_inds, site)

        for(j in 1:length(data_inds)){

            site_with_suffix = site_with_suffixes[j]

            #download data
            out_sub = read.delim(d$data$files$url[data_inds[j]], sep=",",
                stringsAsFactors=FALSE)

            out_sub = resolve_neon_naming_conflicts(out_sub,
                c('specificCond'='specificConductance',
                    'dissolvedOxygenSat'='dissolvedOxygenSaturation'))

            out_sub = mutate(out_sub,
                datetime=as.POSIXct(startDateTime,
                    tz='UTC', format='%Y-%m-%dT%H:%M:%SZ'),
                site=site_with_suffix) %>%
                select(-startDateTime)

            out = bind_rows(out, out_sub)
        }
    }

    return(out)
}

#make this into a csv ####
neonprods = tibble(prodID=c('DP1.20093.001', 'DP1.20288.001', 'DP1.20267.001',
    'DP4.00133.001', 'DP4.00130.001', 'DP1.20048.001', 'DP1.20193.001'),
    prod=c('chemistry', 'waterqual', 'gageht', 'zqcurve', 'Q', 'Qflowmeter',
    'Qsalt'),
    type=c('grab', 'sensor', 'grab', 'other', 'sensor', 'grab', 'grab'),
    notes=c('','','','only HOPB and GUIL', 'only HOPB', '',''))
# readr::write_csv(neonprods, 'data_acquisition/data/neon/neon_products_temp.csv')

# grab_data_products = filter(neonprods, type == 'grab') %>% pull(prodID)
# sensor_data_products = filter(neonprods, type == 'sensor') %>% pull(prodID)
# other_data_products = filter(neonprods, type == 'other') %>% pull(prodID)

# i=2; j=1; sets=site_sets
for(i in 1:nrow(neonprods)){

    #get available datasets for this data product
    prodcode = neonprods$prodID[i]
    prodID = strsplit(prodcode, '\\.')[[1]][2]

    req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
        prodcode))
    txt = httr::content(req, as="text")
    neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)

    #get available urls, sites, and dates
    urls = unlist(neondata$data$siteCodes$availableDataUrls)
    avail_sets = stringr::str_match(urls, '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})')

    #retrieve data by site; log acquisitions (and revisions, once neon actually has them)
    avail_sites = unique(avail_sets[, 2])
    for(j in 1:length(avail_sites)){

        site_sets = avail_sets[avail_sets[, 2] == avail_sites[j], ]

        #filter already held sitemonths from site_sets ####
        # site_sets = site_sets[1:10,]

        retrieval_func = get(paste0('get_', prodcode))
        site_dset = do.call(retrieval_func, list(site_sets))


        write_feather(site_dset,
            glue('data_acquisition/data/neon/raw/{p}_{id}_{site}.feather',
            p=neonprods$prod[i], id=prodID))
    }
}

# for(s in 1:length(sensor_data_products)){
#
#     #get available datasets for this data product
#     product = sensor_data_products[s]
#     req = httr::GET(paste0("http://data.neonscience.org/api/v0/products/",
#         product))
#     txt = httr::content(req, as="text")
#     neondata = jsonlite::fromJSON(txt, simplifyDataFrame=TRUE, flatten=TRUE)
#
#     #get available urls, sites, and dates
#     urls = unlist(neondata$data$siteCodes$availableDataUrls)
#     avail_sets = stringr::str_match(urls, '(?:.*)/([A-Z]{4})/([0-9]{4}-[0-9]{2})')
#
#     # avail_sets = avail_sets[avail_sets[, 2] == 'WALK', ] ####remove this line
#
#     retrieval_func = get(paste0('get_', product))
#     sensor = do.call(retrieval_func, list(avail_sets)) #append to this?
# }

# tidyr::gather(grab_cur,'variable', 'value',
#     data = gather(data, 'Variable', 'Value', Light_PAR:Discharge_m3s)

zip('data/neon.zip', list.files('data/neon', recursive=TRUE, full.names=TRUE))
out = drive_upload("data/neon.zip",
    as_id('https://drive.google.com/drive/folders/0ABfF-JkuRvL5Uk9PVA'))
