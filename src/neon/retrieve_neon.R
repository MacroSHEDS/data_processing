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
library(jsonlite)
library(emayili)

#neon data product codes are of this form: DPx.yyyyy.zzz,
#where x is data level (1=qaqc'd), y is product id, z is revision

setwd('/home/mike/git/macrosheds/')

source('data_acquisition/src/helpers.R')
conf = jsonlite::fromJSON('data_acquisition/config.json')

held_data = jsonlite::fromJSON(
    readr::read_file('data_acquisition/data/neon/held_data.json'))

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
        msg = glue('Problem with upstream/downstream indicator for site ',
            '{site} ({prod}, {month}).', site=site_, prod=prodcode, month=date)
        logging::logwarn(msg, logger='neon.module')
        return(generate_ms_err())
    }

    site_with_suffixes = paste0(site_, updown_suffixes[updown_order])

    return(site_with_suffixes)
}
# out_sub_ = out_sub; replacements = c('specificCond'='specificConductance',
#         'dissolvedOxygenSat'='dissolvedOxygenSaturation')
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
    } else if(! 'startDateTime' %in% out_cols){
        msg = glue('Datetime column not found for site ',
            '{site} ({prod}, {month}).', site=site, prod=prodcode, month=date)
        logging::logwarn(msg, logger='neon.module')
        return(generate_ms_err())
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
# sets = site_sets; held=held_data
get_DP1.20288.001 = function(sets, silent=TRUE){

    out = tibble()

    successes = c()
    for(i in 1:nrow(sets)){

        if(! silent) print(paste0('i=', i, '/', nrow(sets)))

        url = sets[i, 1]
        site = sets[i, 2]
        date = sets[i, 3]

        msg = glue('Processing site ',
            '{site} ({prod}, {month}).', site=site, prod=prodcode, month=date)
        logging::loginfo(msg, logger='neon.module')

        #download a dataset for one site and month
        d = httr::GET(url)
        d = jsonlite::fromJSON(httr::content(d, as="text"))

        data_inds = intersect(grep("expanded", d$data$files$name),
            grep("instantaneous", d$data$files$name))

        site_with_suffixes = determine_upstream_downstream(d, data_inds, site)
        if(is_ms_err(site_with_suffixes)) next

        j_loop_completed = TRUE
        for(j in 1:length(data_inds)){

            site_with_suffix = site_with_suffixes[j]

            #download data
            out_sub = read.delim(d$data$files$url[data_inds[j]], sep=",",
                stringsAsFactors=FALSE)

            out_sub = resolve_neon_naming_conflicts(out_sub,
                c('specificCond'='specificConductance',
                    'dissolvedOxygenSat'='dissolvedOxygenSaturation'))

            if(is_ms_err(out_sub)){
                j_loop_completed = FALSE
                break
            }

            out_sub = mutate(out_sub,
                datetime=as.POSIXct(startDateTime,
                    tz='UTC', format='%Y-%m-%dT%H:%M:%SZ'),
                site=site_with_suffix) %>%
                select(-startDateTime)

            out = bind_rows(out, out_sub)

        }

        if(j_loop_completed) successes = append(successes, date)

    }

    update_held_data(new_dates=successes)

    return(out)
}

neonprods = readr::read_csv('data_acquisition/data/neon/neon_products_temp.csv')

# grab_data_products = filter(neonprods, type == 'grab') %>% pull(prodID)
# sensor_data_products = filter(neonprods, type == 'sensor') %>% pull(prodID)
# other_data_products = filter(neonprods, type == 'other') %>% pull(prodID)

# i=2; j=1; sets=site_sets
email_err_msg = FALSE
for(i in 1:nrow(neonprods)){

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
        email_err_msg = TRUE
        next
    })

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
        # site_sets = site_sets[1:10,]

        retrieval_func = get(paste0('get_', prodcode))
        tryCatch({
            site_dset = do.call(retrieval_func, args=list(sets=site_sets))
        }, error=function(e){
            logging::logerror(e, logger='neon.module')
            email_err_msg = TRUE
            next
        })

        write_feather(site_dset,
            glue('data_acquisition/data/neon/raw/{p}_{id}_{site}.feather',
            p=neonprods$prod[i], id=prodID, site=curr_site))
    }

    # held_data = list()
    readr::write_file(jsonlite::toJSON(held_data),
        'data_acquisition/data/neon/held_data.json')
}

# install.packages('rJava')
if(email_err_msg){
    email_err('neon data acquisition error. check the logs.',
        'mjv22@duke.edu', conf$gmail_pw)
}

# zip('data/neon.zip', list.files('data/neon', recursive=TRUE, full.names=TRUE))
# out = drive_upload("data/neon.zip",
#     as_id('https://drive.google.com/drive/folders/0ABfF-JkuRvL5Uk9PVA'))
