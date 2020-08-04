select_if(~(! all(is.na(.)))) %>%

#undeveloped
zip_dir = function(){
    zip(glue(raw_data_dest, '.zip'), flags='-rj',
        list.files(raw_data_dest, recursive=TRUE, full.names=TRUE))
    
    unlink(raw_data_dest, recursive=TRUE)
}

#undeveloped
unzip_dir = function(){
    unzip(glue(raw_data_dest, '.zip'), exdir=raw_data_dest, overwrite=TRUE)
}

get_flag_types = function(mapping, flags) {
    
    #MUST ENHANCE THIS TO ACCOUNT FOR MULTIPLE FLAGS APPLIED TO THE SAME
    #datapoint. see DB I/O card in projects todo list
    
    flags = paste(flags) #convert special types to char
    lengths = sapply(mapping, length)
    keyvec = rep(names(mapping), times=lengths)
    valvec = paste(unlist(unname(mapping)))
    
    if(any(! flags %in% valvec)){
        unaccounted_for = flags[which(! flags %in% valvec)]
        stop(paste('Missing mapping for flags:',
            paste(unaccounted_for, collapse=', ')))
    }
    
    flag_types = keyvec[match(flags, valvec)]
    
    return(flag_types)
}

resolve_commas = function(vec, comma_standin){
    vec = gsub(',', '\\,', vec, fixed=TRUE)
    vec = gsub(comma_standin, ',', vec)
    return(vec)
}

postgres_arrayify = function(vec){
    vec = paste0('{', vec, '}')
    vec = gsub('\\{\\}', '{""}', vec)
    return(vec)
}

populate_missing_shiny_files = function(domain){

    #this is not yet working. first, shiny needs to be reconfigured to
    #pull all site files as requested. atm precip and pchem are still
    #bound into one file (i.e. precip.feather instead of site1.feather)

    list.files('data/hbef/')

    qq = read_feather('data/hbef/discharge.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/discharge.feather')

    qq = read_feather('data/hbef/flux.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/flux.feather')

    qq = read_feather('data/hbef/pchem.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/pchem.feather')

    qq = read_feather('data/hbef/precip.feather')
    qq = filter(qq, site_name == 'donkey')
    qq = bind_rows(qq, tibble(site_name='ARIK', datetime=as.POSIXct('2019-01-01')))
    write_feather(qq, 'data/neon/precip.feather')

}

#maybe some useful parts for a web scraping function
scrape_web <- function(){

    require(rvest)
    require(R.matlab)

    setwd('~/git/macrosheds/data_acquisition/data/lter/hjandrews')

    dset_urls = list(q=paste0('https://andrewsforest.oregonstate.edu/sites',
        '/default/files/lter/data/weather/portal/MISC/DISCHARGE/data/index.html'))

    for(i in 1:length(dset_urls)){
        read_html(dset_urls[[i]]) %>%
            html_node('td.title') %>%
            html_text()
    }

    # d = readMat('https://andrewsforest.oregonstate.edu/sites/default/files/lter/data/weather/portal/MISC/DISCHARGE/data/discharge_5min_merged.mat')
    for(i in 1:length(dset_urls)){
        d = download.file('https://andrewsforest.oregonstate.edu/sites/default/files/lter/data/weather/portal/MISC/DISCHARGE/data/discharge_5min_merged.mat',
            'provisional/q_merged.mat')
        m = readMat('provisional/q_merged.mat')
    }
}
