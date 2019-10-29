library(rvest)
library(R.matlab)

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
