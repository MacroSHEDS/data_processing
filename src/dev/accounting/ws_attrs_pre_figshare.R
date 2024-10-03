setwd('~/git/macrosheds/portal')

#compile all varnames encountered in ws_traits files
zz = system('find ../data_acquisition/data/webb/sleepers/ws_traits -name "*.feather"', intern = T)
enc_vars = c()
for(z in zz){
    if(grepl('daymet', z)) next
    qq = read_feather(z, columns = 'var')
    enc_vars = c(unique(qq$var), enc_vars)
}

#wtf is this?
po = read_csv('data/general/variables_portalonly.csv')$variable_code

#and all established vars (in ms_vars sheet)
v = read_csv('data/general/variables.csv')

est_vars <- v %>%
    filter(variable_type == 'ws_char') %>%
    pull(variable_code)

#any unaccounted for?
(nonconforms <- setdiff(est_vars, enc_vars))
setdiff(enc_vars, est_vars)

#just collect the raw files from ws_traits when there's a raw/sum dichotomy.
#the "sum" files' variables show up only in the macrosheds avail data and are
#hopefully defined there
sumz <- grep('sum', zz, value = TRUE)

for(sz in rev(sumz)){
    dirn = dirname(sz)
    ext <- str_extract(sz, 'sum_(.+?\\.feather)', group = 1)
    if(sum(grepl(paste0(dirn, '.*?', ext), zz)) > 1){
        sumz <- sumz[! sumz == sz]
    }
}

zzz = c(grep('sum', zz, value = TRUE, invert = T), sumz)

enc_vars2 = c()
for(z in zzz){
    if(grepl('daymet', z)) next
    qq = read_feather(z, columns = 'var')
    enc_vars2 = c(unique(qq$var), enc_vars2)
}

#what's unaccounted for now?
setdiff(est_vars, enc_vars2)
setdiff(enc_vars2, est_vars)

setdiff(po, est_vars) #???
intersect(po, enc_vars)
setdiff(po, enc_vars)
setdiff(po, enc_vars2)

yd = read_feather('data/general/biplot/year.feather')
ydv = unique(yd$var)
setdiff(po, ydv)


intersect(setdiff(po, est_vars), ydv)
setdiff(setdiff(po, est_vars), ydv)

#k. cool. all of this should be cleared up next time we run ws_traits from scratch.
#OR simply sed through the typo names and fix them. probs should do the latter.

# sort(unique(grep('gpp', est_vars, value = TRUE)))
# sort(unique(grep('gpp', enc_vars, value = TRUE)))
# sort(unique(grep('gpp', v_fig, value = TRUE)))

# unique(read_feather('/home/mike/git/macrosheds/data_acquisition/data/lter/hbef/ws_traits/fpar/raw_w4.feather')$var)
# unique(read_feather('/home/mike/git/macrosheds/data_acquisition/data/lter/hbef/ws_traits/fpar/sum_w2.feather')$var)

# now compile all ws_traits vars that show up in the figshare set
v_fig = read_feather('/home/mike/git/macrosheds/data_acquisition/macrosheds_figshare_v1/1_watershed_attribute_data/ws_attr_timeseries/climate.feather',
                     columns = 'var') %>% pull() %>% unique()
v_fig <- c(v_fig, read_feather('/home/mike/git/macrosheds/data_acquisition/macrosheds_figshare_v1/1_watershed_attribute_data/ws_attr_timeseries/hydrology.feather',
                               columns = 'var') %>% pull() %>% unique())
v_fig <- c(v_fig, read_feather('/home/mike/git/macrosheds/data_acquisition/macrosheds_figshare_v1/1_watershed_attribute_data/ws_attr_timeseries/landcover.feather',
                               columns = 'var') %>% pull() %>% unique())
v_fig <- c(v_fig, read_feather('/home/mike/git/macrosheds/data_acquisition/macrosheds_figshare_v1/1_watershed_attribute_data/ws_attr_timeseries/parentmaterial.feather',
                               columns = 'var') %>% pull() %>% unique())
v_fig <- c(v_fig, read_feather('/home/mike/git/macrosheds/data_acquisition/macrosheds_figshare_v1/1_watershed_attribute_data/ws_attr_timeseries/terrain.feather',
                               columns = 'var') %>% pull() %>% unique())
v_fig <- c(v_fig, read_feather('/home/mike/git/macrosheds/data_acquisition/macrosheds_figshare_v1/1_watershed_attribute_data/ws_attr_timeseries/vegetation.feather',
                               columns = 'var') %>% pull() %>% unique())

setdiff(v_fig, est_vars)
setdiff(est_vars, v_fig)

sort(unique(grep('fpar', est_vars, value = TRUE)))
sort(unique(grep('fpar', enc_vars, value = TRUE)))
sort(unique(grep('fpar', v_fig, value = TRUE)))

# unique(read_feather('/home/mike/git/macrosheds/data_acquisition/data/lter/hbef/ws_traits/fpar/raw_w2.feather')$var)

# what about camels-compliant vars? nvm. these are already defined by camels

