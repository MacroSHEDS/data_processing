#get a single dataset of all pchem and schem files

p1 <- list.files('data/czo/catalina_jemez/raw/precip_chemistry__5491/data_v2/', full.names = T)
p2 <- list.files('data/czo/catalina_jemez/raw/precip_chemistry__5492/data_v2/', full.names = T)
s1 <- list.files('data/czo/catalina_jemez/raw/stream_chemistry__4135/data_v2/', full.names = T)
s2 <- list.files('data/czo/catalina_jemez/raw/stream_chemistry__2740/data_v2/', full.names = T)
full_list <- c()
qw = tibble()
qwu = tibble()
for(f in p1){
    qw_ = read_csv(f)
    qw_$sourced = f
    qw_ = rename_with(qw_, ~sub('\\.\\.\\.', '__', .))
    full_list <- c(full_list, colnames(qw_))
    qw = bind_rows(qw, qw_)
    qwu = bind_rows(qwu, qw_[1, ])
}
for(f in p2){
    qw_ = read_csv(f)
    qw_$sourced = f
    qw_ = rename_with(qw_, ~sub('\\.\\.\\.', '__', .))
    full_list <- c(full_list, colnames(qw_))
    qw = bind_rows(qw, qw_)
    qwu = bind_rows(qwu, qw_[1, ])
    # qwu__ = bind_rows(qwu, qw_[1, ])

    # select(qw_[1, ], contains('SO4'))
    # select(qw_, contains('SO4'))
    # select(qwu, contains('SO4'))
    # sss = select(qwu__, contains('SO4'))
    # if(ncol(sss) > 3) stop()
    # qwu = qwu___
}
for(f in s1){
    qw_ = read_csv(f)
    qw_$sourced = f
    qw_ = rename_with(qw_, ~sub('\\.\\.\\.', '__', .))
    full_list <- c(full_list, colnames(qw_))
    qw = bind_rows(qw, qw_)
    qwu = bind_rows(qwu, qw_[1, ])
}
for(f in s2){
    qw_ = read_csv(f)
    qw_$sourced = f
    qw_ = rename_with(qw_, ~sub('\\.\\.\\.', '__', .))
    full_list <- c(full_list, colnames(qw_))
    qw = bind_rows(qw, qw_)
    qwu = bind_rows(qwu, qw_[1, ])
}

#unique units for each var
zz = lapply(qwu, function(x) sort(unique(x)))
zz[sort(names(zz))]

#occurrance of varnames
table(full_list)

#some vars to ignore when unit converting (see domain helpers for full list
setdiff(names(table(full_list)), unlist(catalina_varname_priorities, use.names = F)) %>%
    discard(~grepl('__', .)) %>%
    discard(~grepl('-$', .)) %>%
    discard(~grepl('-', .)) %>%
    paste(collapse = "', '")

# qqq = filter(qw, ! is.na(Cond))
# unique(qqq$sourced)
# unique(qqq$EC)
#
# qqq = filter(qw, ! is.na(EClab))
# unique(qqq$sourced)
# unique(qqq$EC)
# unique(qqq$EClab)

# qqq = filter(qw, ! is.na(`Sr isotopes`))
# qqq$Sr[1:2]
# qqq$`Sr isotopes`[1:2]

#error code variants
lapply(qw, function(x) unique(grep('-', x[2:length(x)], value=T)))
# lapply(qw, function(x) unique(grep('^999', x[2:length(x)], value=T))) %>% keep(~length(.) > 0)
# lapply(qw, function(x) unique(grep('^99', x[2:length(x)], value=T))) %>% keep(~length(.) > 0)

#site code variants
table(qw$SiteCode)
table(qw$SiteCode)->eee
grep('HG', names(eee), value=T)

#datetime variants
qw$DateTime[4]
zzz = lubridate::mdy_hm(qw$DateTime)
qw$DateTime[is.na(zzz)]

#detlims
qw$DateTime[2]
filter(qw, DateTime == '(Detection limit)') %>% View()

filter(qw, grepl('etection', DateTime))
