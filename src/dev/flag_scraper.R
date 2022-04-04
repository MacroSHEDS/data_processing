

get_uniq_coments <- function(d){
    look <- select(d, contains('__|flg'))
    
    all_vars <- c()
    for(i in 1:length(names(look))){
        vars <- look %>%
            select(i) %>%
            pull() %>%
            unique()
        
        all_vars <- c(all_vars, vars)
    }
    
    unique(all_vars)
}

get_uniq_coments(d)[is.na(as.numeric(get_uniq_coments(d)))]


unique(d$day)
unique(d$month)
unique(d$year)

d %>%
    filter(var == 'GN_NH4_N') %>%
    ggplot(aes(datetime, val, col = ms_status)) + 
    geom_point()

d %>%
    filter(var == 'GN_NO3_N') %>%
    ggplot(aes(datetime, val, col = ms_status)) + 
    geom_point()

d %>%
    filter(var == 'GN_Ca') %>%
    ggplot(aes(datetime, val, col = ms_status)) + 
    geom_point()

data/lter/niwot/raw/stream_chemistry__



d <- read_feather('data/lter/niwot/munged/stream_chemistry__9/SADDLE_007.feather')
look %>%
    ggplot(aes(datetime, val, col = ms_status)) +
    geom_point()

# discharge__111, discharge__102, discharge__74

