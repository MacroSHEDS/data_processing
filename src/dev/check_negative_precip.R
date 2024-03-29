library(tidyverse)

#checking data/.../derived (might need work now) ####

all_sites <- tibble()
for(dmnrow in 1:nrow(network_domain)){
    network <- network_domain$network[dmnrow]
    domain <- network_domain$domain[dmnrow]

    all_fils <- list.files(glue('data/{n}/{d}/derived/precipitation__ms900/',
                                n = network,
                                d = domain),
                           full.names = TRUE)

    if(length(all_fils) == 0) next

    dom_sites <- tibble()
    for(s in 1:length(all_fils)){
        precip <- read_feather(all_fils[s])

        p_nrow <- nrow(precip)

        sumz <- precip %>%
            filter(val < 0)
        if(nrow(sumz) == 0) next

        sumz <- sumz %>%
            group_by(site_code) %>%
            summarise(n = n()) %>%
            mutate(percent = n/!!p_nrow)

        dom_sites <- rbind(dom_sites, sumz)
    }

    all_sites <- rbind(all_sites, dom_sites)
}

#checking EDI final output ####

setwd('~/git/macrosheds/data_acquisition/eml/data_links')
zz = list.files(pattern = '^timeseries_.*?\\.csv$')
negvars = tibble()
for(z in zz){
    dmn = str_match(z, '^timeseries_([a-z_]+)\\.csv$')[, 2]
    print(dmn)
    negvars = read_csv(z) %>%
        filter(val < 0) %>%
        distinct(site_code, var) %>%
        mutate(domain = dmn) %>%
        bind_rows(negvars)
    print(negvars)
}
