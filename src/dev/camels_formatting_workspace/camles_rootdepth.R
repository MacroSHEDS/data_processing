library(tidyverse)

root_depth_tib <- read_csv('neon_camels_attr/constant/root_depth_tib.csv')

all_vals <- tibble()
for(i in 1:nrow(root_depth_tib)){
    
    vals <- root_depth_tib[i,]
    
    if(is.na(vals$a)) next
    
    all_tib = tibble(a = vals$a,
                     b = vals$b,
                     d = seq(0, vals$d, by = 0.001),
                     e = exp(1),
                     y = (0.5*(e^-(a*d) + e^-(b*d)))) %>%
        mutate(y = round(y, 3))
    
    # 10, 25, 50, 75, and 99 
    
    mean_depth_50 <- quantile(all_tib$d, probs = c(.5))
    mean_depth_99 <- quantile(all_tib$d, probs = c(.99))
    
    fin <- tibble(id = vals$type,
                  mean_root_depth_50 = mean_depth_50,
                  mean_root_depth_99 = mean_depth_99)
    
    all_vals <- rbind(all_vals, fin)
}

write_csv(all_vals, 'neon_camels_attr/data/large/igbp_root_depth_means.csv')

