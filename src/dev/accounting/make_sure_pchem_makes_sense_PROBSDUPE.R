library(ggplot2)
library(tidyr)

# dplyr::glimpse(p_pre)
# skimr::skim(p_pre)
# DataExplorer::introduce(p_pre)
# visdat::vis_dat(p_pre)

#BOULDER
p_pre = read_feather('data/czo/boulder/munged/precipitation__2435/BT_Met.feather')
pc_pre = read_feather('data/czo/boulder/munged/precip_chemistry__3639/BT_Met.feather')

p_mid = read_feather('data/czo/boulder/derived/precipitation__ms006/BT_Met.feather')

p_aft = read_feather('data/czo/boulder/derived/precipitation__ms900/BC_SW_20.feather')
pc_aft = read_feather('data/czo/boulder/derived/precip_chemistry__ms901/BC_SW_20.feather')

fx = read_feather('data/czo/boulder/derived/precip_flux_inst__ms902/BC_SW_20.feather')

# skimr::skim(p_pre)
# skimr::skim(p_mid)
# skimr::skim(p_aft)

# p_pre = tidyr::complete(p_pre, datetime = seq(min(datetime), max(datetime), by = 'day'))
# p_mid = tidyr::complete(p_mid, datetime = seq(min(datetime), max(datetime), by = 'day'))
# p_aft = tidyr::complete(p_aft, datetime = seq(min(datetime), max(datetime), by = 'day'))

ggplot(p_pre) +
    # sw(scale_x_datetime(limits = as.POSIXct(c('2010-01-01', '2011-01-01')))) +
    geom_line(aes(x = datetime, y = val), size = 3) +
    geom_line(data = p_mid, aes(x = datetime, y = val), color = 'red', size = 2) +
    geom_line(data = p_aft, aes(x = datetime, y = val), color = 'blue')

#pchem

# var_ = 'GN_Al'
# pc_pre = pc_pre %>% filter(var == var_) %>% tidyr::complete(datetime = seq(min(datetime), max(datetime), by = 'day'))
# pc_aft = pc_aft %>% filter(var == var_) %>% tidyr::complete(datetime = seq(min(datetime), max(datetime), by = 'day'))

# skimr::skim(pc_pre)
# skimr::skim(pc_aft)

pc_pre <- pc_pre %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()
pc_aft <- pc_aft %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()
fx <- fx %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()

# ggplot(pc_pre) +
#     # sw(scale_x_datetime(limits = as.POSIXct(c('2010-01-01', '2011-01-01')))) +
#     geom_line(aes(x = datetime, y = val), size = 2) +
#     geom_line(data = pc_aft, aes(x = datetime, y = val), color = 'blue')

scale_fac = 0.01
ggplot(pc_pre) +
    geom_line(aes(x = datetime, y = val), size = 3) +
    geom_line(data = pc_aft, aes(x = datetime, y = val), color = 'blue', size = 2) +
    geom_line(data = fx, aes(x = datetime, y = val * scale_fac), color = 'red', size = 1) +
    # scale_y_continuous(sec.axis = sec_axis(~ ., name = "FX Scale")) +
    facet_wrap(~var, scales = "free_y") +
    labs(title = "", x = "Datetime", y = "Value") +
    theme_minimal()


# BONANZA
p_pre = read_feather('data/lter/bonanza/munged/precipitation__167/HR1A.feather')
pc_pre = read_feather('data/lter/bonanza/munged/precip_chemistry__157/CRREL.feather')

p_aft = read_feather('data/lter/bonanza/derived/precipitation__ms900/C2.feather')
pc_aft = read_feather('data/lter/bonanza/derived/precip_chemistry__ms901/C2.feather')

fx = read_feather('data/lter/bonanza/derived/precip_flux_inst__ms902/C2.feather')

pc_pre <- pc_pre %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()
pc_aft <- pc_aft %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()
fx <- fx %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()

scale_fac = 0.01
ggplot(pc_pre) +
    geom_line(aes(x = datetime, y = val), size = 3) +
    geom_line(data = pc_aft, aes(x = datetime, y = val), color = 'blue', size = 2) +
    geom_line(data = fx, aes(x = datetime, y = val * scale_fac), color = 'red', size = 1) +
    facet_wrap(~var, scales = "free_y") +
    labs(title = "", x = "Datetime", y = "Value") +
    # scale_x_datetime(limits = as.POSIXct(c('2002-01-01', '2003-01-01'))) +
    theme_minimal()
