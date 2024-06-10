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
p_pre <- p_pre %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()
p_aft <- p_aft %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()

scale_fac = 0.01
ggplot(pc_pre) +
    geom_line(aes(x = datetime, y = val), size = 3) +
    geom_line(data = pc_aft, aes(x = datetime, y = val), color = 'blue', size = 2) +
    geom_line(data = fx, aes(x = datetime, y = val * scale_fac), color = 'red', size = 1) +
    geom_line(data = p_pre, aes(x = datetime, y = val), color = 'purple', size = 2) +
    geom_line(data = p_aft, aes(x = datetime, y = val), color = 'orange', size = 1) +
    facet_wrap(~var, scales = "free_y") +
    labs(title = "", x = "Datetime", y = "Value") +
    scale_x_datetime(limits = as.POSIXct(c('2005-01-01', '2006-01-01'))) +
    theme_minimal()


# CATALINA
p_pre = read_feather('data/czo/catalina_jemez/derived/precipitation__ms003/MCZOB_met.feather')
pc_pre = read_feather('data/czo/catalina_jemez/derived/precip_chemistry__ms004/MCZOB_met.feather')

p_aft = read_feather('data/czo/catalina_jemez/derived/precipitation__ms900/FLUME_MCZOB.feather')
pc_aft = read_feather('data/czo/catalina_jemez/derived/precip_chemistry__ms901/FLUME_MCZOB.feather')

fx = read_feather('data/czo/catalina_jemez/derived/precip_flux_inst__ms902/FLUME_MCZOB.feather')

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
p_pre <- p_pre %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()
p_aft <- p_aft %>%
    group_by(var) %>%
    tidyr::complete(datetime = seq(min(datetime), max(datetime), by = "day")) %>%
    ungroup()

scale_fac = 0.1

ggplot(pc_pre) +
    geom_line(aes(x = datetime, y = val), linewidth = 3) +
    geom_line(data = pc_aft, aes(x = datetime, y = val), color = 'blue', size = 2) +
    geom_line(data = fx, aes(x = datetime, y = val * scale_fac), color = 'red', size = 1) +
    geom_line(data = p_pre, aes(x = datetime, y = val), color = 'purple', size = 2) +
    geom_line(data = p_aft, aes(x = datetime, y = val), color = 'orange', size = 1) +
    facet_wrap(~var, scales = "free_y") +
    labs(title = "", x = "Datetime", y = "Value") +
    # scale_x_datetime(limits = as.POSIXct(c('2005-01-01', '2006-01-01'))) +
    theme_minimal()

#look for missing pflux
for(v in unique(pc_pre$var)){
    zpc = filter(pc_pre, var == !!v, ! is.na(val)) %>% pull(datetime)
    zp = filter(p_pre, ! is.na(val)) %>% pull(datetime)
    should_have_fx = as.POSIXct(intersect(zpc, zp), tz = 'UTC', origin = '1970-01-01')
    zf = filter(fx, var == !!v, ! is.na(val)) %>% pull(datetime)
    missing = as.POSIXct(setdiff(should_have_fx, zf), tz = 'UTC', origin = '1970-01-01')
}
missing[1] %in% zf

#     zpca = filter(pc_aft, var == !!v, ! is.na(val)) %>% pull(datetime)
#     zpa = filter(p_aft, ! is.na(val)) %>% pull(datetime)
#     should_have_fxa = as.POSIXct(intersect(zpca, zpa), tz = 'UTC', origin = '1970-01-01')
#     zf = filter(fx, var == !!v, ! is.na(val)) %>% pull(datetime)
#     missing2 = as.POSIXct(setdiff(should_have_fxa, zf), tz = 'UTC', origin = '1970-01-01')
#
# missing[1] %in% zpca
# missing[1] %in% zpa
