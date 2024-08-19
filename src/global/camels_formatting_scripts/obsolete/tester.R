all_fils <- list.files('../timeseries_experimentation/neon_camels_attr/data/ms_attributes/', recursive = T, full.names = T)


cilmate_files <- grep('clim.feather', all_fils, value = T)

all_limate <- map_dfr(cilmate_files, read_feather)

nrow(all_limate) == length(unique(all_limate$site_code))


geol_files <- grep('geol.feather', all_fils, value = T)

all_geol <- map_dfr(geol_files, read_feather)

nrow(all_geol) == length(unique(all_geol$site_code))


topo_files <- grep('topo.feather', all_fils, value = T)

all_topo <- map_dfr(topo_files, read_feather)

nrow(all_topo) == length(unique(all_topo$site_code))


vege_files <- grep('vege.feather', all_fils, value = T)

all_vege <- map_dfr(vege_files, read_feather)

nrow(all_vege) == length(unique(all_vege$site_code))


soil_files <- grep('soil.feather', all_fils, value = T)

all_soil <- map_dfr(soil_files, read_feather)

nrow(all_soil) == length(unique(all_soil$site_code))



look <- read_feather('../data_processing/clim.feather')
look <- read_feather('../data_processing/topo.feather')
look <- read_feather('../data_processing/soil.feather')

look <- read_feather('neon_camels_attr/data/large/east_river/')

all_fils <- list.files('neon_camels_attr/data/ms_attributes/', full.names = T,
                       recursive = T)

all_clim <- grep('clim.feather', all_fils, value = T)
clim <- map_dfr(all_clim, read_feather)

all_clim <- grep('daymet', all_fils, value = T)

all_old_fils <- list.files('../macrosheds_support/ms_attributes/', recursive = T,
                           full.names = T)
old_clim <- grep('clim.feather', all_old_fils, value = T)
old_clim_data <- map_dfr(old_clim, read_feather)

look <- full_join(old_clim_data, clim, by = 'site_code')


old_climate <- read_feather('../macrosheds_support/ms_attributes/boulder/daymet_full_climate.feather')

new_climate <- read_feather('neon_camels_attr/data/ms_attributes/boulder/daymet_full_climate.feather')

test <- full_join(old_climate, new_climate, by = c('site_code', 'date'))

ggplot(test, aes(pet.x, pet.y)) +
    geom_point() +
    geom_abline()

look %>%
    ggplot(aes(pet_mean.x, pet_mean.y)) +
    geom_point() +
    geom_abline() +
    labs(x = 'Old pet_mean', y = 'New pet_mean')

all_fils <- list.files('neon_camels_attr/data/ms_attributes/', full.names = T, recursive = T)

topo_files <- grep('topo.feather', all_fils, value = T)

file.remove(topo_files)


