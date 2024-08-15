1. recompute_camels_climate.R (shouldn't need to be rerun unless they update camels)
	(renamed from camels_get_alpha.R)
    sources helpers
    writes clim.feather (recomputed attributes)
    writes pet timeseries (are these being used, or just a derelict from q_sim?)
2. recompute_camels_soil.R
	(renamed from camles_soil.R) 
    writes soil.feather
3. camels_attr.R
    sources helpers, rootdepth, daymet_pet
    writes clim, vege, topo, geol, soil.feather
1.5? daymet_pet.R
    writes climate_pet.feather



