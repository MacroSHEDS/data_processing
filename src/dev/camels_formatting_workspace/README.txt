1. append_pet_to_daymet.R
	gets PET column, appends to daymet data from ws_traits
2. camelsesque_climate.R (shouldn't need to be rerun unless they update camels)
	(renamed from camels_get_alpha.R)
    sources helpers
    writes clim.feather (recomputed attributes)
    writes pet timeseries (are these being used, or just a derelict from q_sim?)
3. camelselque_soil.R
	(renamed from camles_soil.R) 
    sources helpers
    writes soil.feather
4. camelsesque_veg_topo_geol_soil.R
	(renamed from camels_attr.R)
    sources helpers
	runs rootdepth loop that used to be separate miniscript
    writes clim, vege, topo, geol, soil.feather


do i need this?

write_csv(select(daymet_d_supp, date, site_code, pet),
                  glue('scratch/camels_assembly/camels_pet_isolate/{s}.csv'))
