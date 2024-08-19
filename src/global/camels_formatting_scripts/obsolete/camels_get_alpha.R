
# setwd('~/git/macrosheds/qa_experimentation/')

#there are still paths that need to be updated to run this as-is. last time,
#i skipped re-downloading wherever possible and referenced the q_sim project input files.
#search for "***"

source('src/global/camels_formatting_scripts/camels_helpers.R')

dir.create('scratch/camels_assembly/camels_pet_isolate', recursive = TRUE, showWarnings = FALSE)

#### Check camels a ####
# Downloaded at https://ral.ucar.edu/solutions/products/camels
camels_alpha_fils <- list.files('~/ssd2/q_sim/in/CAMELS/basin_dataset_public_v1p2/model_output_daymet/model_output/flow_timeseries/daymet/',
                                recursive = T, full.names = T)

camels_alpha_fils <- camels_alpha_fils[grepl('59_model_output', camels_alpha_fils)]
camels_alpha_fils <- camels_alpha_fils[! grepl('soil', camels_alpha_fils)]

folds <- stringr::str_pad(1:18, 2, 'left', '0')

site_map <- list()
for(i in 1:length(folds)){

    all_fil <- list.files(paste0('~/ssd2/q_sim/in/CAMELS/basin_dataset_public_v1p2/basin_mean_forcing/daymet/', folds[i]))

    sites <- unique(str_split_fixed(all_fil, '_', n = Inf)[,1])

    site_map[[folds[i]]] <- sites
}

#    # downloaded at https://ral.ucar.edu/solutions/products/camels
download.file('https://ral.ucar.edu/sites/default/files/public/product-tool/camels-catchment-attributes-and-meteorology-for-large-sample-studies-dataset-downloads/camels_attributes_v2.0.zip',
              destfile = 'neon_camels_attr/data/large/attributes.zip') #path obsolete ***
# xx = httr::HEAD('https://ral.ucar.edu/sites/default/files/public/product-tool/camels-catchment-attributes-and-meteorology-for-large-sample-studies-dataset-downloads/camels_attributes_v2.0.zip')
# httr::parse_http_date(xx$headers$`last-modified`)
try(unzip('neon_camels_attr/data/large/attributes.zip', #***
      exdir = 'neon_camels_attr/data/large/attributes')) #***

camels_sites <- read_delim('~/ssd2/q_sim/in/CAMELS/camels_attributes_v2.0/camels_topo.txt',
                           col_types = cols('c')) %>%
    select(site_code = gauge_id, latitude = gauge_lat, longitude = gauge_lon, elev = elev_mean)

#     # downloaded at https://ral.ucar.edu/solutions/products/camels
camels_shapes_fils <- st_read('~/ssd2/q_sim/in/CAMELS/HCDN_nhru_final_671.shp')

all_fils <- list.files('~/ssd2/q_sim/in/CAMELS/basin_dataset_public_v1p2/basin_mean_forcing/daymet/',
                       full.names = T, recursive = T)

a_cof <- raster::raster('~/ssd2/q_sim/in/CAMELS/aptt1_30s/aptt1')

camles_fin <- tibble()
for(i in 1:length(site_map)) {
    sites <- site_map[[i]]

    for(s in 1:length(sites)) {

        # Read in daymet data
        daymet_t <- try(read_file(glue('~/ssd2/q_sim/in/CAMELS/basin_dataset_public_v1p2/basin_mean_forcing/daymet/{f}/{p}_lump_cida_forcing_leap.txt',
                                   f = names(site_map[i]),
                                   p = sites[s])))

        if(inherits(daymet_t, 'try-error')){
            daymet_t <- read_file(grep(sites[s], all_fils, value = T))
        }

        new <- str_replace_all(daymet_t, '\t', ' ')

        daymetfin <- try(read_delim(new, delim = ' ', skip = 3) %>%
            mutate(date = ymd(paste(Year, Mnth, Day, sep = '-'))) %>%
            select(date, dayl = `dayl(s)`, prcp = `prcp(mm/day)`, srad = `srad(W/m2)`,
                   swe = `swe(mm)`, tmax = `tmax(C)`, tmin = `tmin(C)`, vp = `vp(Pa)`) %>%
            mutate(site_code = !!sites[s]))

        if(inherits(daymetfin, 'try-error')) next

        modsfin <- left_join(daymetfin, camels_sites, by = 'site_code')

        # Get alpha
        neon_shp <- camels_shapes_fils %>%
            filter(hru_id == !!sites[s]) %>%
            sf::st_transform(., sf::st_crs(a_cof))

        if(nrow(neon_shp) == 0){
            new_site <- substr(sites[s], 2, nchar(sites[s]))
            neon_shp <- camels_shapes_fils %>%
                filter(hru_id == !!new_site) %>%
                sf::st_transform(., sf::st_crs(a_cof))
            if(nrow(neon_shp) == 0) {
                new_site <- paste0(0, sites[s])
                neon_shp <- camels_shapes_fils %>%
                    filter(hru_id == !!new_site) %>%
                    sf::st_transform(., sf::st_crs(a_cof))

                if(nrow(neon_shp) == 0) next
            }
        }

        croped <- raster::crop(a_cof, neon_shp)
        maksed <- raster::mask(croped, neon_shp)

        vals <- raster::values(maksed)

        mean_val <- mean(vals, na.rm = T)/100

        modsfin_a <- modsfin %>%
            mutate(alpha = !!mean_val)

        modsfin <- calc_daymet_pet(modsfin_a)

        hijacking_this_for_camels_pet <- modsfin %>%
            mutate(temp = (t_min + t_max)/2) %>%
            mutate(pet = ifelse(pet < 0, 0, pet)) %>%
            mutate(pet = ifelse(is.na(pet), 0, pet))

        write_csv(select(hijacking_this_for_camels_pet,
                         date, site_code, pet),
                  glue('scratch/camels_assembly/camels_pet_isolate/{ss}.csv',
                  # glue('data/CAMELS_macrosheds_combined/camels_pet_isolate/{ss}.csv',
                       ss = sites[s]))

        look <- hijacking_this_for_camels_pet %>%
            filter(date >= ymd('1989-10-01'),
                   date <= ymd('2009-09-30'))

        look <- compute_clim_indices_camels(temp = look$temp, prec = look$prcp,
                                            pet = look$pet, day = look$date, tol = 0.1) %>%
            mutate(site_code = !!sites[s])

        camles_fin <- rbind(camles_fin, look)
    }
}

write_feather(camles_fin, 'scratch/camels_assembly/clim.feather')
