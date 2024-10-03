library(tidyverse)
library(feather)
library(lubridate)
library(glue)
library(sf)

options(timeout = 10800)

# setwd('../timeseries_experimentation/')
setwd('~/git/macrosheds/qa_experimentation/')

dir.create('neon_camels_attr/data', showWarnings = FALSE)
dir.create('neon_camels_attr/data/large', showWarnings = FALSE)
dir.create('neon_camels_attr/data/ms_attributes', showWarnings = FALSE)

#### Prep ####

# Download realvent data
# Get an estimate for alpha
# data source: https://doi.pangaea.de/10.1594/PANGAEA.868808?format=html#download

# xx = httr::HEAD('https://hs.pangaea.de/model/ESRN-Database/30arc-sec/aptt1_30s.zip')
# xx = httr::HEAD('https://ral.ucar.edu/sites/default/files/public/product-tool/camels-catchment-attributes-and-meteorology-for-large-sample-studies-dataset-downloads/basin_timeseries_v1p2_metForcing_obsFlow.zip')
# xx = httr::HEAD('https://ral.ucar.edu/sites/default/files/public/product-tool/camels-catchment-attributes-and-meteorology-for-large-sample-studies-dataset-downloads/basin_set_full_res.zip')
# httr::parse_http_date(xx$headers$`last-modified`)
download.file('https://hs.pangaea.de/model/ESRN-Database/30arc-sec/aptt1_30s.zip',
              destfile = 'neon_camels_attr/data/large/alpha.zip')
try(unzip('neon_camels_attr/data/large/alpha.zip',
          exdir = 'neon_camels_attr/data/large/alpha'))

download.file('https://ral.ucar.edu/sites/default/files/public/product-tool/camels-catchment-attributes-and-meteorology-for-large-sample-studies-dataset-downloads/basin_timeseries_v1p2_metForcing_obsFlow.zip',
              destfile = 'neon_camels_attr/data/large/neon_forcings.zip')
unzip('neon_camels_attr/data/large/neon_forcings.zip',
      exdir = 'neon_camels_attr/data/large/forcings')

download.file('https://ral.ucar.edu/sites/default/files/public/product-tool/camels-catchment-attributes-and-meteorology-for-large-sample-studies-dataset-downloads/basin_set_full_res.zip',
              destfile = 'neon_camels_attr/data/large/camels_shapes.zip')
unzip('neon_camels_attr/data/large/camels_shapes.zip',
      exdir = 'neon_camels_attr/data/large/shapes')

# Helper functions to calculate camles climate attributes
source('neon_camels_attr/src/camels_helpers.R')
source('neon_camels_attr/src/camles_rootdepth.R')

conf <- jsonlite::fromJSON('../data_acquisition/config.json',
                           simplifyDataFrame = FALSE)

site_data <- sm(googlesheets4::read_sheet(
    conf$site_data_gsheet,
    na = c('', 'NA'),
    col_types = 'ccccccccnnnnncccc'
))

vars <- sm(googlesheets4::read_sheet(
  conf$variables_gsheet,
  na = c('', 'NA'),
  col_types = 'cccccccnnccnn'
))

network_domain <- site_data %>%
  select(network, domain) %>%
  distinct() %>%
  arrange(network, domain)

# network_domain <- filter(network_domain, network == 'streampulse')
## been just running this for a single network. set i here.
for(i in 1:nrow(network_domain)){

  network <<- network_domain[i,1]
  domain <<- network_domain[i,2]

  # climate_output <- try(source('neon_camels_attr/src/daymet_pet.R'), silent = T)
  #
  # if(inherits(climate_output, 'try-error')) next


dir.create(glue('neon_camels_attr/data/ms_attributes/{d}',
                d = domain), recursive = T)

#### CLIMATE ####

# NEON data

daymet <- read_feather(glue('neon_camels_attr/data/{n}_{d}_climate_pet.feather',
                            n = network,
                            d = domain)) %>%
    filter(date >= ymd('1989-10-01'),
           date <= ymd('2009-09-30'))

sites <- unique(daymet$site_code)

all_neon_climate <- tibble()
for(p in 1:length(sites)){

    one_site <- daymet %>%
        filter(site_code == !!sites[p]) %>%
        mutate(temp = (tmin + tmax)/2) %>%
        mutate(pet = ifelse(pet < 0, 0, pet)) %>%
        mutate(pet = ifelse(is.na(pet), 0, pet))

    one_site <- compute_clim_indices_camels(temp = one_site$temp, prec = one_site$prcp,
                                        pet = one_site$pet, day = one_site$date, tol = 0.1) %>%
        mutate(site_code = !!sites[p])

    all_neon_climate <- rbind(all_neon_climate, one_site)

}

# for(ii in 15:25){
for(ii in 1:nrow(network_domain)){

      network <<- network_domain[ii,1]
      domain <<- network_domain[ii,2]

      climate_output <- try(source('neon_camels_attr/src/daymet_pet.R', local = TRUE), silent = T)

      if(inherits(climate_output, 'try-error')) next


    dir.create(glue('neon_camels_attr/data/ms_attributes/{d}',
                    d = domain), recursive = T, showWarnings = FALSE)

    #### CLIMATE ####

    # NEON data

    daymet <- read_feather(glue('neon_camels_attr/data/{n}_{d}_climate_pet.feather',
                                n = network,
                                d = domain)) %>%
        filter(date >= ymd('1989-10-01'),
               date <= ymd('2009-09-30'))

    sites <- unique(daymet$site_code)

    all_neon_climate <- tibble()
    for(p in 1:length(sites)){

        one_site <- daymet %>%
            filter(site_code == !!sites[p]) %>%
            mutate(temp = (tmin + tmax)/2) %>%
            mutate(pet = ifelse(pet < 0, 0, pet)) %>%
            mutate(pet = ifelse(is.na(pet), 0, pet))

        one_site <- compute_clim_indices_camels(temp = one_site$temp, prec = one_site$prcp,
                                            pet = one_site$pet, day = one_site$date, tol = 0.1) %>%
            mutate(site_code = !!sites[p])

        all_neon_climate <- rbind(all_neon_climate, one_site)

    }

    write_feather(all_neon_climate, glue('neon_camels_attr/data/ms_attributes/{d}/clim.feather',
                                         d = domain))



    #### VEGETATION ####


    # Landcover
        # Generated by general loop in data_acquisition
    modis_land_fils <- list.files(glue('../data_acquisition/data/{n}/{d}/ws_traits/modis_igbp/',
                                       d = domain,
                                       n = network), full.names = T)

    # 2002–2014
    modis_landcover <- map_dfr(modis_land_fils, read_feather)

if(! nrow(modis_landcover) == 0){

    landcover <- modis_landcover %>%
        filter(year >= 2002 & year <= 2016) %>%
        group_by(site_code, var) %>%
        summarise(mean = mean(val)) %>%
        mutate(var = sub('...', '', var))

    landcover_classes <- read_csv('neon_camels_attr/constant/modis_land_cover.csv') %>%
        select(var = macrosheds_code, dom_land_cover = `IGBP name`, id = class_code)

    root_depth <- read_csv('neon_camels_attr/data/large/igbp_root_depth_means.csv')

    landcover_root <- left_join(landcover, landcover_classes, by = 'var') %>%
        left_join(., root_depth) %>%
        mutate(mean = mean/100) %>%
        mutate(root_depth_p_50 = mean*mean_root_depth_50,
               root_depth_p_99 = mean*mean_root_depth_99) %>%
        group_by(site_code) %>%
        summarise(root_depth_50 = sum(root_depth_p_50, na.rm = T),
                  root_depth_99 = sum(root_depth_p_99, na.rm = T))


    landcover <- landcover %>%
        group_by(site_code) %>%
        filter(mean == max(mean)) %>%
        ungroup() %>%
        distinct(site_code, .keep_all = T)



    landcover_fin <- left_join(landcover, landcover_classes, by = 'var') %>%
        mutate(dom_land_cover_frac = mean/100) %>%
        select(site_code, dom_land_cover_frac, dom_land_cover)

    all_veg <- left_join(landcover_fin, landcover_root)

    # Generated by general loop data_acquisition
    nlcd_fil <- list.files(glue('../data_processing/data/{n}/{d}/ws_traits/nlcd/',
                                n = network,
                                d = domain), full.names = T)

    nlcd <- map_dfr(nlcd_fil, read_feather) %>%
        filter(year == 2011) %>%
        filter(var %in% c('lg_nlcd_forest_dec', 'lg_nlcd_forest_evr', 'lg_nlcd_forest_mix')) %>%
        group_by(site_code) %>%
        summarise(frac_forest = sum(val)/100)

    veg_fin <- left_join(nlcd, all_veg)

    write_feather(veg_fin, glue('neon_camels_attr/data/ms_attributes/{d}/vege.feather',
                                d = domain))

}

    landcover <- modis_landcover %>%
        filter(year >= 2002 & year <= 2016) %>%
        group_by(site_code, var) %>%
        summarise(mean = mean(val)) %>%
        mutate(var = sub('...', '', var))

    landcover_classes <- read_csv('neon_camels_attr/constant/modis_land_cover.csv') %>%
        select(var = macrosheds_code, dom_land_cover = `IGBP name`, id = class_code)

    root_depth <- read_csv('neon_camels_attr/data/large/igbp_root_depth_means.csv')

    landcover_root <- left_join(landcover, landcover_classes, by = 'var') %>%
        left_join(., root_depth) %>%
        mutate(mean = mean/100) %>%
        mutate(root_depth_p_50 = mean*mean_root_depth_50,
               root_depth_p_99 = mean*mean_root_depth_99) %>%
        group_by(site_code) %>%
        summarise(root_depth_50 = sum(root_depth_p_50, na.rm = T),
                  root_depth_99 = sum(root_depth_p_99, na.rm = T))

    # Generated by general loop data_acquisition
topo_fils <- list.files(glue('../data_processing/data/{n}/{d}/ws_traits/terrain/',
                             n = network,
                             d = domain), full.name = T)

topo <- map_dfr(topo_fils, read_feather) %>%
    filter(var %in% c('te_slope_mean', 'te_elev_mean')) %>%
    pivot_wider(names_from = 'var', values_from = 'val') %>%
    select(site_code, elev_mean = te_elev_mean, slope_mean = te_slope_mean)

    landcover <- landcover %>%
        group_by(site_code) %>%
        filter(mean == max(mean)) %>%
        ungroup() %>%
      distinct(site_code, .keep_all = T)

    landcover_fin <- left_join(landcover, landcover_classes, by = 'var') %>%
        mutate(dom_land_cover_frac = mean/100) %>%
        select(site_code, dom_land_cover_frac, dom_land_cover)

    all_veg <- left_join(landcover_fin, landcover_root)

        # Generated by general loop data_acquisition
    nlcd_fil <- list.files(glue('../data_acquisition/data/{n}/{d}/ws_traits/nlcd/',
                                n = network,
                                d = domain), full.names = T)

    nlcd <- map_dfr(nlcd_fil, read_feather) %>%
        filter(year == 2011) %>%
        filter(var %in% c('lg_nlcd_forest_dec', 'lg_nlcd_forest_evr', 'lg_nlcd_forest_mix')) %>%
        group_by(site_code) %>%
        summarise(frac_forest = sum(val)/100)

    veg_fin <- left_join(nlcd, all_veg)

    write_feather(veg_fin, glue('neon_camels_attr/data/ms_attributes/{d}/vege.feather',
                                d = domain))

    #### TOPOLOGY ####


    # macrosheds site data sheet
    neon_sites <- site_data %>%
        filter(domain == !!pull(domain),
               site_type == 'stream_gauge') %>%
        mutate(area = ws_area_ha/100) %>%
        select(site_code, gauge_lat = latitude, gauge_lon = longitude, area)

        # Generated by general loop data_acquisition
    topo_fils <- list.files(glue('../data_acquisition/data/{n}/{d}/ws_traits/terrain/',
                                 n = network,
                                 d = domain), full.name = T)
    topo <- map_dfr(topo_fils, read_feather) %>%
        filter(var %in% c('te_slope_mean', 'te_elev_mean')) %>%
        pivot_wider(names_from = 'var', values_from = 'val') %>%
        select(site_code, elev_mean = te_elev_mean, slope_mean = te_slope_mean)

    topo_fin <- full_join(neon_sites, topo) %>%
        mutate(slope_mean = slope_mean*10)

    write_feather(topo_fin, glue('neon_camels_attr/data/ms_attributes/{d}/topo.feather',
                                 d = domain))

    #### GEOLOGY ####

        # Generated by general loop data_acquisition
    lith_fils <- list.files(glue('../data_acquisition/data/{n}/{d}/ws_traits/lithology/',
                                 n = network,
                                 d = domain), full.names = TRUE)

    lith <- map_dfr(lith_fils, read_feather)

    sites <- unique(lith$site_code)

if(! nrow(lith) == 0){

    sites <- unique(lith$site_code)

    all_tib <- tibble()
    for(p in 1:length(sites)) {
        lith_sites <- lith %>%
            filter(site_code == !!sites[p]) %>%
            arrange(desc(val))
        largest_name <- str_split_fixed(pull(lith_sites[1,2]), '_', n = Inf)[1,4]
        largest_val <- pull(lith_sites[1,3])

        second_name <- str_split_fixed(pull(lith_sites[2,2]), '_', n = Inf)[1,4]
        second_val <- pull(lith_sites[2,3])

        one_sites <- tibble(site_code = sites[p],
                            name = c('geol_class_1st', 'geol_class_2nd'),
                            var = c(largest_name, second_name),
                            val = c(largest_val, second_val))
        all_tib <- rbind(all_tib, one_sites)

    }

    lith_carb <- lith %>%
        filter(var == 'pn_geol_class_SC') %>%
        select(site_code, carbonate_rocks_frac = val) %>%
        mutate(carbonate_rocks_frac = carbonate_rocks_frac/100)

    vars_ <- vars %>%
        filter(variable_subtype == 'Lithology') %>%
        mutate(look = str_split_fixed(variable_code, '_', n = Inf)[,3]) %>% #if this fails, change the 3 to a 4
        mutate(n = nchar(variable_name))

    vars_['geo_name'] <- substr(vars_$variable_name, 16, vars_$n)

    vars_ <- vars_ %>%
        select(var = look, geo_name)

    all_lith <- left_join(all_tib, vars_, by = 'var') %>%
        select(-var) %>%
        pivot_wider(names_from = 'name', values_from = c('val', 'geo_name')) %>%
        select(site_code, geol_1st_class = geo_name_geol_class_1st, glim_1st_class_frac = val_geol_class_1st,
               geol_2nd_class = geo_name_geol_class_2nd, glim_2nd_class_frac = val_geol_class_2nd) %>%
        mutate(glim_1st_class_frac = glim_1st_class_frac/100,
               glim_2nd_class_frac = glim_2nd_class_frac/100) %>%
        mutate(geol_2nd_class = ifelse(glim_2nd_class_frac == 0, NA, geol_2nd_class))

    # Generated by general loop data_acquisition
    glhymps_fils <- list.files(glue('../data_processing/data/{n}/{d}/ws_traits/glhymps/',
                                    n = network,
                                    d = domain), full.names = T)

    glhymps <- map_dfr(glhymps_fils, read_feather) %>%
        filter(var %in% c('pm_sub_surf_porosity_mean', 'pm_sub_surf_permeability_mean')) %>%
        pivot_wider(names_from = 'var', values_from = 'val') %>%
        select(site_code, geol_porosity = pm_sub_surf_porosity_mean,
               geol_permeability = pm_sub_surf_permeability_mean)

    final_geol <- left_join(all_lith, lith_carb) %>%
        left_join(., glhymps)

    write_feather(final_geol, glue('neon_camels_attr/data/ms_attributes/{d}/geol.feather',
                                   d = domain))
}

    vars_['geo_name'] <- substr(vars_$variable_name, 16, vars_$n)

    vars_ <- vars_ %>%
        select(var = look, geo_name)

    all_lith <- left_join(all_tib, vars_, by = 'var') %>%
        select(-var) %>%
        pivot_wider(names_from = 'name', values_from = c('val', 'geo_name')) %>%
        select(site_code, geol_1st_class = geo_name_geol_class_1st, glim_1st_class_frac = val_geol_class_1st,
               geol_2nd_class = geo_name_geol_class_2nd, glim_2nd_class_frac = val_geol_class_2nd) %>%
        mutate(glim_1st_class_frac = glim_1st_class_frac/100,
               glim_2nd_class_frac = glim_2nd_class_frac/100) %>%
        mutate(geol_2nd_class = ifelse(glim_2nd_class_frac == 0, NA, geol_2nd_class))

        # Generated by general loop data_acquisition
    glhymps_fils <- list.files(glue('../data_acquisition/data/{n}/{d}/ws_traits/glhymps/',
                                    n = network,
                                    d = domain), full.names = T)

    glhymps <- map_dfr(glhymps_fils, read_feather) %>%
        filter(var %in% c('pm_sub_surf_porosity_mean', 'pm_sub_surf_permeability_mean')) %>%
        pivot_wider(names_from = 'var', values_from = 'val') %>%
        select(site_code, geol_porosity = pm_sub_surf_porosity_mean,
               geol_permeability = pm_sub_surf_permeability_mean)

    final_geol <- left_join(all_lith, lith_carb) %>%
        left_join(., glhymps)

    write_feather(final_geol, glue('neon_camels_attr/data/ms_attributes/{d}/geol.feather',
                                   d = domain))

    #### SOIL ####

    # Generated by general loop data_acquisition
    soil_fils <- list.files(glue('../data_acquisition/data/{n}/{d}/ws_traits/nrcs_soils/',
                                 n = network,
                                 d = domain), full.names = T)

    if(length(soil_fils) == 0){

    } else{
        soils <- map_dfr(soil_fils, read_feather)

        soil_fin <- soils %>%
            ungroup() %>%
            select(-pctCellErr) %>%
            filter(var %in% c('pf_soil_org', 'pf_soil_sand', 'pf_soil_silt', 'pf_soil_clay')) %>%
            pivot_wider(names_from = 'var', values_from = 'val') %>%
            select(site_code, sand_frac = pf_soil_sand, silt_frac = pf_soil_silt, clay_frac = pf_soil_clay,
                   organic_frac = pf_soil_org)

        write_feather(soil_fin, glue('neon_camels_attr/data/ms_attributes/{d}/soil.feather',
                                     n = network,
                                     d = domain))
    }
    if(length(soil_fils) == 0){

    } else{
      soils <- map_dfr(soil_fils, read_feather)

      soil_fin <- soils %>%
        ungroup() %>%
        select(-pctCellErr) %>%
        filter(var %in% c('pf_soil_org', 'pf_soil_sand', 'pf_soil_silt', 'pf_soil_clay')) %>%
        pivot_wider(names_from = 'var', values_from = 'val') %>%
        select(site_code, sand_frac = pf_soil_sand, silt_frac = pf_soil_silt, clay_frac = pf_soil_clay,
               organic_frac = pf_soil_org)

      write_feather(soil_fin, glue('neon_camels_attr/data/ms_attributes/{d}/soil.feather',
                                   n = network,
                                   d = domain))
    }

}

# Move daily climate data to domain folder
for(ii in 1:nrow(network_domain)) {

  network <<- network_domain[ii,1]
  domain <<- network_domain[ii,2]

  climate_path <- glue('neon_camels_attr/data/{n}_{d}_climate_pet.feather',
                       n = network,
                       d = domain)

  new_path <- glue('neon_camels_attr/data/ms_attributes/{d}/daymet_full_climate.feather',
                   n = network,
                   d = domain)

  file.copy(climate_path, new_path)
}

dir.create('data/ms_in_camels_format/camels', showWarnings = FALSE)
file.copy('neon_camels_attr/data/large/clim.feather', 'data/ms_in_camels_format/camels/clim.feather',
          overwrite = TRUE)

}
