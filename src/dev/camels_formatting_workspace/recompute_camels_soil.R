#TODO: some sitefiles can't be generated because the requested pixel dimensions are too large. probs fixable.

source('~/ssd2/q_sim/src/lstm_dungeon/camels_helpers.R')

dir.create('scratch/camels_assembly/recomputed_attributes/soil', showWarnings = FALSE)
dir.create('scratch/camels_assembly/recomputed_attributes/soil_fails', showWarnings = FALSE)

camels_shapes <- st_read('~/ssd2/q_sim/in/CAMELS/HCDN_nhru_final_671.shp') %>%
    rename(site_code = hru_id)

clst_type <- ifelse(.Platform$OS.type == 'windows', 'PSOCK', 'FORK')
ncores <- parallel::detectCores() %/% 2
clst <- parallel::makeCluster(spec = ncores, type = clst_type)
doParallel::registerDoParallel(clst)

print(paste('recomputing camels soil metrics using', ncores, 'cores. may take a while.'))

sites <- camels_shapes$site_code
catch <- foreach(i = seq_along(sites)) %do% {

    s <- sites[i]

    if(file.exists(paste0('scratch/camels_assembly/recomputed_attributes/soil/', s, '.csv'))){
        return()
    }

    soil_tib <- try({
        sm(get_nrcs_soils(
            nrcs_var_name = c(
                'soil_org' = 'om_r', # percent
                'soil_sand' = 'sandtotal_r', # percent
                'soil_silt' = 'silttotal_r', # percent
                'soil_clay' = 'claytotal_r', # percent
                # mass/volume
                # 'soil_partical_density' = 'partdensity',
                # micrometers per second
                # 'soil_ksat' = 'ksat_r', centimeters of water per centimeter of
                # soil, quantity of water that the soil is capable of storing
                # for use by plants
                'soil_awc' = 'awc_r',
                # Water content, 1/10 bar, is the amount of soil water retained
                # at a tension of 15 bars, expressed as a volumetric percentage
                # of the whole soil material.
                'soil_water_0.1bar' = 'wtenthbar_r',
                # Water content, 1/3 bar, is the amount of soil water retained
                # at a tension of 15 bars, expressed as a volumetric percentage
                # of the whole soil material. 15 bar = wilting point
                'soil_water_0.33bar' = 'wthirdbar_r',
                # Water content, 15 bar, is the amount of soil water retained at
                # a tension of 15 bars, expressed as a volumetric percentage of
                # the whole soil material. 15 bar = field capacity
                'soil_water_15bar' = 'wfifteenbar_r',
                # Water content, 0 bar, is the amount of soil water retained at
                # a tension of 15 bars, expressed as a volumetric percentage of
                # the whole soil material.
                'soil_water_0bar' = 'wsatiated_r',
                # percent of carbonates, by weight, in the fraction of the soil
                # less than 2 millimeters in size. 'pf_soil_carbonate' =
                # 'caco3_r', percent, by weight hydrated calcium sulfates in the
                # fraction of the soil less than 20 millimeters in size
                # 'pf_soil_gypsum' = 'gypsum_r', Cation-exchange capacity
                # (CEC-7) is the total amount of extractable cations that can be
                # held by the soil, expressed in terms of milliequivalents per
                # 100 grams of soil at neutrality (pH 7.0)
                'soil_cat_exchange_7' = 'cec7_r',
                # Effective cation-exchange capacity refers to the sum of
                # extractable cations plus aluminum expressed in terms of
                # milliequivalents per 100 grams of soil
                # 'pf_soil_cat_exchange_eff' = 'ecec_r', Electrical conductivity
                # (EC) is the electrolytic conductivity of an extract from
                # saturated soil paste, expressed as decisiemens per meter at 25
                # degrees C. 'pf_soil_elec_cond' = 'ec_r', Sodium adsorption
                # ratio is a measure of the amount of sodium (Na) relative to
                # calcium (Ca) and magnesium (Mg) in the water extract from
                # saturated soil paste. It is the ratio of the Na concentration
                # divided by the square root of one-half of the Ca + Mg
                # concentration. Soils that have SAR values of 13 or more may be
                # characterized by an increased dispersion of organic matter and
                # clay particles, reduced saturated hydraulic conductivity
                # (Ksat) and aeration, and a general degradation of soil
                # structure. 'pf_soil_SAR' = 'sar_r', pH is the 1:1 water
                # method. A crushed soil sample is mixed with an equal amount of
                # water, and a measurement is made of the suspension.
                'soil_ph' = 'ph1to1h2o_r'),
            #Bulk density, one-third bar, is the ovendry weight of the soil
            #material less than 2 millimeters in size per unit volume of soil at
            #water tension of 1/3 bar, expressed in grams per cubic centimeter
            #'pf_soil_bulk_density' = 'dbthirdbar_r'), Linear extensibility
            #refers to the change in length of an unconfined clod as moisture
            #content is decreased from a moist to a dry state. It is an
            #expression of the volume change between the water content of the
            #clod at 1/3- or 1/10-bar tension (33kPa or 10kPa tension) and oven
            #dryness. The volume change is reported as percent change for the
            #whole soil 'soil_linear_extend' = 'lep_r', Liquid limit (LL) is one
            #of the standard Atterberg limits used to indicate the plasticity
            #characteristics of a soil. It is the water content, on a percent by
            #weight basis, of the soil (passing #40 sieve) at which the soil
            #changes from a plastic to a liquid state 'soil_liquid_limit' =
            #'ll_r', Plasticity index (PI) is one of the standard Atterberg
            #limits used to indicate the plasticity characteristics of a soil.
            #It is defined as the numerical difference between the liquid limit
            #and plastic limit of the soil. It is the range of water content in
            #which a soil exhibits the characteristics of a plastic solid.
            #'soil_plasticity_index' = 'pi_r'
            site = s,
            ws_boundaries = camels_shapes))
    })

    if(! inherits(soil_tib, 'try-error') && ! is_ms_exception(soil_tib)){
        write_csv(soil_tib, glue('scratch/camels_assembly/recomputed_attributes/soil/{s}.csv'))
    } else {
        file.create(glue('scratch/camels_assembly/recomputed_attributes/soil_fails/{s}'))
    }
}

parallel::stopCluster(clst)

list.files('scratch/camels_assembly/recomputed_attributes/soil', full.names = TRUE) %>%
    map_dfr(~read_csv(., show_col_types = FALSE)) %>%
    filter(! var %in% c('soil_cat_exchange_7', 'soil_ph')) %>%
    mutate(n = nchar(site_code)) %>%
    mutate(site_code = ifelse(n == 7, paste0(0, site_code), site_code)) %>%
    select(-n, -year, -pctCellErr) %>%
    filter(var %in% c('soil_org', 'soil_sand', 'soil_silt', 'soil_clay')) %>%
    pivot_wider(names_from = 'var', values_from = 'val') %>%
    rename(sand_frac = soil_sand,
           silt_frac = soil_silt,
           clay_frac = soil_clay,
           organic_frac = soil_org) %>%
    write_csv('scratch/camels_assembly/recomputed_attributes/soil.csv')
