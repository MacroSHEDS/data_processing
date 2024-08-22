drop_var_prefix <- function(x){
    
    unprefixed <- substr(x, 4, nchar(x))
    
    return(unprefixed)
}

extract_var_prefix <- function(x){
    
    prefix <- substr(x, 1, 2)
    
    return(prefix)
}

parse_molecular_formulae <- function(formulae){
    
    #`formulae` is a vector
    
    # formulae = c('C', 'C4', 'Cl', 'Cl2', 'CCl', 'C2Cl', 'C2Cl2', 'C2Cl2B2')
    # formulae = 'BCH10He10PLi2'
    # formulae='Mn'
    
    conc_vars = str_match(formulae, '^(?:OM|TM|DO|TD|UT|UTK|TK|TI|TO|DI)?([A-Za-z0-9]+)_?')[,2]
    two_let_symb_num = str_extract_all(conc_vars, '([A-Z][a-z][0-9]+)')
    conc_vars = str_remove_all(conc_vars, '([A-Z][a-z][0-9]+)')
    one_let_symb_num = str_extract_all(conc_vars, '([A-Z][0-9]+)')
    conc_vars = str_remove_all(conc_vars, '([A-Z][0-9]+)')
    two_let_symb = str_extract_all(conc_vars, '([A-Z][a-z])')
    conc_vars = str_remove_all(conc_vars, '([A-Z][a-z])')
    one_let_symb = str_extract_all(conc_vars, '([A-Z])')
    
    constituents = mapply(c, SIMPLIFY=FALSE,
                          two_let_symb_num, one_let_symb_num, two_let_symb, one_let_symb)
    
    return(constituents) # a list of vectors
}

calculate_molar_mass <- function(molecular_formula){
    
    if(length(molecular_formula) > 1){
        stop('molecular_formula must be a string of length 1')
    }
    
    parsed_formula = parse_molecular_formulae(molecular_formula)[[1]]
    
    molar_mass = combine_atomic_masses(parsed_formula)
    
    return(molar_mass)
}

combine_atomic_masses <- function(molecular_constituents){
    
    #`molecular_constituents` is a vector
    
    xmat = str_match(molecular_constituents,
                     '([A-Z][a-z]?)([0-9]+)?')[, -1, drop=FALSE]
    elems = xmat[,1]
    mults = as.numeric(xmat[,2])
    mults[is.na(mults)] = 1
    molecular_mass = sum(PeriodicTable::mass(elems) * mults)
    
    return(molecular_mass) #a scalar
}

convert_molecule <- function(x, from, to){
    
    #e.g. convert_molecule(1.54, 'NH4', 'N')
    
    molecule_real <- ms_vars %>%
        filter(variable_code == !!from) %>%
        pull(molecule)
    
    if(!is.na(molecule_real)) {
        from <- molecule_real
    }
    
    from_mass <- calculate_molar_mass(from)
    to_mass <- calculate_molar_mass(to)
    converted_mass <- x * to_mass / from_mass
    
    return(converted_mass)
}

convert_to_gl <- function(x, input_unit, molecule) {
    
    molecule_real <- ms_vars %>%
        filter(variable_code == !!molecule) %>%
        pull(molecule)
    
    if(!is.na(molecule_real)) {
        formula <- molecule_real
    } else {
        formula <- molecule
    }
    
    if(grepl('eq', input_unit)) {
        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]
        
        if(length(valence) == 0) {stop('Varible is likely missing from ms_vars')}
        x = (x * calculate_molar_mass(formula)) / valence
        
        return(x)
    }
    
    if(grepl('mol', input_unit)) {
        x = x * calculate_molar_mass(formula)
        
        return(x)
    }
    
    return(x)
    
}

convert_unit <- function(x, input_unit, output_unit){
    
    units <- tibble(prefix = c('n', "u", "m", "c", "d", "h", "k", "M"),
                    convert_factor = c(0.000000001, 0.000001, 0.001, 0.01, 0.1, 100,
                                       1000, 1000000))
    
    old_fraction <- as.vector(str_split_fixed(input_unit, "/", n = Inf))
    old_top <- as.vector(str_split_fixed(old_fraction[1], "", n = Inf))
    
    if(length(old_fraction) == 2) {
        old_bottom <- as.vector(str_split_fixed(old_fraction[2], "", n = Inf))
    }
    
    new_fraction <- as.vector(str_split_fixed(output_unit, "/", n = Inf))
    new_top <- as.vector(str_split_fixed(new_fraction[1], "", n = Inf))
    
    if(length(new_fraction == 2)) {
        new_bottom <- as.vector(str_split_fixed(new_fraction[2], "", n = Inf))
    }
    
    old_top_unit <- str_split_fixed(old_top, "", 2)[1]
    
    if(old_top_unit %in% c('g', 'e', 'q', 'l') || old_fraction[1] == 'mol') {
        old_top_conver <- 1
    } else {
        old_top_conver <- as.numeric(filter(units, prefix == old_top_unit)[,2])
    }
    
    old_bottom_unit <- str_split_fixed(old_bottom, "", 2)[1]
    
    if(old_bottom_unit %in% c('g', 'e', 'q', 'l') || old_fraction[2] == 'mol') {
        old_bottom_conver <- 1
    } else {
        old_bottom_conver <- as.numeric(filter(units, prefix == old_bottom_unit)[,2])
    }
    
    new_top_unit <- str_split_fixed(new_top, "", 2)[1]
    
    if(new_top_unit %in% c('g', 'e', 'q', 'l') || new_fraction[1] == 'mol') {
        new_top_conver <- 1
    } else {
        new_top_conver <- as.numeric(filter(units, prefix == new_top_unit)[,2])
    }
    
    new_bottom_unit <- str_split_fixed(new_bottom, "", 2)[1]
    
    if(new_bottom_unit %in% c('g', 'e', 'q', 'l') || new_fraction[2] == 'mol') {
        new_bottom_conver <- 1
    } else {
        new_bottom_conver <- as.numeric(filter(units, prefix == new_bottom_unit)[,2])
    }
    
    new_val <- x*old_top_conver
    new_val <- new_val/new_top_conver
    
    new_val <- new_val/old_bottom_conver
    new_val <- new_val*new_bottom_conver
    
    return(new_val)
}


convert_from_gl <- function(x, input_unit, output_unit, molecule, g_conver) {
    
    molecule_real <- ms_vars %>%
        filter(variable_code == !!molecule) %>%
        pull(molecule)
    
    if(!is.na(molecule_real)) {
        formula <- molecule_real
    } else {
        formula <- molecule
    }
    
    if(grepl('eq', output_unit) && grepl('g', input_unit) ||
       grepl('eq', output_unit) && g_conver) {
        
        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]
        if(length(valence) == 0 | is.na(valence)) {stop('Varible is likely missing from ms_vars')}
        x = (x * valence) / calculate_molar_mass(formula)
        
        return(x)
    }
    
    if(grepl('mol', output_unit) && grepl('g', input_unit) ||
       grepl('mol', output_unit) && g_conver) {
        
        x = x / calculate_molar_mass(formula)
        
        return(x)
    }
    
    if(grepl('mol', output_unit) && grepl('eq', input_unit) && !g_conver) {
        
        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]
        if(length(valence) == 0) {stop('Varible is likely missing from ms_vars')}
        x = (x * calculate_molar_mass(formula)) / valence
        
        x = x / calculate_molar_mass(formula)
        
        return(x)
    }
    
    if(grepl('eq', output_unit) && grepl('mol', input_unit) && !g_conver) {
        
        x = x * calculate_molar_mass(formula)
        
        valence = ms_vars$valence[ms_vars$variable_code %in% molecule]
        if(length(valence) == 0) {stop('Varible is likely missing from ms_vars')}
        x = (x * valence)/calculate_molar_mass(formula)
        
        return(x)
    }
    
    return(x)
    
}

ms_conversions_ <- function(d,
                           keep_molecular,
                           convert_units_from,
                           convert_units_to){
    
    #d: a macrosheds tibble that has already been through ms_cast_and_reflag
    #keep_molecular: a character vector of molecular formulae to be
    #   left alone. Otherwise these formulae: NO3, SO4, PO4, SiO2, NH4, NH3, NO3_NO2
    #   will be converted according to the atomic masses of their main
    #   constituents. For example, NO3 should be converted to NO3-N within
    #   macrosheds, but passing 'NO3' to keep_molecular will leave it as NO3.
    #   The only time you'd want to do this is when a domain provides both
    #   forms. In that case we would process both forms separately, converting
    #   neither.
    #convert_units_from: a named character vector. Names are variable names
    #   without their sample-regimen prefixes (e.g. 'DIC', not 'GN_DIC'),
    #   and values are the units of those variables. Omit variables that don't
    #   need to be converted.
    #convert_units_to: a named character vector. Names are variable names
    #   without their sample-regimen prefixes (e.g. 'DIC', not 'GN_DIC'),
    #   and values are the units those variables should be converted to.
    #   Omit variables that don't need to be converted.
    
    #checks
    # cm <- ! missing(convert_molecules)
    cuF <- ! missing(convert_units_from) && ! is.null(convert_units_from)
    cuT <- ! missing(convert_units_to) && ! is.null(convert_units_to)
    
    if(sum(cuF, cuT) == 1){
        stop('convert_units_from and convert_units_to must be supplied together')
    }
    if(length(convert_units_from) != length(convert_units_to)){
        stop('convert_units_from and convert_units_to must have the same length')
    }
    cu_shared_names <- base::intersect(names(convert_units_from),
                                       names(convert_units_to))
    if(length(cu_shared_names) != length(convert_units_to)){
        stop('names of convert_units_from and convert_units_to must match')
    }
    
    vars <- drop_var_prefix(d$var)
    
    convert_molecules <- c('NO3', 'SO4', 'PO4', 'SiO2', 'SiO3', 'NH4', 'NH3',
                           'NO3_NO2')
    
    if(! missing(keep_molecular)){
        if(any(! keep_molecular %in% convert_molecules)){
            stop(glue('keep_molecular must be a subset of {cm}',
                      cm = paste(convert_molecules,
                                 collapse = ', ')))
        }
        convert_molecules <- convert_molecules[! convert_molecules %in% keep_molecular]
    }
    
    convert_molecules <- convert_molecules[convert_molecules %in% unique(vars)]
    
    molecular_conversion_map <- list(
        NH4 = 'N',
        NO3 = 'N',
        NH3 = 'N',
        SiO2 = 'Si',
        SiO3 = 'Si',
        SO4 = 'S',
        PO4 = 'P',
        NO3_NO2 = 'N')
    
    # if(cm){
    #     if(! all(convert_molecules %in% names(molecular_conversion_map))){
    #         miss <- convert_molecules[! convert_molecules %in%
    #                                       names(molecular_conversion_map)]
    #         stop(glue('These molecules either need to be added to ',
    #                   'molecular_conversion_map, or they should not be converted: ',
    #                   paste(miss, collapse = ', ')))
    #     }
    # }
    
    #handle molecular conversions, like NO3 -> NO3_N
    
    for(v in convert_molecules){
        
        d$val[vars == v] <- convert_molecule(x = d$val[vars == v],
                                             from = v,
                                             to = unname(molecular_conversion_map[v]))
        
        check_double <- str_split_fixed(unname(molecular_conversion_map[v]), '', n = Inf)[1,]
        
        if(length(check_double) > 1 && length(unique(check_double)) == 1) {
            molecular_conversion_map[v] <- unique(check_double)
        }
        
        new_name <- paste0(d$var[vars == v], '_', unname(molecular_conversion_map[v]))
        
        d$var[vars == v] <- new_name
    }
    
    # Converts input to grams if the final unit contains grams
    for(i in 1:length(convert_units_from)){
        
        unitfrom <- convert_units_from[i]
        unitto <- convert_units_to[i]
        v <- names(unitfrom)
        
        g_conver <- FALSE
        if(grepl('mol|eq', unitfrom) && grepl('g', unitto) ||
           v %in% convert_molecules){
            
            d$val[vars == v] <- convert_to_gl(x = d$val[vars == v],
                                              input_unit = unitfrom,
                                              molecule = v)
            
            g_conver <- TRUE
        }
        
        #convert prefix
        d$val[vars == v] <- convert_unit(x = d$val[vars == v],
                                         input_unit = unitfrom,
                                         output_unit = unitto)
        
        #Convert to mol or eq if that is the output unit
        if(grepl('mol|eq', unitto)) {
            
            d$val[vars == v] <- convert_from_gl(x = d$val[vars == v],
                                                input_unit = unitfrom,
                                                output_unit = unitto,
                                                molecule = v,
                                                g_conver = g_conver)
        }
    }
    
    return(d)
}

scale_flux_by_area <- function(network_domain, site_data){
    
    #this reads all flux data in data_acquisition/data and in portal/data,
    #   and scales it by watershed area. Originally this was only done for portal
    #   data, and originally each source file (*_flux_inst.feather) was retained
    #   after it was used to generate a *_flux_inst_scaled.feather. Now, the source
    #   file is removed after the scaled file is created. Thus, all our
    #   flux data is converted to kg/ha/d, and every flux file and
    #   directory gets a name change when this function runs.
    
    #It would of course be more efficient to do this scaling within flux derive
    #   kernels, but this solution works fine and doesn't require major
    #   modification or rebuilding of the dataset.
    
    #TODO: scale flux within derive kernels eventually. it'll make for clearer
    #   documentation
    
    ws_areas <- site_data %>%
        filter(as.logical(in_workflow)) %>%
        select(domain, site_code, ws_area_ha) %>%
        plyr::dlply(.variables = 'domain',
                    .fun = function(x) select(x, -domain))
    
    domains <- names(ws_areas)
    
    #the original engine of this function, which still only converts portal data
    engine_for_portal <- function(flux_var, domains, ws_areas){
        
        for(dmn in domains){
            
            files <- try(
                {
                    list.files(path = glue('../portal/data/{d}/{v}',
                                           d = dmn,
                                           v = flux_var),
                               full.names = FALSE,
                               recursive = FALSE)
                },
                silent = TRUE
            )
            
            if('try-error' %in% class(files) || length(files) == 0) next
            
            dir.create(path = glue('../portal/data/{d}/{v}_scaled',
                                   d = dmn,
                                   v = flux_var),
                       recursive = TRUE,
                       showWarnings = FALSE)
            
            for(fil in files){
                
                d <- read_feather(glue('../portal/data/{d}/{v}/{f}',
                                       d = dmn,
                                       v = flux_var,
                                       f = fil))
                
                d <- d %>%
                    mutate(val = errors::set_errors(val, val_err)) %>%
                    select(-val_err) %>%
                    arrange(site_code, var, datetime) %>%
                    left_join(ws_areas[[dmn]],
                              by = 'site_code') %>%
                    mutate(val = sw(val / ws_area_ha)) %>%
                    select(-ws_area_ha)
                
                d$val_err <- errors(d$val)
                d$val <- errors::drop_errors(d$val)
                
                write_feather(x = d,
                              path = glue('../portal/data/{d}/{v}_scaled/{f}',
                                          d = dmn,
                                          v = flux_var,
                                          f = fil))
            }
        }
    }
    
    engine_for_portal(flux_var = 'stream_flux_inst',
                      domains = domains,
                      ws_areas = ws_areas)
    
    engine_for_portal(flux_var = 'precip_flux_inst',
                      domains = domains,
                      ws_areas = ws_areas)
    
    unscaled_portal_flux_dirs <- dir(path = '../portal/data',
                                     pattern = '*_flux_inst',
                                     include.dirs = TRUE,
                                     full.names = TRUE,
                                     recursive = TRUE)
    
    unscaled_portal_flux_dirs <-
        unscaled_portal_flux_dirs[! grepl(pattern = '(/documentation/|inst_scaled)',
                                          x = unscaled_portal_flux_dirs)]
    
    lapply(X = unscaled_portal_flux_dirs,
           FUN = unlink,
           recursive = TRUE)
    
    #the new engine, for scaling flux data within data_acquisition/data
    engine_for_data_acquis <- function(flux_var, network_domain, ws_areas){
        
        for(i in 1:nrow(network_domain)){
            
            ntw <- network_domain$network[i]
            dmn <- network_domain$domain[i]
            
            flux_var_dir <- try(
                {
                    ff <- list.files(path = glue('data/{n}/{d}/derived',
                                                 n = ntw,
                                                 d = dmn),
                                     # v = flux_var),
                                     pattern = flux_var,
                                     full.names = FALSE,
                                     recursive = FALSE)
                    
                    ff <- ff[! grepl(pattern = 'inst_scaled',
                                     x = ff)]
                },
                silent = TRUE
            )
            
            if('try-error' %in% class(flux_var_dir) || length(flux_var_dir) == 0) next
            
            prodcode <- prodcode_from_prodname_ms(flux_var_dir)
            
            dir.create(path = glue('data/{n}/{d}/derived/{v}_scaled__{pc}',
                                   n = ntw,
                                   d = dmn,
                                   v = flux_var,
                                   pc = prodcode),
                       recursive = TRUE,
                       showWarnings = FALSE)
            
            files <- list.files(path = glue('data/{n}/{d}/derived/{fvd}',
                                            n = ntw,
                                            d = dmn,
                                            fvd = flux_var_dir),
                                pattern = '*.feather',
                                full.names = TRUE,
                                recursive = TRUE)
            
            for(f in files){
                
                d <- read_feather(f)
                
                d <- d %>%
                    mutate(val = errors::set_errors(val, val_err)) %>%
                    select(-val_err) %>%
                    arrange(site_code, var, datetime) %>%
                    left_join(ws_areas[[dmn]],
                              by = 'site_code') %>%
                    mutate(val = sw(val / ws_area_ha)) %>%
                    select(-ws_area_ha)
                
                d$val_err <- errors(d$val)
                d$val <- errors::drop_errors(d$val)
                
                f_scaled <- sub(pattern = 'inst__',
                                replacement = 'inst_scaled__',
                                x = f)
                
                write_feather(x = d,
                              path = f_scaled)
            }
        }
    }
    
    engine_for_data_acquis(flux_var = 'stream_flux_inst',
                           network_domain = network_domain,
                           ws_areas = ws_areas)
    
    engine_for_data_acquis(flux_var = 'precip_flux_inst',
                           network_domain = network_domain,
                           ws_areas = ws_areas)
    
    unscaled_acquisition_flux_dirs <- dir(path = 'data',
                                          pattern = '*_flux_inst',
                                          include.dirs = TRUE,
                                          full.names = TRUE,
                                          recursive = TRUE)
    
    unscaled_acquisition_flux_dirs <-
        unscaled_acquisition_flux_dirs[! grepl(pattern = '(/documentation/|_scaled)',
                                               x = unscaled_acquisition_flux_dirs)]
    
    unscaled_acquisition_flux_dirs <-
        unscaled_acquisition_flux_dirs[grepl(pattern = '/derived/',
                                             x = unscaled_acquisition_flux_dirs)]
    
    lapply(X = unscaled_acquisition_flux_dirs,
           FUN = unlink,
           recursive = TRUE)
    
    return(invisible())
}
