sleepers_stream_chem_var_info <- list(
    "K.ueq.L"        = c('ueq/L', 'mg/L', 'K'),
    "Mg.ueq.L"       = c('ueq/L', 'mg/L', 'Mg'),
    "Na.ueq.L"       = c('ueq/L', 'mg/L', 'Na'),
    "Ca.ueq.L"       = c('ueq/L', 'mg/L', 'Ca'),
    "Cl.ueq.L"       = c('ueq/L', 'mg/L', 'Cl'),
    "Si.umol.L"      = c('umol/L', 'mg/L', 'Si'),
    "Fe.ug.L"        = c('ug/L',  'mg/L', 'Fe'),
    "Li.ug.L"        = c('ug/L',  'mg/L', 'Li'),
    "Mn.ug.L"        = c('ug/L',  'mg/L', 'Mn'),
    "Al.ug.L"        = c('ug/L',  'mg/L', 'Al'),
    "Ba.ug.L"        = c('ug/L',  'mg/L', 'Ba'),
    "Sr.ug.L"        = c('ug/L',  'mg/L', 'Sr'),
    "NO3.ueq.L"      = c('ueq/L', 'mg/L', 'NO3'),
    "SO4.ueq.L"      = c('ueq/L', 'mg/L', 'SO4'),
    "NH4.ueq.L"      = c('ueq/L', 'mg/L', 'NH4'),
    "PO4.mg.P.L"    = c('mg/L', 'mg/L', 'PO4_P'),
    "DOC.mg.L"      = c('mg/L', 'mg/L', 'DOC'),
    "TDN.mg.L"      = c('mg/L', 'mg/L', 'TDN'),
    "TDP.mg.L"      = c('mg/L', 'mg/L', 'TDP'),
    "Temp.C"        = c('degrees C', 'degrees C', 'temp'),
    "pH"            = c('unitless', 'unitless', 'pH'),
    "SpCond.uS.cm"   = c('uS/cm', 'uS/cm', 'spCond'),
    "d13C"          = c('permil', 'permil', 'd13C'),
    "d18O.NO3"      = c('permil', 'permil', 'd18O_NO3'),
    "d2H"           = c('permil', 'permil', 'dD'),
    "d15N.NO3"      = c('permil', 'permil', 'd15N_NO3'),
    "d18O"          = c('permil', 'permil', 'd18O'),
    "87Sr.86Sr"     = c('unitless', 'unitless', '87Sr_86Sr'),
    "ANC.ueq.L"      = c('ueq/l', 'eq/l', 'ANC'),
    "UV254.cm.1"    = c('AU/cm', 'AU/cm', 'abs254'),
    "SUVA.L.mg.m.1" = c('L/mgm', 'L/mgm', 'SUVA'),
    "Alo.ug.L"       = c('ug/L', 'mg/L', 'OMAl'),
    "Alm.ug.L"       = c('ug/L', 'mg/L', 'TMAl')
)

update_sleepers_detlims <- function(d,
                                    sleepers_stream_chem_var_info,
                                    no_bdl_vars){

    names_units <- sleepers_stream_chem_var_info %>%
        plyr::ldply() %>%
        rename(variable_original = 1, unit_original = 2,
               unit_converted = 3, variable_converted = 4) %>%
        mutate(variable_original = variable_converted)

    #update detection limits for this domain
    detlim_pre <- d %>%
        filter(! var %in% no_bdl_vars,
               val < 0) %>%
        distinct(var, val, .keep_all = TRUE) %>%
        mutate(start_date = as.Date(datetime),
               var = drop_var_prefix(var)) %>%
        select(start_date, var, val) %>%
        arrange(var, start_date) %>%
        group_by(var) %>%
        mutate(end_date = lead(start_date) - 1) %>%
        ungroup() %>%
        left_join(names_units, by = c('var' = 'variable_converted')) %>%
        rename(detection_limit_original = val,
               variable_converted = var) %>%
        mutate(domain = !!domain,
               prodcode = !!prodname_ms,
               # variable_converted = NA,
               # variable_original = ,
               detection_limit_converted = NA,
               detection_limit_original = abs(detection_limit_original),
               precision = NA,
               sigfigs = NA,
               # unit_converted = ,
               # unit_original = ,
               # start_date = ,
               # end_date = ,
               added_programmatically = FALSE
        )

    sleepers_detlims <- standardize_detection_limits(
        dls = detlim_pre,
        vs = ms_vars,
        update_on_gdrive = FALSE
    ) %>%
        mutate(added_programmatically = TRUE)

    detlims_update <- anti_join(
        sleepers_detlims, domain_detection_limits,
        by = c('domain', 'prodcode', 'variable_converted', 'variable_original',
               'detection_limit_original', 'start_date', 'end_date')
    )

    if(nrow(detlims_update)){
        ms_write_confdata(detlims_update,
                          which_dataset = 'domain_detection_limits',
                          to_where = 'remote',
                          overwrite = FALSE) #append
    }

    return(invisible())
}
