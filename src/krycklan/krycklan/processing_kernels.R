
#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- download_from_googledrive

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- download_from_googledrive

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS003 <- download_from_googledrive

#stream_temperature: STATUS=READY
#. handle_errors
process_0_VERSIONLESS004 <- download_from_googledrive

#ws_boundary: STATUS=READY
#. handle_errors
process_0_VERSIONLESS005 <- download_from_googledrive

#precipitation: STATUS=READY
#. handle_errors
process_0_VERSIONLESS006 <- download_from_googledrive

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    rawfile <- list.files(rawfile, full.names = TRUE)

    d <- read_delim(rawfile, delim = '\t', skip = 4, col_types = cols(.default = "c"))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT+2',
                         site_code_col = 'SiteID',
                         alt_site_code = list('Site1' = '1',
                                              'Site2' = '2',
                                              'Site4' = '4',
                                              'Site5' = '5',
                                              'Site6' = '6',
                                              'Site7' = '7',
                                              'Site9' = '9',
                                              'Site10' = '10',
                                              'Site12' = '12',
                                              'Site13' = '13',
                                              'Site14' = '14',
                                              'Site15' = '15',
                                              'Site16' = '16',
                                              'Site18' = '18',
                                              'Site20' = '20'),
                         data_cols =  c('Q l/s' = 'discharge'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    rawfile <- list.files(rawfile, full.names = TRUE)

    d <- read.csv(rawfile,
                  stringsAsFactors = FALSE,
                  colClasses = "character")

    colnames(d) <- str_remove_all(colnames(d), 'Â')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT+2',
                         site_code_col = 'SiteID',
                         alt_site_code = list('Svartberget' = '1'),
                         data_cols =  c('EC.µS.cm' = 'spCond',
                                        'pH...25.C.' = 'pH',
                                        'TOC.mg.C.l' = 'TOC',
                                        'NO2.N.µg.N.l' = 'NO2_N',
                                        'NH4.N.µg.N.l' = 'NH4_N',
                                        'NO3.N.µg.N.l' = 'NO3_N',
                                        'tot.N.mg.N.l' = 'TN',
                                        'SO4.SO4.µg.SO4.l' = 'SO4',
                                        'PO4.P.µg.P.l' = 'PO4_P',
                                        'S.SO4.µg.S.l' = 'SO4_S',
                                        'Cu.µg.l' = 'Cu',
                                        'Cs.µg.l' = 'Cs',
                                        'Mn.µg.l' = 'Mn',
                                        'Na.µg.l' = 'Na',
                                        'Bi.µg.l' = 'Bi',
                                        'Co.µg.l' = 'Co',
                                        'Cl.µg.l' = 'Cl',
                                        'Cd.µg.l' = 'Cd',
                                        'Ca.µg.l' = 'Ca',
                                        'Ce.µg.l' = 'Ce',
                                        'B.µg.l' = 'B',
                                        'Br.µg.l' = 'Br',
                                        'Be.µg.l' = 'Be',
                                        'Cr.µg.l' = 'Cr',
                                        'P.µg.l' = 'P',
                                        'Os.µg.l' = 'Os',
                                        'Ru.µg.l' = 'Ru',
                                        'Nd.µg.l' = 'Nd',
                                        #'Gd µg/l' = 'Gd',
                                        'Ti.µg.l' = 'Ti',
                                        'Tb.µg.l' = 'Tb',
                                        'Se.µg.l' = 'Se',
                                        'Li.µg.l' = 'Li',
                                        'Y.µg.l' = 'Y',
                                        'Nb.µg.l' = 'Nb',
                                        'Tm.µg.l' = 'Tm',
                                        'Sc.µg.l' = 'Sc',
                                        'Sm.µg.l' = 'Sm',
                                        'V.µg.l' = 'V',
                                        'Dy.µg.l' = 'Dy',
                                        'Te.µg.l' = 'Te',
                                        'Rb.µg.l' = 'Rb',
                                        'Ta.µg.l' = 'Ta',
                                        'Zn.µg.l' = 'Zn',
                                        'Gd.µg.l' = 'Gd',
                                        'Tl.µg.l' = 'Tl',
                                        'Lu.µg.l' = 'Lu',
                                        'Si.µg.l' = 'Si',
                                        'Mg.µg.l' = 'Mg',
                                        'F.µg.l' = 'F',
                                        'U.µg.l' = 'U',
                                        'Pt.µg.l' = 'Pt',
                                        'Er.µg.l' = 'Er',
                                        'Zr.µg.l' = 'Zr',
                                        'K.µg.l' = 'K',
                                        'Hg.µg.l' = 'Hg',
                                        'Ho.µg.l' = 'Ho',
                                        'Th.µg.l' = 'Th',
                                        'I.µg.l' = 'I',
                                        'Fe.µg.l' = 'Fe',
                                        'Au.µg.l' = 'Au',
                                        'Ni.µg.l' = 'Ni',
                                        'Hf.µg.l' = 'Hf',
                                        'Sn.µg.l' = 'Sn',
                                        'Sr.µg.l' = 'Sr',
                                        'S.µg.l' = 'S',
                                        'La.µg.l' = 'La',
                                        'Ir.µg.l' = 'Ir',
                                        'Pd.µg.l' = 'Pb',
                                        #'V µg/l_1' = 'V',
                                        'Pb.µg.l' = 'Pd',
                                        'Al.µg.l' = 'Al',
                                        'Ag.µg.l' = 'Ag',
                                        'Ba.µg.l' = 'Ba',
                                        'Ge.µg.l' = 'Ge',
                                        'Mo.µg.l' = 'Mo',
                                        'Pr.µg.l' = 'Pr',
                                        'As.µg.l' = 'As',
                                        'Yb.µg.l' = 'Yb',
                                        'Sb.µg.l' = 'Sb',
                                        'Eu.µg.l' = 'Eu',
                                        'Rh.µg.l' = 'Rh',
                                        'W.µg.l' = 'W',
                                        'Re.µg.l' = 'Re',
                                        'Ga.µg.l' = 'Ga'),
                         data_col_pattern = '#V#',
                         convert_to_BDL_flag = '<LOD',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_bdl = 'BDL',
                            variable_flags_dirty = 'DIRTY',
                            variable_flags_to_drop = 'DROP')


    d <- ms_conversions(d,
                        convert_units_from = c('NO2_N' = 'ug/l',
                                               # 'NO3_NO2_N' = 'ug/l',
                                               'NH4_N' = 'ug/l',
                                               'NO3_N' = 'ug/l',
                                               # 'SO4' = 'ug/l',
                                               # 'DIN' = 'ug/l',
                                               'PO4_P' = 'ug/l',
                                               'SO4_S' = 'ug/l',
                                               # 'TP' = 'ug/l',
                                               'Cu' = 'ug/l',
                                               'Cs' = 'ug/l',
                                               'Mn' = 'ug/l',
                                               'Na' = 'ug/l',
                                               'Bi' = 'ug/l',
                                               'Co' = 'ug/l',
                                               'Cl' = 'ug/l',
                                               'Cd' = 'ug/l',
                                               'Ca' = 'ug/l',
                                               'Ce' = 'ug/l',
                                               'B' = 'ug/l',
                                               'Br' = 'ug/l',
                                               'Be' = 'ug/l',
                                               'Cr' = 'ug/l',
                                               'P' = 'ug/l',
                                               'Os' = 'ug/l',
                                               'Ru' = 'ug/l',
                                               'Nd' = 'ug/l',
                                               'Ga' = 'ug/l',
                                               'Gd' = 'ug/l',
                                               'Ti' = 'ug/l',
                                               'Tb' = 'ug/l',
                                               'Se' = 'ug/l',
                                               'Li' = 'ug/l',
                                               'Y' = 'ug/l',
                                               'Nb' = 'ug/l',
                                               'Tm' = 'ug/l',
                                               'Sc' = 'ug/l',
                                               'Sm' = 'ug/l',
                                               'Dy' = 'ug/l',
                                               'Te' = 'ug/l',
                                               'Rb' = 'ug/l',
                                               'Ta' = 'ug/l',
                                               'Zn' = 'ug/l',
                                               'Tl' = 'ug/l',
                                               'Lu' = 'ug/l',
                                               'Si' = 'ug/l',
                                               'Mg' = 'ug/l',
                                               'F' = 'ug/l',
                                               'U' = 'ug/l',
                                               'Pt' = 'ug/l',
                                               'Er' = 'ug/l',
                                               'Zr' = 'ug/l',
                                               'K' = 'ug/l',
                                               'Hg' = 'ug/l',
                                               'Ho' = 'ug/l',
                                               'Th' = 'ug/l',
                                               'I' = 'ug/l',
                                               'Fe' = 'ug/l',
                                               'Au' = 'ug/l',
                                               'Ni' = 'ug/l',
                                               'Hf' = 'ug/l',
                                               'Sn' = 'ug/l',
                                               'Sr' = 'ug/l',
                                               'S' = 'ug/l',
                                               'La' = 'ug/l',
                                               'Ir' = 'ug/l',
                                               'Pb' = 'ug/l',
                                               'V' = 'ug/l',
                                               'Pd' = 'ug/l',
                                               'Al' = 'ug/l',
                                               'Ag' = 'ug/l',
                                               'Ba' = 'ug/l',
                                               'Ge' = 'ug/l',
                                               'Mo' = 'ug/l',
                                               'Pr' = 'ug/l',
                                               'As' = 'ug/l',
                                               'Yb' = 'ug/l',
                                               'Sb' = 'ug/l',
                                               'Eu' = 'ug/l',
                                               'Rh' = 'ug/l',
                                               'W' = 'ug/l',
                                               'Re' = 'ug/l'),
                                               # 'CH4_C' = 'ug/l',
                                               # 'DIC' = 'ug/l',
                                               # 'CO2_C' = 'ug/l'),
                        convert_units_to = c('NO2_N' = 'mg/l',
                                             # 'NO3_NO2_N' = 'mg/l',
                                             'NH4_N' = 'mg/l',
                                             'NO3_N' = 'mg/l',
                                             # 'SO4' = 'mg/l',
                                             # 'DIN' = 'mg/l',
                                             'PO4_P' = 'mg/l',
                                             'SO4_S' = 'mg/l',
                                             # 'TP' = 'mg/l',
                                             'Cu' = 'mg/l',
                                             'Cs' = 'mg/l',
                                             'Mn' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'Bi' = 'mg/l',
                                             'Co' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'Cd' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Ce' = 'mg/l',
                                             'B' = 'mg/l',
                                             'Br' = 'mg/l',
                                             'Be' = 'mg/l',
                                             'Cr' = 'mg/l',
                                             'P' = 'mg/l',
                                             'Os' = 'mg/l',
                                             'Ru' = 'mg/l',
                                             'Nd' = 'mg/l',
                                             'Ga' = 'mg/l',
                                             'Gd' = 'mg/l',
                                             'Ti' = 'mg/l',
                                             'Tb' = 'mg/l',
                                             'Se' = 'mg/l',
                                             'Li' = 'mg/l',
                                             'Y' = 'mg/l',
                                             'Nb' = 'mg/l',
                                             'Tm' = 'mg/l',
                                             'Sc' = 'mg/l',
                                             'Sm' = 'mg/l',
                                             'V' = 'mg/l',
                                             'Dy' = 'mg/l',
                                             'Te' = 'mg/l',
                                             'Rb' = 'mg/l',
                                             'Ta' = 'mg/l',
                                             'Zn' = 'mg/l',
                                             'Tl' = 'mg/l',
                                             'Lu' = 'mg/l',
                                             'Si' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'F' = 'mg/l',
                                             'U' = 'mg/l',
                                             'Pt' = 'mg/l',
                                             'Er' = 'mg/l',
                                             'Zr' = 'mg/l',
                                             'K' = 'mg/l',
                                             'Hg' = 'mg/l',
                                             'Ho' = 'mg/l',
                                             'Th' = 'mg/l',
                                             'I' = 'mg/l',
                                             'Fe' = 'mg/l',
                                             'Au' = 'mg/l',
                                             'Ni' = 'mg/l',
                                             'Hf' = 'mg/l',
                                             'Sn' = 'mg/l',
                                             'Sr' = 'mg/l',
                                             'S' = 'mg/l',
                                             'La' = 'mg/l',
                                             'Ir' = 'mg/l',
                                             'Pb' = 'mg/l',
                                             'Pd' = 'mg/l',
                                             'Al' = 'mg/l',
                                             'Ag' = 'mg/l',
                                             'Ba' = 'mg/l',
                                             'Ge' = 'mg/l',
                                             'Mo' = 'mg/l',
                                             'Pr' = 'mg/l',
                                             'As' = 'mg/l',
                                             'Yb' = 'mg/l',
                                             'Sb' = 'mg/l',
                                             'Eu' = 'mg/l',
                                             'Rh' = 'mg/l',
                                             'W' = 'mg/l',
                                             'Re' = 'mg/l'),
                                             # 'CH4_C' = 'mg/l',
                                             # 'DIC' = 'mg/l',
                                             # 'CO2_C' = 'mg/l'),
                        keep_molecular = 'SO4')

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS003 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    rawfile <- list.files(rawfile, full.names = TRUE)

    d <- read.csv(rawfile,
                  stringsAsFactors = FALSE,
                  colClasses = 'character')

    colnames(d) <- str_remove_all(colnames(d), 'Â')

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT+2',
                         site_code_col = 'SiteID',
                         alt_site_code = list('Site1' = '1',
                                              'Site2' = '2',
                                              'Site4' = '4',
                                              'Site5' = '5',
                                              'Site6' = '6',
                                              'Site7' = '7',
                                              'Site9' = '9',
                                              'Site10' = '10',
                                              'Site12' = '12',
                                              'Site13' = '13',
                                              'Site14' = '14',
                                              'Site15' = '15',
                                              'Site16' = '16',
                                              'Site18' = '18',
                                              'Site20' = '20',
                                              'Site53' = '53',
                                              'Site54' = '54',
                                              'Site57' = '57',
                                              'Site58' = '58',
                                              'Site59' = '59',
                                              'Site60' = '60',
                                              'Site61' = '61',
                                              'Site62' = '62',
                                              'Site63' = '63',
                                              'Site64' = '64',
                                              'Site65' = '65',
                                              'Site66' = '66'),
                         data_cols =  c('EC.µS.cm' = 'spCond',
                                        'DOC.mg.C.l' = 'DOC',
                                        'NO2.N.NO3.N.µg.N.l' = 'NO3_NO2_N',
                                        'DIC.mg.l' = 'DIC',
                                        'tot.P.µg.P.l' = 'TP',
                                        'DIN.µg.l' = 'DIN',
                                        'pH...25.C.' = 'pH',
                                        'TOC.mg.C.l' = 'TOC',
                                        'NO2.N.µg.N.l' = 'NO2_N',
                                        'NH4.N.µg.N.l' = 'NH4_N',
                                        'NO3.N.µg.N.l' = 'NO3_N',
                                        'tot.N.mg.N.l' = 'TN',
                                        'SO4.SO4.µg.SO4.l' = 'SO4',
                                        'CH4.C.µg.l' = 'CH4_C',
                                        'PO4.P.µg.P.l' = 'PO4_P',
                                        'S.SO4.µg.S.l' = 'SO4_S',
                                        'CO2.C.mg.l' = 'CO2_C',
                                        'Cu.µg.l' = 'Cu',
                                        'Cs.µg.l' = 'Cs',
                                        'Mn.µg.l' = 'Mn',
                                        'Na.µg.l' = 'Na',
                                        'Bi.µg.l' = 'Bi',
                                        'Co.µg.l' = 'Co',
                                        'Cl.µg.l' = 'Cl',
                                        'Cd.µg.l' = 'Cd',
                                        'Ca.µg.l' = 'Ca',
                                        'Ce.µg.l' = 'Ce',
                                        'B.µg.l' = 'B',
                                        'Br.µg.l' = 'Br',
                                        'Be.µg.l' = 'Be',
                                        'Cr.µg.l' = 'Cr',
                                        'P.µg.l' = 'P',
                                        'Os.µg.l' = 'Os',
                                        'Ru.µg.l' = 'Ru',
                                        'Nd.µg.l' = 'Nd',
                                        #'Gd µg/l' = 'Gd',
                                        'Ti.µg.l' = 'Ti',
                                        'Tb.µg.l' = 'Tb',
                                        'Se.µg.l' = 'Se',
                                        'Li.µg.l' = 'Li',
                                        'Y.µg.l' = 'Y',
                                        'Nb.µg.l' = 'Nb',
                                        'Tm.µg.l' = 'Tm',
                                        'Sc.µg.l' = 'Sc',
                                        'Sm.µg.l' = 'Sm',
                                        'V.µg.l' = 'V',
                                        'Dy.µg.l' = 'Dy',
                                        'Te.µg.l' = 'Te',
                                        'Rb.µg.l' = 'Rb',
                                        'Ta.µg.l' = 'Ta',
                                        'Zn.µg.l' = 'Zn',
                                        'Gd.µg.l' = 'Gd',
                                        'Tl.µg.l' = 'Tl',
                                        'Lu.µg.l' = 'Lu',
                                        'Si.µg.l' = 'Si',
                                        'Mg.µg.l' = 'Mg',
                                        'F.µg.l' = 'F',
                                        'U.µg.l' = 'U',
                                        'Pt.µg.l' = 'Pt',
                                        'Er.µg.l' = 'Er',
                                        'Zr.µg.l' = 'Zr',
                                        'K.µg.l' = 'K',
                                        'Hg.µg.l' = 'Hg',
                                        'Ho.µg.l' = 'Ho',
                                        'Th.µg.l' = 'Th',
                                        'I.µg.l' = 'I',
                                        'Fe.µg.l' = 'Fe',
                                        'Au.µg.l' = 'Au',
                                        'Ni.µg.l' = 'Ni',
                                        'Hf.µg.l' = 'Hf',
                                        'Sn.µg.l' = 'Sn',
                                        'Sr.µg.l' = 'Sr',
                                        'S.µg.l' = 'S',
                                        'La.µg.l' = 'La',
                                        'Ir.µg.l' = 'Ir',
                                        'Pd.µg.l' = 'Pb',
                                        #'V µg/l_1' = 'V',
                                        'Pb.µg.l' = 'Pd',
                                        'Al.µg.l' = 'Al',
                                        'Ag.µg.l' = 'Ag',
                                        'Ba.µg.l' = 'Ba',
                                        'Ge.µg.l' = 'Ge',
                                        'Mo.µg.l' = 'Mo',
                                        'Pr.µg.l' = 'Pr',
                                        'As.µg.l' = 'As',
                                        'Yb.µg.l' = 'Yb',
                                        'Sb.µg.l' = 'Sb',
                                        'Eu.µg.l' = 'Eu',
                                        'Rh.µg.l' = 'Rh',
                                        'W.µg.l' = 'W',
                                        'Re.µg.l' = 'Re',
                                        'Ga.µg.l' = 'Ga'),
                         data_col_pattern = '#V#',
                         convert_to_BDL_flag = '<LOD', #confirmed: only non-numeric value in any data column
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_bdl = 'BDL')

    d <- ms_conversions(d,
                        convert_units_from = c('NO2_N' = 'ug/l',
                                               'NO3_NO2_N' = 'ug/l',
                                               'NH4_N' = 'ug/l',
                                               'NO3_N' = 'ug/l',
                                               'SO4' = 'ug/l',
                                               'DIN' = 'ug/l',
                                               'PO4_P' = 'ug/l',
                                               'SO4_S' = 'ug/l',
                                               'TP' = 'ug/l',
                                               'Cu' = 'ug/l',
                                               'Cs' = 'ug/l',
                                               'Mn' = 'ug/l',
                                               'Na' = 'ug/l',
                                               'Bi' = 'ug/l',
                                               'Co' = 'ug/l',
                                               'Cl' = 'ug/l',
                                               'Cd' = 'ug/l',
                                               'Ca' = 'ug/l',
                                               'Ce' = 'ug/l',
                                               'B' = 'ug/l',
                                               'Br' = 'ug/l',
                                               'Be' = 'ug/l',
                                               'Cr' = 'ug/l',
                                               'P' = 'ug/l',
                                               'Os' = 'ug/l',
                                               'Ru' = 'ug/l',
                                               'Nd' = 'ug/l',
                                               'Ga' = 'ug/l',
                                               'Gd' = 'ug/l',
                                               'Ti' = 'ug/l',
                                               'Tb' = 'ug/l',
                                               'Se' = 'ug/l',
                                               'Li' = 'ug/l',
                                               'Y' = 'ug/l',
                                               'Nb' = 'ug/l',
                                               'Tm' = 'ug/l',
                                               'Sc' = 'ug/l',
                                               'Sm' = 'ug/l',
                                               'V' = 'ug/l',
                                               'Dy' = 'ug/l',
                                               'Te' = 'ug/l',
                                               'Rb' = 'ug/l',
                                               'Ta' = 'ug/l',
                                               'Zn' = 'ug/l',
                                               'Tl' = 'ug/l',
                                               'Lu' = 'ug/l',
                                               'Si' = 'ug/l',
                                               'Mg' = 'ug/l',
                                               'F' = 'ug/l',
                                               'U' = 'ug/l',
                                               'Pt' = 'ug/l',
                                               'Er' = 'ug/l',
                                               'Zr' = 'ug/l',
                                               'K' = 'ug/l',
                                               'Hg' = 'ug/l',
                                               'Ho' = 'ug/l',
                                               'Th' = 'ug/l',
                                               'I' = 'ug/l',
                                               'Fe' = 'ug/l',
                                               'Au' = 'ug/l',
                                               'Ni' = 'ug/l',
                                               'Hf' = 'ug/l',
                                               'Sn' = 'ug/l',
                                               'Sr' = 'ug/l',
                                               'S' = 'ug/l',
                                               'La' = 'ug/l',
                                               'Ir' = 'ug/l',
                                               'Pb' = 'ug/l',
                                               'Pd' = 'ug/l',
                                               'Al' = 'ug/l',
                                               'Ag' = 'ug/l',
                                               'Ba' = 'ug/l',
                                               'Ge' = 'ug/l',
                                               'Mo' = 'ug/l',
                                               'Pr' = 'ug/l',
                                               'As' = 'ug/l',
                                               'Yb' = 'ug/l',
                                               'Sb' = 'ug/l',
                                               'Eu' = 'ug/l',
                                               'Rh' = 'ug/l',
                                               'W' = 'ug/l',
                                               'Re' = 'ug/l',
                                               'CH4_C' = 'ug/l',
                                               'DIC' = 'ug/l',
                                               'CO2_C' = 'ug/l'),
                        convert_units_to = c('NO2_N' = 'mg/l',
                                             'NO3_NO2_N' = 'mg/l',
                                             'NH4_N' = 'mg/l',
                                             'NO3_N' = 'mg/l',
                                             'SO4' = 'mg/l',
                                             'DIN' = 'mg/l',
                                             'PO4_P' = 'mg/l',
                                             'SO4_S' = 'mg/l',
                                             'TP' = 'mg/l',
                                             'Cu' = 'mg/l',
                                             'Cs' = 'mg/l',
                                             'Mn' = 'mg/l',
                                             'Na' = 'mg/l',
                                             'Bi' = 'mg/l',
                                             'Co' = 'mg/l',
                                             'Cl' = 'mg/l',
                                             'Cd' = 'mg/l',
                                             'Ca' = 'mg/l',
                                             'Ce' = 'mg/l',
                                             'B' = 'mg/l',
                                             'Br' = 'mg/l',
                                             'Be' = 'mg/l',
                                             'Cr' = 'mg/l',
                                             'P' = 'mg/l',
                                             'Os' = 'mg/l',
                                             'Ru' = 'mg/l',
                                             'Nd' = 'mg/l',
                                             'Ga' = 'mg/l',
                                             'Gd' = 'mg/l',
                                             'Ti' = 'mg/l',
                                             'Tb' = 'mg/l',
                                             'Se' = 'mg/l',
                                             'Li' = 'mg/l',
                                             'Y' = 'mg/l',
                                             'Nb' = 'mg/l',
                                             'Tm' = 'mg/l',
                                             'Sc' = 'mg/l',
                                             'Sm' = 'mg/l',
                                             'V' = 'mg/l',
                                             'Dy' = 'mg/l',
                                             'Te' = 'mg/l',
                                             'Rb' = 'mg/l',
                                             'Ta' = 'mg/l',
                                             'Zn' = 'mg/l',
                                             'Tl' = 'mg/l',
                                             'Lu' = 'mg/l',
                                             'Si' = 'mg/l',
                                             'Mg' = 'mg/l',
                                             'F' = 'mg/l',
                                             'U' = 'mg/l',
                                             'Pt' = 'mg/l',
                                             'Er' = 'mg/l',
                                             'Zr' = 'mg/l',
                                             'K' = 'mg/l',
                                             'Hg' = 'mg/l',
                                             'Ho' = 'mg/l',
                                             'Th' = 'mg/l',
                                             'I' = 'mg/l',
                                             'Fe' = 'mg/l',
                                             'Au' = 'mg/l',
                                             'Ni' = 'mg/l',
                                             'Hf' = 'mg/l',
                                             'Sn' = 'mg/l',
                                             'Sr' = 'mg/l',
                                             'S' = 'mg/l',
                                             'La' = 'mg/l',
                                             'Ir' = 'mg/l',
                                             'Pb' = 'mg/l',
                                             'Pd' = 'mg/l',
                                             'Al' = 'mg/l',
                                             'Ag' = 'mg/l',
                                             'Ba' = 'mg/l',
                                             'Ge' = 'mg/l',
                                             'Mo' = 'mg/l',
                                             'Pr' = 'mg/l',
                                             'As' = 'mg/l',
                                             'Yb' = 'mg/l',
                                             'Sb' = 'mg/l',
                                             'Eu' = 'mg/l',
                                             'Rh' = 'mg/l',
                                             'W' = 'mg/l',
                                             'Re' = 'mg/l',
                                             'CH4_C' = 'mg/l',
                                             'DIC' = 'mg/l',
                                             'CO2_C' = 'mg/l'),
                        keep_molecular = 'SO4')

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#stream_temperature: STATUS=READY
#. handle_errors
process_1_VERSIONLESS004 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    rawfile <- list.files(rawfile, full.names = TRUE)

    d <- read_delim(rawfile, delim = '\t', skip = 4, col_types = cols(.default = "c"))

    d <- d %>%
        rename(temp_c = 3)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT+2',
                         site_code_col = 'SiteID',
                         alt_site_code = list('Site1' = '1',
                                              'Site2' = '2',
                                              'Site4' = '4',
                                              'Site5' = '5',
                                              'Site6' = '6',
                                              'Site7' = '7',
                                              'Site9' = '9',
                                              'Site10' = '10',
                                              'Site12' = '12',
                                              'Site13' = '13',
                                              'Site14' = '14',
                                              'Site15' = '15',
                                              'Site16' = '16',
                                              'Site18' = '18',
                                              'Site20' = '20',
                                              'Site53' = '53',
                                              'Site54' = '54',
                                              'Site57' = '57',
                                              'Site58' = '58',
                                              'Site59' = '59',
                                              'Site60' = '60',
                                              'Site61' = '61',
                                              'Site62' = '62',
                                              'Site63' = '63',
                                              'Site64' = '64',
                                              'Site65' = '65',
                                              'Site66' = '66'),
                         data_cols =  c('temp_c' = 'temp'),
                         data_col_pattern = '#V#',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#ws_boundary: STATUS=READY
#. handle_errors
process_1_VERSIONLESS005 <- function(network, domain, prodname_ms, site_code, component) {

    rawdir = glue('data/{n}/{d}/raw/{p}/{s}',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code)

    file_path <- list.files(rawdir, full.names = TRUE)

    temp_path <- tempdir()

    zipped_files <- unzip(zipfile = file_path,
                          exdir = temp_path,
                          overwrite = TRUE)

    projstring <- choose_projection(unprojected = TRUE)

    d <- sf::st_read(glue('{tp}/ws_boundary',
                          tp = temp_path),
                     stringsAsFactors = FALSE,
                     quiet = TRUE) %>%
        select(site_code = Huvudomr,
               geometry = geometry) %>%
        mutate(site_code = paste0('Site', site_code))

    # Watershed boundaries don't include subbasins of if they are also gauges
    # comnining them here
    single_sites <- d %>%
        filter(site_code %in% c('Site1', 'Site2', 'Site3', 'Site4', 'Site5',
                                'Site8', 'Site10', 'Site20', 'Site21', 'Site22'))

    sites6 <- d %>%
        filter(site_code %in% c('Site6', 'Site5')) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        fill_sf_holes() %>%
        mutate(site_code = 'Site6') %>%
        rename(geometry = x)

    sites7 <- d %>%
        filter(site_code %in% c('Site7', 'Site2', 'Site4')) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        fill_sf_holes() %>%
        mutate(site_code = 'Site7') %>%
        rename(geometry = x)

    sites9 <- d %>%
        filter(site_code %in% c('Site9', 'Site6', 'Site2', 'Site7', 'Site4', 'Site5')) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        fill_sf_holes() %>%
        mutate(site_code = 'Site9') %>%
        rename(geometry = x)

    sites12 <- d %>%
        filter(site_code %in% c('Site12', 'Site1', 'Site3', 'Site10')) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        fill_sf_holes() %>%
        mutate(site_code = 'Site12') %>%
        rename(geometry = x)

    sites13 <- d %>%
        filter(site_code %in% c('Site13', 'Site8')) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        mutate(site_code = 'Site13') %>%
        rename(geometry = x)

    sites14 <- d %>%
        filter(site_code %in% c('Site14', 'Site21', 'Site20')) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        #fill_sf_holes() %>%
        mutate(site_code = 'Site14') %>%
        rename(geometry = x)

    sites15 <- d %>%
        filter(site_code %in% c('Site15', 'Site22')) %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        #fill_sf_holes() %>%
        mutate(site_code = 'Site15') %>%
        rename(geometry = x)

    sites16 <- d %>%
        sf::st_union() %>%
        sf::st_as_sf() %>%
        #fill_sf_holes() %>%
        mutate(site_code = 'Site16') %>%
        rename(geometry = x)

    all_sheds <- rbind(sites16, sites15, sites6, sites7, sites12, sites13,
                       sites14, sites9, single_sites) %>%
        sf::st_transform(., crs = projstring)

    kry_gauges <- site_data %>%
        filter(network == 'krycklan',
               in_workflow == 1,
               site_type == 'stream_gauge') %>%
        pull(site_code)

    all_sheds <- all_sheds %>%
        filter(site_code %in% !!kry_gauges)

    unlink(zipped_files)

    sites <- unique(all_sheds$site_code)

    for(s in 1:length(sites)){

        d_site <- all_sheds %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = TRUE)
    }

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_1_VERSIONLESS006 <- function(network, domain, prodname_ms, site_code, component) {


    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code)

    rawfile <- list.files(rawfile, full.names = TRUE)

    d <- read_delim(rawfile, delim = ';', col_types = cols(.default = "c")) %>%
        mutate(site = 'Svartberget') %>%
        rename(precip = 2) %>%
        filter(! is.na(Datum))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Datum' = '%Y-%m-%d'),
                         datetime_tz = 'Etc/GMT+2',
                         site_code_col = 'site',
                         data_cols =  c('precip' = 'precipitation'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('-', 'mm', 'Dygntotal', 'NaN'),
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- qc_hdetlim_and_uncert(d, prodname_ms = prodname_ms)

    d <- synchronize_timestep(d)

    sites <- unique(d$site_code)

    for(s in 1:length(sites)){

        d_site <- d %>%
            filter(site_code == !!sites[s])

        write_ms_file(d = d_site,
                      network = network,
                      domain = domain,
                      prodname_ms = prodname_ms,
                      site_code = sites[s],
                      level = 'munged',
                      shapefile = FALSE)
    }

    return()
}

#derive kernels ####

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__VERSIONLESS003',
                                           'stream_temperature__VERSIONLESS004'))

    return()
}

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms003 <- precip_gauge_from_site_data

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_precip_pchem_pflux

