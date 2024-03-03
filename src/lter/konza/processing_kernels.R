#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_7 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_8 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_9 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_10 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_50 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_51 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_20 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_21 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_16 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')
    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_43 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')
    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_4 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.csv')
    return()
}

#precip_gauge_locations; stream_gauge_locations: STATUS=READY
#. handle_errors
process_0_230 <- function(set_details, network, domain){

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = '.zip')
    return()
}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_7 <- function(network, domain, prodname_ms, site_code,
                        component){

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code,
                     c = component)

    d <- read.csv(rawfile1, colClasses = "character")

    d <- d %>%
        mutate(num_d = nchar(RECDAY)) %>%
        mutate(num_m = nchar(RECMONTH)) %>%
        mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
        mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('RECYEAR' = '%Y',
                                              'month' = '%m',
                                              'day' = '%d'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'WATERSHED',
                         alt_site_code = list('N04D' = 'n04d'),
                         data_cols =  c('MEANDISCHARGE' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                              'INCOMPLETE_FLAG'),
                         is_sensor = TRUE,
                         set_to_NA = '.')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                         'MAINTENANCE_FLAG' = '1',
                                                         'INCOMPLETE_FLAG' = '1'),
                            summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                       'MAINTENANCE_FLAG' = c('0', NA),
                                                       'INCOMPLETE_FLAG' = c('0', NA)))

    # Convert from cm/s to liters/s
    d <- d %>%
      mutate(val = val*1000)

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_8 <- function(network, domain, prodname_ms, site_code,
                        component){

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code,
                     c = component)

    d <- read.csv(rawfile1, colClasses = "character")

    d <- d %>%
        mutate(num_d = nchar(RECDAY)) %>%
        mutate(num_m = nchar(RECMONTH)) %>%
        mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
        mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('RECYEAR' = '%Y',
                                              'month' = '%m',
                                              'day' = '%d'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'WATERSHED',
                         alt_site_code = list('N20B' = 'n20b'),
                         data_cols =  c('MEANDISCHARGE' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                              'INCOMPLETE_FLAG'),
                         is_sensor = TRUE,
                         set_to_NA = '.')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                         'MAINTENANCE_FLAG' = '1',
                                                         'INCOMPLETE_FLAG' = '1'),
                            summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                       'MAINTENANCE_FLAG' = c('0', NA),
                                                       'INCOMPLETE_FLAG' = c('0', NA)))

    # Convert from cm/s to liters/s
    d <- d %>%
        mutate(val = val*1000)

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_9 <- function(network, domain, prodname_ms, site_code,
                        component){

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code,
                     c = component)

    d <- read.csv(rawfile1, colClasses = "character")

    d <- d %>%
        mutate(num_d = nchar(RECDAY)) %>%
        mutate(num_m = nchar(RECMONTH)) %>%
        mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
        mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('RECYEAR' = '%Y',
                                              'month' = '%m',
                                              'day' = '%d'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'WATERSHED',
                         alt_site_code = list('N01B' = 'n01b'),
                         data_cols =  c('MEANDISCHARGE' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                              'INCOMPLETE_FLAG'),
                         is_sensor = TRUE,
                         set_to_NA = '.')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                         'MAINTENANCE_FLAG' = '1',
                                                         'INCOMPLETE_FLAG' = '1'),
                            summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                       'MAINTENANCE_FLAG' = c('0', NA),
                                                       'INCOMPLETE_FLAG' = c('0', NA)))

    # Convert from cm/s to liters/s
    d <- d %>%
        mutate(val = val*1000)

    return(d)
}

#discharge: STATUS=READY
#. handle_errors
process_1_10 <- function(network, domain, prodname_ms, site_code,
                         component){

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code,
                     c = component)

    d <- read.csv(rawfile1, colClasses = "character")

    d <- d %>%
      mutate(num_d = nchar(RECDAY)) %>%
      mutate(num_m = nchar(RECMONTH)) %>%
      mutate(day = ifelse(num_d == 1, paste0('0', as.character(RECDAY)), as.character(RECDAY))) %>%
      mutate(month = ifelse(num_m == 1, paste0('0', as.character(RECMONTH)), as.character(RECMONTH)))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('RECYEAR' = '%Y',
                                              'month' = '%m',
                                              'day' = '%d'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'WATERSHED',
                         alt_site_code = list('N02B' = 'n02b'),
                         data_cols =  c('MEANDISCHARGE' = 'discharge'),
                         data_col_pattern = '#V#',
                         summary_flagcols = c('QUAL_FLAG', 'MAINTENANCE_FLAG',
                                              'INCOMPLETE_FLAG'),
                         is_sensor = TRUE,
                         set_to_NA = '.')

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list('QUAL_FLAG' = '1',
                                                         'MAINTENANCE_FLAG' = '1',
                                                         'INCOMPLETE_FLAG' = '1'),
                            summary_flags_clean = list('QUAL_FLAG' = c('0', NA),
                                                       'MAINTENANCE_FLAG' = c('0', NA),
                                                       'INCOMPLETE_FLAG' = c('0', NA)))

    # Convert from cm/s to liters/s
    d <- d %>%
      mutate(val = val*1000)

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_50 <- function(network, domain, prodname_ms, site_code,
                         component){

    rawfile1 <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                     n = network,
                     d = domain,
                     p = prodname_ms,
                     s = site_code,
                     c = component)

    d <- read.csv(rawfile1,
                  colClasses = 'character',
                  quote = '')

    d <- d %>%
        as_tibble() %>%
        mutate(RecTime = ifelse(RecTime == '.', 1200, RecTime)) %>%
        mutate(num_t = nchar(RecTime)) %>%
        mutate(num_d = nchar(RecDay)) %>%
        mutate(time = case_when(num_t == 1 ~ paste0('010', RecTime),
                                num_t == 2 ~ paste0('01', RecTime),
                                num_t == 3 ~ paste0('0', RecTime),
                                num_t == 4 ~ as.character(RecTime),
                                is.na(num_t) ~ '1200')) %>%
        mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
        select(-num_t, -RecTime, -num_d, -RecDay)

    NO3_codes <- c('No3 Below det limit', 'NO3 < det limit', 'NO3<det limit', 'no3 < det limit',
        'no3 below det limit', 'no3 and tp < det limit',
        'no3 not detectable', 'no3 below det limit', 'bison upstream; no3 < det limit 90.5 0',
        'no3 < det limit(0.4)', 'no3 < det limit(0.0);  tp < srp', 'no3 < det limit(0.4);  tnp=3x',
        'no3 below det', 'no3 < det limit; doc is OK-ran@1:1;  broke ice at 9am..',
        'no3 < det limit; broke ice at 9am; sample at 1128', 'no3 < det limit; only ditch from K1a flowing',
        'no3 < det limit; took from large pool below tube', 'no3 below det;  murky water',
        'no3 < det limit; tnp=c', 'no3<detection limit', 'NO3 < det limit',
        'new equipment installed; NO3 < det limit', 'NO3<det limit', 'lots of leaf litter and NO3 < det limit')

    NH4_codes <- c('NH4 <det limit', 'nh4<det limit', 'no height; frozen over;nh4<det limit')

    TN_codes <- c()

    SRP_codes <- c('srp < det limit', 'srp < det limit; doc = 2x', 'tank frozen over; srp/tp<detection limit',
                   'mininimal flow particulates in sample; srp<detection limit; no/srpx3',
                   'srp<detection limit', 'rain/mist during sampling; srp<detection limit')

    TP_codes <- c('slightly dirty=snowmelt. TP below Det limit', 'TP below Det limit',
                  'nh4=2X. TP below Det limit', 'nh4=2X; bison in area on 01/29. TP below Det limit',
                  'TP<Det limit', 'tp < det limit', 'no3 and tp < det limit',
                  'tp< det limit', 'SF: tp < det limit', 'tp(2.1) < det limit',
                  'nh4 = 3x; doc=2x(b); tn=b; tp < det limit', 'tp < det limit; mist while sampling',
                  'tp < det limit; tank putting off a sulfur smell', 'tnp=3x; tp < det limit; cleaned algae build-up from pipe; pool smells of sulfur',
                  'nh4=2x(b); tn=b; tp < det limit', 'tp (2.5) < det limit', 'tp(2.6) < det limit',
                  'tp(2.9)<det limit; broke ice to get ht', 'tnp=2x(c); tp < det limit',
                  'tp(2.5) < det limit;  samples collected in morning but processed at 5:30pm - burn crew',
                  'tp < det limit; ice and algae', 'tp <det limit; algae and ice; had to break ice to get sample. Ice too thick to break to get height',
                  'no/srp=x2; tp < det limit', 'doc/no/srp = 2x;  tp < det limit; sulfur smell coming from tank; algae on pipe',
                  'tp<detection limit', 'tank frozen over; srp/tp<detection limit')

    TP_flags <- c('no3 < det limit(0.0);  tp < srp', 'tp < det limit; light rain when sampling; murky',
                  "\"srp>tp", 'srp/tpx2; srp>tp; water level has decreased dramatically; used ion syringe to ',
                  'srp>tp; tpx2; leaf build-up continues')

    COMMENTS_flags <- c(grep('bison', unique(d$COMMENTS), value = T), grep('cow|cows', unique(d$COMMENTS), value = T),
                        grep('experiment', unique(d$COMMENTS), value = T), grep('turkey', unique(d$COMMENTS), value = T),
                        'guy upstream sampling', grep('ducks', unique(d$COMMENTS), value = T),
                        grep('people|People', unique(d$COMMENTS), value = T))

    DOC_codes <- c()

    d <- d %>%
        mutate(TP = ifelse(COMMENTS %in% !!TP_codes & TP == '.', 'BDL', TP),
               NO3 = ifelse(COMMENTS %in% !!NO3_codes & NO3 == '.', 'BDL', NO3),
               NH4 = ifelse(COMMENTS %in% !!NH4_codes & NH4 == '.', 'BDL', NH4),
               SRP = ifelse(COMMENTS %in% !!SRP_codes & SRP == '.', 'BDL', SRP),
               TN = ifelse(COMMENTS %in% !!TN_codes & TN == '.', 'BDL', TN),
               DOC = ifelse(COMMENTS %in% !!DOC_codes & DOC == '.', 'BDL', DOC)
               ) %>%
        mutate(TP_code = ifelse(COMMENTS %in% !!TP_codes & TP != 'BDL', 'BDL', NA),
               NO3_code = ifelse(COMMENTS %in% !!NO3_codes & NO3 != 'BDL', 'BDL', NA),
               NH4_code = ifelse(COMMENTS %in% !!NH4_codes & NH4 != 'BDL', 'BDL', NA),
               SRP_code = ifelse(COMMENTS %in% !!SRP_codes & SRP != 'BDL', 'BDL', NA),
               TN_code = ifelse(COMMENTS %in% !!TN_codes & TN != 'BDL', 'BDL', NA),
               DOC_code = ifelse(COMMENTS %in% !!DOC_codes & DOC != 'BDL', 'BDL', NA)
        )  %>%
        mutate(TP_code = ifelse(COMMENTS %in% !!TP_flags & is.na(TP_code), 'dirty', TP_code)) %>%
        filter(WATERSHED %in% c('n04d', 'n02b', 'n20b', 'n01b', 'nfkc', 'hokn',
                                'sfkc', 'tube', 'kzfl', 'shan', 'hikx')) %>%
        mutate(comments = ifelse(COMMENTS %in% !!COMMENTS_flags, 1, 0))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('RecYear' = '%Y',
                                              'RecMonth' = '%m',
                                              'day' = '%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'WATERSHED',
                         alt_site_code = list('N04D' = 'n04d',
                                              'N02B' = 'n02b',
                                              'N20B' = 'n20b',
                                              'N01B' = 'n01b',
                                              'NFKC' = 'nfkc',
                                              'HOKN' = 'hokn',
                                              'SFKC' = 'sfkc',
                                              'TUBE' = 'tube',
                                              'KZFL' = 'kzfl',
                                              'SHAN' = 'shan',
                                              'HIKX' = 'hikx'),
                         data_cols =  c('NO3', 'NH4'='NH4_N', 'TN', 'SRP'='orthophosphate_P',
                                        'TP', 'DOC'),
                         data_col_pattern = '#V#',
                         var_flagcol_pattern = '#V#_code',
                         summary_flagcols = 'comments',
                         set_to_NA = c('.', ''),
                         convert_to_BDL_flag = c('BDL', 'bdl'),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            variable_flags_to_drop = 'ensuring other flags get ms_status of 0',
                            variable_flags_dirty = 'dirty',
                            variable_flags_bdl = 'BDL',
                            summary_flags_clean = list('comments' = c(0, NA)),
                            summary_flags_dirty = list('comments' = c(1)))

    # Discrepancy in konza meta data that list DOC as both mg/l and ug/l
    d <- ms_conversions(d,
                        # convert_molecules = c('NO3', 'SO4', 'PO4', 'SiO2',
                        #                       'NH4', 'NH3'),
                        convert_units_from = c(NO3 = 'ug/l', NH4_N = 'ug/l',
                                               TN = 'ug/l', orthophosphate_P = 'ug/l',
                                               TP = 'ug/l'),
                        convert_units_to = c(NO3 = 'mg/l', NH4_N = 'mg/l',
                                             TN = 'mg/l', orthophosphate_P = 'mg/l',
                                             TP = 'mg/l'))

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_51 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile1,
                         datetime_cols = list('RecYear' = '%Y',
                                              'RecMonth' = '%m',
                                              'RecDay' = '%d',
                                              'RecTime' = '%H%M'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'Site',
                         alt_site_code = list('N04D' = 'n04d',
                                              'N02B' = 'n02b',
                                              'N20B' = 'n20b',
                                              'N01B' = 'n01b'),
                         data_cols =  c('Conduct' = 'spCond'),
                         data_col_pattern = '#V#',
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA)

    d <- filter(d, site_code %in% c('N04D', 'N02B', 'N20B', 'N01B'))

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_20 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile1, colClasses = "character")

    d <- d %>%
      mutate(Rectime = ifelse(Rectime == '.', 1200, Rectime)) %>%
      mutate(num_t = nchar(Rectime)) %>%
      mutate(num_d = nchar(RecDay)) %>%
      mutate(time = case_when(num_t == 1 ~ paste0('010', Rectime),
                              num_t == 2 ~ paste0('01', Rectime),
                              num_t == 3 ~ paste0('0', Rectime),
                              num_t == 4 ~ as.character(Rectime),
                              is.na(num_t) ~ '1200')) %>%
      mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
      select(-num_t, -Rectime, -num_d, -RecDay)

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('RecYear' = '%Y',
                                              'RecMonth' = '%m',
                                              'day' = '%d',
                                              'time' = '%H%M'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'Watershed',
                         data_cols =  c('TSS', 'VSS'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'comments',
                         set_to_NA = c('.', ''),
                         is_sensor = FALSE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list(comments = 'ensuring all get ms_status=0'),
                            summary_flags_dirty = list(comments = 'ensuring all get ms_status = 0'))

    return(d)
}

#stream_chemistry: STATUS=PENDING
#. handle_errors
process_1_21 <- function(network, domain, prodname_ms, site_code,
                         component) {
    # Meta data says precip chem in everything (so idk what that's about)

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile1, colClasses = "character")

    d <- d %>%
      mutate(site = case_when(component == 'ASW011' ~ 'N02B',
                              component == 'ASW012' ~ 'N04D'))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('Date' = '%m/%d/%Y',
                                              'Time' = '%H:%M:%S'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'site',
                         data_cols =  c('SpCond' = 'spCond',
                                        'Temp' = 'temp',
                                        'pH' = 'pH',
                                        'ODO' = 'DO',
                                        'ODOsat' = 'DO_sat'),
                         data_col_pattern = '#V#',
                         set_to_NA = '',
                         summary_flagcols = 'Comments',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_to_drop = list('Comments' = 'REMOVE'),
                            summary_flags_clean = list('Comments' = c('')))

    # Convert from cm/s to liters/s
    d <- d %>%
      mutate(val = ifelse(var == 'IS_spCond', val*1000, val))

    return(d)
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_16 <- function(network, domain, prodname_ms, site_code,
                         component){


  rawfile = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                  n = network,
                  d = domain,
                  p = prodname_ms,
                  s = site_code,
                  c = component)

  d <- read.csv(rawfile, colClasses = "character") %>%
    mutate(RecDay = ifelse(nchar(RecDay) == 1, paste0(0, RecDay), RecDay))

  d <- ms_read_raw_csv(preprocessed_tibble = d,
                       datetime_cols = list('RecYear' = '%Y',
                                            'RecMonth' = '%m',
                                            'RecDay' = '%d'),
                       datetime_tz = 'US/Central',
                       site_code_col = 'Watershed',
                       alt_site_code = list('N04D' = 'n04d',
                                            'N02B' = 'n02b',
                                            'N01B' = 'n01b',
                                            'N20B' = 'n20b'),
                       data_cols =  c('Tmean' = 'temp'),
                       data_col_pattern = '#V#',
                       sampling_type = 'I',
                       is_sensor = TRUE)

  d <- ms_cast_and_reflag(d,
                          varflag_col_pattern = NA)

  return(d)
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_1_43 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile1, colClasses = "character")

    d <- d %>%
      mutate(num_d = nchar(RecDay)) %>%
      mutate(day = ifelse(num_d == 1, paste0('0', as.character(RecDay)), as.character(RecDay))) %>%
      select(-num_d, -RecDay)  %>%
      filter(! Watershed %in% c('001d', 'n01d', ''))

    d <- ms_read_raw_csv(preprocessed_tibble = d,
                         datetime_cols = list('RecYear' = '%Y',
                                              'RecMonth' = '%m',
                                              'day' = '%d'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'Watershed',
                         alt_site_code = list('002C' = c('R20B', '001c', 'r20b', '001c'),
                                              '020B' = '020b',
                                              'HQ' = c('00HQ', '00hq', 'hq'),
                                              'N4DF' = c('N04D', 'n04d'),
                                              'N01B' = 'n01b'),
                         data_cols =  c('NO3'='NO3_N', 'NH4'='NH4_N', 'TPsN'='TPN',
                                        'SRP'='orthophosphate_P', 'TPP'),
                         data_col_pattern = '#V#',
                         set_to_NA = c('', '.', ' '),
                         summary_flagcols = 'Comments',
                         is_sensor = FALSE)

    clean_comments <- is.na(d$Comments) |
        grepl('^(?:rain|snow|cloudy ?=? ?)?[0-9; \\)\\(\\/\\.=\\-]*$', d$Comments)
    # unique(na.omit(d[clean_comments,]$Comments)) #verify
    d$Comments <- 'dirty'
    d$Comments[clean_comments] <- 'clean'

    d <- ms_cast_and_reflag(d,
                            varflag_col_pattern = NA,
                            summary_flags_dirty = list(Comments = 'dirty'),
                            summary_flags_clean = list(Comments = 'clean'))

    d <- ms_conversions(d,
                        convert_units_from = c(NO3_N = 'ug/l',
                                               NH4_N = 'ug/l',
                                               TPsN = 'ug/l',
                                               orthophosphate_P = 'ug/l',
                                               TPP = 'ug/l'),
                        convert_units_to = c(NO3_N = 'mg/l',
                                             NH4_N = 'mg/l',
                                             TPsN = 'mg/l',
                                             orthophosphate_P = 'mg/l',
                                             TPP = 'mg/l'))

    return(d)
}

#precipitation: STATUS=READY
#. handle_errors
process_1_4 <- function(network, domain, prodname_ms, site_code,
                         component) {

    rawfile1 = glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- ms_read_raw_csv(filepath = rawfile1,
                         datetime_cols = list('RecDate' = '%m/%e/%Y'),
                         datetime_tz = 'US/Central',
                         site_code_col = 'watershed',
                         alt_site_code = list('HQ' = 'HQ02'),
                         data_cols =  c('ppt' = 'precipitation'),
                         data_col_pattern = '#V#',
                         summary_flagcols = 'Comments',
                         set_to_NA = c('.'),
                         # sampling_type = 'I',
                         is_sensor = TRUE)

    d <- ms_cast_and_reflag(
        d,
        varflag_col_pattern = NA,
        summary_flags_clean = list(Comments = c('', ' ', '8.0mm due')),
        summary_flags_to_drop = list(Comments = 'just making sure every other flag ends up as ms_status 1')
    )

    return(d)
}

#precip_gauge_locations; stream_gauge_locations: STATUS=READY
#. handle_errors
process_1_230 <- function(network, domain, prodname_ms, site_code,
                        component) {

    rawzip <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.zip',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    rawpath <- glue('data/{n}/{d}/raw/{p}/{s}',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    zipped_files <- unzip(zipfile = rawzip,
                          exdir = rawpath,
                          overwrite = TRUE)

    projstring <- choose_projection(unprojected = TRUE)

    if(prodname_ms == 'precip_gauge_locations__230') {
    gauges <- sf::st_read(paste0(rawpath, '/', component)) %>%
      mutate(site_code = case_when(RAINGAUGE == 'PPTSE' ~'002C',
                                   RAINGAUGE == 'PPT4B' ~ '004B',
                                   RAINGAUGE == 'PPTUB' ~ '020B',
                                   RAINGAUGE == 'PPTK4' ~ 'K01B',
                                   RAINGAUGE == 'PPTN1B' ~ 'N01B',
                                   RAINGAUGE == 'PPTN2B' ~ 'N02B',
                                   RAINGAUGE == 'PPTN4FL' ~ 'N4DF',
                                   RAINGAUGE == 'PPTN4PC' ~ 'N4DU',
                                   RAINGAUGE == 'PPTUA' ~ 'R01A',
                                   RAINGAUGE == 'PPTHQ2' ~ 'HQ02')) %>%
      filter(! is.na(site_code)) %>%
      select(site_code, geometry) %>%
      sf::st_transform(projstring) %>%
      arrange(site_code)
    } else {
      gauges <- sf::st_read(paste0(rawpath, '/', component)) %>%
        filter(! is.na(DATES_SAMP),
               STATION != 'ESH',
               STATION != 'ESF') %>%
        select(site_code = STATION, geometry) %>%
        sf::st_transform(projstring) %>%
        arrange(site_code)
    }

    unlink(zipped_files)

    return(gauges)
}

#derive kernels ####

#discharge: STATUS=READY
#. handle_errors
process_2_ms001 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('discharge__7',
                                           'discharge__9',
                                           'discharge__8',
                                           'discharge__10'))
    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_2_ms002 <- function(network, domain, prodname_ms) {

    combine_products(network = network,
                     domain = domain,
                     prodname_ms = prodname_ms,
                     input_prodname_ms = c('stream_chemistry__50',
                                           'stream_chemistry__51',
                                           'stream_chemistry__20',
                                           'stream_chemistry__16'))

    return()
}

#precip_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms005 <- precip_gauge_from_site_data

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms006 <- stream_gauge_from_site_data

#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms003 <- derive_stream_flux

#precip_pchem_pflux: STATUS=READY
#. handle_errors
process_2_ms004 <- derive_precip_pchem_pflux
