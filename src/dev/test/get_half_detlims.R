d = tibble(datetime = as.Date(c('1998-01-02', '1988-01-01', '1999-01-01', '2000-01-01',
                                '2000-01-01', '2000-01-01', '2000-01-01', '1950-01-01', '2023-01-01')),
           site_code = 'chili',
           var = c('GN_Fe', 'GN_Mn', 'GN_Mn', 'GN_Al', 'GN_Cl', 'GN_Ca', 'GN_qq', 'GN_Mn', 'GN_Mn'),
           val = 1:9,
           ms_status = 2)
prodname_ms = 'stream_chemistry__VERSIONLESS003'
domain = 'walker_branch'
which_='hdetlim'

#for testing case 1 with start and end dates
detlims = domain_detection_limits
detlims = detlims[34:38,]
detlims = bind_rows(detlims, tibble(domain = c('hbef', 'walker_branch', 'walker_branch',
                                               'walker_branch', 'walker_branch', 'walker_branch',
                                               'walker_branch'),
                                    prodcode = c('stream_chemistry__VERSIONLESS003',
                                                 'aa', 'bb', 'aa', 'bb',
                                                 'stream_chemistry__VERSIONLESS003',
                                                 'stream_chemistry__VERSIONLESS003'),
                                    variable_converted = c('Fe', 'Fe', 'Fe', 'Cl', 'Cl', 'DOC', 'Zn'),
                                    detection_limit_converted = 10,
                                    precision = 1,
                                    start_date = NA,
                                    end_date = NA))

#for testing case 1 with no dates
detlims$start_date = detlims$end_date = NA
detlims = detlims[c(1, 3:12),]

