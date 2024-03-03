
fff=tibble(datetime=as.POSIXct(1:5, origin=as.Date('1970-01-01')), site_code='BC_SW_12',
       `GN_Mn__|dat`=1:5, `GN_Mn__|flg`=c('', NA, 'A', 'B', 'C'),
       `GN_NH4__|dat`=1:5, `GN_NH4__|flg`=c('', NA, 'X', 'Y', 'Z'),
       sum_flag=c('1', '2', '3', NA, ''),
       sum_flag2=c('1', '2', '3', NA, '')
)

ms_cast_and_reflag(fff,
                   varflag_col_pattern = NA,
                   # summary_flags_clean = list(sum_flag=c('1', '2'), sum_flag2 = '3'),
                   # summary_flags_dirty = list(sum_flag=c('3'), sum_flag2 = ''),
                   # summary_flags_to_drop = list(sum_flag=c(''), sum_flag2 = c('1', '2')),
                   summary_flags_clean = list(sum_flag='1'),
                   summary_flags_dirty = list(sum_flag='2'),
                   summary_flags_to_drop = list(sum_flag='3'),
                   summary_flags_bdl = list(sum_flag='')
)

ms_cast_and_reflag(fff,
                   variable_flags_clean=c('A', 'X', 'Y', ''),
                   # variable_flags_clean='A',
                   variable_flags_dirty='C',
                   variable_flags_to_drop='Z',
                   variable_flags_bdl='B'
)

tibble(x = c(1, NA, 3), y = c(NA, NA, 3)) %>%
    # filter(x %in% 1:2)
    filter(! x %in% 1:2)
