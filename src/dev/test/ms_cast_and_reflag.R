
        fff=tibble(datetime=as.POSIXct(1:5, origin=as.Date('1970-01-01')), site_code='BC_SW_12',
               `GN_Mn__|dat`=runif(5, 0, 1), `GN_Mn__|flg`=c('', NA, 'B', 'A', 'C'),
               `GN_NH4__|dat`=runif(5, 0, 1), `GN_NH4__|flg`=c('', NA, 'Z', 'B', 'C'),
               test_flag=c('B', 'Z', 'C', NA, ''))
    ms_cast_and_reflag(fff,
                       variable_flags_dirty='C',
                       variable_flags_to_drop='Z',
                       variable_flags_bdl='B',
                       summary_flags_to_drop = list(test_flag='Z'),
                       summary_flags_dirty = list(test_flag='C'),
                       summary_flags_bdl = list(test_flag='B'))
