aa <- tibble()
bb <- tibble()
for(ss in neon_streams){
    print(ss)
    aa_ <- stackByTable_keep_zips(glue('data/neon/neon/raw/stream_chemistry__DP1.20093.001/{ss}/filesToStack{neonprodcode}'))
    # if('swc_externalLabDataByAnalyte' %in% names(aa_)){
    #     print('gg')
    #     aa <- bind_rows(aa, aa_$swc_externalLabDataByAnalyte)
    # }
    if('swc_externalLabSummaryData' %in% names(aa_)){
        print('gg')
        bb <- bind_rows(bb, aa_$swc_externalLabSummaryData)
    } else {
        warning('!!!')
    }
}

# write_csv(aa,'/tmp/all_neon_chem.csv')
# aa=read_csv('/tmp/all_neon_chem.csv')

#how common is missing unit?
missing_unit <- filter(aa, is.na(analyteUnits) & ! grepl('UV Abs|pH', analyte))
message(paste('dropping', nrow(missing_unit), 'records with unspecified units (total', nrow(aa), ')'))

aa <- aa %>%
    filter(! is.na(analyteUnits) | grepl('UV Abs|pH', analyte))

#are there any dupe units?
var_unit_pairs <- distinct(aa, analyte, analyteUnits) %>%
    filter(! is.na(analyteUnits)) %>%
    arrange(analyte)
any(duplicated(var_unit_pairs))

#any conflicting detection limits?
bb2 = bb %>%
# rawd$swc_externalLabSummaryData %>%
    distinct(analyte, analyteUnits, methodDetectionLimit, startDate, endDate) %>%
    arrange(analyte, analyteUnits)

View(bb2)

bb3 = bb2 %>%
    group_by(analyte) %>%
    mutate(
        overlapL = map2_int(startDate, endDate, ~{
            sum(.x < bb2$startDate & .y > bb2$startDate) > 0
        }),
        overlapR = map2_int(startDate, endDate, ~{
            sum(.x < bb2$endDate & .y > bb2$endDate) > 0
        }),
        overlapO = map2_int(startDate, endDate, ~{
            sum(.x < bb2$startDate & .y > bb2$endDate) > 0
        }),
        overlapI = map2_int(startDate, endDate, ~{
            sum(.x > bb2$startDate & .y < bb2$endDate) > 0
        }),
    ) %>%
    # summarize(blurgh = across(starts_with('overlap'), ~ . > 0)) %>%
    ungroup()
