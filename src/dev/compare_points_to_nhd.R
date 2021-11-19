library(tidyverse)
library(nhdplusTools)
library(sf)
library(googlesheets4)
library(ggplot2)
library(mapview)
mv <- mapview::mapview

options(timeout = 5000)

nhd_hr_dir <- '~/git/macrosheds/data_acquisition/data/general/nhd_hr'
mapview_save_dir <- '~/git/macrosheds/data_acquisition/output/sites_vs_NHD'
mapview_save_dir2 <- '~/git/macrosheds/data_acquisition/output/sites_vs_NHM'

#setup ####

buf <- function(site, buf_dist){

    site_buf <- sf::st_buffer(x = site,
                              dist = buf_dist)
    site_box <- st_bbox(site_buf)

    subset_file <- tempfile(fileext = '.gpkg')

    subset <- try({
        nhdplusTools::subset_nhdplus(bbox = site_box,
                                     output_file = subset_file,
                                     nhdplus_data = 'download',
                                     return_data = TRUE,
                                     overwrite = FALSE,
                                     out_prj = 4326) %>%
            suppressMessages()
    }, silent = TRUE)

    if(inherits(subset, 'try-error') || ! length(subset)){# || nrow(subset[[1]]) < 2){
        print('incrementing buffer distance by 500')
        buf_dist <- buf_dist + 500
        if(buf_dist > 5000) stop()
        buf(site = site, buf_dist)
    } else {
        return(list(subset = subset, box = site_box))
    }
}

# site_csv <- read_csv('~/git/macrosheds/data_acquisition/data/general/site_data.csv')
site_csv <- suppressMessages(googlesheets4::read_sheet(
    'https://docs.google.com/spreadsheets/d/1Xd38tvB0upHjDRDa5qalGN2Ors6HHWZKpT4Bw0dDqU4/edit?usp=drive_web&ouid=111793152718629438266',
    na = c('', 'NA'),
    col_types = 'ccccccccnnnnnccc'
))

sites <- filter(site_csv,
                in_workflow == 1,
                site_type == 'stream_gauge')

sites$NHD_COMID <- '?'
manual_input <- 1:233
total_len <- nrow(sites)
sites$NHD_COMID[manual_input] <- c('HR only', 'too small', 'HR only', '6729679',
                                   'HR only', 'HR only', '6729787', 'HR only',
                                   'too small', '23773411', 'HR only', 'HR only',
                                   'HR only', 'HR only', 'HR only', 'HR only',
                                   'HR only', 'HR only', '23774053', '3424530',
                                   '18548462', '18211220', '1239639', '3649284',
                                   '8444872', '698676', '22050327', '7690025',#28
                                   '24505800', '22048111', 'HR only', '20440650',
                                   '2889452', '2964310', '1306285', '23773423',#COMO, BLDE, PRIN, MCRA,
                                   'nonCONUS', 'nonCONUS', '11722717', 'nonCONUS',#first two AK, last PR: '800035089'
                                   'nonCONUS', 'HR only', '18841314', '18208464', #first PR 800026322
                                   'HR only', '22144520', '18841358', 'HR only',#48
                                   '18841356', 'HR only', '11689212', '11688596',#... GFCP, GFGB
                                   '11688596', '11689106', 'too small', '11689186',#GFVN on same reach as GFGB
                                   '11688418', 'too small', '11689206', '2889280',
                                   '2889770', '2889186', 'too small', 'HR only',#GREEN4...
                                   'too small', '17595459', '17595361', '17595359',#68
                                   '17595433', '17594763', '17594741', '17595453', #same as next
                                   '17595453', '17594769', '17594785', '17595305',
                                   '17596097', '17595477', '17595369', '17595473',
                                   '17596159', '17595473', '17596161', 'nonCONUS', #bonanza, then mcmurdo
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', '5860599', '5862611', #plum
                                   '5862611', '5862581', 'nonCONUS', 'nonCONUS',#112
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS', #arctic
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',#140
                                   'nonCONUS', 'nonCONUS', '2889384', '2889360', #boulder
                                   '2889410', '2891254', 'too small', '17827556',
                                   '17827556', '17826162', '17826162', '17827558',
                                   '17826228', 'HR only', 'HR only', 'too small',
                                   'HR only', '17826228', 'HR only', 'HR only',#160
                                   'HR only', 'HR only', 'nonCONUS', 'nonCONUS', #luquillo
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'HR only', 'too small', 'too small',
                                   '3775221', 'too small', 'too small', 'too small',
                                   'HR only', 'too small', 'HR only', '1332754',#180
                                   '1332754', '1332672', '1332674', '1332674',
                                   'HR only', 'HR only', '1332198', '1332190',
                                   '1332186', '1332186', 'HR only', '1332204',
                                   '1332224', '13633173', 'HR only', 'HR only',
                                   'HR only', 'HR only', 'HR only', 'HR only',#200
                                   'HR only', '22050299', '22050299', '22050323', #krew. some questionable point-segment associations in here
                                   '23903201', 'HR only', 'HR only', 'HR only',
                                   '9643235', '9643251', '9643235', 'HR only',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS', #krycklan
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',
                                   'nonCONUS', 'nonCONUS', 'nonCONUS', 'nonCONUS',#224
                                   'nonCONUS', '22125024', 'HR only', '4681928',
                                   '4682266', '4682628', 'too small', '2679458',
                                   '2679458')
# for(i in seq_len(nrow(sites))){

# loop 1: NHDPlusV2 or NHD-HR (kinda obsolete) ####

#this loop is for identifying whether a point is on the NHDPlusV2 or the NHD-HR,
#   or neither. for NHM seg_ids, see the next loop
prev_huc4 <- 'none'
for(i in 1:total_len){

    print('---')
    print(i)
    site <- sites[i, ]
    dmn <- site$domain
    site_code <- site$site_code

    print(paste(dmn, site_code))

    if(site$domain %in% c('arctic', 'bonanza', 'luquillo', 'krycklan', 'mcmurdo') ||
       site$site_code %in% c('CARI', 'OKSR')){
        print('not in CONUS')
        next
    }

    site <- sf::st_point(c(site$longitude, site$latitude)) %>%
                         sf::st_sfc(crs = 4326)
    # mv(site)

    nextt <- FALSE
    comid <- tryCatch({
        nhdplusTools::discover_nhdplus_id(site)
    }, error = function(e) nextt <<- TRUE)
    if(nextt) {
        print('couldnt find comid')
        next
    }

    # flowline <- nhdplusTools::navigate_nldi(list(featureSource = 'comid',
    #                                              featureID = comid),
    #                                         mode = 'upstreamTributaries',
    #                                         data_source = '')

    out <- suppressWarnings(buf(site = site, 1000))
    subset <- out$subset
    site_box <- out$box

    huc12 <- get_huc12(site)
    print(huc12$huc12)
    huc4 <- substr(huc12$huc12, 1, 4)[1]
    nhdplusTools::download_nhdplushr(nhd_hr_dir,
                                     hu_list = huc4) %>%
        invisible()

    if(huc4 != prev_huc4){
        HRflowlines <- nhdplusTools::get_nhdplushr(file.path(nhd_hr_dir,
                                                             substr(huc4, 1, 2)),
                                                             # paste0('NHDPLUS_H_',
                                                             #        huc4,
                                                             #        '_HU4_GDB.gdb')),
                                                   file.path(nhd_hr_dir,
                                                             paste0(huc4, '.gpkg')),
                                                   layers = 'NHDFlowline',
                                                   proj = 4326)$NHDFlowline
    } else {
        print('using previous NHD HR HUC')
    }

    prev_huc4 <- huc4

    NHD_HR <- suppressWarnings(sf::st_crop(HRflowlines, site_box))

    NHDPlus <- subset$NHDFlowline_Network
    # catchments <- subset$CatchmentSP
    # upstream <- nhdplusTools::get_UT(flowlines, comid)

    dist_to_nearest_NHDPlus_flowline <- min(st_distance(NHDPlus, site))
    print(paste('Dist to NHDPlus:', round(dist_to_nearest_NHDPlus_flowline, 2), 'm'))
    dist_to_nearest_NHDHR_flowline <- min(st_distance(NHD_HR, site))
    print(paste('Dist to NHD_HR:', round(dist_to_nearest_NHDHR_flowline, 2), 'm'))

    xx = mv(NHD_HR, color = 'darkslategray3') + mv(NHDPlus, color='deepskyblue4') + mv(site, color='red')
    mapview_save_path <- file.path(mapview_save_dir,
                                   paste0(dmn, '_', site_code, '.html'))
    mapview::mapshot(xx,
                     url = mapview_save_path)
    print(paste('map saved to', mapview_save_path))
    print(xx)

    # gg <- ggplot() +
    #     geom_sf(data = NHD_HR, color = 'darkslategray3') +
    #     geom_sf(data = NHDPlus, color = 'deepskyblue4') +
    #     geom_sf(data = site, color = 'red') +
    #     coord_sf()
    #
    # print(gg)

    system('spd-say "chili chili chili"')
    x <- readline(cat('This point is on: [A] an NHDPlus flowline, [B] an NHD_HR flowline, or [C] neither >\n'))

    if(x == 'A'){
        sites[i, 'NHD_COMID'] <- as.character(comid)
        print(comid)
    } else if(x == 'B'){
        sites[i, 'NHD_COMID'] <- 'HR only'
    } else if(x == 'C'){
        sites[i, 'NHD_COMID'] <- 'too small'
    } else {
        stop(paste("'A', 'B', or 'C'"))
    }
}

#TODO save back to googlesheets

# load NHM GDB and filter sites ####

#where are the sites with COMIDs?
sites <- sites %>%
    # filter(! NHD_COMID %in% c('nonCONUS', 'too small', 'HR only')) %>%
    st_as_sf(coords = c('longitude', 'latitude')) %>%
    st_set_crs(4326)

mv(sites)

#NHDV1 regions: 01, 02, 03, 04, 05, 06, 07, 08, 10U, 10L, 11, 14-15, 17, 18

# v1_flowlines <- st_read('NHDPlus01/Hydrography/nhdflowline.shp') %>%
#     st_set_crs(4326)
# mv(st_zm(v1_flowlines)) + mv(comid_sites[1, ])

nhm <- st_read(dsn = '~/git/macrosheds/qa_experimentation/data/NHMv1/GF_nat_reg.gdb',
               layer = 'nsegmentNationalIdentifier') %>%
    st_transform(4326)


# loop 2: NHM v1 ####

#for NHM seg_ids. skips sites that weren't identified as coinciding with
#the NHDPlusV2 above. completed NHM_SEGID vector commented below

sites$NHM_SEGID <- 'non-NHDPlus'
for(i in 60:total_len){

    if(sites$NHD_COMID[i] %in% c('nonCONUS', 'too small', 'HR only')) next

    print('---')
    print(i)
    site <- sites[i, ]
    dmn <- site$domain
    site_code <- site$site_code
    print(paste(dmn, site_code))

    site_buf <- sf::st_buffer(x = site,
                              dist = 10000)
    site_box <- st_bbox(site_buf)

    nhm_crop <- suppressWarnings(sf::st_crop(nhm, site_box))

    closest_ind <- which.min(st_distance(nhm_crop, site))
    segid <- nhm_crop[closest_ind, ]$seg_id_nat

    xx <- mv(nhm_crop) + mv(sites[i, ])
    mapview_save_path <- file.path(mapview_save_dir2,
                                   paste0(dmn, '_', site_code, '.html'))
    mapview::mapshot(xx,
                     url = mapview_save_path)
    print(paste('map saved to', mapview_save_path))
    print(xx)

    # system('spd-say "strudel"')
    x <- readline(cat('This point is: [A] on an NHMV1 flowline, or [B] not >\n'))

    if(x == 'A'){
        sites[i, 'NHM_SEGID'] <- as.character(segid)
        print(sites[, c('domain', 'site_code', 'NHD_COMID', 'NHM_SEGID')], n = i)
        print(segid)
    } else if(x == 'B'){
        sites[i, 'NHM_SEGID'] <- 'too small'
    } else {
        stop(paste("'A', 'B', or 'C'"))
    }
}

paste(sites$NHM_SEGID, collapse = "', '")
# sites$NHM_SEGID <- c('non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'too small',
# 'non-NHDPlus', 'non-NHDPlus', 'too small', 'non-NHDPlus', 'non-NHDPlus', '50262',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'too small', '5021', '8815', '8998', 'too small', 'too small',
# 'too small', '34201', 'too small', '1097', 'too small', '55986', 'non-NHDPlus', '45211',
# 'too small', 'too small', '38288', 'too small', 'non-NHDPlus', 'non-NHDPlus', '26516',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', '27753', 'too small', 'non-NHDPlus', 'too small',
# 'too small', 'non-NHDPlus', '27754', 'non-NHDPlus', '3847', '3825', '3825', '3842', 'non-NHDPlus',
# '3846', '3820', 'non-NHDPlus', 'too small', 'too small', 'too small', 'too small', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', '56455', '55409', 'too small', 'too small', '55400', 'too small',
# '55410', '55410', 'too small', 'too small', 'too small', '55411', 'too small', 'too small',
# 'too small', '55417', 'too small', 'too small', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'too small',
# 'too small', 'too small', '606', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'too small',
# 'too small', '24503', '24510', 'non-NHDPlus', 'too small', 'too small', 'too small', 'too small', 'too small',
# 'too small', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'too small', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'too small', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', '42949', '42949', 'too small', '42949', '42949', 'non-NHDPlus',
# 'non-NHDPlus', 'too small', 'too small', 'too small', '42949', 'non-NHDPlus', 'too small', 'too small', 'too small',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'too small',
# 'too small', 'too small', 'too small', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', '6647', 'too small', '6647',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus', 'non-NHDPlus',
# 'too small', 'non-NHDPlus', 'too small', '2073', '2073', 'non-NHDPlus', 'too small', 'too small')

# save results; get list of COMIDs/SEGIDs and USGS gage numbers to send to Parker Norton ####

# write_csv(sites, '~/git/macrosheds/qa_experimentation/data/site_id_lists/sites_COMIDS_SEGIDS.csv')
sites <- read_csv('~/git/macrosheds/qa_experimentation/data/site_id_lists/sites_COMIDS_SEGIDS.csv')

camqd <- '/home/mike/git/macrosheds/qa_experimentation/data/CAMELS/basin_dataset_public_v1p2/usgs_streamflow'

qf <- list.files(camqd, recursive = TRUE)
camels_basins <- str_match(qf, '^[0-9]+/([0-9]+)_streamflow_qc.txt$')[, 2]

write_csv(tibble(usgs_gage_id = camels_basins),
          '~/git/macrosheds/qa_experimentation/data/site_id_lists/priority3_gageIDs.csv')
#
# #OBSOLETE first cut. this assumed NHMv1 could reference NHDPlusV2 COMIDS
# filt <- sites %>%
#     select(domain, site_code, NHD_COMID) %>%
#     filter(! NHD_COMID %in% c('nonCONUS', 'too small', 'HR only'))
# sites %>%
#     select(domain, site_code, NHD_COMID) %>%
#     filter(NHD_COMID %in% c('HR only'))
# sites %>%
#     select(domain, site_code, NHD_COMID) %>%
#     filter(NHD_COMID %in% c('nonCONUS'))
# sites %>%
#     select(domain, site_code, NHD_COMID) %>%
#     filter(NHD_COMID %in% c('too small'))
#
# priority1 <- filt %>%
#     filter(domain == 'neon') %>%
#     select(NHD_COMID)
#
# priority2 <- filt %>%
#     filter(domain != 'neon') %>%
#     select(NHD_COMID)
#
# write_csv(priority1,
#           '~/git/macrosheds/qa_experimentation/data/site_id_lists/priority1_COMIDs.csv')
#
# write_csv(priority2,
#           '~/git/macrosheds/qa_experimentation/data/site_id_lists/priority2_COMIDs.csv')

#second cut. uses seg_id_nat from the actual NHM flowlines
filt <- sites %>%
    select(domain, site_code, NHM_SEGID) %>%
    filter(! NHM_SEGID %in% c('non-NHDPlus', 'too small'))
sites %>%
    select(domain, site_code, NHM_SEGID) %>%
    filter(NHM_SEGID %in% c('non-NHDPlus')) %>%
    nrow() #138
sites %>%
    select(domain, site_code, NHM_SEGID) %>%
    filter(NHM_SEGID %in% c('too small')) %>%
    nrow() #58
nrow(filt) #37

write_csv(data.frame(NHM_SEGID = filt$NHM_SEGID),
          '~/git/macrosheds/qa_experimentation/data/site_id_lists/seg_id_nats.csv')

#map MS sites to NHMv1 segids

left_join(filt, site_csv)
