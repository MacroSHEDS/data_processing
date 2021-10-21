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

prev_huc4 <- 'none'
for(i in c(205:total_len)){

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


# get list of COMIDs and USGS gage numbers to send to Parker Norton ####

camqd <- '/home/mike/git/macrosheds/qa_experimentation/data/CAMELS/basin_dataset_public_v1p2/usgs_streamflow'

qf <- list.files(camqd, recursive = TRUE)
camels_basins <- str_match(qf, '^[0-9]+/([0-9]+)_streamflow_qc.txt$')[, 2]

write_csv(tibble(usgs_gage_id = camels_basins),
          '~/git/macrosheds/qa_experimentation/data/site_id_lists/priority3_gageIDs.csv')

filt <- sites %>%
    select(domain, site_code, NHD_COMID) %>%
    filter(! NHD_COMID %in% c('nonCONUS', 'too small', 'HR only'))

priority1 <- filt %>%
    filter(domain == 'neon') %>%
    select(NHD_COMID)

priority2 <- filt %>%
    filter(domain != 'neon') %>%
    select(NHD_COMID)

write_csv(priority1,
          '~/git/macrosheds/qa_experimentation/data/site_id_lists/priority1_COMIDs.csv')

write_csv(priority2,
          '~/git/macrosheds/qa_experimentation/data/site_id_lists/priority2_COMIDs.csv')
