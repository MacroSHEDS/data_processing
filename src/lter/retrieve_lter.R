# library(RCurl)
# library(tidyverse)
# library(googlesheets)
# library(tidylog)

#PASTA terminology:
#packageId ex: knb-lter-arc.1226.2 where arc=site, 1226=identifier, 2=revision
#each package contains 1 or more elements (e.g. datasets) with elementIds

lter_download = function(lter_dir, dmn){

    #src_df must have the following columns (CURRENTLY GRABBED FROM GSHEETS):
        #site: LTER site code,
        #identifier: LTER data product id,
        #macrosheds_version: the currently held LTER package version
            #set to -1 initially, so that it will be overwritten on first run.
        #domain: must match subdir name for domain in lter folder,
        #pretty_name: will be printed to the user (in bookdown)
        #type: dateset category, e.g. "snow depth"
        #in_workflow: 0=ignored, 1=included (currently handled in global env)

    #lter_dir is the local parent folder for all lter domains,
        #e.g. HJ Andrews, HBEF, etc.
        # should be specifiec as absolute path

    #dmn must match subdir name for domain in lter folder.
        #this will also be used to filter src_df

    `%>%` = magrittr::`%>%`
    dl_endpoint = 'https://pasta.lternet.edu/package/data/eml/'
    name_endpoint = 'https://pasta.lternet.edu/package/name/eml/'
    vsn_endpoint = 'https://pasta.lternet.edu/package/eml/'

    #get site, identifier, version, etc information from gsheets
    #this is temporary. should replace google sheet with db table eventually
    #to prevent collisions, circumvent possibility of network errors.
    gsheet = googlesheets::gs_title('lter_package_ids')
    src_df = googlesheets::gs_read(gsheet)
    src_df_filt = tidylog::filter(src_df, in_workflow == 1)
    src_df_filt = tidylog::filter(src_df_filt, domain == dmn)

    for(i in 1:nrow(src_df_filt)){

        site = src_df_filt$site[i]
        identifier = src_df_filt$identifier[i]
        ms_version = src_df_filt$macrosheds_version[i]

        #get current highest version of data package within LTER PASTA
        vsn_request = paste0(vsn_endpoint, site, '/', identifier)
        lter_version = RCurl::getURLContent(vsn_request)
        lter_version = as.numeric(stringr::str_match(lter_version,
            '[0-9]+$')[1]) #tidyfy this

        new_vsn_available = ms_version < lter_version
        if(new_vsn_available){

            #get IDs and names of elements within data package
            name_request = paste0(name_endpoint, site, '/', identifier, '/',
                lter_version)
            reqdata = RCurl::getURLContent(name_request)
            reqdata = strsplit(reqdata, '\n')[[1]]
            reqdata = stringr::str_match(reqdata, '([0-9a-zA-Z]+),([0-9a-zA-Z]+)')
            element_ids = reqdata[,2]
            element_names = reqdata[,3]

            #delete any previously downloaded, obsolete files for this package
            rawdir = paste0(lter_dir, '/', dmn, '/raw/', src_df_filt$type[i])
            unlink(rawdir, recursive=TRUE)
            dir.create(rawdir, showWarnings=FALSE, recursive=TRUE)

            #acquire new files
            for(j in 1:length(element_names)){
                data_request = paste0(dl_endpoint, site, '/', identifier, '/',
                    lter_version)
                rawfile = paste0(rawdir, '/', element_names[j], '.csv')
                download.file(url=paste0(data_request, '/', element_ids[j]),
                    destfile=rawfile, cacheOK=FALSE, method='curl')
            }

            addtl_info = ifelse(new_vsn_available && ms_version != -1,
                paste0('\n\tRemoved version ', ms_version, '\n'), '\n')
            cat(paste0(
                i, ': Downloaded ', src_df_filt$type[i], ' version ',
                lter_version, ' (', src_df_filt$pretty_name[i], ') to\n\t',
                rawdir, addtl_info
            )) #will cat() work in bookdown?

            #update held version number
            src_df[src_df$site == site & src_df$identifier == identifier,
                'macrosheds_version'] = lter_version

        } else {

            cat(paste0(
                i, ': Skipped ', src_df_filt$type[i], ' version ', lter_version,
                ' (', src_df_filt$pretty_name[i], '). Already latest version.\n'
            ))
        }

    }

    #update vsn numbers in gsheets
    #this is temporary. should replace google sheet with db table eventually
    #to prevent collisions, circumvent possibility of network errors.
    googlesheets::gs_edit_cells(gsheet, input=src_df)
}
