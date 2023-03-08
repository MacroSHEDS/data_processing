## #retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_VERSIONLESS001 <- download_from_googledrive

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_VERSIONLESS002 <- function(set_details, network, domain) {
    # query built on MCES portal, this by WS 20230307
    mces_query <- "https://eims.metc.state.mn.us/Download?startDate=01-01-0001&endDate=12-31-9999&siteIds=FC0002;EA0008;SA0082;VA0010;RI0013;VR0020;BA0022;BS0019;BR0003;SI0001;CR0009;CA0017;BL0035;BE0020;NM0018;PU0039&excludeParameterIds=2191;2196;2194;2273;2197;2208;2093;2147;2199;2236;2251;2253;2255;2257;2259;2261;2274;2263;2265;2266;2268;2270&format=csv"

    raw_data_dest <- glue('data/{n}/{d}/raw/{p}/{s}',
                          n = network,
                          d = domain,
                          p = prodname_ms,
                          s = set_details$site_code)

    dir.create(path = raw_data_dest,
               showWarnings = FALSE,
               recursive = TRUE)

    rawfile <- glue('{rd}/{c}.csv',
                    rd = raw_data_dest,
                    c = set_details$component)

    R.utils::downloadFile(
             ## url = "http://eims.metc.state.mn.us/Download?startDate=01-01-0001&endDate=12-31-9999&siteIds=FC0002;EA0008;SA0082;VA0010;RI0013;VR0020;BA0022;BS0019;BR0003;SI0001;CR0009;CA0017;BL0035;BE0020;NM0018;PU0039&excludeParameterIds=2191;2196;2194;2273;2197;2208;2093;2147;2199;2236;2251;2253;2255;2257;2259;2261;2274;2263;2265;2266;2268;2270&format=csv",
             url = set_details$url,
             filename = rawfile,
             skip = FALSE,
             overwrite = TRUE,
             method = 'libcurl')


    res <- httr::HEAD(set_details$url)

    last_mod_dt <- strptime(x = substr(res$headers$`last-modified`,
                                       start = 1,
                                       stop = 19),
                            format = '%Y-%m-%dT%H:%M:%S') %>%
        with_tz(tzone = 'UTC')

    deets_out <- list(url = paste(set_details$url, ''),
                      access_time = as.character(with_tz(Sys.time(),
                                                         tzone = 'UTC')),
                      last_mod_dt = last_mod_dt)

    return(deets_out)
}

## #munge kernels ####
#discharge: STATUS=READY
#. handle_errors
process_1_VERSIONLESS001 <- function(network, domain, prodname_ms, site_code, component) {

    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.xlsx',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d_sheets <- readxl::excel_sheets(rawfile)

    for(sheet in d_sheets) {
      this_d <- readxl::read_xlsx(rawfile, sheet = sheet)

        if(any(grepl('Discharge', names(this_d)))) {

          this_d <- this_d %>%
              mutate(site_code = sheet)

          if(!exists('d_sheets_combined')) {
            d_sheets_combined <- this_d
          } else {
            d_sheets_combined <- rbind(d_sheets_combined, this_d)
          }
        }
    }



}

#stream_chemistry: STATUS=READY
#. handle_errors
process_1_VERSIONLESS002 <- function(network, domain, prodname_ms, site_code, component) {
    rawfile <- glue('data/{n}/{d}/raw/{p}/{s}/{c}.csv',
                    n = network,
                    d = domain,
                    p = prodname_ms,
                    s = site_code,
                    c = component)

    d <- read.csv(rawfile)
}

#derive kernels ####
#stream_flux_inst: STATUS=READY
#. handle_errors
process_2_ms002 <- derive_stream_flux

#stream_gauge_locations: STATUS=READY
#. handle_errors
process_2_ms004 <- stream_gauge_from_site_data
