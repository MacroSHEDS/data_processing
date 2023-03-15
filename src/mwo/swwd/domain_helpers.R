# MWO SWWD domain helpers
# list of sites needed
# (potential) shared system for pulling in SWWD based off of pattern found in download link URL
get_url_swwd_prod <- function(site_name, prodname = 'stream_chem') {
  swwd_wq_url_base <- 'http://wq.swwdmn.org/data'

  if(prodname == 'stream_chem') {
    prod_url_ext <- 'manual/table.xlsx'
  } else if (prodname == 'discharge') {
    prod_url_ext <- 'gauge/table.xlsx'
  } else {
    stop('invalid prodname')
  }

  swwd_wq_url_download <- glue::glue('{base}/{s}/{p}',
                                     base = swwd_wq_url_base,
                                     s = site_name,
                                     p = prod_url_ext
                                     )
  return(swwd_wq_url_download)
}
