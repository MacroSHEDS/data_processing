# general kernels #### 

#npp: STATUS=READY 
#. handle_errors
process_3_ms005 <- function(network, domain, prodname_ms, ws_boundry) {
  
  ue(get_gee_standard(network=network, 
                      domain=domain, 
                      gee_id='UMT/NTSG/v2/LANDSAT/NPP', 
                      band='annualNPP', 
                      prodname='npp', 
                      rez=30,
                      ws_boundry=ws_boundry))
  return()
}

#gpp: STATUS=READY 
#. handle_errors
process_3_ms006 <- function(network, domain, prodname_ms, ws_boundry) {
  
  ue(get_gee_standard(network=network, 
                      domain=domain, 
                      gee_id='UMT/NTSG/v2/LANDSAT/GPP', 
                      band='GPP', 
                      prodname='gpp', 
                      rez=30,
                      ws_boundry=ws_boundry))
  return()
}

#lai; fpar: STATUS=READY 
#. handle_errors
process_3_ms007 <- function(network, domain, prodname_ms, ws_boundry) {
  
  if(prodname_ms == 'lai__ms007') {
    ue(get_gee_standard(network=network, 
                        domain=domain, 
                        gee_id='MODIS/006/MOD15A2H', 
                        band='Lai_500m', 
                        prodname='lai', 
                        rez=500,
                        ws_boundry=ws_boundry))
  }
  
  if(prodname_ms == 'fpar__ms007') {
    ue(get_gee_standard(network=network, 
                        domain=domain, 
                        gee_id='MODIS/006/MOD15A2H', 
                        band='Fpar_500m', 
                        prodname='fpar', 
                        rez=500,
                        ws_boundry=ws_boundry))
  }
  return()
}

#tree_cover; veg_cover; bare_cover: STATUS=READY 
#. handle_errors
process_3_ms008 <- function(network, domain, prodname_ms, ws_boundry) {
  
  if(prodname_ms == 'tree_cover__ms008') {
    ue(get_gee_standard(network=network, 
                        domain=domain, 
                        gee_id='MODIS/006/MOD44B', 
                        band='Percent_Tree_Cover', 
                        prodname='tree_cover', 
                        rez=500,
                        ws_boundry=ws_boundry))
  }
  
  if(prodname_ms == 'veg_cover__ms008') {
    ue(get_gee_standard(network=network, 
                        domain=domain, 
                        gee_id='MODIS/006/MOD44B', 
                        band='Percent_NonTree_Vegetation', 
                        prodname='veg_cover', 
                        rez=500,
                        ws_boundry=ws_boundry))
  }
  
  if(prodname_ms == 'bare_cover__ms008') {
    ue(get_gee_standard(network=network, 
                        domain=domain, 
                        gee_id='MODIS/006/MOD44B', 
                        band='Percent_NonVegetated', 
                        prodname='bare_cover', 
                        rez=500,
                        ws_boundry=ws_boundry))
  }
  return()
}

#prism_precip; prism_temp_mean: STATUS=READY 
#. handle_errors
process_3_ms009 <- function(network, domain, prodname_ms, ws_boundry) {
  
  if(prodname_ms == 'prism_precip__ms009') {
    ue(get_gee_standard(network=network, 
                        domain=domain, 
                        gee_id='OREGONSTATE/PRISM/AN81d', 
                        band='ppt', 
                        prodname='prism_precip', 
                        rez=4000,
                        ws_boundry=ws_boundry))
  }
  
  if(prodname_ms == 'prism_temp_mean__ms009') {
    ue(get_gee_standard(network=network, 
                        domain=domain, 
                        gee_id='OREGONSTATE/PRISM/AN81d', 
                        band='tmean', 
                        prodname='prism_temp_mean', 
                        rez=4000,
                        ws_boundry=ws_boundry))
  }
  return()
}

#start_season; end_season; max_season: STATUS=READY 
#. handle_errors 
process_3_ms010 <- function(network, domain, prodname_ms, ws_boundry) {
 
  if(prodname_ms == 'start_season__ms010') {
    sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                  time='start_season', ws_boundry=ws_boundry))
  }
 
 if(prodname_ms == 'max_season__ms010') {
   sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                          time='max_season', ws_boundry=ws_boundry))
 }
 
 if(prodname_ms == 'end_season__ms010') {
   sm(get_phonology(network=network, domain=domain, prodname_ms=prodname_ms, 
                          time='end_season', ws_boundry=ws_boundry))
 }
 
}

