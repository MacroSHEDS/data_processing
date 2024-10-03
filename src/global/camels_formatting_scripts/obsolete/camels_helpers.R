# This is a copy of the python script from: https://github.com/neuralhydrology/neuralhydrology/blob/master/neuralhydrology/datautils/pet.py
# Used to calculate ET from daymet (using the CAMLES framework)

sm = suppressMessages
sw = suppressWarnings

# Helper functions 
get_slope_svp_curve <- function(t_mean) {
    # Python function has np.exp
    delta = 4098 * (0.6108 * exp((17.27 * t_mean) / (t_mean + 237.3))) / ((t_mean + 237.3)**2)
}

get_net_sw_srad <- function(s_rad, albedo = 0.23) {
    
    net_srad = (1- albedo) * s_rad
    
    return(net_srad)
}

get_sol_decl <- function(doy) {
    sol_dec = 0.409 * sin((2 * pi) / 365 * doy - 1.39)
    return(sol_dec)
}

get_sunset_hour_angle <- function(lat, sol_dec) {
    
    term = -tan(lat) * tan(sol_dec)
    term[term < -1] = -1
    term[term > 1] = 1
    sha = acos(term)
    return(sha)
    
}

get_clear_sky_rad <- function(elev, et_rad){
    cs_rad = (0.75 + 2 * 10e-5 * elev) * et_rad
}

get_avp_tmin <- function(t_min) {
    avp = 0.611 * exp((17.27 * t_min) / (t_min + 237.3))
    
    return(avp)
}

get_net_outgoing_lw_rad <- function(t_min, t_max, s_rad, cs_rad, a_vp) {
    
    term1 = ((t_max + 273.16)**4 + (t_min + 273.16)**4) / 2  # conversion in K in equation
    term2 = 0.34 - 0.14 * sqrt(a_vp)
    term3 = 1.35 * s_rad / cs_rad - 0.35
    stefan_boltzman = 4.903e-09
    net_lw = stefan_boltzman * term1 * term2 * term3
    
    return(net_lw)
}

get_net_rad <- function(sw_rad, lw_rad){
    val <- sw_rad - lw_rad
    return(val)
}

get_atmos_pressure <- function(elev) {
    temp = (293.0 - 0.0065 * elev) / 293.0
    val = (temp^5.26) * 101.3
    
    return(val)
}

get_extraterra_rad <- function(lat, sol_dec, sha, ird){
    term1 = (24 * 60) / pi * 0.082 * ird
    term2 = sha * sin(lat) * sin(sol_dec) + cos(lat) * cos(sol_dec) * sin(sha)
    et_rad = term1 * term2
    
    return(et_rad)
}

get_ird_earth_sun <- function(doy) {
    ird = 1 + 0.033 * cos((2 * pi) / 365 * doy)
    
    return(ird)
}

get_psy_const <- function(atm_pressure) {
    val = 0.000665 * atm_pressure
    return(val)
}

srad_from_t <- function(et_rad, cs_rad, t_min, t_max, coastal=FALSE) {
    if(coastal){
        adj = 0.19
    } else{
        adj = 0.16
    }
    
    sol_rad = adj * sqrt(t_max - t_min) * et_rad
    
    min_val <- min(sol_rad, cs_rad)
    return(min_val) 
}

calc_daymet_pet <- function(df) {
    
    df <- df %>%
        rename(t_min = tmin,
               t_max = tmax,
               s_rad = srad,
               lat = latitude) %>%
        mutate(doy = yday(date)) %>%
        mutate(lat = lat *(pi/180)) %>%
        mutate(t_mean = 0.5 * (t_max + t_min)) %>%
        mutate(slope_svp = get_slope_svp_curve(t_mean)) %>%
        mutate(s_rad = s_rad * 0.0864) %>% # conversion Wm-2 -> MJm-2day-1
        mutate(in_sw_rad = get_net_sw_srad(s_rad)) %>%
        mutate(sol_dec = get_sol_decl(doy)) %>%
        mutate(sha = get_sunset_hour_angle(lat, sol_dec)) %>%
        mutate(ird = get_ird_earth_sun(doy)) %>%
        mutate(et_rad = get_extraterra_rad(lat, sol_dec, sha, ird)) %>%
        mutate(cs_rad = get_clear_sky_rad(elev, et_rad)) %>%
        mutate(a_vp = get_avp_tmin(t_min)) %>%
        mutate(out_lw_rad = get_net_outgoing_lw_rad(t_min, t_max, s_rad, cs_rad, a_vp)) %>%
        mutate(net_rad = get_net_rad(in_sw_rad, out_lw_rad)) %>%
        mutate(atm_pressure = get_atmos_pressure(elev)) %>%
        mutate(gamma = get_psy_const(atm_pressure)) %>%
        # mutate(alpha = 1.26) %>% # Calibrated in CAMELS, here static
        mutate(lambda = 2.45) %>%  # Kept constant, MJkg-1
        mutate(pet = (alpha / lambda) * (slope_svp * net_rad) / (slope_svp + gamma)) %>%
        mutate(pet = pet * 0.408) %>%
        mutate(alpha_look = (pet/0.408)*lambda*(1/(net_rad*slope_svp)) * (slope_svp + gamma))
}

calc_daymet_pet_ca <- function(df) {
    
    df <- df %>%
        rename(t_min = tmin,
               t_max = tmax,
               s_rad = srad,
               lat = latitude) %>%
        mutate(doy = yday(date)) %>%
        mutate(lat = lat *(pi/180)) %>%
        mutate(t_mean = 0.5 * (t_max + t_min)) %>%
        mutate(slope_svp = get_slope_svp_curve(t_mean)) %>%
        mutate(s_rad = s_rad * 0.0864) %>% # conversion Wm-2 -> MJm-2day-1
        mutate(in_sw_rad = get_net_sw_srad(s_rad)) %>%
        mutate(sol_dec = get_sol_decl(doy)) %>%
        mutate(sha = get_sunset_hour_angle(lat, sol_dec)) %>%
        mutate(ird = get_ird_earth_sun(doy)) %>%
        mutate(et_rad = get_extraterra_rad(lat, sol_dec, sha, ird)) %>%
        mutate(cs_rad = get_clear_sky_rad(elev, et_rad)) %>%
        mutate(a_vp = get_avp_tmin(t_min)) %>%
        mutate(out_lw_rad = get_net_outgoing_lw_rad(t_min, t_max, s_rad, cs_rad, a_vp)) %>%
        mutate(net_rad = get_net_rad(in_sw_rad, out_lw_rad)) %>%
        mutate(atm_pressure = get_atmos_pressure(elev)) %>%
        mutate(gamma = get_psy_const(atm_pressure)) %>%
        # mutate(alpha = 1.26) %>% # Calibrated in CAMELS, here static
        mutate(lambda = 2.45) %>%  # Kept constant, MJkg-1
        #mutate(pet = (alpha / lambda) * (slope_svp * net_rad) / (slope_svp + gamma)) %>%
        #mutate(pet = pet * 0.408) %>%
        mutate(alpha = (PET/0.408)*lambda*(1/(net_rad*slope_svp)) * (slope_svp + gamma))
}




get_hydro_year<-function(d,hy_cal){
    
    # input variables:
    # d: array of dates of class Date
    # hy_cal: hydrological year calendar, current options are 'oct_us_gb', 'sep_br' and 'apr_cl'
    
    if(class(d)!='Date'){stop('d should be of class Date - use as.Date')}
    
    m<-as.numeric(format(d,'%m')) # extract month
    y<-as.numeric(format(d,'%Y')) # extract year
    hy<-y                         # create array for hydrological year
    
    if(hy_cal=='oct_us_gb'){      # USA and Great Britian
        
        hy[m>=10]<-(hy[m>=10]+1)    # hydrological year 2010 starts on Oct 1st 2009 and finishes on Sep 30th 2010
        
    } else if(hy_cal=='sep_br'){  # Brazil
        
        hy[m>=9]<-(hy[m>=9]+1)      # hydrological year 2010 starts on Sep 1st 2009 and finishes on Aug 31st 2010
        
    } else if(hy_cal=='apr_cl'){  # Chile
        
        hy[m<=3]<-(hy[m<=3]-1)      # hydrological year 2010 starts on Apr 1st 2010 and finishes on Mar 31st 2011
        
    } else {
        
        stop(paste0('Unkown hydrological year calendar:',hy_cal))
        
    }
    
    return(hy)
    
}

get_hydro_year_stats<-function(d,hy_cal){
    
    # note: this function includes get_hydro_year and should be used instead
    
    # input variables:
    # d: array of dates of class Date
    # hy_cal: hydrological year calendar, current options are 'oct_us_gb', 'sep_br' and 'apr_cl'
    
    if(class(d)!='Date'){stop('d should be of class Date - use as.Date')}
    
    m<-as.numeric(format(d,'%m')) # extract month
    y<-as.numeric(format(d,'%Y')) # extract year
    hy<-y                         # create array for hydrological year
    
    if(hy_cal=='oct_us_gb'){      # USA and Great Britian
        
        hy[m>=10]<-(hy[m>=10]+1)    # hydrological year 2010 starts on Oct 1st 2009 and finishes on Sep 30th 2010
        start_hy<-as.Date(paste0(hy-1,'-10-01'))
        
    } else if(hy_cal=='sep_br'){  # Brazil
        
        hy[m>=9]<-(hy[m>=9]+1)      # hydrological year 2010 starts on Sep 1st 2009 and finishes on Aug 31st 2010
        start_hy<-as.Date(paste0(hy-1,'-09-01'))
        
    } else if(hy_cal=='apr_cl'){  # Chile
        
        hy[m<=3]<-(hy[m<=3]-1)      # hydrological year 2010 starts on Apr 1st 2010 and finishes on Mar 31st 2011
        start_hy<-as.Date(paste0(hy,'-04-01'))
        
    } else {
        
        stop(paste0('Unkown hydrological year calendar:',hy_cal))
        
    }
    
    day_of_hy<-as.numeric(d-start_hy+1) # days since the beginning of the hydro year
    
    if(any(day_of_hy<1|day_of_hy>366)){
        
        stop('Error when computing day of hydro year')
        
    }
    
    return(data.frame(hy,day_of_hy))
    
}

# Determine the season based on the month - returns full season name

month2season<-function(m){
    
    if(!is.numeric(m)){m<-as.numeric(m)}
    
    s<-m
    s[m%in%c(12,1,2)]<-'winter'
    s[m%in%3:5]<-'spring'
    s[m%in%6:8]<-'summer'
    s[m%in%9:11]<-'autumn'
    
    return(as.factor(s))
    
}

# Determine the season based on the month - returns season abbreviation

month2sea<-function(m){
    
    if(!is.numeric(m)){m<-as.numeric(m)}
    
    s<-m
    s[m%in%c(12,1,2)]<-'djf'
    s[m%in%3:5]<-'mam'
    s[m%in%6:8]<-'jja'
    s[m%in%9:11]<-'son'
    
    return(as.factor(s))
    
}

# Deal with missing values in an array

find_avail_data_array<-function(x,tol){
    
    # input variables:
    # x: array (i.e. time series) to scrutinise
    # tol: tolerated fraction of missing values (e.g. 0.05 for 5%)
    
    # purpose:
    # determine the time steps for which data are available (i.e. not NA) and check
    # if the fraction of NA elements is greater than the tolerance threshold tol
    
    # returns:
    # - an array of TRUE/FALSE values indicating for which array elements data are available
    # OR
    # - an array of NAs if the fraction of NA elements exceeds the tolerance threshold
    
    avail_data<-!(is.na(x)) # time steps for which data are available
    
    if(sum(!avail_data)>=tol*length(x)){ # more than tol*100 % of the time series are missing
        
        return(x*NA)       # return a vector of NA values
        
    } else {
        
        return(avail_data) # return a vector of TRUE/FALSE values
        
    }
    
}

# Deal with missing values in several arrays organised as a data.frame

find_avail_data_df<-function(x,tol){
    
    # input variables:
    # x: data.frame (i.e. several time series covering the same period) to scrutinise
    # tol: tolerated fraction of missing values (e.g. 0.05 for 5%)
    
    # purpose:
    # determine the time steps for which data are available for all the time series
    # (joint availibility) and check for this joint array if the NA fraction is below
    # the given tolerance threshold - see find_avail_data_array
    
    # determine time steps for which data are available (i.e. not NA) for all the time series
    joint_avail<-apply(x,1,function(x) ifelse(all(!is.na(x)),TRUE,NA))
    
    # compare fraction of missing values to prescribed thershold
    avail_data<-find_avail_data_array(joint_avail,tol)
    
    return(avail_data)
    
}


# https://github.com/naddor/camels/blob/master/clim/clim_indices.R
### PURPOSE

# This document contains R functions to compute climatic indices. These functions have been used and are still used to produce the CAMELS datasets. The wrapper compute_clim_indices_camels enables the computation of the indices selected for the original CAMELS paper (Addor et al., 2017, HESS).

# For some indices, several formulations have been implemented and the resulting estimates are returned as a
# data.frame. Alternative formulations can be added. The objective is to assess the sensitvity of the results to
# the formulation of the climatic indices.

### LOAD FUNCTIONS
### WRAPPER TO COMPUTE STANDARD CAMELS CLIMATIC INDICES

compute_clim_indices_camels<-function(temp,prec,pet,day,tol){
    
    ind_berghuijs<-compute_climate_indices_berghuijs(temp,prec,pet,day,tol)
    ind_extreme_precip<-compute_extreme_precip_indices(prec,day,rel_hp_thres=5,abs_lp_thres=1,tol)
    
    return(data.frame(p_mean           = ind_berghuijs$p_mean,
                      pet_mean         = ind_berghuijs$pet_mean,
                      aridity          = ind_berghuijs$aridity,
                      p_seasonality    = ind_berghuijs$p_seasonality,
                      frac_snow        = ind_berghuijs$frac_snow_daily,
                      high_prec_freq   = ind_extreme_precip$high_prec_freq,
                      high_prec_dur    = ind_extreme_precip$high_prec_dur,
                      high_prec_timing = ind_extreme_precip$high_prec_timing,
                      low_prec_freq    = ind_extreme_precip$low_prec_freq,
                      low_prec_dur     = ind_extreme_precip$low_prec_dur,
                      low_prec_timing  = ind_extreme_precip$low_prec_timing))
    
}

### FUNCTIONS FOR INDIVIDUAL SIGNATURES

# input variables:
# temp: temperature time series
# prec: precipitation time series
# pet: potential evapotranspiration time series
# day: date array of class "Date"
# tol: tolerated proportion of NA values in time series

compute_climate_indices_berghuijs<-function(temp,prec,pet,day,tol){
    
    # the combination of aridity, fraction of precipitation falling as snow and precipitation
    # seasonality was proposed by Berghuijs et al., 2014, WRR, doi:10.1002/2014WR015692
    
    # check data availibility
    avail_data<-find_avail_data_df(data.frame(temp,prec,pet),tol)
    
    if(any(!is.na(avail_data))){ # only compute indices if joint data availibility is high enough
        
        # aridity
        p_mean<-mean(prec,na.rm=TRUE)
        pet_mean<-mean(pet,na.rm=TRUE)
        aridity<-pet_mean/p_mean
        
        # extract day of year
        t_julian<-strptime(format(day,'%Y%m%d'),'%Y%m%d')$yday
        
        # estimate annual temperature and precipitation cycles using sine curves
        # nls (nonlinear least squares function) is used for the non-linear regression
        # a first guess is needed for the phase shift of precipiation (s_p)
        s_p_first_guess<-90-which.max(rapply(split(prec,format(day,'%m')),mean,na.rm=TRUE))*30
        s_p_first_guess<-s_p_first_guess%%360 # if necessary, convert to a value between 0 and 360
        
        fit_temp = nls(temp ~ mean(temp,na.rm=TRUE)+delta_t*sin(2*pi*(t_julian-s_t)/365.25),start=list(delta_t=5,s_t=-90))
        fit_prec = nls(prec ~ mean(prec,na.rm=TRUE)*(1+delta_p*sin(2*pi*(t_julian-s_p)/365.25)),start=list(delta_p=0.4,s_p=s_p_first_guess))
        
        s_p<-summary(fit_prec)$par['s_p','Estimate']
        delta_p<-summary(fit_prec)$par['delta_p','Estimate']
        s_t<-summary(fit_temp)$par['s_t','Estimate']
        delta_t<-summary(fit_temp)$par['delta_t','Estimate']
        
        # seasonality and timing of precipitation
        delta_p_star<-delta_p*sign(delta_t)*cos(2*pi*(s_p-s_t)/365.25)
        
        # fraction of precipitation falling as snow - using sine curves
        # see original paper by Woods, 2009, Advances in Water Resources, 10.1016/j.advwatres.2009.06.011
        t_0<-1 # temp thershold [°C]
        t_star_bar<-(mean(temp,na.rm=TRUE)-t_0)/abs(delta_t)
        
        if (t_star_bar>1){ # above freezing all year round
            
            f_s<-0
            
        } else if (t_star_bar<(-1)){ # below freezing all year round
            
            f_s<-1
            
        } else {
            
            # there is a square in the original Woods paper (Eq. 13) lacking in the Berghuijs paper (Eq. 6a)
            f_s<-1/2-asin(t_star_bar)/pi-delta_p_star/pi*sqrt(1-t_star_bar^2)
            
        }
        
        # fraction of precipitation falling as snow - using daily temp and precip values
        if(any(temp<=0&prec>0,na.rm=TRUE)){
            
            f_s_daily<-sum(prec[temp<=0])/sum(prec)
            
        } else {
            
            f_s_daily<-0
            
        }
        
        return(data.frame(aridity=aridity,p_mean=p_mean,pet_mean=pet_mean,
                          p_seasonality=delta_p_star,frac_snow_sine=f_s,frac_snow_daily=f_s_daily))
        
    } else { # return NA if data availibility is too low
        
        return(data.frame(aridity=NA,p_mean=NA,pet_mean=NA,
                          p_seasonality=NA,frac_snow_sine=NA,frac_snow_daily=NA))
        
    }
}

compute_extreme_precip_indices<-function(prec,day,rel_hp_thres,abs_lp_thres,tol){
    
    # input variables:
    # rel_hp_thres: the high precipitation threshold is relative [mean daily precipitation]
    # abs_lp_thres: the low precipitation threshold is absolute [mm/day]
    
    # check data availibility
    avail_data<-find_avail_data_array(prec,tol)
    
    if(any(!is.na(avail_data))){ # only compute indices if joint data availibility is high enough
        
        # if(any(diff(day)>1)){stop('The time series must be continious')}
        
        # extract season and hydrological year
        s<-as.factor(month2sea(format(day,'%m')))
        
        # frequency and duration of high intensity precipitation events
        hp<-prec>=rel_hp_thres*mean(prec,na.rm=TRUE)
        hp[is.na(hp)]<-F # if no precip data available, consider it is not an event
        hp_length<-nchar(strsplit(paste(ifelse(hp,'H','-'),collapse=''),'-')[[1]]) # compute number of consecutive high precip days
        hp_length<-hp_length[hp_length>0]
        if(sum(hp_length)!=sum(hp)){stop('Unexpected total number of high precip days')}
        
        if(length(hp_length)>0){ # at least one high precipitation event in the provided time series
            
            hp_freq<-sum(hp)/length(hp)*365.25
            hp_dur<-mean(hp_length)
            hp_sea<-rapply(split(hp[hp],s[hp],drop=TRUE),length)
            
            #if(max(rank(hp_sea)%%1!=0)){ # if tie between seasons with the most days with high precipitation, set timing to NA
            
            #hp_timing<-NA
            
            #} else{
            
            hp_timing<-names(hp_sea)[which.max(hp_sea)]
            
            #}
            
        } else { # not a single high precipitation event in the provided time series
            
            hp_freq<-0
            hp_dur<-0
            hp_timing<-NA
            
        }
        
        # frequency and duration of low intensity precipitation events
        lp<-prec<abs_lp_thres
        lp[is.na(lp)]<-F # if no precip data available, consider it is not an event
        lp_length<-nchar(strsplit(paste(ifelse(lp,'L','-'),collapse=''),'-')[[1]]) # compute number of consecutive low precip days
        lp_length<-lp_length[lp_length>0]
        
        lp_freq<-sum(lp)/length(lp)*365.25
        lp_dur<-mean(lp_length)
        lp_sea<-rapply(split(lp[lp],s[lp],drop=TRUE),length)
        
        if(max(rank(lp_sea)%%1!=0)){ # if tie between seasons with the most days with low precipitation, set timing to NA
            
            lp_timing<-NA
            
        } else{
            
            lp_timing<-names(lp_sea)[which.max(lp_sea)]
            
        }
        
        return(data.frame(high_prec_freq=hp_freq,high_prec_dur=hp_dur,high_prec_timing=hp_timing,
                          low_prec_freq=lp_freq,low_prec_dur=lp_dur,low_prec_timing=lp_timing))
        
    } else { #return NA if data availibility is too low
        
        return(data.frame(high_prec_freq=NA,high_prec_dur=NA,high_prec_timing=NA,
                          low_prec_freq=NA,low_prec_dur=NA,low_prec_timing=NA))
        
    }
}

get_nrcs_soils <- function(network, domain, nrcs_var_name, site, ws_boundaries){

    # Use soilDB to download  soil map unit key (mukey) calssification raster
    site_boundary <- ws_boundaries %>%
        filter(hru_id == !!site)

    bb <- sf::st_bbox(site_boundary)

    soil <- try(sw(soilDB::mukey.wcs(aoi = bb, db = 'gssurgo', quiet = TRUE)))

    # should build a chunking method for this
    if(class(soil) == 'try-error'){

        this_var_tib <- tibble(site_code = site,
                               year = NA,
                               var =  names(nrcs_var_name),
                               val = NA)

        return(this_var_tib)
    }

    mukey_values <- unique(soil@data@values)

    #### Grab soil vars

    # Query Soil Data Acess (SDA) to get the component key (cokey) in each mukey.
    #### each map unit made up of componets but components do not have
    #### spatial informaiton associted with them, but they do include information
    #### on the percentage of each mukey that is made up of each component.
    #### This informaiton on compositon is held in the component table in the
    #### comppct_r column, givin in a percent

    mukey_sql <- soilDB::format_SQL_in_statement(mukey_values)
    component_sql <- sprintf("SELECT cokey, mukey, compname, comppct_r, majcompflag FROM component WHERE mukey IN %s", mukey_sql)
    component <- sm(soilDB::SDA_query(component_sql))

    # Check is soil data is available
    if(length(unique(component$compname)) == 1 &&
       unique(component$compname) == 'NOTCOM'){

        this_var_tib <- tibble(site_code = site, year = NA,
                               var =  names(nrcs_var_name), val = NA)

        return(this_var_tib)
    }

    cokey <- unique(component[,1])
    cokey <- data.frame(ID=cokey)

    # Query SDA for the componets to get infromation on their horizons from the
    #### chorizon table. Each componet is made up of soil horizons (layer of soil
    #### vertically) identified by a chkey.
    #### Informaiton about the depth of each horizon is needed to calculate weighted
    #### averges of any parameter for the whole soil column
    #### hzdept_r = depth to top of horizon
    #### hzdepb_r = depth to bottom of horizon
    #### om_r = percent organic matter
    cokey_sql <- soilDB::format_SQL_in_statement(cokey$ID)
    chorizon_sql <- sprintf(paste0('SELECT cokey, chkey, hzname, desgnmaster, hzdept_r, hzdepb_r, ',
                                   paste(nrcs_var_name, collapse = ', '),
                                   ' FROM chorizon WHERE cokey IN %s'), cokey_sql)

    full_soil_data <- sm(soilDB::SDA_query(chorizon_sql))

    # Calculate weighted average for the entire soil column. This involves 3 steps.
    #### First the component's weighted average of all horizones. Second, the
    #### weighted avergae of each component in a map unit. And third, the weighted
    #### average of all mukeys in the watershed (weighted by their area)

    # cokey weighted average of all horizones
    soil_data_joined <- full_join(full_soil_data, component, by = c("cokey"))

    all_soil_vars <- tibble()
    for(s in 1:length(nrcs_var_name)){

        this_var <- unname(nrcs_var_name[s])

        soil_data_one_var <- soil_data_joined %>%
            select(cokey, chkey, hzname, desgnmaster, hzdept_r, hzdepb_r,
                   !!this_var, mukey, compname, comppct_r, majcompflag)

        cokey_size <- soil_data_one_var %>%
            filter(!is.na(.data[[this_var]])) %>%
            mutate(layer_size = hzdepb_r-hzdept_r) %>%
            group_by(cokey) %>%
            summarise(cokey_size = sum(layer_size)) %>%
            ungroup()

        cokey_weighted_av <- soil_data_one_var %>%
            filter(!is.na(.data[[this_var]])) %>%
            mutate(layer_size = hzdepb_r-hzdept_r) %>%
            left_join(., cokey_size, by = 'cokey') %>%
            mutate(layer_prop = layer_size/cokey_size) %>%
            mutate(value_mat_weith = .data[[this_var]] * layer_prop)  %>%
            group_by(mukey, cokey) %>%
            summarise(value_comp = sum(value_mat_weith),
                      comppct_r = unique(comppct_r)) %>%
            ungroup()

        # mukey weighted average of all compenets
        mukey_weighted_av <- cokey_weighted_av %>%
            group_by(mukey) %>%
            mutate(comppct_r_sum = sum(comppct_r)) %>%
            summarise(value_mukey = sum(value_comp*(comppct_r/comppct_r_sum)))

        # Watershed weighted average
        site_boundary_p <- sf::st_transform(site_boundary, crs = sf::st_crs(soil))

        soil_masked <- sw(terra::mask(soil, site_boundary_p))

        watershed_mukey_values <- soil_masked@data@values %>%
            as_tibble() %>%
            filter(!is.na(value)) %>%
            group_by(value) %>%
            summarise(n = n()) %>%
            rename(mukey = value) %>%
            left_join(mukey_weighted_av, by = 'mukey')


        # Info on soil data
        # query SDA's Mapunit Aggregated Attribute table (muaggatt) by mukey.
        # table information found here: https://sdmdataaccess.sc.egov.usda.gov/documents/TableColumnDescriptionsReport.pdf
        # https://www.nrcs.usda.gov/wps/portal/nrcs/detail/soils/survey/geo/?cid=nrcs142p2_053631
        # how the tables relate using mukey, cokey, and chkey is here: https://www.nrcs.usda.gov/Internet/FSE_DOCUMENTS/nrcs142p2_050900.pdf
        if(all(is.na(watershed_mukey_values$value_mukey))){

            na_prop <- 100
            watershed_value <- NA
        } else{

            total_cells <- sum(watershed_mukey_values$n)
            na_cells <- watershed_mukey_values %>%
                filter(is.na(value_mukey))
            na_cells <- sum(na_cells$n)

            watershed_mukey_values_weighted <- watershed_mukey_values %>%
                filter(!is.na(value_mukey)) %>%
                mutate(sum = sum(n, na.rm = TRUE)) %>%
                mutate(prop = n/sum)

            watershed_mukey_values_weighted <- watershed_mukey_values_weighted %>%
                mutate(weighted_av = prop*value_mukey)

            watershed_value <- sum(watershed_mukey_values_weighted$weighted_av,
                                   na.rm = TRUE)

            watershed_value <- round(watershed_value, 2)
            na_prop <- round((100*(na_cells/total_cells)), 2)
        }

        this_var_tib <- tibble(year = NA,
                               site_code = site,
                               var =  names(nrcs_var_name[s]),
                               val = watershed_value,
                               pctCellErr = na_prop)

        all_soil_vars <- rbind(all_soil_vars, this_var_tib)
    }

    return(all_soil_vars)

    # #Used to visualize raster
    # for(i in 1:nrow(watershed_mukey_values)){
    #   soil_masked@data@values[soil_masked@data@values == pull(watershed_mukey_values[i,1])] <- pull(watershed_mukey_values[i,3])
    # }
    # soil_masked@data@isfactor <- FALSE
    #
    # mapview::mapview(soil_masked)
    # raster::plot(soil_masked)

    # Other table in SDA system
    # # component table
    # compnent_sql <- soilDB::format_SQL_in_statement(cokey$ID)
    # component_sql <- sprintf("SELECT cokey, runoff, compname, compkind, comppct_r, majcompflag, otherph, localphase, slope_r, hydricrating, taxorder, taxsuborder, taxsubgrp, taxpartsize FROM component WHERE cokey IN %s", compnent_sql)
    # component_return <- soilDB::SDA_query(component_sql)
    #
    # chtext
    # compnent_sql <- soilDB::format_SQL_in_statement(full_soil_data$chkey)
    # component_sql <- sprintf("SELECT texture, stratextsflag, rvindicator, texdesc, chtgkey FROM chtexturegrp WHERE chkey IN %s", compnent_sql)
    # component_return <- soilDB::SDA_query(component_sql)

    # component_sql <- sprintf("SELECT musym, brockdepmin, wtdepannmin, wtdepaprjunmin, niccdcd FROM muaggatt WHERE mukey IN %s", mukey_sql)
    # component_return <- soilDB::SDA_query(component_sql)
    #
    # # corestrictions table
    # corestrictions_sql <- sprintf("SELECT cokey, reskind, resdept_r, resdepb_r, resthk_r FROM corestrictions WHERE cokey IN %s", compnent_sql)
    # corestrictions_return <- soilDB::SDA_query(corestrictions_sql)
    #
    # # cosoilmoist table
    # cosoilmoist_sql <- sprintf("SELECT cokey,  FROM cosoilmoist WHERE cokey IN %s", compnent_sql)
    # corestrictions_return <- soilDB::SDA_query(corestrictions_sql)
    #
    # # pores
    # chkey_sql <- soilDB::format_SQL_in_statement(unique(full_soil_data$chkey))
    # cokey_to_chkey_sql_pores <- sprintf(paste0('SELECT chkey, poresize FROM chpores WHERE chkey IN %s'), chkey_sql)
    #
    # soil_pores <- soilDB::SDA_query(cokey_to_chkey_sql_pores)

}
