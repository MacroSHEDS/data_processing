### PURPOSE

# This document contains R functions to compute streamflow indices (also referred to as hydrological signatures).
# These functions have been used and are still used to produce the CAMELS datasets. The wrapper
# compute_hydro_signatures_camels enables the computation of the signatures selected for the original CAMELS paper
# (Addor et al., 2017, HESS).

# For some signatures, several formulations have been implemented and the resulting estimates are returned as a # data.frame. Alternative formulations can be added. The objective is to assess the sensitvity of the results to
# the formulation of the hydrological signatures.

### LOAD FUNCTIONS

# source('src/camels/time/time_tools.R') # for month2sea and get_hydro_year
source('src/global/camels_formatting_scripts/actual_camels_code/time/time_tools.R') # for month2sea and get_hydro_year

### WRAPPER AND PARAMETER VALUES TO COMPUTE STANDARD CAMELS HYDROLOGICAL SIGNATURES

# q_mean         - Mean daily discharge
# runoff_ratio   - Runoff ratio (ratio of mean daily discharge to mean daily precipitation)
# stream_elas    - Streamflow precipitation elasticity (sensitivity of streamflow to changes in precipitation at the annual time scale)
# slope_fdc      - Slope of the flow duration curve (between 33rd and 66th streamflow percentiles)
# baseflow_index - Baseflow index (ratio of mean daily baseflow to mean daily discharge)
# hfd_mean       - Mean half-flow date (date on which the cumulative discharge since October first reaches half of the annual discharge)
# Q5             -  5% Flow quantile (low flow)
# Q95            - 95% Flow quantile (high flow)
# high_q_freq    - Frequency of high-flow days (>9 times the median daily flow)
# high_q_dur     - Frequency of high-flow days (>9 times the median daily flow)
# low_q_freq     - Frequency of low-flow days (<0.2 times the mean daily flow)
# low_q_dur      - Average duration of low-flow events (number of consecutive days <0.2 times the mean daily flow)
# zero_q_freq    - Frequency of days with Q = 0 mm/day

compute_hydro_signatures_camels<-function(q,p=NULL,d,tol,hy_cal,qpd=NULL){ #MIKE EDITED: added qpd param, added p default

  # input variables:
  # q: discharge time series
  # p: precipitation time series (or NULL to ignore)
  # d: date array of class "Date"
  # qpd: q, p, and d params together, in the case where p is actually available. when p is unavailable, or where it's sparse, we still want to calculate all the q stuff using all the q data #MIKE EDITED
  # tol: tolerated fraction of NA values in time series
  # hy_cal: hydrological year calendar: oct_us_gb OR sep_br OR apr_cl

  qxx<-compute_qXX(q,thres=c(0.05,0.95),tol)
  hf_stats<-compute_hf_freq_dur(q,d,tol)
  lf_stats<-compute_lf_freq_dur(q,d,tol)
  bfi<-comp_i_bf(q,d,alpha=0.925,passes=3,tol)

  return(data.frame(q_mean         = compute_q_mean(q,d,tol)$q_mean_yea,
                    # runoff_ratio   = comp_r_qp(q,p,tol),
                    # stream_elas    = comp_e_qp(q,p,d,tol,hy_cal)$e_qp_sanka),
                    runoff_ratio   = ifelse(! nrow(qpd), NA_real_, comp_r_qp(qpd$q,qpd$p,tol)), #MIKE EDITED
                    stream_elas    = ifelse(! nrow(qpd), NA_real_, comp_e_qp(qpd$q,qpd$p,qpd$d,tol,hy_cal)$e_qp_sanka), #MIKE EDITED
                    slope_fdc      = comp_s_fdc(q,tol)$sfdc_sawicz_2011,
                    baseflow_index_landson = bfi$i_bf_landson,
                    # baseflow_index_lfstat = bfi$i_bf_lfstat,
                    hfd_mean       = compute_hfd_mean_sd(q,d,tol,hy_cal)$hfd_mean,
                    Q5             = qxx$q95,
                    Q95            = qxx$q5,
                    high_q_freq    = hf_stats$hf_freq,
                    high_q_dur     = hf_stats$hf_dur,
                    low_q_freq     = lf_stats$lf_freq,
                    low_q_dur      = lf_stats$lf_dur,
                    zero_q_freq    = compute_no_flow(q,thres=0,tol)))

}

### FUNCTIONS FOR INDIVIDUAL SIGNATURES

# q_mean         - Mean daily discharge

compute_q_mean<-function(q,d,tol){

  avail_data<-find_avail_data_array(q,tol)

  q_mean_yea<-NA
  q_mean_djf<-NA
  q_mean_jja<-NA

  if(any(!is.na(avail_data))){

    q_mean_yea<-mean(q[avail_data])
    sea<-month2sea(format(d[avail_data],'%m')) # determine season
    table_sea<-table(sea)

    if(! all(c('djf', 'jja') %in% names(table_sea))){ #MIKE EDITED: added this chunk
        q_mean<-data.frame(q_mean_yea,q_mean_djf,q_mean_jja,row.names='')
        return(q_mean)
    }

    # if the number of days in DJF and JJA do not differ significantly
    if(abs(table_sea[['djf']]-table_sea[['jja']])<0.05*table_sea[['djf']]){

      q_sea<-rapply(split(q[avail_data],sea),mean)

      q_mean_djf<-q_sea['djf']
      q_mean_jja<-q_sea['jja']

    }
  }

  q_mean<-data.frame(q_mean_yea,q_mean_djf,q_mean_jja,row.names='')

  return(q_mean)
}

# runoff_ratio   - Runoff ratio (ratio of mean daily discharge to mean daily precipitation)

comp_r_qp<-function(q,p,tol){

  avail_data<-find_avail_data_df(data.frame(q,p),tol) # time steps for which obs and sim are available

  r_qp<-mean(q[avail_data])/mean(p[avail_data])

  if((!is.na(r_qp))&r_qp>1){

    warning(paste('Runoff ratio is greater than 1:',r_qp))

  }

  return(r_qp)

}

# stream_elas    - Streamflow precipitation elasticity (sensitivity of streamflow to changes in precipitation at the annual time scale)

comp_e_qp<-function(q,p,d,tol,hy_cal){

  if(length(q)!=length(d)|length(p)!=length(d)){stop('P, Q and D must have the same length')}

  avail_data<-find_avail_data_df(data.frame(q,p),tol) # time steps for which precipitation and streamflow data are available

  hy<-get_hydro_year(d,hy_cal)

  if(any(table(hy)<365)){warning('Not all the hydrological years are complete')}

  mp_tot<-mean(p[avail_data],na.rm=TRUE) # mean long-term precip
  mq_tot<-mean(q[avail_data],na.rm=TRUE) # mean long-term discharge

  mp<-rapply(split(p[avail_data],hy[avail_data]),mean,na.rm=TRUE) # mean annual precip
  mq<-rapply(split(q[avail_data],hy[avail_data]),mean,na.rm=TRUE) # mean annual discharge

  # Anomaly computed with respect to previous year (Sawicz et al., 2011, HESS)
  dp_sawicz<-diff(mp) # precip difference between two consecutive years
  dq_sawicz<-diff(mq) # discharge difference between two consecutive years

  e_qp_sawicz<-median((dq_sawicz/mq_tot)/(dp_sawicz/mp_tot))
  if(all(is.na(names(e_qp_sawicz)))){ #MIKE EDITED (addition)
      e_qp_sawicz <- unname(e_qp_sawicz)#MIKE EDITED (addition)
  }#MIKE EDITED (addition)

  # Anomaly computed with respect to long-term mean (Sankarasubramanian et al., 2001, WRR)
  dp_sanka<-mp-mp_tot
  dq_sanka<-mq-mq_tot

  e_qp_sanka<-median((dq_sanka/mq_tot)/(dp_sanka/mp_tot))

  # Return both estimates
  e_qp<-data.frame(e_qp_sawicz=e_qp_sawicz,e_qp_sanka=e_qp_sanka)

  return(e_qp)

}

# slope_fdc      - Slope of the flow duration curve (between the log-transformed 33rd and 66th streamflow percentiles)

comp_s_fdc<-function(q,tol){

  # time for which obs are available this also set the whole timeseries
  # as unavailable is the proportion of NA values is greater than tol
  avail_data<-find_avail_data_array(q,tol)

  # initilise estimates, which will be overwritten if conditions to compute SFDC are met
  sfdc_sawicz_2011<-NA
  sfdc_yadav_2007<-NA
  sfdc_mcmillan_2017<-NA
  sfdc_addor_2017<-NA

  if(any(!is.na(avail_data))){

    # define quantiles for the FDC
    quant<-seq(0,1,0.001)
    fdc<-as.numeric(rev(quantile(q[avail_data],quant))) # rev because probability of exceedance

    # retrieve Q33 and Q66
    q33<-fdc[quant==0.33] # flow exceeded 33% of the time
    q66<-fdc[quant==0.66] # flow exceeded 66% of the time
    q_med<-fdc[quant==0.50] # median flow
    q_mean<-mean(q[avail_data])

    # plot FDC
    # plot(quant,fdc,log='y',ylab='Discharge [mm/day]',xlab='Percentage time flow is exceeded',type='l')
    # points(c(0.33,0.66),c(q33,q66),col='red',pch=16)

    if(q66!=0&!is.na(q66)){ # if more than a third of values are 0, log(q66) can't be computed

      # Sawicz et al 2011, Eq. 3: 10.5194/hess-15-2895-2011
      # "the slope of the FDC is calculated between the 33rd and 66th streamflow percentiles,
      # since at semi-log scale this represents a relatively linear part of the FDC"
      sfdc_sawicz_2011<-(log(q33)-log(q66))/(0.66-0.33)

      # Yadav et al 2007, Table 3: 10.1016/j.advwatres.2007.01.005
      # "Slope of part of curve between the 33% and 66% flow exceedance values of streamflow normalized by their means"
      # Also used by Westerberg and McMillan 2015, Table 2: 10.5194/hess-19-3951-2015
      # "Slope of the FDC between the 33 and 66% exceedance values of streamflow normalised by its mean (Yadav et al., 2007)"
      sfdc_yadav_2007<-(q33/q_mean-q66/q_mean)/(0.66-0.33)

      # McMillan et al 2017, see text and Figure 1b: 10.1002/hyp.11300
      # "slope in the interval 0.33 to 0.66, in log space, normalised by median flow"
      sfdc_mcmillan_2017<-(log(q33/q_med)-log(q66/q_med))/(0.66-0.33)

      # Addor et al 2017: in this paper, standard quantiles (i.e. corresponding to probability of non-exceedence)
      # were used, leading to estimates slightly different from those obtained following Sawzic et al. 2011
      q33_quant<-as.numeric(quantile(q[avail_data],0.33)) # corresponds to flow exceeded 67% (100-33%) of the time
      q66_quant<-as.numeric(quantile(q[avail_data],0.66)) # corresponds to flow exceeded 34% (100-66%) of the time
      sfdc_addor_2017<-(log(q66_quant)-log(q33_quant))/(0.66-0.33)

    }
  }

  return(data.frame(sfdc_yadav_2007,sfdc_sawicz_2011,sfdc_mcmillan_2017,sfdc_addor_2017))

}

# baseflow_index - Baseflow index (ratio of mean daily baseflow to mean daily discharge)

comp_i_bf<-function(q,d,alpha,passes,tol){

  if(sum(is.na(q))/length(q)>=tol){ # not using avail_data here, since it would remove time steps with NA, and thereby alter the consistency of the time series and bias the baseflow separation

    i_bf_landson<-NA
    i_bf_lfstat<-NA

  } else {

    # Ladson et al. (2013). “A standard approach to baseflow separation using the Lyne and Hollick filter.”
    # Australian Journal of Water Resources 17(1): 173-180.
    # https://tonyladson.wordpress.com/2013/10/01/a-standard-approach-to-baseflow-separation-using-the-lyne-and-hollick-filter/#comments
    source('https://raw.github.com/TonyLadson/BaseflowSeparation_LyneHollick/master/BFI.R') # source code
    dat_landson<-BFI(q,alpha,passes,ReturnQbase=TRUE)
    bf_landson<-dat_landson$Qbase

    # lfstat package based on Tallaksen, L. M. and Van Lanen, H. A. J. 2004 Hydrological Drought: Processes and
    # Estimation Methods for Streamflow and Groundwater. Developments in Water Science 48, Amsterdam: Elsevier.
    # require(lfstat)
    # q_dat<-data.frame(flow=q,day=as.numeric(format(d,'%d')),month=as.numeric(format(d,'%m')),year=format(d,'%Y'))
    # lf_dat<-createlfobj(q_dat,hyearstart=10)
    # lf_dat <- filter(lf_dat, !is.na(baseflow))# hyearstart, integer between 1 and 12, indicating the start of the hydrological year, 10 for october
    # bf_lfstat<-lf_dat$baseflow
    # q <- lf_dat$flow

    # compute IBF
    if(length(bf_landson)!=length(q)){stop('Baseflow time series derived using Landson does not match length of Q_OBS')}
    # if(length(bf_lfstat)!=length(q)){stop('Baseflow time series derived using lfstat does not match length of Q_OBS')}

    # find avaiable data
    # avail_data<-find_avail_data_df(data.frame(q,bf_landson,bf_lfstat),tol) # time steps for which q,bf_landson,bf_lfstat are available

    # i_bf_landson<-sum(bf_landson[avail_data])/sum(q[avail_data])
    # i_bf_lfstat<-sum(bf_lfstat[avail_data])/sum(q[avail_data])

    i_bf_landson<-sum(bf_landson)/sum(q)

  }

  # return(data.frame(i_bf_landson,i_bf_lfstat))
    return(data.frame(i_bf_landson))

}

# Half flow date (Court, 1962): the date on which the cumulative discharge since the beginning of the hydrological year reaches half of the annual discharge

# Note: the date considered here is the number of days since the beginning of the hydrological year.
# Using days since 1st Jan instead is problematic in catchments with a half flow date occuring close to the end of the calendar year,
# as it leads to both large (e.g. 360) and small (e.g. 10) HFDs depending on the year, which biases the mean HFD.

compute_hfd_mean_sd<-function(q,d,tol,hy_cal){

  # check data availibility
  avail_data<-find_avail_data_array(q,tol)

  if(all(is.na(avail_data))){ # fraction of missing values over the whole period is above tol

    return(data.frame(hfd_mean=NA,hfd_sd=NA)) # return NA

  } else {

    hy_stats<-get_hydro_year_stats(d,hy_cal) # determine hydrological year for given calendar

    hy_q<-split(q[avail_data],hy_stats$hy[avail_data]) # discharge for each hydrological year
    hy_d<-split(hy_stats$day_of_hy[avail_data],hy_stats$hy[avail_data]) # number of days since beginning of hydrological year

    date_hfd<-c() # date of half flow for each hydrological year in days since the beginning of hydrological year

    for(y in names(hy_q)){ # loop through hydrological years

      if(sum(hy_q[[y]])==0){# hfd can't be computed if annual discharge is 0

        date_hfd[y]<-NA

      } else {

        i<-which(cumsum(hy_q[[y]])>0.5*sum(hy_q[[y]]))[1] # index of the first day above half of annual total
        date_hfd[y]<-as.numeric(hy_d[[y]][i])             # number of days since beginning of hydro year

      }
    }

    if(any(date_hfd<0|date_hfd>366,na.rm=TRUE)){

      stop(paste('Unexpected value half flow date:',date_hfd[date_hfd<0|date_hfd>366]))

    }

    return(data.frame(hfd_mean=mean(date_hfd,na.rm=TRUE),hfd_sd=sd(date_hfd,na.rm=TRUE)))

  }
}

# Flow precentiles

compute_qXX<-function(q,thres,tol){

  if(any(thres<0|thres>1)){stop('Threshold must be between 0 and 1')}

  avail_data<-find_avail_data_array(q,tol) # time steps for which obs and sim are available

  if(all(is.na(avail_data))){ # if there is more than tol% of missing value

    qXX<-data.frame(t(rep(NA,length(thres))))

  }else{

    qXX<-data.frame(t(quantile(q[avail_data],1-thres)))

  }

  names(qXX)=paste('q',thres*100,sep='')

  return(qXX)

}

# Frequency and duration of high flows

compute_hf_freq_dur<-function(q,d,tol){

  avail_data<-find_avail_data_array(q,tol)   # time steps for which obs and sim are available

  if(all(is.na(avail_data))){

    return(data.frame(hf_freq=NA,hf_dur=NA))

  } else {

    hf<-q[avail_data]>9*median(q[avail_data])  # time steps considered as high flows

    if(any(hf)){

      # in dry conditions, the median of q can be 0, so when using >, days with no discharge are high flow...
      # when using > instead not >=, every day with some discharge is a high flow...

      # Mean duration of daily high flow events
      hf_bin<-paste(as.numeric(hf),collapse='')        # a string where one or more 1 indicate a high flow event
      hf_dur_noise<-rapply(strsplit(hf_bin,0),nchar)   # use strsplit to isolate successive time steps with high discharge
      hf_dur<-mean(hf_dur_noise[hf_dur_noise>0])       # mean duration

      # Average number of daily high-flow events per year
      # Note: I used to split the time series into hydrological years but this has 2 drawbacks:
      # 1: it can split extreme events (esp. low flows) in two
      # 2: when there is missing data, they are not accounted for when the mean is computed over all the years
      # Hence, I compute the number of time steps considered as high flow for the whole period and then
      # divide it by the number of time steps with available data

      # hf_hy<-split(hf,get_hydro_year(d[avail_data])) # old method: split time series into hydrological years
      # hf_freq<-mean(rapply(hf_hy,sum))               # compute mean number of time steps considered as high flow per year

      hf_freq<-sum(hf)/sum(avail_data)*365.25

    } else {

      hf_freq<-0
      hf_dur<-0

    }

    return(data.frame(hf_freq,hf_dur))

  }
}

# Frequency and duration of low flows

compute_lf_freq_dur<-function(q,d,tol){

  avail_data<-find_avail_data_array(q,tol)    # time steps for which obs and sim are available

  if(all(is.na(avail_data))){

    return(data.frame(lf_freq=NA,lf_dur=NA))

  } else {

    lf<-q[avail_data]<=0.2*mean(q[avail_data])  # time steps considered as low flows

      if(any(lf)){

      # Mean duration of daily high flow events
      lf_bin<-paste(as.numeric(lf),collapse='')        # a string where one or more 1 indicate a low flow event
      lf_dur_noise<-rapply(strsplit(lf_bin,0),nchar)   # use strsplit to isolate successive time steps with low discharge
      lf_dur<-mean(lf_dur_noise[lf_dur_noise>0])       # mean duration

      # Average number of daily low flow events per year - see commment in compute_lf_freq_dur
      # lf_hy<-split(lf,get_hydro_year(d[avail_data]))  # old method: split time series into hydrological years
      # lf_freq<-mean(rapply(lf_hy,sum))                # compute mean number of time steps considered as high flow per year

      lf_freq<-sum(lf)/sum(avail_data)*365.25

    } else {

      lf_freq=0
      lf_dur=0

    }

    return(data.frame(lf_freq,lf_dur))

  }
}

# Proportion of time series with discharge below or at a given threshold

compute_no_flow<-function(q,thres,tol){

  avail_data<-find_avail_data_array(q,tol)

  return((sum(q[avail_data]<=thres)/length(q[avail_data]))*365)

}

compute_q_seas<-function(q,d,tol){

  avail_data<-find_avail_data_array(q,tol)

  q_year<-compute_q_mean(q,d,tol)$q_mean_yea
  q_mon<-rapply(split(q[avail_data],format(d[avail_data],'%m')),mean)

  return(sum(abs(q_mon-q_year))/(q_year*12))

}

compute_q_peak<-function(q,d,tol){

  avail_data<-find_avail_data_array(q,tol)

  if(all(is.na(q[avail_data]))){

    return(NA)

  } else{

    which.max(rapply(split(q[avail_data],format(d[avail_data],'%m')),mean))

  }
}
