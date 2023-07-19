install.packages("shiny")
remotes::install_github("streampulse/StreamPULSE", dependencies = TRUE)

library(dataRetrieval)
library(shiny)
library(StreamPULSE)


res = request_results(sitecode='NC_Ellerbe', year=2023)
ava = query_available_data(region = 'NC', site = "Ellerbe")
d = res$model_results$fit$daily


ellerbeCP_sc = 'NC_EllerbeCP'
sd = '2020-11-24'
ed = '2022-07-10'
ellerbecp_data = request_data(sitecode = ellerbeCP_sc, startdate = sd, enddate = ed)


scrape_data <- function(sites) {
  
  site_dataframes <- list()

  for (site in sites) {
    
    ava_data <- tryCatch({
      query_available_data(region = "NC", site = site)
    }, error = function(e) {
      print(paste("Error in querying data for site", site, ":", e))
      return(NULL)
    })
    
    if (!is.null(ava_data)) {
      
      start_year <- as.integer(strsplit(as.character(ava_data$datebounds$firstRecord), "-")[[1]][1])
      end_year <- as.integer(strsplit(as.character(ava_data$datebounds$lastRecord), "-")[[1]][1])
      

      d_site <- data.frame()
      for (year in start_year:end_year) {

        res_year <- tryCatch({
          request_results(sitecode = paste("NC", site, sep = "_"), year = as.character(year))
        }, error = function(e) {
          print(paste("Error in requesting results for site", site, "year", year, ":", e))
          return(NULL)
        })
        
        if (!is.null(res_year)) {
          d_site <- dplyr::bind_rows(d_site, res_year$model_results$fit$daily)
        }
      }

      site_dataframes[[site]] <- d_site
    }
  }
  return(site_dataframes)
}

sites <- c("Ellerbe", "EllerbeCP", "EllerbeClub", "EllerbeGlenn", "EllerbeTrinity", "ColeMill", "Eno", "Mud", "NHC", "UNHC", "UEno", "Stony")
site_dataframes <- scrape_data(sites)

for (df_name in names(site_dataframes)) {
  df <- site_dataframes[[df_name]]  
  dim_str <- paste(dim(df), collapse = ", ")
  cat("Data frame '", df_name, "': ", dim_str, "\n", sep = "")
}

separate_df_foo <- function(site_dataframes) {
  for(df_name in names(site_dataframes)) {
    assign(paste0(df_name, "_df"), site_dataframes[[df_name]], envir = .GlobalEnv)
  }
}


separate_df_foo(site_dataframes)



colemi_n <- "USGS-02085039"
siteNumb_cm <- "02085039"

ellerbe_glenn_n = '02085000'
ellerbe_glenn_n_o = 'USGS_02085000'


siteAvailable <- whatNWISdata(siteNumber = ellerbe_glenn_n)
siteAvailable <- filter(siteAvailable, siteAvailable$count_nu>5)

parmsAvailable <- unique(na.omit(siteAvailable$parm_cd))

siteWQ <- readWQPqw(siteNumbers = c(ellerbe_glenn_n_o), parameterCd = parmsAvailable)
try_2 <- readNWISdv(siteNumb_cm, "00060")

e_club = 'USGS_0208675010'
e_c_n = '0208675010'
site_ec <-whatNWISdata(siteNumber = e_c_n)
parmsAvailable <- unique(na.omit(site_ec$parm_cd))
siteWQ <- readWQPqw(siteNumbers = c(e_club), parameterCd = parmsAvailable)


e_glenn = 'USGS_02086849'
e_g_n = '02086849'
site_eg <-whatNWISdata(siteNumber = e_g_n)
parmsAvailable <- unique(na.omit(site_eg$parm_cd))
siteAvailable <- filter(siteAvailable, siteAvailable$count_nu>5)
siteWQ <- readWQPqw(siteNumbers = c(e_glenn), parameterCd = parmsAvailable)


res = request_results(sitecode='NC_Ellerbe', year=2018)
ava = query_available_data(region = 'NC', site = "Ellerbe")
d = res$model_results$fit$daily


request_data_to_all_sites <- function(site_list) {
  results_list <- list()
  
  for (site in site_list) {
    site_code <- paste("NC", site, sep = "_")
    results_list[[site]] <- request_data(site_code)
  }
  return(results_list)
}
results <- request_data_to_all_sites(sites)

