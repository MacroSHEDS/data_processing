library(httr)
library(jsonlite)
library(tidyr)
library(plyr)
library(data.table)
library(dtplyr)
library(tidyverse)
# library(lubridate)
library(feather)
library(glue)
library(logging)
library(emayili)
library(neonUtilities)
library(tinsel)

setwd('/home/mike/git/macrosheds/data_acquisition')
conf = jsonlite::fromJSON('config.json')
email_err_msg = list()

source('src/global_helpers.R')
source_decoratees('src/global_helpers.R') #parse decorators

source('src/lter/hbef/retrieve.R')

if(length(email_err_msg)){
    email_err(email_err_msg, conf$report_emails, conf$gmail_pw)
}
