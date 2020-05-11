library(tidyverse)
library(magrittr)
library(stringr)


qcew_path <- "H:/Major Projects 2017 2018/DASHBOARD/BUSINESSVITALITY/QCEW-annual-workers/"
msas <- read_csv("CO_MSAs.csv")

msas %<>% mutate(bls_fips = paste0("C", substr(FIPS, 1,4)))

single_files <- paste0(c(2005:2015), "_annual_singlefile.zip")

qcews <- vector(mode = "list", length = length(single_files))
              
for (i in c(1:length(single_files))){
  qcews[[i]]<- read_csv(paste0(qcew_path, single_files[i]))
  #qcews[[i]] %<>% filter(str_detect(area_fips, "^08"), 
   #                               industry_code == 10,
    #                              own_code == 0)
  
 qcews[[i]] %<>% filter(area_fips %in% msas$bls_fips,
                        industry_code == 10,
                        own_code == 0)
}


qcew = bind_rows(qcews)


write_csv(qcew, "CO_qcew_data_by_MSA_2005_to_2015.csv")
