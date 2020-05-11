## ---------------------------
##
## Script name: combine_extracted_dfs.R
##
## Purpose of script:
##  Take the single year results of a script that extracted some vars from
##   Bulk Patent Bibliographic Data (dopar_convert_ipbg_to_CO_records.R) and
##   aggregate, and bind them into multi year Data Frames
##
## Author: A. C. R.
##
## Date Created: 2020-2-06
##
## Copyright: Public Domain. CC0
##
## ---------------------------
##
## Notes: Assumes all data is complete. no checks for missing files.
##
##
## ---------------------------------------------------------------------------

library(tidyverse)
library(readxl)
library(magrittr)
library(tools)
library(stringi)
library(data.table) # workaround for Windows Encoding Problem


# set working directory as needed. 
setwd("H:/Major Projects 2020/Patent Grant Stats/R files/")


# import crosswalk files
# this sheet is used as a dictionary match from common CO city
# mispellings/abbrevs to exact (e.g.: Devner to Denver)
# not specific to this use. Just a general crosswalk.
common_CO_misspellings <- read.csv("../crosswalks/misspellings.csv",
  stringsAsFactors = FALSE,
  encoding = "UTF-8"
)


colnames(common_CO_misspellings) <- c("Wrong", "Right")

# My files had some unusual encoding issues for specific lines.
# I suspect it's a R + Windows specific issue.
x <- which(stri_enc_mark(common_CO_misspellings$Right) != "UTF-8")
if (x > 0) {
  common_CO_misspellings$Right[x] <- stri_encode(
    common_CO_misspellings$Right[x],
    from = "UTF-8", to = "ASCII"
  )
}

x <- which(stri_enc_mark(common_CO_misspellings$Wrong) == "UTF-8")
if (x > 0) {
  common_CO_misspellings$Wrong[x] <- stri_encode(common_CO_misspellings$Wrong[x],
    from = "UTF-8", to = "ASCII"
  )
}

common_CO_misspellings$Wrong <- trimws(tolower(common_CO_misspellings$Wrong))
common_CO_misspellings$Right <- trimws(tolower(common_CO_misspellings$Right))


# this is a city-to-county crosswalk built off of the base provided by
# census in it's PLACE list (available through FTP site)
# it matches a named place to the counties it is within.
CO_place_list <- read_csv("../crosswalks/place_to_county_crosswalk.csv")
CO_place_list$`Common Name` <- tolower(CO_place_list$`Common Name`)
x <- which(stri_enc_mark(CO_place_list$`Common Name`) == "UTF-8")

if (x > 0) {
  CO_place_list$`Common Name`[-x] <- stri_encode(CO_place_list$`Common Name`[-x],
    from = "UTF-8", to = "ASCII"
  )
}

CO_place_list$`Common Name` <- tolower(CO_place_list$`Common Name`)

# just a simple list
CO_county_list <- read_lines("../crosswalks/Colorado_County_List.txt")

# DEFINE FUNCTIONS

# helper
`%notin%` <- Negate(`%in%`)

# clean_up_names: expects valid char vector. doesn't handle bad char encoding.
clean_up_names <- function(x) {
  if (validEnc(x)) {
    x <- tolower(trimws(x))
    # random punctuation and using standard abbreviations cause issues
    x <- gsub("\\.|\\,|", "", gsub("ft", "fort", x), perl = TRUE)
    # this was a choice, 
    # b/c some not-proper encodings are actually matching in the place list.
    # as long as case is as expected.
  }
  return(x)
}


# get_right_spelling: match a misspelled city to its actual name using crosswalk
get_right_spelling <- function(city_name) {
  # some places are just oddly stated counties
  city_name <- ifelse(grepl("county of ", city_name),
    paste0(gsub("county of ", "", city_name), " county"),
    city_name
  )

  # most issues are spelling mistakes or weird abbrevs
  if (city_name %in% common_CO_misspellings$Wrong) {
    city_name <- unlist(common_CO_misspellings$Right[which(
      common_CO_misspellings$Wrong == city_name
    )[1]])
  }
  return(city_name)
}

# read_and_bind_files: expects a char vector of id names and a file prefix
# files are expected to have identical column names to each other
# files are named as "file_[prefix]_[file_id].csv"
# the prefix and file_id are from the raw bulk data files.
# there is no error handling here.
read_and_bind_files <- function(batch_of_ids, file_prefix) {
  num_ids <- length(batch_of_ids)
  df_list <- list(length = "num_ids")

  for (i in 1:num_ids) {
    df_list[[i]] <- read_csv(paste0(in_file_path, file_prefix, "_", batch_of_ids[i], ".csv"),
      locale = locale(encoding = "UTF-8"),
      col_types = cols(.default = "c")
    )
  }

  return(bind_rows(df_list))
}


# clean and correct names: trims and removes punctuation from valid enc. strings
# then checks names against common mispellings
# meant to only be used for COLORADO cities
# only uses first position inventor
# uses global city crosswalk and city misspelling dictionary variables.
# calls helper functions defined above
filter_clean_and_correct_city_names <- function(df) {
  df2 <- df %>% filter(inventor_position == 1 & inventor_state == "CO")
  df2 %<>% rowwise() %>%
    mutate(inventor_city = clean_up_names(inventor_city))
  # %>%
  df2 %<>% rowwise() %>% mutate(inventor_city = unname(get_right_spelling(inventor_city)))
  return(df2)
}

#aggregate to county: takes DF with cities that have matched to crosswalk
# adds fractional portion of county granted per record (based on city)
# to return a "summary" df with county names and estimated patent counts.
aggregate_to_county <- function(df) {
  county_df <- data.frame(
    county_name = CO_county_list,
    count = 0
  )

  for (i in c(1:dim(county_df)[1])) {
    x <- filter(df, (
      COUNTY_1 == county_df$county_name[i] |
        COUNTY_2 == county_df$county_name[i] |
        COUNTY_3 == county_df$county_name[i]))
    county_df$count[i] <- sum(x$WEIGHT)
  }

  return(county_df)
}



## MAIN

# runs a year worth of extracts at a time.
# pick the years to run at once
data_years <- c(2005:2019)

# allocate empty lists to be populated in loop
all_county_dfs <- list(length = length(data_years))
all_unmatched_dfs <- list(length = length(data_years))
all_matched_dfs <- list(length = length(data_years))

# THE LOOP: process a year worth of records.
for (j in c(1:length(data_years))) {
  start_time <- Sys.time()   # track completion time; pushed to console on each loop.

  data_year <- data_years[j]
  # all extracted files have been dumped in "Extracts/[YYYY]" 
  in_file_path <- paste0("../Extracts/", data_year, "/")

  # those extracts live in two types of CSVS
  # two types of csvs, a "bibliographic" with unique, and then (one to one)
  # an "inventor" file that matches bib by document id number (many to one)
  inventor_prefix <- "inventor_data_file"
  bib_prefix <- "bib_data_file"


  # the extracted csvs are in "../Extracts/[YYYY]" 
  # each file maps to the name of the raw data file it was extracted from
  # hence the re_use of that list here -- its the raw file name minus ".xml"
  batch_of_ids <- readLines(
    paste0("../Raw Data/", data_year, "/list_of_", data_year, "_xml_files.txt")
  )
  batch_of_ids <- sub(".xml", "", batch_of_ids)


  bib_df <- read_and_bind_files(batch_of_ids, bib_prefix)
  invent_df <- read_and_bind_files(batch_of_ids, inventor_prefix)

  # get out the CO based 1st inventor cities
  co_inventors <- filter_clean_and_correct_city_names(invent_df)

  df_joined <- left_join(co_inventors, CO_place_list,
    by = c("inventor_city" = "Common Name")
  )

  df_joined <- left_join(df_joined, bib_df, by = c("docnum_to_inventor" = "document_number"))

  # drop rows that haven't had a placename match
  df_matched <- df_joined[complete.cases(df_joined[, c("PLACENAME")]), ]

  # store dropped rows for QA purposes
  df_not_matched <- df_joined[!(complete.cases(df_joined[, c("PLACENAME")])), ]

  county_summary <- aggregate_to_county(df_matched)

  county_summary <- rbind(
    county_summary,
    data.frame(county_name = "Unknown", count = dim(df_not_matched)[1])
  )

  new_var_name <- sym(paste0("", as.character(data_year)))

  county_summary %<>% rename(!!new_var_name := count)


  # save snapshots by year
  save(
    list = c("df_matched", "df_not_matched", "county_summary"),
    file = paste0(data_year, "_USPTO_summary.Rdata")
  )

  all_county_dfs[[j]] <- county_summary
  all_matched_dfs[[j]] <- df_matched
  all_unmatched_dfs[[j]] <- df_not_matched
  end_time <- Sys.time()  # time tracking 
  print(paste0(  # report out on time
    "completed ", data_year,
    ". This took: ", end_time - start_time
  ))
}



all_county_dfs_bind <- bind_cols(all_county_dfs)
all_matched_dfs_bind <- bind_rows(all_matched_dfs)
all_unmatched_dfs_bind <- bind_rows(all_unmatched_dfs)

x <- colnames(all_county_dfs_bind)[grep("county_name\\d+", colnames(all_county_dfs_bind))]
all_county_dfs_bind %<>% select(-x)

write_csv(all_county_dfs_bind, paste0("CO_county_patents_aggregated_", Sys.Date(), ".csv"))
write_csv(all_matched_dfs_bind, paste0("CO_inventors_matched_", Sys.Date(), ".csv"))
write_csv(all_unmatched_dfs_bind, paste0("CO_inventors_unmatched_", Sys.Date(), ".csv"))
