## ---------------------------
##
## Script name: get_USPTO_bulk.R
##
## Purpose of script: download multiple years worth of data from the USPTO
##   bulk data repo. To support work on behalf of
##   the Larimer County (Colorado) Workforce Development Board and NoCo REDI
##
## Author: A. C. R.
##
## Date Created: 2020-1-31
##
## Copyright: Public Domain. CC0
##
## ---------------------------
##
## Notes: no error handling. no niceties.
##
##
## ---------------------------


library(httr)
library(rvest)
library(xml2)
library(selectr)
library(purrr)


# setwd as needed.


# This needs to be the location of the dataset you want
# no idea if this works with non-redbook, non-biblio data sets.
base_url <- "https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic"

# each year is just in a subdir off of the base_url. easy.
years <- c(2005:2019)

# iterate per year to download files.
walk(years, ~ {
  url <- file.path(base_url, .x)
  page <- read_html(url)

  # extract all .zip file links
  html_nodes(page, xpath = ".//a[contains(@href, '.zip')]") %>%
    html_attr("href") %>%
    paste0(url, "/", .) -> zip_urls

  # create a folder for them to go into
  # assumes its a subdirectory of the current working directory.
  file_year <- paste0("USPTO", .x)
  dir.create(file_year)

  # then download all those files
  # the first file listed in each directory isn't one I want,
  # so, it gets left out.
  walk(zip_urls[-c(1)], ~ {
    message("Downloading: ", .x)

    httr::GET(
      url = .x,
      httr::write_disk(file.path(file_year, basename(.x)))
    )

    Sys.sleep(8) # take a break; site prefers long ones.
  })
})
