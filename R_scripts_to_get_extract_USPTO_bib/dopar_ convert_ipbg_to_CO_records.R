## ---------------------------
##
## Script name: dopar_convert_ipbg_to_CO_Records.R
##
## Purpose of script:
##  extracted some vars from
##   Bulk Patent Bibliographic Data XML
##  save as CSVs (bibliographic one-to-one and inventor, many-to-one)
## each csv file matches source XML file name.
## extracts a limited range of values from the XML
## works for extracting these limited vars from XML for 2005-present (2020)
## it is set up to parallelize this process. see comments.
##
## Author: A. C. R.
##
## Date Created: 2020-1-30
##
## Copyright: Public Domain. CC0
##
## ---------------------------
##
## Notes: Assumes all data is complete. no checks for missing files.
##
## MULTIPLE DTDs exist across years. This just only runs up against one difference
## in file structure. modifications to this script may need to be sensitive to
## these differences.
## 
## on a intel core i7-8700 CPU; one year of data takes approximately 
## 1.6 - 2 hours of clock time using 4 "cores" (LP)
##
## ---------------------------------------------------------------------------



library(tidyverse)
library(xml2)
library(magrittr)
library(compiler) # precompile slow functions Jand JIT
library(foreach) # loop that support doParallel
library(doParallel) # paralellize read_xml and processing

# https://bulkdata.uspto.gov/
# a vector with the file names

# the path where the raw files are stored
file_dir <- "H:/Major Projects 2020/Patent Grant Stats/"

# my workflow is to handle 1 year at a time and manage that manually.
data_year <- 2005

# file paths for input/output
xml_path <- paste0(file_dir, "Raw Data/", data_year, "/")
output_path <- paste0(file_dir, "Extracts/")

# doParallel log
log_name <- paste0("log", data_year, ".txt")

# each directory contains a list of the xml files contained. this
# is the file name from the bulk data site and local copy.
# assumed to be accurate: no error checking later on.
xml_files <- read_lines(paste0(xml_path, "list_of_", data_year, "_xml_files.txt"))


# the effective root node for records
node_string <- "//us-bibliographic-data-grant"

################################################################
# FUNCTIONS
enableJIT(1) # JIT compile for small speed gains

# Patent front page files are LARGE and slow.
get_and_read_xml <- cmpfun(function(xml_file) {

  # READ and process non-conforming XML into something useful.
  doc_text <- readLines(paste0(xml_path, xml_file))

  doc_xml_text <- doc_text[1]
  x1 <- grep(doc_xml_text, doc_text, fixed = TRUE)

  # the XML is nonconforming with multiple XML declarations
  # it would rather be read as individual files per record
  # (despite being stored as 1 week worth of records per file)
  # in tests I found reading/processing it that way with xml2:: was even slower.
  # this is a workaround.
  doc_text <- doc_text[-x1]
  doc_text <- purrr::prepend(doc_text, doc_xml_text)

  doc_text_flat <- paste(doc_text, collapse = "")

  # READ XML
  return(suppressWarnings(xml2::read_xml(doc_text_flat,
    encoding = "UTF-8",
    as_html = TRUE, verbose = FALSE,
    options = c(
      "NOERROR", "NOWARNING",
      "NONET"
    ),
    quiet = TRUE
  )))
})

# This is the next slowest part. SLOW conversion
convert_xml_to_list <- cmpfun(function(xml_data, node_string) {
  node_list <- xml2::as_list(xml2::xml_find_all(xml_data, node_string))
  return(node_list)
})


# pare: dump non-utility patents.
pare_to_utility_patents <- cmpfun(function(node_list) {
  x <- unlist(lapply(node_list, function(x) {
    attr(x$`application-reference`, "appl-type")
  }))
  # lists contain "utility" and non-utility (design, reissues, etc)
  utility_ind <- which(x == "utility")
  ut_data <- node_list[utility_ind]

  return(ut_data)
})


# extract xml details: start pulling out the details in flatter formats
# return bibliographic 1-to-1 items as a df
extract_bibdata_from_list <- cmpfun(function(ut_data) {
  document_number <- unlist(lapply(
    ut_data,
    function(x) {
      x$`publication-reference`$`document-id`$`doc-number`[[1]]
    }
  ))

  doc_kind <- unlist(lapply(
    ut_data,
    function(x) {
      x$`publication-reference`$`document-id`$kind
    }
  ))

  doc_app_date <- unlist(lapply(
    ut_data,
    function(x) {
      x$`application-reference`$`document-id`$date
    }
  ))
  doc_date <- unlist(lapply(ut_data, function(x) {
    x$`publication-reference`$`document-id`$date[[1]]
  }))
  invention_title <- unlist(lapply(
    ut_data,
    function(x) {
      x$`invention-title`[[1]]
    }
  ))


  return(data.frame(
    document_number,
    doc_kind,
    doc_date,
    doc_app_date,
    invention_title
  ))
})

# return a df with inventor data (many to one)
# doc number is a vector, ideally bib_df$document_number
# this deals with 2005 -2014 DTD for inventor data
extract_2014_inventor_data_from_list <- function(ut_data, document_number) {
  num_inventors <- sum(unlist(lapply(
    ut_data,
    function(x) {
      length(x$`parties`$applicants)
    }
  )))

  docnum_to_inventor <- rep("", num_inventors)
  inventor_position <- rep("", num_inventors)
  inventor_first_name <- rep("", num_inventors)
  inventor_last_name <- rep("", num_inventors)
  inventor_city <- rep("", num_inventors)
  inventor_state <- rep("", num_inventors)
  inventor_country <- rep("", num_inventors)
  inventor_index <- 1

  for (i in c(1:length(ut_data))) {
    n_inventors <- length(ut_data[[i]]$`parties`$applicants)
    if (n_inventors >= 1) {
      for (j in c(1:n_inventors)) {
        docnum_to_inventor[inventor_index] <- document_number[i]

        if (!(is.null(ut_data[[i]]$`parties`$applicants[[j]]$addressbook$`first-name`)) &
          length(ut_data[[i]]$`parties`$applicants[[j]]$addressbook$`first-name`) > 0) {
          inventor_first_name[inventor_index] <- unlist(
            ut_data[[i]]$`parties`$applicants[[j]]$addressbook$`first-name`
          )
        }

        if (!(is.null(ut_data[[i]]$`parties`$applicants[[j]]$addressbook$`last-name`)) &
          length(ut_data[[i]]$`parties`$applicants[[j]]$addressbook$`last-name`) > 0) {
          inventor_last_name[inventor_index] <- unlist(
            ut_data[[i]]$`parties`$applicants[[j]]$addressbook$`last-name`
          )
        }

        if (!(is.null(
          ut_data[[i]]$`parties`$applicants[[j]]$addressbook$address$city
        ))) {
          inventor_city[inventor_index] <- unlist(
            ut_data[[i]]$`parties`$applicants[[j]]$addressbook$address$city
          )
        }

        if (!(is.null(ut_data[[i]]$`parties`$applicants[[j]]$addressbook$address$country))) {
          inventor_country[inventor_index] <- unlist(
            ut_data[[i]]$`parties`$applicants[[j]]$addressbook$address$country
          )
        }


        if (!(is.null(
          ut_data[[i]]$`parties`$applicants[[j]]$addressbook$address$state
        ))) {
          inventor_state[inventor_index] <- unlist(
            ut_data[[i]]$`parties`$applicants[[j]]$addressbook$address$state
          )
        }

        inventor_position[inventor_index] <- j
        inventor_index <- inventor_index + 1
      }
    }
  }


  # sometimes this is returned as vector, and sometimes its a list.
  if (typeof(inventor_city) == "list") {
    inventor_city <- unlist(inventor_city)
  }

  return(data.frame(
    docnum_to_inventor,
    inventor_position,
    inventor_first_name,
    inventor_last_name,
    inventor_city,
    inventor_state,
    inventor_country
  ))
}

# return a df with inventor data (many to one)
# doc number is a vector, ideally bib_df$document_number
# this deals with 2014+ DTDs for inventor data
extract_inventor_data_from_list <- function(ut_data, document_number) {
  num_inventors <- sum(unlist(lapply(
    ut_data,
    function(x) {
      length(x$`us-parties`$inventors)
    }
  )))

  docnum_to_inventor <- rep("", num_inventors)
  inventor_position <- rep("", num_inventors)
  inventor_first_name <- rep("", num_inventors)
  inventor_last_name <- rep("", num_inventors)
  inventor_city <- rep("", num_inventors)
  inventor_state <- rep("", num_inventors)
  inventor_country <- rep("", num_inventors)
  inventor_index <- 1

  for (i in c(1:length(ut_data))) {
    n_inventors <- length(ut_data[[i]]$`us-parties`$inventors)
    if (n_inventors >= 1) {
      for (j in c(1:n_inventors)) {
        docnum_to_inventor[inventor_index] <- document_number[i]

        if (!(is.null(ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$`first-name`)) &
          length(ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$`first-name`) > 0) {
          inventor_first_name[inventor_index] <- unlist(
            ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$`first-name`
          )
        }

        if (!(is.null(ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$`last-name`)) &
          length(ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$`last-name`) > 0) {
          inventor_last_name[inventor_index] <- unlist(
            ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$`last-name`
          )
        }

        if (!(is.null(
          ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$address$city
        ))) {
          inventor_city[inventor_index] <- unlist(
            ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$address$city
          )
        }

        if (!(is.null(ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$address$country))) {
          inventor_country[inventor_index] <- unlist(
            ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$address$country
          )
        }


        if (!(is.null(
          ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$address$state
        ))) {
          inventor_state[inventor_index] <- unlist(
            ut_data[[i]]$`us-parties`$inventors[[j]]$addressbook$address$state
          )
        }

        inventor_position[inventor_index] <- j
        inventor_index <- inventor_index + 1
      }
    }
  }


  # sometimes this is returned as vector, and sometimes its a list.
  if (typeof(inventor_city) == "list") {
    inventor_city <- unlist(inventor_city)
  }


  return(data.frame(
    docnum_to_inventor,
    inventor_position,
    inventor_first_name,
    inventor_last_name,
    inventor_city,
    inventor_state,
    inventor_country
  ))
}


###################################
# MAIN LOOP


# for testing purposes, assign xml_file; this is assigned in foreach loop.
#xml_file <- xml_files[1]

# foreach() will extract wanted values in parallel to reduce total time.
# use more than 1, but less than n cores (to prevent application crash)
# FYI: detectCores believes my 6 core / 12 LP machine has "12 cores"
# using more than 6 slows all other applications to a useless crawl.
ncores <- detectCores()
cl <- makeCluster(ncores %/% 3, outfile = log_name) 
registerDoParallel(cl)



total_start_time <- Sys.time() # tracking clock time; may print to log

xml_files = xml_files
foreach(xml_file = xml_files, .packages = c("xml2", "utils")) %dopar% { # %do% {

  loop_start_time <- Sys.time()
  xml_file_base <- sub(".xml", "", xml_file)
  xml_data <- get_and_read_xml(xml_file)


  node_list <- convert_xml_to_list(xml_data, node_string)
  
  # without manual rm() and gc(), I'm seeing memory issues when running parallel
  rm(xml_data) # free memory. 

  node_list <- pare_to_utility_patents(node_list)
  bib_df <- extract_bibdata_from_list(node_list)
  if (data_year <= 2014) {
    inventor_df <- extract_2014_inventor_data_from_list(node_list, as.character(bib_df$document_number))
  } else {
    inventor_df <- extract_inventor_data_from_list(node_list, as.character(bib_df$document_number))
  }

  rm(node_list) # parallel mem probs. see rm/gc comment above.

  # WRITE TO FILE

  bib_data_file_name <- paste0(output_path, "bib_data_file_", xml_file_base, ".csv")
  inventor_data_file_name <- paste0(output_path, "inventor_data_file_", xml_file_base, ".csv")

  
  write.csv(bib_df, bib_data_file_name, row.names = FALSE)
  write.csv(inventor_df, inventor_data_file_name, row.names = FALSE)

  loop_end_time <- Sys.time()
  
  # weirdly this seems to be platform dependent 
  # as to whether the message ends up in the log.
  capture.output(paste0("DONE WITH", xml_file_base, "-- It took: "), append = TRUE)
  capture.output(loop_end_time - loop_start_time, append = TRUE)

  gc(verbose = FALSE)
}

total_end_time <- Sys.time()
print(total_end_time - total_start_time)


# Close out the Parallel
registerDoSEQ() # no idea, but prevents error
stopCluster(cl)
