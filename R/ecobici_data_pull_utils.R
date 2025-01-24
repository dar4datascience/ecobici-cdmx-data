
# Crawl Historical Information --------------------------------------------

library(rvest)
library(dplyr)
library(lubridate)

Sys.setlocale("LC_TIME", "es_ES.utf8")

# Function to extract year-month and convert to Date
extract_year_month <- function(file_name) {
  # Match year and month in YYYY_MM, YYYY-MM, or YYYYMM formats
  match <- sub(".*?(\\d{4})[-_]?([0-1][0-9]).*", "\\1-\\2", file_name)
  # Validate format (ensure it's valid YYYY-MM)
  if (grepl("^\\d{4}-\\d{2}$", match)) {
    return(match) # Return in YYYY-MM format
  } else {
    return(NA) # Return NA if no valid year-month found
  }
}
library(lubridate)

# Function to parse year-month with Spanish month name
parse_spanish_month_name <- function(input_string) {
  # Define Spanish month names as a vector
  spanish_months <- c("enero", "febrero", "marzo", "abril", "mayo", "junio", 
                      "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")
  
  # Extract year and month name from input string (e.g., "2024_enero")
  match <- sub("^(\\d{4})_([a-z]+)$", "\\1 \\2", input_string)
  
  # Extract the year and month name
  year_month <- strsplit(match, " ")[[1]]
  year <- year_month[1]
  month_name <- tolower(year_month[2])
  
  # Find the corresponding month number
  month <- which(spanish_months == month_name)
  
  # Create a date string in the format YYYY-MM-01
  date_string <- paste0(year, "-", sprintf("%02d", month), "-01")
  
  # Parse the date string to Date class
  parsed_date <- as.Date(date_string)
  
  return(parsed_date)
}

historic_page_ecobici_hrefs_all <- read_html("https://ecobici.cdmx.gob.mx/datos-abiertos/") %>% 
  html_elements("a") %>% 
  html_attr("href")
  
historical_ecobici_csv_hrefs <- historic_page_ecobici_hrefs_all[grepl("wp-content/uploads/", historic_page_ecobici_hrefs_all)]  %>% 
  tibble::tibble(
    download_urls = .
  ) %>% 
  mutate(
    csv_name = basename(download_urls),
    period = sapply(csv_name, extract_year_month)
  ) %>% 
  mutate(
    period = ym(period) 
  ) %>% 
  mutate(
    # preiod= gsub("^(ecobici_|datosabiertos_)", "", csv_name), 
    #   preiod = gsub(".csv", "", preiod),
    # preido = parse_spanish_month_name(preiod),
    period = coalesce(
      period,
      gsub("^(ecobici_|datosabiertos_)(.*)\\.csv$", "\\2", csv_name) %>% 
        parse_spanish_month_name()
    )
  )








# Get Stationcoalesce()# Get Station Map From LIve Map -------------------------------------------




# Process From To Trips ---------------------------------------------------


