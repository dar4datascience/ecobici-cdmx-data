# Chek Data ---------------------------------------------------------------


check_ecobici_data_completeness <- function(folder = "data_sink", historical_ecobici_csv_hrefs) {
  
  # for now because previous funciton returns just null
  folder <- "data_sink"
  
  # List all parquet files
  files <- dir_ls(folder, glob = "*.parquet")
  
  # Extract year and month
  df <- tibble(
    file = files,
    date = files %>% path_file() %>% str_remove(".parquet") %>% ymd()
  ) %>%
    mutate(file_year = year(date), month = month(date)) 
  
  # Check completeness
  expected <- expand.grid(file_year = unique(df$file_year), month = 1:12)
  
  completeness <- expected %>%
    left_join(df, by = c("file_year", "month")) %>%
    mutate(missing = is.na(file)) %>%
    group_by(file_year) %>%
    summarize(missing_months = list(month[missing])) |> 
    ungroup()
  
  # now filter to get the problematic file_years
  years_2_retry <- completeness |> 
    #select(file_year) |> 
    dplyr::filter(
      between(file_year, 2011, year(Sys.Date()) -1 )
    ) |> 
    mutate(
      missing_months = as.character(missing_months)
    )  |> 
    dplyr::filter(
      stringr::str_length(missing_months) > 10
    ) |> 
    pull(
      file_year
    )
  
  urls_to_retry <- historical_ecobici_csv_hrefs |> 
    dplyr::filter(
      year(period) %in% years_2_retry
    )
  
  return(urls_to_retry)
}


# Crawl Historical Information --------------------------------------------

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

# Function to parse year-month with Spanish month name
parse_spanish_month_name <- function(input_string) {
  tryCatch({
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
  }, error = function(e) {
    # Return NULL in case of error
    return(NA)
  })
}

# Define the function to fetch and process Ecobici CDMX historical monthly CSV URLs
fetch_ecobici_cdmx_historical_monthly_csv_urls <- function() {
  
  # Step 1: Fetch the webpage
  cli_alert_info("Step 1: Fetching the historical page from the Ecobici CDMX website.")
  
  historic_page_ecobici_hrefs_all <- tryCatch({
    read_html("https://ecobici.cdmx.gob.mx/datos-abiertos/") %>% 
      html_elements("a") %>% 
      html_attr("href")
  }, error = function(e) {
    cli_alert_danger("Failed to fetch webpage: {e$message}")
    return(NULL)
  })
  
  if (is.null(historic_page_ecobici_hrefs_all)) {
    cli_alert_danger("Page fetching failed. Exiting function.")
    return(NULL)
  }
  
  cli_alert_success("Historical page successfully fetched.")
  
  # Step 2: Filter and clean the URLs
  cli_alert_info("Step 2: Filtering and cleaning the URLs.")
  
  historical_ecobici_csv_hrefs <- historic_page_ecobici_hrefs_all[grepl("wp-content/uploads/", historic_page_ecobici_hrefs_all)] %>% 
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
      clean_period = gsub("^(ecobici_|datosabiertos_)(.*)\\.csv$", "\\2", csv_name),
      clean_period = gsub("^datos_abiertos_(.*)\\.csv$", "\\1", clean_period),
      cleaner_period = sapply(clean_period, parse_spanish_month_name),
      period = coalesce(period, as.Date(cleaner_period))
    ) %>% 
    mutate(
      full_download_url = paste0('https://ecobici.cdmx.gob.mx', download_urls)
    )
  
  cli_alert_success("URLs successfully filtered and cleaned.")
  
  # Return the cleaned tibble of URLs and metadata
  return(historical_ecobici_csv_hrefs)
}

# Example usage:
# cleaned_urls <- fetch_ecobici_cdmx_historical_monthly_csv_urls()


# count number of files per year: its all good. add as test
# Define the function to validate the complete years (12 months)
validate_complete_years <- function(historical_ecobici_csv_hrefs) {
  
  # Step 1: Extract the year and count occurrences
  cli_alert_info("Step 1: Extracting year from the 'period' and counting occurrences.")
  
  period_counts <- historical_ecobici_csv_hrefs %>% 
    select(period) %>% 
    mutate(
      period = year(period)  # Extract only the year from the period
    ) %>% 
    count(period)
  
  cli_alert_success("Step 1: Year extraction and counting completed.")
  
  # Step 2: Validate completeness (should be 12 months per year)
  cli_alert_info("Step 2: Validating if each year has 12 entries (months).")
  
  missing_years <- period_counts %>% 
    dplyr::filter(n != 12)
  
  if (nrow(missing_years) > 0) {
    cli_alert_danger("Validation failed! The following years are missing months or have extra months:")
    cli_alert_danger("{missing_years$period} with {missing_years$n} entries.")
  } else {
    cli_alert_success("Validation successful! All years have 12 months.")
  }
  
  # Return the period_counts for further inspection if needed
  return(period_counts)
}

read_from_downloading_n_preprocess <- function(url, timeout = 300) {
  cli_alert_info("Setting options and increasing timeout to {timeout} seconds.")
  
  # Set timeout option
  options(timeout = max(timeout, getOption("timeout")))
  
  file_name <- basename(url)
  
  output_path <- glue::glue("landing/{file_name}")
  
  cli::cli_inform(
    glue::glue("Downloading {url} to outputh path {output_path}")
  )
  
  # Download FIle
  download.file(url, output_path, )

  on.exit(file.remove(output_path))
  
  # Step 1: Read data from URL using arrow
  cli_alert_info("Reading CSV data from landing: {url}")
  raw_df <- tryCatch({
    arrow::read_csv_arrow(output_path)
  }, error = function(e) {
    cli_alert_danger("Failed to read data from URL: {url}. Error: {e$message}")
    return(NULL)
  })
  
  #print(raw_df)
  
  if (is.null(raw_df)) {
    cli_alert_danger("Data loading failed. Exiting function.")
    return(NULL)
  }
  
  cli_alert_success("Data successfully loaded from File")
  
  # Step 2: Clean the data
  cli_alert_info("Cleaning data: renaming columns, removing unnecessary ones, and parsing dates and times.")
  
  clean_df <- raw_df |>
    janitor::clean_names() |> 
    select(!c(genero_usuario, edad_usuario)) |> 
    mutate(
      fecha_arribo = dmy(fecha_arribo),
      fecha_retiro = dmy(fecha_retiro),
      hora_retiro = hms(hora_retiro),
      hora_arribo = hms(hora_arribo)
    )
  
  #write_csv_arrow(clean_df, "examine.csv")
  
  #print(colnames(clean_df))
  
  #glimpse(clean_df)
  
  clean_df <- clean_df |> 
    mutate(
      retiro_datetime_stamp = update(fecha_retiro, hours = hour(hora_retiro), minutes = minute(hora_retiro), seconds = second(hora_retiro)),
      arribo_datetime_stamp = update(fecha_arribo, hours = hour(hora_arribo), minutes = minute(hora_arribo), seconds = second(hora_arribo))
    ) 
  
  clean_df <- clean_df |> 
    mutate(
      duracion_viaje_secs = difftime(arribo_datetime_stamp, retiro_datetime_stamp),
      duracion_viaje_mins = difftime(arribo_datetime_stamp, retiro_datetime_stamp, units = "mins")
    ) |> 
    select(
      !c(fecha_arribo,
         fecha_retiro,
         hora_retiro,
         hora_arribo)
    )
  
  cli_alert_success("Data cleaned and processed successfully.")
  
  # Return the cleaned data frame
  return(clean_df)
}

# Define the function to read data from URL and preprocess
read_from_url_n_preprocess <- function(url, timeout = 300) {
  cli_alert_info("Setting options and increasing timeout to {timeout} seconds.")
  
  # Set timeout option
  options(timeout = max(timeout, getOption("timeout")))
  
  # Step 1: Read data from URL using arrow
  cli_alert_info("Reading CSV data from URL: {url}")
  raw_df <- tryCatch({
    arrow::read_csv_arrow(url)
  }, error = function(e) {
    cli_alert_danger("Failed to read data from URL: {url}. Error: {e$message}")
    return(NULL)
  })
  
  #print(raw_df)
  
  if (is.null(raw_df)) {
    cli_alert_danger("Data loading failed. Exiting function.")
    return(NULL)
  }
  
  cli_alert_success("Data successfully loaded from URL.")
  
  # Step 2: Clean the data
  cli_alert_info("Cleaning data: renaming columns, removing unnecessary ones, and parsing dates and times.")
  
  clean_df <- raw_df |>
    janitor::clean_names() |> 
    select(!c(genero_usuario, edad_usuario)) |> 
    mutate(
      fecha_arribo = dmy(fecha_arribo),
      fecha_retiro = dmy(fecha_retiro),
      hora_retiro = hms(hora_retiro),
      hora_arribo = hms(hora_arribo)
    )
  
  #write_csv_arrow(clean_df, "examine.csv")
    
    #print(colnames(clean_df))
    
    #glimpse(clean_df)
    
  clean_df <- clean_df |> 
    mutate(
      retiro_datetime_stamp = update(fecha_retiro, hours = hour(hora_retiro), minutes = minute(hora_retiro), seconds = second(hora_retiro)),
      arribo_datetime_stamp = update(fecha_arribo, hours = hour(hora_arribo), minutes = minute(hora_arribo), seconds = second(hora_arribo))
    ) 
  
  clean_df <- clean_df |> 
    mutate(
      duracion_viaje_secs = difftime(arribo_datetime_stamp, retiro_datetime_stamp),
      duracion_viaje_mins = difftime(arribo_datetime_stamp, retiro_datetime_stamp, units = "mins")
    ) |> 
    select(
      !c(fecha_arribo,
         fecha_retiro,
         hora_retiro,
         hora_arribo)
    )
  
  cli_alert_success("Data cleaned and processed successfully.")
  
  # Return the cleaned data frame
  return(clean_df)
}


process_ecobici_online_month_data <- function(full_download_url,period) {
  
  # Step 1: Read the data
  cli_alert_info("Step 1: Reading data from File: {full_download_url}")
  
  clean_df <- tryCatch({
    read_from_downloading_n_preprocess(full_download_url)
    #read_from_url_n_preprocess(full_download_url)  # Assuming this function works
  }, error = function(e) {
    cli_alert_danger("Failed to read data from File: {full_download_url}. Error: {e$message}")
    return(NULL)
  })
  
  if (nrow(clean_df) == 0) {
    cli_alert_danger("Data loading failed. Exiting function.")
    return(NULL)
  }
  
  cli_alert_success("Data successfully loaded and preprocessed.")
  
  # Step 2: Export to Parquet
  cli_alert_info("Step 2: Exporting cleaned data to Parquet format.")
  
  # Define the path to save the Parquet file
  output_path <- paste0("data_sink/", period, ".parquet")
  
  # Write to Parquet
  tryCatch({
    clean_df |> 
      arrow::write_parquet(output_path)
  }, error = function(e) {
    cli_alert_danger("Failed to write Parquet file to {output_path}. Error: {e$message}")
    return(NULL)
  })
  
  cli_alert_success("Data successfully exported to Parquet file: {output_path}")
  
  return(clean_df)
}

sequential_process_of_urls <- function(historical_ecobici_csv_hrefs){
  
  # filter df to avoid redownloading all
  historical_ecobici_csv_hrefs <- check_ecobici_data_completeness(folder = "data_sink",
                                                                  historical_ecobici_csv_hrefs)
  
  
  safe_process_ecobici_online_month_data <- purrr:::safely(process_ecobici_online_month_data)
  
  full_download_url <- historical_ecobici_csv_hrefs[['full_download_url']]
  period <- historical_ecobici_csv_hrefs[['period']]
  
  results <- map2(full_download_url, 
             period,
             ~ safe_process_ecobici_online_month_data(.x, .y)
  )
  
  return(results)
  
}

process_all_rows_safely <- function(historical_ecobici_csv_hrefs) {
  
  # safely purrr would do a better jerb
  
  # Use purrr::pwalk to iterate over the rows of the dataframe
  cli_alert_info("Step 1: Starting to process each row in the dataframe.")
  
  result_list <- tryCatch({
    full_download_url <- historical_ecobici_csv_hrefs[['full_download_url']]
    period <- historical_ecobici_csv_hrefs[['period']]
    
    dispose_cores <- future::availableCores() - 2
    
    plan(multicore, workers = dispose_cores)
    
    future_map2(full_download_url, 
         period,
         ~ process_ecobici_online_month_data(.x, .y)  # Apply your processing function here
    ,
    .progress = TRUE)
  }, error = function(e) {
    cli_alert_danger("An error occurred while processing the rows: {e$message}")
    return(NULL)
  })
  
  if (is.null(result_list)) {
    cli_alert_danger("Row processing failed. Exiting function.")
    return(NULL)
  }
  
  cli_alert_success("All rows have been processed successfully.")
  
  # Optionally, return the result list (which will contain the processed data from each row)
  return(result_list)
}

#library(duckdb)
#library(DBI)


#quack <- dbConnect(duckdb())

#pending extensiosn
# dbExecute(quack, "INSTALL httpfs; LOAD httpfs;")

# fails due to type inconsistency
#eexample_tbl <- dbGetQuery(quack, 'SELECT * FROM read_csv("https://ecobici.cdmx.gob.mx/wp-content/uploads/2024/03/2024-02.csv", sample_size = -1)')

# dbGetQuery(quack, 'FROM sniff_csv("https://ecobici.cdmx.gob.mx/wp-content/uploads/2024/03/2024-02.csv")')

# library(dbplyr)
# # hora retiro and arribo is baldly parsed
# example_tbl <- tbl(quack, sql('SELECT Bici, Ciclo_Estacion_Retiro, Fecha_Retiro, Hora_Retiro, Ciclo_EstacionArribo, "Fecha Arribo", Hora_Arribo FROM read_csv("https://ecobici.cdmx.gob.mx/wp-content/uploads/2024/03/2024-02.csv",
# auto_detect=false,
# columns = {
#     "Bici": "INTEGER",  "Ciclo_Estacion_Retiro": "INTEGER",
#     "Fecha_Retiro": "DATE",
#     "Hora_Retiro": "TIME",
#     "Ciclo_Estacion_Arribo": "INTEGER",
#     "Fecha_Arribo": "DATE",
#     "Hora_Arribo": "TIME"
# }')) 


# wirjst case download.file() process delete
# Get Stationcoalesce()# Get Station Map From LIve Map -------------------------------------------

# Define the function to fetch and process the latest station information
get_latest_station_information <- function() {
  
  # Step 1: Fetch the live station data
  cli_alert_info("Step 1: Fetching the live station information from the API.")
  
  station_json_data_live <- tryCatch({
    read_json("https://gbfs.mex.lyftbikes.com/gbfs/en/station_information.json")
  }, error = function(e) {
    cli_alert_danger("Failed to fetch station data. Error: {e$message}")
    return(NULL)
  })
  
  if (is.null(station_json_data_live)) {
    cli_alert_danger("Fetching station data failed. Exiting function.")
    return(NULL)
  }
  
  cli_alert_success("Station information successfully fetched.")
  
  # Step 2: Process the 'last_updated' field and station data
  cli_alert_info("Step 2: Processing the station data and 'last_updated' field.")
  
  stations_data_df <- tibble::tibble(
    last_updated = as.POSIXct(station_json_data_live$last_updated, origin = "1970-01-01", tz = "UTC"),
    station_data = tidyr::nest(
      station_json_data_live$data$stations %>% 
        map(\(x) as.data.frame(x)) %>% 
        list_rbind()
    )
  )
  
  cli_alert_success("Station data processing completed.")
  
  # Step 3: Extract individual station data
  cli_alert_info("Step 3: Extracting individual station data.")
  
  station_data <- station_json_data_live$data$stations %>% 
    map(\(x) as.data.frame(x)) %>% 
    list_rbind()
  
  cli_alert_success("Individual station data extraction completed.")
  
  # Return the results: a list containing both the processed dataframe and individual station data
  return(list(
    stations_data_df = stations_data_df,
    station_data = station_data
  ))
}

# Example usage:
# station_info <- get_latest_station_information()

# Process From To Trips ---------------------------------------------------







# Combine parquets into single duckdb  ------------------------------------


# Function to ingest Parquet files into DuckDB
ingest_parquet_files <- function(folder = "data_sink") {
  
  # Initialize DuckDB connection
  con <- dbConnect(duckdb::duckdb(), "data_sink.duckdb")
  
  on.exit(dbDisconnect(con))
  
  # List all parquet files
  files <- dir_ls(folder, glob = "*.parquet")
  
  for (file in files) {
    # Extract date-based name for the table
    date_str <- path_file(file) %>% str_remove(".parquet")
    table_name <- paste0("data_", gsub("-", "_", date_str))  # e.g., "data_2024_08_01"
    
    # Load Parquet file into DuckDB
    query <- sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", table_name, file)
    dbExecute(con, query)
    
    message(sprintf("Ingested %s into DuckDB as %s", file, table_name))
  }
}
