# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tibble",
               "furrr",
               "future",
               "cli",
               "janitor",
               "arrow",
               "dbplyr",
               "dplyr",
               "jsonlite",
               "purrr",
               "rvest",
               "lubridate"), # Packages that your targets need for their tasks.
  # format = "qs", # Optionally set the default storage format. qs is fast.
  #
  # Pipelines that take a long time to run may benefit from
  # optional distributed computing. To use this capability
  # in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller that scales up to a maximum of two workers
  # which run as local R processes. Each worker launches when there is work
  # to do and exits if 60 seconds pass with no tasks to run.
  #
  controller = crew::crew_controller_local(workers = 4, seconds_idle = 15)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package.
  # For the cloud, see plugin packages like {crew.aws.batch}.
  # The following example is a controller for Sun Grid Engine (SGE).
  # 
  #   controller = crew.cluster::crew_controller_sge(
  #     # Number of workers that the pipeline can scale up to:
  #     workers = 10,
  #     # It is recommended to set an idle time so workers can shut themselves
  #     # down if they are not running tasks.
  #     seconds_idle = 120,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.2".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.
ADD STEP TO CHECK AVAIABLE FILES AND SEARCH PENDNG ONES BEFORE 2015 WHICH MAKES SENSE...... 2015 IS GOOD I JUST WANT IT COMPKETE
# Replace the target list below with your own:
list(
  tar_target(
    name = historical_ecobici_csv_hrefs,
    command = fetch_ecobici_cdmx_historical_monthly_csv_urls()
    # format = "qs" # Efficient storage for general data objects.
  ),
  tar_target(
    name = validation_historical_ecobici_csv_hrefs, # missing a month in 2010 which is expected
    command = validate_complete_years(historical_ecobici_csv_hrefs)
  ),
  tar_target(
    name = latest_station_information,
    command = get_latest_station_information()
  ),
  tar_target(
    name = ecobici_data_process,
    command = process_all_rows_safely(historical_ecobici_csv_hrefs)
  )
  # tarchetypes::tar_group_by(ecobici_data_groups,
  #                           process_ecobici_online_month_data(historical_ecobici_csv_hrefs),
  #                           download_urls,
  #                           period), 
  # tar_target(
  #   name = list_ecobici_records,
  #   command = pmap(as.data.frame(historical_ecobici_csv_hrefs), function(...) {
  #     row <- list(...)  # Capture the row as a list
  #     return(row)       # Return the row
  #   })
  # ),
  # tar_target(
  #   ecobici_data_process,
  #   process_ecobici_online_month_data(list_ecobici_records),
  #   pattern = map(list_ecobici_records)
  # )
  # tar_target(
  #   name = ecobici_data_groups,
  #   command = historical_ecobici_csv_hrefs |>
  #     group_by(download_urls, period) |> 
  #     tar_group(),
  #   iteration = "group"
  # ),
  # tar_target(
  #   # iterate dynamicaly https://books.ropensci.org/targets/dynamic.html#list-iteration
  #   name = ecobici_online_month_data,
  #   command = process_ecobici_online_month_data(ecobici_data_groups),
  #   pattern = map(ecobici_data_groups)
  # )
)
