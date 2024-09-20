rm(list=ls())
library(tidyverse)
library(here) # for having relative a relative path
library(future.apply)

source("carbonplus_main.R")

# Read in init_file.json from sensitive-data folder
init_file <- here("../sensitive-data", "init_file.json")

# Test settings
# settings <- list(
#   n_runs = 2,
#   se_field_carbon_in = 0.1,
#   se_inputs_nonfarm = 0.025,
#   debug_mode = T,
#   save2mongoDB = F,
#   server = "prod", # prod",  # One of: "prod", "dev", "test", "test_pioneers"
#   use_test_climate = F,
#   use_test_soil = F,
#   spinup_years = 30, # 300 for production runs
#   dynamic_baseline = T,
#   calc_tree_emissions = T,
#   new_grass_mixture = T,
#   dr_ratio_from_litter = T, # if FALSE, set the dr_ratio based on irrigation. if TRUE, set it based on the type of litter
#   # settings for debugging -- should be turned off for production runs
#   fill_missing_irrigation = F, # for debugging: assume non-irrigated for missing irrigation data
#   output_sense_checks = F,
#   output_landuse_plots = F,
#   # settings to output XLSX files
#   output_xlsx_for_excel_model = F,
#   output_xlsx_inputs_raw = F # outputs all data that enables data completeness checks, and stops code after extraction functions
# )

## Production settings
settings <- list(
  n_runs = 2,
  se_field_carbon_in = 0.1,
  se_inputs_nonfarm = 0.025,
  debug_mode = F,
  save2mongoDB = F,
  server = "prod", # prod",  # One of: "prod", "dev", "test", "test_pioneers"
  use_test_climate = F,
  use_test_soil = F,
  spinup_years = 30, # 300 for production runs
  dynamic_baseline = T,
  calc_tree_emissions = F,
  new_grass_mixture = T,
  dr_ratio_from_litter = T, # if FALSE, set the dr_ratio based on irrigation. if TRUE, set it based on the type of litter
  # settings for debugging -- should be turned off for production runs
  fill_missing_irrigation = F,  # for debugging: assume non-irrigated for missing irrigation data
  output_sense_checks = F,
  output_landuse_plots = F,
  # settings to output XLSX files
  output_xlsx_for_excel_model = F,
  output_xlsx_inputs_raw = F # outputs all data that enables data completeness checks, and stops code after extraction functions
)

out <- carbonplus_main(init_file=init_file, settings=settings, db_farmId=farmId)
