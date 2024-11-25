rm(list=ls())
library(tidyverse)
library(here) # for having relative a relative path
library(future.apply)

source("carbonplus_main.R")

# Settings
settings <- list(
  
  # Input and output settings
  local_inputs_path = "./example-farm-data",
  local_use_json = F,  # if TRUE will read local inputs from json files, else from csv
  write_out_inputs = T,
  
  # Uncertainty calculations settings
  n_runs = 2, # 100 for production run
  se_field_carbon_in = 0.1, # 0.1 for production run
  se_inputs_nonfarm = 0.025, # 0.025 for production run
  
  # Model function settings
  spinup_years = 300, # 300 for production runs
  dynamic_baseline = T,
  calc_tree_emissions = T,
  new_grass_mixture = T,
  dr_ratio_from_litter = T, # if FALSE, set the dr_ratio based on irrigation. if TRUE, set it based on the type of litter
  correct_forage_mismatch = T, # if TRUE, correct forage mismatch by adjusting the forage potential
  forage_tolerance = 0.8, # tolerance for forage potential as fraction of forage grazed
  
  # Debugging settings -- turn off for production runs
  debug_mode = T,
  fill_missing_irrigation = F, # for debugging: assume non-irrigated for missing irrigation data
  output_sense_checks = F,
  
  # Settings to output XLSX files
  output_xlsx_inputs_raw = F # outputs all data that enables data completeness checks, and stops code after extraction functions
  
)

## Run a single farm from local input files
# All input files must be in a folder named as the local_run_farmId and inside the local_inputs_path (in settings)
# Required input files are: climate.csv	farmInfo.json	npp.csv env_zone.csv	farm_data.json	soil.csv
out <- carbonplus_main(settings=settings, local_run_farmId="example_myfarm")

