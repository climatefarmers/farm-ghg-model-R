carbonplus_main <- function(init_file, settings, db_farmId=NA, JSONfile=NA){
  
  ####################################################################
  # This script has the following functions:
  # - prepare log files
  # - set general model settings
  # - get farm data from mongoDB
  # - get farm environmental zone
  # - read in factors
  # - process input data
  # - call the soil model and emissions calculations
  # - upload resulting co2eq to mongoDB
  ####################################################################
  # Settings should be a list with:
  # n_runs: Integer. The number of runs for getting uncertainties. Using 100 for production runs.
  # sd_field_carbon_in: Positive decimal. standard error of the carbon inputs. Using 0.1 as default.
  # debug_mode: Skips fetching some data and uses local climate and soil data. Writes out more data. Removes added error.
  # save2mongoDB: Set to TRUE for production runs to save to database
  # # To copy the practice of a single year to all others
  # server: Server to use. One of: "prod", dev", "test"
  # bareground: How baseline bare ground values should be determined: "envzone": uses a regional common practice, "reported": uses the reported current practice (year0) or "none": bare ground always FALSE
  ####################################################################

  
  ## Loading libraries ---------------------------------------------------------
  
  library(pacman)
  p_load('pacman', 'SoilR', 'mongolite', 'tidyverse',
          'aws.s3', 'log4r', 'jsonlite',
         'httr', 'logger', 'ncdf4', 'ncdf4.helpers',
         'openxlsx2', 'cowplot',
         'here', 'tidyverse')
  #'soilassessment',
  
  
  ## Prepare log files ---------------------------------------------------------
  # clear and prepare log and output directory
  if(!dir.exists('logs')) {dir.create('logs')}
  if(!dir.exists('output')) {dir.create('output')}
  # unlink(file.path("logs", "*"), recursive = TRUE)
  # unlink(file.path("output", "*"), recursive = TRUE)  # Tim turning this off to keep the output files
  
  my_logfile = file.path('logs', paste('out_', db_farmId, "_", str_replace_all(Sys.time(), c(" "="__", ":"="_")),'.log',sep=""))
  my_console_appender = console_appender(layout = default_log_layout())
  my_file_appender = file_appender(my_logfile, append = TRUE, 
                                   layout = default_log_layout())
  my_logger <- log4r::logger(threshold = "INFO", 
                             appenders= list(my_console_appender,my_file_appender))
  
  ## Checking model settings -------------------------------------------------------
  
  if(settings$debug_mode & settings$save2mongoDB) {stop("Must set debug_mode to FALSE when setting save2mongoDB to TRUE.")}
  
  ## Fetching Data -----------------------------------------------------------
  
  init_data <- fromJSON(init_file)
  # Set environmental variables for AWS 
  Sys.setenv(
    "AWS_ACCESS_KEY_ID" = init_data$AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY" = init_data$AWS_SECRET_ACCESS_KEY,
    "AWS_DEFAULT_REGION" = init_data$AWS_DEFAULT_REGION
  )
  
  source(file.path("soil","run_soil_model.R"), local = TRUE)
  source(file.path("get_emissions.R"), local = TRUE)
  source(file.path("dynamic_baseline.R"), local = TRUE)
  source(file.path("get_leakage.R"), local = TRUE)
  source(file.path("utility_functions.R"), local = TRUE)
  source(file.path("get_tree_carbon.R"), local = TRUE)
  # source(file.path("test_functions.R"), local = TRUE) # Delete?
  
  ## Get the farm data from the JSON file or MongoDB ---------------------------

  # Check that only one source of farm data was provided
  if(!is.na(db_farmId) & !is.na(JSONfile)){
    stop("Both farmId AND JSON files were passed to the model. Please choose only one.")
  }

  if(is.na(db_farmId) & is.na(JSONfile)){
    stop("Both farmId AND JSON files are missing. One must be passed.")
  }

  if(settings$server == "prod") {
    connection_string = init_data$prod$connection_string
    db <- init_data$prod$db
    collection <- init_data$prod$collection
  } else if(settings$server == "dev") {
    connection_string = init_data$dev$connection_string
    db <- init_data$dev$db
    collection <- init_data$dev$collection
  } else if(settings$server == "test") {
    connection_string = init_data$test$connection_string
    db <- init_data$test$db
    collection <- init_data$test$collection
  } else if(settings$server == "test_DBv2_audit2023") {
    connection_string = init_data$test_DBv2_audit2023$connection_string
    db <- init_data$test_DBv2_audit2023$db
    collection <- init_data$test_DBv2_audit2023$collection
  } else {stop("Wrong value for variable: server")
  }
  
  if(!is.na(JSONfile)){
    stop("JSON file reading not implemented yet. Format doesn't match the mongolite one.")
    # monitoringData <- fromJSON(JSONfile, simplifyVector = FALSE)[[1]] # Format not matching mongolite
  } else {
    # pull the monitoring data
    # use "iterate" instead of "find" as it can return the same format as jsonlite::fromJSON (though it will be a bit slower)
    farm_collection <- mongo(collection=collection, db=db, url=connection_string)
    monitoringData <- fromJSON(farm_collection$iterate(paste0('{"farmId":"',db_farmId,'"}'))$json())  # convert to json and back to list
    ## Testing different ways to fetch json data
    # monitoringData2 <- farm_collection$iterate(paste0('{"farmId":"',db_farmId,'"}'))  # convert to json and back to list
    # monitoringData3 <- farm_collection$find(paste0('{"farmId":"',db_farmId,'"}'))  # convert to json and back to list
    # monitoringData4 <- fromJSON(here("../test_files/troya_monitoring_2024-06-10.json"), simplifyVector = TRUE, simplifyDataFrame = FALSE)[[1]]
    # monitoringData5 <- fromJSON(here("../test_files/troya_monitoring_2024-06-10.json"))
  }
  
  # Email and farm ID
  email <- monitoringData$email
  db_farmId <- monitoringData$farmId
  
  log4r::info(my_logger, paste0("farmId (monitoringdatas) = ", db_farmId, ", email = ", email))
  
  
  ## Retrieve farmInfo and add to the monitoringData object
  farmInfo_collection <- mongo(collection=init_data$prod$collection_farms, db=init_data$prod$db, url=init_data$prod$connection_string)
  farmInfo <- farmInfo_collection$iterate(query = paste0('{"_id":{"$oid":"', db_farmId, '"}}'))
  
  if(length(farmInfo$json()) > 0){
    farmInfo <- fromJSON(farmInfo_collection$iterate(query = paste0('{"_id":{"$oid":"', db_farmId, '"}}'))$json())
    monitoringData$farmInfo <- farmInfo$farmInfo
  } else if (settings$output_xlsx) {
    log4r::error(my_logger, paste0("ERROR: farmId (monitoringdatas) = ", db_farmId, " not found in farmInfo collection."))
  } else {
    log4r::error(my_logger, paste0("ERROR: farmId (monitoringdatas) = ", db_farmId, " not found in farmInfo collection."))
    return()
  }
  
  ## Fetching pedo-climatic zone
  farm_pars_collection <- mongo(collection=init_data$prod$collection_farmparameters, db=init_data$prod$db, url=init_data$prod$connection_string)
  pars_farmId <- monitoringData$farmInfo$farmId
  farm_parameters <-  farm_pars_collection$find(paste0('{"farmId":"', pars_farmId,'"}'))
  
  # Check if missing. Delete?
  if (nrow(farm_parameters) > 0){
    farm_EnZ <- farm_parameters$enz
    
  } else {
    log4r::error(my_logger, paste0("ERROR: farmparameters collection doesn't contain info on EnZ for farmId (monitoringdatas): ", db_farmId, "."))
    farm_EnZ <- "Mediterranean north"
  }
  if (!(farm_EnZ %in% c('Mediterranean north', 'Mediterranean south'))) {
    log4r::error(my_logger, paste0("ERROR: farmId (monitoringdatas) = ", db_farmId, " has an unknown EnZ: ", farm_EnZ, '. Stopping.'))
    return()
  }
  
  ## Fetching NPP data
  npp_collection <- mongo(collection=init_data$prod$collection_parcel_remote_sensing, db=init_data$prod$db, url=init_data$prod$connection_string)
  npp_object <- npp_collection$iterate(paste0('{"farmId":"',db_farmId,'"}'))
  
  if(length(npp_object$json()) > 0){
    npp_data <- fromJSON(npp_collection$iterate(paste0('{"farmId":"',db_farmId,'"}'))$json())
  } else if (settings$output_xlsx){
    log4r::error(my_logger, paste0("ERROR: farmId (monitoringdatas) = ", db_farmId, " not found in NPP collection."))
    return()
  } else {
    log4r::warn(my_logger, paste0("WARNING: farmId (monitoringdatas) = ", db_farmId, " not found in NPP collection."))
    return()
  }
  
  
  ## Extracting and Processing Inputs -----------------------------------------
  
  source("data_extraction_functions.R", local = TRUE)
  source("data_processing_functions.R", local = TRUE)
  source("data_checking_functions.R", local = TRUE)
  

  ## Reading in calculation factors from csv files
  factors <- list()
  factors$factors_livestock <- read_csv(file.path("data", "factors_livestock.csv"), show_col_types = FALSE)
  factors$factors_crops <- read_csv(file.path("data", "factors_crops.csv"), show_col_types = FALSE)
  factors$factors_natural_area <- read_csv(file.path( "data", "factors_natural_area.csv"), show_col_types = FALSE) %>% 
    filter(pedo_climatic_area==farm_EnZ)
  factors$factors_pastures <- read_csv(file.path("data", "factors_pastures.csv"), show_col_types = FALSE)
  factors$factors_tillage <- read_csv(file.path("data", "factors_tillage.csv"), show_col_types = FALSE)
  factors$factors_co2eq <- read_csv(file.path("data", "factors_co2eq.csv"), show_col_types = FALSE)
  factors$factors_fertilizer <- read_csv(file.path("data", "factors_fertilizer.csv"), show_col_types = FALSE)
  factors$factors_fuel_emissions <- read_csv(file.path("data", "factors_fuel_emissions.csv"), show_col_types = FALSE)
  factors$factors_fuel_operations <- read_csv(file.path("data", "factors_fuel_operations.csv"), show_col_types = FALSE)
  factors$factors_perennials_trees <- read_csv(file.path("data", "factors_perennials_trees.csv"), show_col_types = FALSE)
  factors$factors_methane <- read_csv(file.path("data", "factors_methane.csv"), show_col_types = FALSE) %>%
    filter(climate == factors$factors_natural_area$climate_zone) %>% select(-climate)
  factors$factors_n2o_emission <- read_csv(file.path("data", "factors_n2o_emission.csv"), show_col_types = FALSE)
  factors$factors_others <- read_csv(file.path("data", "factors_others.csv"), show_col_types = FALSE)
  factors$factors_organicmatter <- read_csv(file.path("data", "factors_organicmatter.csv"), show_col_types = FALSE)
  factors$factors_uncertainties <- read_csv(file.path("data", "factors_uncertainties.csv"), show_col_types = FALSE)
  # read in grouped parcel info
  grouped_parcels <- read.csv("../sensitive-data/farm_parcel_grouping.csv", fileEncoding="UTF-8")
  print("Finished reading factors.")
  
  ## Farm data extraction -----------------------------------------------------
  ## Define start index of yearly data
  years <- monitoringData$yearlyFarmData$year
  start_index <- as.numeric(monitoringData$projectStartYear) - years[1] - 2
  # Check if start_index is 2018 if not, raise warning, because I'm not sure what will happen then
  # Check if baseline data is missing
  start_bl <- as_tibble(monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues)
  if (nrow(start_bl) == 0){
    log4r::error(my_logger, paste0("---> ERROR: First year of baseline data is missing for farmId (monitoringdatas) and email: ", db_farmId, " | ", email, ". \n Potential thread that all baseline data is missing."))
    return()
  }
  
  inputs_raw <- list()
  ## Fixed farm and parcel inputs
  inputs_raw$inputs_parcel_fixed <- get_fixed_parcel_inputs(monitoringData, start_index)
  inputs_raw$inputs_farm_fixed <- get_fixed_farm_inputs(monitoringData, inputs_raw$inputs_parcel_fixed)
  
  ## Yearly farm inputs
  
  # Fuel & fertilizer
  inputs_raw$inputs_fuel_direct <- get_fuel_inputs_direct(monitoringData, start_index)
  inputs_raw$inputs_fuel_indirect <- get_fuel_inputs_indirect(monitoringData, start_index)
  inputs_raw$inputs_fertilizer <- get_fertilizer_inputs(monitoringData, start_index)
  
  # Livestock
  inputs_livestock_infarm <- get_livestock_inputs(monitoringData, factors$factors_livestock, start_index)
  inputs_raw$inputs_livestock_category <- inputs_livestock_infarm$livestock_category
  inputs_raw$inputs_livestock_species <- inputs_livestock_infarm$livestock_species
  inputs_raw$inputs_livestock_outfarm <- inputs_livestock_infarm$livestock_outfarm
  inputs_raw$inputs_grazing_management <- inputs_livestock_infarm$grazing_management

  ## Yearly parcel inputs

  # Landuse
  inputs_raw$inputs_landuse <- get_landuse_inputs(monitoringData, start_index)
  
  # Organic amendments
  inputs_raw$inputs_organicmatter <- get_organicmatter_inputs(monitoringData, factors$factors_organicmatter, start_index)
  
  # Monthly grazing
  inputs_raw$inputs_grazing_monthly <- get_grazing_inputs_monthly(monitoringData, start_index)
  
  # Tillage
  inputs_raw$inputs_tillage <- get_tillage_inputs(monitoringData, start_index)
  
  # Irrigation
  inputs_raw$inputs_irrigation <- get_irrigation_inputs(monitoringData, start_index)
  if (settings$fill_missing_irrigation) {
    inputs_raw$inputs_irrigation <- inputs_raw$inputs_irrigation %>%
      mutate(irrigation = if_else(is.na(irrigation), FALSE, irrigation))
    log4r::warn(my_logger, "Filling missing irrigation values: assuming non-irrigated for missing data.")
  }
  
  # Annual crops and fallow
  inputs_annualcrops_fallow <- get_annualcrops_fallow_inputs(monitoringData, start_index)
  inputs_raw$inputs_annualcrops <- inputs_annualcrops_fallow$inputs_annual_crops
  inputs_raw$inputs_baresoil <- inputs_annualcrops_fallow$inputs_bare_soil

  # Perennial crops and trees
  inputs_perennials <- get_perennials_inputs(monitoringData, start_index)
  inputs_raw$inputs_perennialcrops <- inputs_perennials$inputs_perennialcrops #Data for tree biomass calculation
  inputs_raw$inputs_perennialprod <- inputs_perennials$inputs_perennialprod #Harvest and residue from perennial crops, including trees
  inputs_raw$inputs_trees_felled <- get_felled_trees_inputs(monitoringData, start_index)
  
  # Pasture
  inputs_raw$inputs_pasture <- get_pasture_inputs(monitoringData, start_index)
  
  # NPP
  inputs_raw$inputs_npp <- get_npp_inputs(npp_data)
  
  # For farmers with grouped parcels: delete the ones with no data
  inputs_raw <- delete_grouped_parcels(inputs_raw, db_farmId, grouped_parcels)
  
  ## Outputs raw data in xlsx format and returns
  # this is only for extracting raw data
  if (settings$output_xlsx_inputs_raw) {
    save_dir_xlsx <- 'output/xlsx_raw_inputs'
    ifelse (!dir.exists(save_dir_xlsx), dir.create(save_dir_xlsx, recursive = TRUE), unlink(paste0(save_dir_xlsx,"/", email, ".xlsx"))) 
    inputs_temp <- clean_raw_inputs(inputs_raw)
    write_list_to_xlsx(inputs_temp, path = save_dir_xlsx, prefix = email)
    return()
  }
  
  log4r::info(my_logger, "Finished farm data extraction.")
  
  ## Farm data processing -----------------------------------------------------
  inputs_processed <- inputs_raw

  # Join raw data with periods that define project and baseline years based on the given project start year
  periods <- get_periods(
    monitoringData, 
    inputs_processed$inputs_farm_fixed$project_start_year,
    start_index
  )
  
  for (name in names(inputs_processed)) {
    input <- inputs_processed[[name]]
    if ("year" %in% colnames(input)) {
      input <- left_join(input, periods, by = "year")
      inputs_processed[[name]] <- input
    }
  }

  inputs_area <- process_area_inputs(inputs_processed$inputs_parcel_fixed, inputs_processed$inputs_farm_fixed)
  inputs_processed$inputs_parcel_fixed <- inputs_area[[1]]
  factors$factors_area_calc <- inputs_area[[2]]
  
  inputs_processed$inputs_perennials <- process_perennials_inputs( # area factor
    inputs_processed$inputs_perennialcrops,
    inputs_processed$inputs_perennialprod,
    factors$factors_perennials_trees,
    inputs_processed$inputs_parcel_fixed,
    factors$factors_area_calc
    )
  
  inputs_processed$inputs_trees_felled <- process_trees_felled_inputs(
    inputs_processed$inputs_trees_felled,
    factors$factors_perennials_trees
    )
  
  inputs_processed$inputs_annualcrops <- process_annualcrops_inputs( # area factor
    inputs_processed$inputs_annualcrops,
    inputs_processed$inputs_parcel_fixed,
    factors$factors_crops,
    factors$factors_area_calc
  )

  inputs_processed$inputs_pasture <- process_pasture_inputs( # area factor
    inputs_processed$inputs_pasture,
    inputs_processed$inputs_parcel_fixed,
    factors$factors_pastures,
    factors$factors_others,
    factors$factors_area_calc,
    settings$new_grass_mixture
  )
  
  inputs_processed$inputs_npp <- process_npp_inputs(
    inputs_raw$inputs_npp
  )
  
  # NOTE: to decide baresoil and perennial crops should still be added
  inputs_processed$inputs_landuse <- process_landuse_inputs(
    inputs_processed$inputs_parcel_fixed,
    inputs_processed$inputs_landuse,
    inputs_processed$inputs_annualcrops,
    inputs_processed$inputs_baresoil,
    inputs_processed$inputs_pasture,
    inputs_processed$inputs_perennials,
    periods
    )
  
  inputs_processed$inputs_livestock <- process_livestock_inputs(
    inputs_processed$inputs_livestock_category,
    inputs_processed$inputs_livestock_outfarm,
    factors$factors_livestock,
    factors$factors_methane
    )

  inputs_processed$inputs_organicmatter <- process_organicmatter_inputs( # area factor
    inputs_processed$inputs_organicmatter, 
    inputs_processed$inputs_parcel_fixed,
    factors$factors_organicmatter,
    factors$factors_area_calc
    )

  inputs_grazing <- process_grazing_inputs( # grazing data
    inputs_processed$inputs_grazing_monthly,
    inputs_processed$inputs_parcel_fixed,
    inputs_processed$inputs_livestock,
    inputs_processed$inputs_organicmatter,
    inputs_processed$inputs_landuse,
    inputs_processed$inputs_npp,
    factors$factors_others
    )
  
  inputs_processed$inputs_grazing_cover <- inputs_grazing[[1]]
  inputs_processed$inputs_grazing_parcels <- inputs_grazing[[2]] 
  inputs_processed$inputs_fodder <- inputs_grazing[[3]]
  grazing_error_messages <- inputs_grazing[[4]]
  
  inputs_processed$inputs_productivity <- process_productivity_inputs(
    inputs_processed$inputs_landuse,
    inputs_processed$inputs_grazing_cover,
    inputs_processed$inputs_grazing_parcels
    )
  
  inputs_processed$inputs_baresoil <- process_baresoil_inputs(
    inputs_raw$inputs_baresoil,
    monitoringData,
    start_index,
    periods
    )
  
  inputs_processed$inputs_tillage <- process_tillage_inputs(
    inputs_raw$inputs_tillage,
    factors$factors_tillage,
    monitoringData,
    farm_EnZ,
    start_index,
    periods
  )

  inputs_processed$inputs_fuel_indirect <- process_indirect_fuel_inputs(
    inputs_processed$inputs_fuel_indirect, # has year_index
    factors$factors_fuel_operations
    )

  # # Split the livestock into parcels
  inputs_processed$inputs_livestock_parcels <- process_livestock_parcels(
    inputs_processed$inputs_grazing_parcels,
    inputs_processed$inputs_livestock,
    inputs_processed$inputs_livestock_species
    )
  
  print("Finished extracting inputs.")

  # Farm data checking
  if(settings$output_sense_checks) {
    save_dir_errors <- 'output/data_check/errors'
    if (!dir.exists(save_dir_errors)) {dir.create(save_dir_errors)}
    sense_checks <- check_all_inputs(inputs_raw, inputs_processed, grazing_error_messages, save_dir_errors, email)
    # return()
  }
  
  if (settings$output_landuse_plots) {
    save_dir <- paste0('output/data_check/individual_farmer/', email, '/landuse_plots/')
    if (!dir.exists(save_dir)) {dir.create(save_dir, recursive = TRUE)}
    save_dir_prod <- paste0('output/data_check/landuse/productivity_tables/')
    if (!dir.exists(save_dir_prod)) {dir.create(save_dir_prod, recursive = TRUE)}
    save_dir_prod2 <- paste0('output/data_check/landuse/productivity_tables_refined/')
    if (!dir.exists(save_dir_prod2)) {dir.create(save_dir_prod2, recursive = TRUE)}
    save_dir_comb_plot <- 'output/data_check/landuse/00_combined_plots/'
    if (!dir.exists(save_dir_comb_plot)) {dir.create(save_dir_comb_plot, recursive = TRUE)}
    # write out the productivity data for remote sensing
    # need to do this BEFORE the dynamic baseline adjustment (as this deletes the dates and extends this dataframe)
    write_simple_productivity(inputs_processed$inputs_productivity, # inputs_processed$inputs_perennialcrops, 
                              inputs_processed$inputs_landuse, 
                              save_dir_prod, save_dir_prod2, db_farmId)
    # plot the landuse timeline
    plot_landuse_timeline(inputs_processed$inputs_landuse, inputs_processed$inputs_perennialcrops,
                          inputs_processed$inputs_parcel_fixed, save_dir, save_dir_comb_plot, email)
    # return()  # 26 june: turning this on just for now to examine the sense checks with different farms
  }

  ## Dynamic baseline adjustment ------------------------------------------------
  project_years <- seq(as.numeric(inputs_raw$inputs_farm_fixed$project_start_year), max(years))
  inputs_processed$inputs_productivity <- scale_inputs_for_dynamic_baseline(
    inputs_processed$inputs_productivity,
    inputs_processed$inputs_npp,
    inputs_processed$inputs_farm_fixed,
    project_years
  )
  
  
  # if(settings$debug_mode) {
  #   # set up directories for saving
  #   save_dir <- paste0('output/data_debug/', email)
  #   ifelse (!dir.exists(save_dir), dir.create(save_dir, recursive = TRUE), unlink(paste0(save_dir,"/*")))
  #   # save the inputs objects
  #   inputs_raw <- clean_raw_inputs(inputs_raw)
  #   write_list_to_csvs(inputs_raw, path = save_dir, prefix = "raw_")
  #   write_list_to_csvs(inputs_processed, path = save_dir, prefix = "processed_")
  #   
  #   save_dir_xlsx <- 'output/data_debug/xlsx'
  #   ifelse (!dir.exists(save_dir_xlsx), dir.create(save_dir_xlsx), unlink(paste0(save_dir_xlsx,"/*")))
  #   write_list_to_xlsx(inputs_raw, path = save_dir_xlsx, prefix = paste0(email, "_raw"))
  #   write_list_to_xlsx(inputs_processed, path = save_dir_xlsx, prefix = paste0(email, "_processed"))
  # }
  
  if(settings$output_xlsx_for_excel_model) {
    # also write the non-cleaned versions
    inputs_excel <- clean_raw_inputs_for_excel_model(inputs_raw, factors$factors_perennials_trees, inputs_processed$inputs_grazing_cover, inputs_processed$inputs_grazing_parcels)
    save_dir_xlsx <- 'output/excel_model/xlsx_inputs/'
    if (!dir.exists(save_dir_xlsx)) {dir.create(save_dir_xlsx)}
    write_list_to_xlsx(inputs_excel, path = save_dir_xlsx, prefix = email, sideways=TRUE)
    return()
  }

      
  ## Running the tree biomass, soil model and emissions calculations -------------------------

  # tree biomass
  if(settings$calc_tree_emissions) {
    tree_carbon_results <- get_tree_carbon(
      inputs_processed$inputs_perennials, 
      periods, inputs_processed$inputs_parcel_fixed,
      inputs_processed$inputs_trees_felled,
      email)
    inputs_processed$tree_biomass <- tree_carbon_results$tree_biomass
    inputs_processed$tree_soil_inputs <- tree_carbon_results$tree_soil_inputs
    tree_biomass_removals <- tree_carbon_results$tree_biomass %>%
      select(year, co2_trees_uncert) %>% rename(CO2eq_removals_tonnes = co2_trees_uncert)
  } else {
    tree_biomass_removals <- tibble(year=project_years, CO2eq_removals_tonnes=0)
    inputs_processed$tree_soil_inputs <- tibble(.rows = 0)
  }

  # emissions
  # note: a negative value means a REDUCTION in emissions relative to baseline
  emission_results <- get_emissions(init_data=init_data,
                                    farm_EnZ = farm_EnZ,
                                    inputs = inputs_processed,
                                    factors = factors,
                                    periods = periods)
  emissions_wide <- emission_results$emissions
  emissions_long <- emission_results$emissions_long
  emission_diffs_by_source <- emission_results$emission_diffs_by_source
  emission_diffs_by_gas <-  emission_results$emission_diffs_by_gas
  emission_diffs_total <- emission_results$emission_diffs_total
  
  # leakage 
  leakage_results <- get_leakage(inputs_processed)
  leakage_annual <- leakage_results$leakage_annual
  
  # soil model
  soil_model_results <- run_soil_model(init_data=init_data,
                                     monitoringData=monitoringData,
                                     farm_EnZ=farm_EnZ,
                                     inputs=inputs_processed,
                                     factors=factors,
                                     settings=settings,
                                     periods=periods)
  
  soil_ERRs <- calculate_soil_ERRs(soil_model_results, inputs_processed)
  soil_results_monthly <- soil_ERRs$soc_monthly
  yearly_results <- soil_ERRs$yearly_results

  # join the emissions and soil results
  yearly_results$CO2eq_emissions <- emission_diffs_total$kgCO2_eq_diff / 1000 # convert to tonnes
  yearly_results <- left_join(yearly_results,
                              leakage_annual %>% filter(year_index >= 0),
                              by=c("year", "year_index")) 

  # join the tree biomass results
  yearly_results <- left_join(yearly_results, 
                              tree_biomass_removals %>% select(year, CO2eq_removals_tonnes), 
                              by='year') %>%
    mutate(CO2eq_removals_tonnes = if_else(is.na(CO2eq_removals_tonnes), 0, CO2eq_removals_tonnes))
  yearly_results <- yearly_results %>%
    mutate(CO2eq_t_total = CO2eq_soil_gain_95conf + CO2eq_removals_tonnes - CO2eq_emissions - CO2eq_leakage)
  # calculate total CO2eq per hectare
  yearly_results$area <- inputs_processed$inputs_farm_fixed$area_parcels
  yearly_results$CO2eq_t_per_ha <- yearly_results$CO2eq_t_total / yearly_results$area
  
  # check for NA values
  has_na <- data.frame(
    emissions = any(is.na(emissions_long)),
    parcel_Cinputs = any(is.na(soil_model_results$parcel_Cinputs)),
    leakage = any(is.na(leakage_annual$CO2eq_leakage))
  )
  if (any(has_na)) {
    log4r::error(my_logger, paste0("NA values found in the following dataframes: ", 
                                   paste(names(has_na)[has_na==T], collapse=", ")))
  }
  
  readLines(my_logfile)
  
  ## Push results to mongoDB ---------------------------------------------------
  
  if(settings$save2mongoDB) {
    # create an object to store all the data
    results <- list()
    results$farmId <- db_farmId
    results$email <- email
    results$farmInfo <- monitoringData$farmInfo
    
    # Get code version and time info
    results$modelInfo <- list(
      modelVersion = paste0("R-model-version: ", 
                            system2(command = "git", args = "describe", stdout = TRUE)),
      resultsGenerationYear = format(Sys.time(), "%Y"),
      resultsGenerationTime = format(Sys.time(), "%Y-%m-%d %H:%M")
    )
    
    results$modelResults <- as.list(yearly_results %>% 
                                      select(year, CO2eq_t_total, CO2eq_soil_gain_95conf, CO2eq_emissions, CO2eq_leakage))
    results$emissionsDetailed <- as.list(emission_diffs_by_source)
    
    results$modelSettings <- settings
    
    results$has_na <- as.list(has_na)
    
    # convert to json
    results_json <- jsonlite::toJSON(results, pretty=T, auto_unbox=T)
    
    # Upload to database
    # note: if a result already exists for  this farmer, this will result in a new entry
    # (i.e. it does not overwrite any existing results)
    carbonresults_collection = mongo(collection="carbonresults", db=db, url=connection_string)
    carbonresults_collection$insert(results_json)
    
  }

  ## Log Messages --------------------------------------------------------------
  
  log4r::info(my_logger,'Total soil CO2eq: ', 
              sum(round(yearly_results$CO2eq_soil_gain_95conf, 3)),
              '.\nCredits per year (before emission reductions): ', 
              paste0(round(yearly_results$CO2eq_soil_gain_95conf, 3), collapse=', '),
              '.\nArea considered: ', round(sum(inputs_processed$inputs_parcel_fixed$area), 2), ' ha.', 
              "\nNumber of runs: ", settings$n_runs,
              "\nStandard error used for extrinsic uncertainty of practices (Cinputs): ",
              settings$se_field_carbon_in
  )
  
  ## Write data to files -----------------------------------------------------

  save_dir <- paste0('output/model_results/', email, '/')
  
  # Inputs
  inputs_to_write <- inputs_processed
  inputs_to_write$model_settings <- data.frame(settings)
  inputs_to_write$climate_inputs <- soil_model_results$present_climate
  inputs_to_write$soil_data <- as.data.frame(soil_model_results$soil_data)
  inputs_to_write$env_zone <- data.frame('env_zone'=farm_EnZ, 'soil_model_version'=soil_model_results$model_version)
  write_list_to_csvs(inputs_to_write, path = paste0(save_dir, 'inputs/'), prefix = "", simplify_names=T)
  write_list_to_csvs(inputs_raw, path = paste0(save_dir, 'inputs_raw/'), prefix = "", simplify_names=T)
  
  # Outputs
  outputs_to_write <- list(
    # soil model
    parcel_Cinputs = soil_model_results$parcel_Cinputs,
    C_inputs_long = soil_model_results$C_inputs_long,
    dr_ratios_biomass = soil_model_results$parcel_dr_ratios_biomass,
    SOC_baseline_and_project_totals = soil_ERRs$soc_farm,
    SOC_baseline_and_project_parcels = soil_ERRs$soc_parcels,
    # leakage
    leakage = leakage_annual,
    # emissions
    emissions_wide = emissions_wide,
    emissions_long = emissions_long,
    emission_diffs_by_source = emission_diffs_by_source,
    emission_diffs_by_gas = emission_diffs_by_gas,
    emission_diffs_total = emission_diffs_total,
    # combined
    yearly_results = yearly_results,
    # checks
    has_na = has_na
  )
  write_list_to_csvs(outputs_to_write, path = paste0(save_dir, 'outputs/'), prefix = "", simplify_names=F)
  
  ## Plotting ------------------------------------------------------------------
  plot_dir <- paste0(save_dir, 'plots/')
  if (!dir.exists(plot_dir)) {dir.create(plot_dir, recursive = TRUE)}
  
  name <- paste0("Results: ", email)
  
  soil_results_monthly <- soil_results_monthly %>% 
    mutate(time = paste0(cal_year,'-',month), time = ym(time))
  graph <- ggplot(data = soil_results_monthly, aes(x = time, y = SOC_ha, colour=scenario)) +
    geom_line()+
    #geom_errorbar(aes(ymin=SOC_farm_mean-SOC_farm_sd, ymax=SOC_farm_mean+SOC_farm_sd), width=.1) +
    scale_color_manual(values = c("darkred","#5CB85C"),labels = c("baseline","project"))+
    theme(legend.position = "bottom")+
    labs(title = name)+
    ylab("SOC (tonnes per hectare)") +
    theme_bw()
  # print(graph)
  ggsave(graph, file = paste0(plot_dir, 'SOC_timeseries.png'), width=8, height=4, dpi=100)
  
  barplot1 <- ggplot(yearly_results %>% select(-c(area, CO2eq_t_per_ha, soil_has_na)), aes(x=year, group = 1)) +
    geom_hline(yintercept=0, color='black', linewidth=0.3) +
    geom_bar(aes(y=CO2eq_soil_gain_mean), stat="identity", fill="brown", alpha=0.7) +
    geom_bar(aes(y=CO2eq_t_total), stat="identity", fill="green", alpha=0.7) +
    geom_bar(aes(y=CO2eq_leakage), stat="identity", fill="red", alpha=0.7) +
    geom_bar(aes(y=CO2eq_emissions), stat="identity", fill="blue", alpha=0.7) +
    geom_errorbar(aes(ymin = CO2eq_soil_gain_mean-1.96*CO2eq_soil_gain_sd,
                      ymax = CO2eq_soil_gain_mean+1.96*CO2eq_soil_gain_sd, color = "95% CI"), colour="black", width=.5, show.legend = T) +
    xlab("Time")+
    ylab("Number of credits issuable (per year)") +
    theme_bw()
  # print(barplot1)
  # ggsave(barplot1, file = paste0(plot_dir, 'barplot1.png'), width=6, height=4, dpi=100)
  
  barplot_data <- yearly_results %>% 
    select(-c(area, CO2eq_t_per_ha, soil_has_na)) %>% 
    mutate(
      CO2eq_emissions_red = -CO2eq_emissions, # -ve value is emission reduction --> +ve value is emission reduction
      CO2eq_leakage = -CO2eq_leakage, # +ve value is leakage --> -ve value is leakage
    ) %>%
    filter(year_index>0) %>%
    select(-c(CO2eq_soil_cum, CO2eq_soil_gain_mean, CO2eq_soil_gain_sd, CO2eq_emissions)) %>%
    pivot_longer(!c(year, year_index, period)) %>%
    mutate(name_short = str_replace(name, "CO2eq_", ""))
  barplot2 <- ggplot(barplot_data %>% filter(!is.na(value))) + 
    geom_hline(yintercept=0, color='black', linewidth=0.3) +
    geom_bar(aes(x = factor(year), y = value, fill = name_short), position = "dodge", stat = "identity", width=0.5) +
    theme_bw() +
    labs(x="", y="CO2eq (tonnes)", title="Annual CO2eq emissions and removals") +
    scale_fill_brewer(palette = "Set1")
  # print(barplot2)
  ggsave(barplot2, file = paste0(plot_dir, 'barplot2.png'), width=8, height=4, dpi=100)
  
  # plot parcel C inputs by type over time
  soil_input_types <- c("organic_amendments","trees","livestock","crops_and_pasture")
  Cinp_long <- soil_model_results$parcel_Cinputs %>%
    filter(scenario != "projected_baseline", scenario != "baseline_average") %>%
    pivot_longer(cols = soil_input_types, names_to = "Cinput_type", values_to = "Cinput") %>%
    group_by(year, period, year_index, Cinput_type) %>%
    summarise(tC = sum(Cinput, na.rm = T), 
              tC_ha = tC/sum(area),
              .groups='drop') # sum over all parcels
  all_scenario_tots <- soil_model_results$parcel_Cinputs %>% 
    group_by(scenario, year) %>% 
    summarise(total=sum(total_tC)/sum(inputs_processed$inputs_parcel_fixed$area))
  cinputs <- ggplot(Cinp_long %>% filter(!is.na(tC_ha)), aes(x=year, y=tC_ha, fill=Cinput_type)) +
    geom_vline(xintercept=as.numeric(monitoringData$projectStartYear)-0.5, linetype="dashed", color = "black") +
    geom_text(aes(x=as.numeric(monitoringData$projectStartYear)-0.4, y=0, label="Project"), vjust=1, hjust=0) +
    geom_text(aes(x=as.numeric(monitoringData$projectStartYear)-0.6, y=0, label="Baseline"), vjust=1, hjust=1) +
    geom_bar(stat="identity") +
    # geom_line() + 
    geom_point(data=all_scenario_tots %>% filter(scenario=='projected_baseline'), aes(x=year, y=total, fill='projected baseline (total)'), size=3) +
    labs(title = "Total C inputs by type over time") +
    theme_bw() +
    scale_fill_brewer(palette = "Set1")
  # cinputs
  ggsave(cinputs, file = paste0(plot_dir, 'Cinputs_over_time.png'), width=8, height=4, dpi=100)
  
  # ... same but including the dynamic baseline ...
  # Cinp_long2 <- soil_model_results$parcel_Cinputs %>%
  #   filter(scenario != "baseline_average") %>%
  #   pivot_longer(cols = soil_input_types, names_to = "Cinput_type", values_to = "Cinput") %>%
  #   group_by(year, period, scenario, year_index, Cinput_type) %>%
  #   summarise(tC = sum(Cinput, na.rm = T), 
  #             tC_ha = tC/sum(area),
  #             .groups='drop') # sum over all parcels
  # cinputs2 <- ggplot(Cinp_long2 %>% filter(!is.na(tC_ha)), aes(x=factor(year), y=tC_ha, fill=Cinput_type, group=scenario, color=scenario)) +
  #   # geom_vline(xintercept=as.numeric(monitoringData$projectStartYear)-0.5, linetype="dashed", color = "black") +
  #   geom_bar(stat="identity", position='dodge', linewidth=1) +
  #   labs(title = "Total C inputs by type over time (including dynamic baseline)") +
  #   scale_color_manual(values = c("grey95","black", "grey50")) +
  #   theme_bw() +
  #   scale_fill_brewer(palette = "Set1")
  # cinputs2
  # ggsave(cinputs2, file = paste0(plot_dir, 'Cinputs_over_time_with_projected_baseline.png'), width=8, height=4, dpi=100)
  
  # C inputs per parcel
  parcelC <- soil_model_results$parcel_Cinputs %>%
    filter(scenario != "projected_baseline", scenario != "baseline_average") %>%
    pivot_longer(cols = soil_input_types, names_to = "Cinput_type", values_to = "tC") %>%
    mutate(tC_ha = tC/area)
  nparcels <- nrow(inputs_processed$inputs_parcel_fixed)
  parcelC_plot <- ggplot(parcelC %>% filter(!is.na(tC_ha)), aes(x=year, y=tC_ha, fill=Cinput_type)) + #, group=year, fill=year)) +
    geom_vline(xintercept=as.numeric(monitoringData$projectStartYear)-0.5, linetype="dashed", color = "black") +
    geom_bar(stat="identity", position="stack") +
    facet_wrap(~parcel_name, ncol=5) +
    labs(title = "Total C inputs per parcel over time", y='tonnes C per hectare') +
    theme_bw() +
    scale_fill_brewer(palette = "Set1")
  # parcelC_plot
  ggsave(parcelC_plot, file = paste0(plot_dir, 'parcel_Cinputs_over_time.png'), width=12, height=2*nparcels/5, dpi=100)
  
  # different kinds of emissions
  emissions_clean <- emissions_long %>%
    filter(year_index !=0, scenario != "projected_baseline")
  emissions_clean$source[emissions_clean$source == "dfuel_kg_total"] <- "direct fuel"
  emissions_clean$source[emissions_clean$source == "ifuel_kg_total"] <- "indirect fuel"
  emissions_clean$source[emissions_clean$source == "fert_kg_total_farm"] <- "fertiliser"
  emissions_clean$source[emissions_clean$source == "kg_ent_ferm"] <- "livestock (enteric fermentation, CH4)"
  emissions_clean$source[emissions_clean$source == "kg_manure"] <- "livestock (manure, CH4)"
  emissions_clean$source[emissions_clean$source == "kg_manure_direct"] <- "livestock (manure direct, N2O)"
  emissions_clean$source[emissions_clean$source == "kg_manure_indirect"] <- "livestock (manure indirect, N2O)"
  emissions_clean$source[emissions_clean$source == "kg_n_fixing"] <- "N-fixing crops"
  emission_plot <- ggplot(emissions_clean %>% filter(!is.na(kgCO2_eq)), aes(x=year, y=kgCO2_eq, fill=source)) +
    geom_vline(xintercept=as.numeric(monitoringData$projectStartYear)-0.5, linetype="dashed", color = "black") +
    geom_bar(stat="identity", width=0.66) +
    theme_bw() +
    scale_fill_brewer(palette = "Set1") + 
    labs(x="Year", y="CO2eq (kg)", title="Annual CO2eq emissions by source")
  # emission_plot
  ggsave(emission_plot, file = paste0(plot_dir, 'emissions_by_source.png'), width=8, height=4, dpi=100)
  
  # plot livestock per parcel per year
  ls <- inputs_processed$inputs_grazing_parcels
  ls_parcels <- ggplot(ls %>% filter(!is.na(grazing_frac)), aes(x=parcel_name, y=grazing_frac, group=year, fill=factor(year))) + 
    geom_bar(stat="identity", position='dodge', linewidth=1) +
    coord_flip() +
    theme_bw()
  # ls_parcels
  ggsave(ls_parcels, file = paste0(plot_dir, 'livestock_per_parcel_per_year.png'), width=6, height=8, dpi=100)
  
  ## Move logs to output dir  ---------------------------------------------------------
  time_str <- format(Sys.time(), "%Y-%m-%d %H_%M")
  file.copy(from=my_logfile, 
            to=paste0(save_dir, 'log ', time_str,'.txt'))
  
  # browser()
  ## End function --------------------------------------------------------------
  return(yearly_results)
}
  

