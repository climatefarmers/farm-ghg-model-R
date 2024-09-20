run_soil_model <- function(init_data, monitoringData, farm_EnZ, inputs, factors, settings, periods){
  ## This function has the following components
  ## 1. Get climate data
  ## 2. Get soil data
  ## 3. Calculate C inputs per parcel and scenario
  ## 4. SOC initialisation runs
  ## 5. Model runs
  
  
  ## Log starting run message
  log4r::info(my_logger, "run_soil_model.R started running")
  
  ## Sourcing code from files
  source(file.path("soil", "model_semiArid_functions.R"), local = TRUE)
  source(file.path("soil", "modified_semiArid_functions.R"), local = TRUE)
  source(file.path("soil", "calc_functions_soil_modelling.R"), local = TRUE)
  source("weather_data_pulling_functions.R", local = TRUE)
  
  
  ###### 1. Get climate data ######
  latlon_farm <- rev(colMeans(do.call('rbind', inputs$inputs_parcel_fixed$coordinates)))  # [lat, lon]: must reverse from the inputs df
  
  # Just for testing
  # weather_data <- read_csv(file.path("data", "test_weather_data.csv"), show_col_types = FALSE) # debug line
  # 
  # ## Extracting climate from different periods
  # past_weather <- data.frame(
  #   month = weather_data$month,
  #   evap = weather_data$past_evap,
  #   pevap = weather_data$past_pevap,
  #   precipitation = weather_data$past_precipitation,
  #   temperature = weather_data$past_temperature
  # )
  # climate_rcp4.5 <- data.frame(
  #   month = weather_data$month,
  #   evap = weather_data$future_evap_rcp4.5,
  #   pevap = weather_data$future_pevap_rcp4.5,
  #   precipitation = weather_data$future_precipitation_rcp4.5,
  #   temperature = weather_data$future_temperature_rcp4.5
  # )
  
  if(settings$use_test_climate){  # will skip fetching climate data and use dummy data
    climate_data <- read_csv(file.path("example-farm-data", "climate_data_1950_2022_Alves4.csv"), show_col_types = FALSE) # For testing only
  } else {
    climate_data <- get_past_weather_data(init_data, inputs$inputs_farm_fixed$farm_id, latlon_farm[1], latlon_farm[2], "1950_2022", averaged=FALSE)
    # weather_data <- rbind(get_past_weather_data(init_data, latlon_farm[1], latlon_farm[2], "1950_2022"),
    #                    # get_past_weather_data(init_data, latlon_farm[1], latlon_farm[2], "2021"),
    #                    # get_past_weather_data(init_data, latlon_farm[1], latlon_farm[2], "2022"),
    #                    get_future_weather_data(init_data, latlon_farm[1], latlon_farm[2], scenario="rcp4.5"),
    #                    get_future_weather_data(init_data, latlon_farm[1], latlon_farm[2], scenario="rcp8.5"))
  }
  
  # Set climate for different runs:
  climate_periods <- get_climate_periods(climate_data = climate_data, proj_start_year = as.numeric(inputs$inputs_farm_fixed$project_start_year))
  spinup_climate <- climate_periods$mean_past_climate
  present_climate <- climate_periods$mean_past_climate

  # Choose model version based on climate
  model_version <- ifelse(sum(present_climate$precipitation) / sum(present_climate$pevap) < 0.65 &
                            sum(present_climate$precipitation) < 600, "Semi-arid", "Normal")

  
  ###### 2. Get soil data ######
  soilMapsData <- get_soil_data(inputs$inputs_farm_fixed$pars_farmId, settings)  # Gets soil data from https://maps.isric.org/ (AWS)
  # soil_inputs <- get_soil_inputs(inputs, farms_everything$soilAnalysis, soilMapsData)
  # ^^ can probably delete this line and the associated functions, as it is not used further in the code
  # ^^ (only the irrigation, but it is out of date)

  ###### 3. Calculate C inputs per parcel and scenario ######
  ## individual C input calculations
  # a) inputs NOT affected by the dynamic baseline
  organic_amendments <- get_Cinputs_organic_amendments(inputs$inputs_organicmatter, periods, inputs$inputs_parcel_fixed$parcel_name) # note: this creates a complete data frame for all parcels
  animals <- get_Cinputs_livestock(inputs$inputs_livestock_parcels)
  # b) inputs affected by the dynamic baseline (these already have a projected_baseline)
  crops_and_pasture <- get_Cinputs_crops_and_pasture(inputs$inputs_productivity, inputs$inputs_fodder)
  agroforestry <- get_Cinputs_trees(inputs$tree_soil_inputs, settings$calc_tree_emissions)

  ## Add the projected/dynamic baseline
  # format projected baselines for the inputs that are not affected by the dynamic baseline (i.e. (a) above)
  C_inputs_long <- add_projected_baseline(list(organic_amendments, animals), periods)
  # combine these with the inputs that already have a dynamic baseline (i.e. (b) above)
  C_inputs_long <- bind_rows(C_inputs_long, crops_and_pasture, agroforestry) %>%
    arrange(source, parcel_name, year)

  ## Format and calculate total C inputs
  # convert to wide format
  C_inputs_wide <- C_inputs_long %>% 
    pivot_wider(id_cols = c(parcel_name, year, year_index, period, scenario), 
                names_from = source, 
                values_from = tC)
  # replace na vals
  C_inputs_wide <- C_inputs_wide %>% 
    replace(is.na(.), 0)
  # calculate the sum of inputs from all sources
  sources <- unique(C_inputs_long$source)
  C_inputs_wide <- C_inputs_wide %>% 
    mutate(total_tC = rowSums(across(all_of(sources))))
  # add columns for any missing sources
  missing_sources <- setdiff(c("organic_amendments", "trees", "crops_and_pasture", "livestock"), sources)
  if (length(missing_sources)>0) {
    C_inputs_wide[,missing_sources] <- 0
  }
  # add area and calculate C inputs per ha
  parcel_Cinputs <- right_join(inputs$inputs_parcel_fixed %>% select(parcel_name, parcel_id, area), 
                               C_inputs_wide, 
                              by='parcel_name') %>% 
    mutate(Cinputs_ha = total_tC / area)

  #if(settings$debug_mode) {write_csv(parcel_Cinputs, here("output", "parcel_Cinputs.csv"))}
  
  # # for testing: read from file
  # parcel_Cinputs_test <- read_csv('example-farm-data/example_Cinputs_NEW.csv', show_col_types = F)
  # print('reading parcel_Cinputs from file for testing purposes')
  
  # Attention: YEARLY C inputs are calculated, naming misleading
  # baseline_chosen="baseline"
  # parcel_Cinputs <- data.frame(parcel_ID=c(), 
  #                              scenario=c(),
  #                              year=c(),
  #                              orgamendments_Cinputs=c(), 
  #                              agroforestry_Cinput=c(), 
  #                              animal_Cinput=c(), 
  #                              crop_Cinputs=c(), 
  #                              pasture_Cinputs=c()
  # )
  # 
  # # load("parcel_Cinputs.RData")  # for testing only
  # 
  # years <- 0:10
  # 
  # for(p in 1:length(inputs$parcel_inputs$parcel_ID)) {
  #   
  #   parcel <- inputs$parcel_inputs$parcel_ID[p]
  #   
  #   for(year in years){
  #     scenario <- paste0("year", year)
  #     if(year == 0) scenario <- baseline_chosen
  #     orgamendments_Cinputs <- get_monthly_Cinputs_orgamendments(inputs$inputs_organicmatter, factors$manure_factors, scenario, parcel)
  #     agroforestry_Cinputs <- 0 # get_monthly_Cinputs_agroforestry(inputs$tree_inputs, factors$agroforestry_factors, scenario, parcel, lat_farmer) # TREES NOT COUNTED BEFORE GOOD CHECK OF DATA QUALITY
  #     animal_Cinputs <- get_yearly_Cinputs_animals(inputs$animal_inputs, scenario, parcel)
  #     crop_Cinputs <- get_monthly_Cinputs_crop(inputs$crop_inputs, factors$factors_crops, scenario, parcel, farm_EnZ)
  #     pasture_Cinputs <- get_monthly_Cinputs_pasture(inputs$pasture_inputs, factors$factors_pastures, scenario, parcel, year, settings)
  #     
  #     parcel_Cinputs_temp <- data.frame(parcel_ID = parcel, 
  #                                       scenario = scenario,
  #                                       year = year,
  #                                       orgamendments_Cinputs=orgamendments_Cinputs, 
  #                                       agroforestry_Cinputs=agroforestry_Cinputs, 
  #                                       animal_Cinputs=animal_Cinputs, 
  #                                       crop_Cinputs=crop_Cinputs, 
  #                                       pasture_Cinputs=pasture_Cinputs
  #     )
  #     parcel_Cinputs <- rbind(parcel_Cinputs, parcel_Cinputs_temp)
  #   }
  # }
  # 
  # parcel_Cinputs <- parcel_Cinputs %>% mutate(Cinputs_ha = orgamendments_Cinputs + agroforestry_Cinputs + animal_Cinputs + crop_Cinputs + pasture_Cinputs)
  
  if (length(apply(is.na(parcel_Cinputs), 2, which))==0){
    log4r::info(my_logger, 'parcel C inputs calculations have no NAs.', sep=" ")
  } else {
    log4r::error(my_logger, paste(length(apply(is.na(parcel_Cinputs), 2, which)), 'NAs were found in parcel C inputs calculation results.'))
  }
  # # define the baseline parcel inputs for year_index=0
  # # as the average over all baseline years (ie year_index<0)
  # parcel_Cinputs_base <- parcel_Cinputs %>% 
  #   select(-c(period, scenario, year)) %>%
  #   filter(year_index<0) %>%
  #   group_by(parcel_name, parcel_id) %>%
  #   summarise_all(mean) %>%
  #   mutate(scenario="baseline_average",
  #          period="baseline",
  #          year_index=0,  # this represents the average baseline year
  #          year=NA) %>%
  #   select(colnames(parcel_Cinputs))
  # # add to the overall data frame
  # parcel_Cinputs <- bind_rows(parcel_Cinputs, parcel_Cinputs_base)
  
  # ################# Calculations of additional C inputs compared to baseline per parcel and scenario -- Fernando: NOT USED FURTHER IN CODE!
  # 
  # parcel_Cinputs_addition = merge(x= parcel_Cinputs, 
  #                                 y= inputs$parcel_inputs %>% 
  #                                   select(parcel_ID, area) %>%
  #                                   mutate(farm_frac= paste(round(area/sum(area)*100), '%')), by="parcel_ID") %>% 
  #   group_by(parcel_ID, farm_frac) %>% 
  #   mutate(Cinput_per_ha_project = sum(Cinputs_ha[scenario!=baseline_chosen & scenario!="year0"])/10) %>%
  #   filter(scenario==baseline_chosen) %>%
  #   mutate(additional_Cinput_per_ha = round(Cinput_per_ha_project - Cinputs_ha, 2), 
  #          relative_increase=paste(as.character(ifelse(Cinputs_ha==0, NA, as.integer((Cinput_per_ha_project - Cinputs_ha) / Cinputs_ha*100))), '%'), 
  #          additional_Cinput_total = round(unique(area)*(Cinput_per_ha_project - Cinputs_ha), 1)) 
  # additional_Cinput_total_farm = sum(parcel_Cinputs_addition$additional_Cinput_total)
  # parcel_Cinputs_addition = parcel_Cinputs_addition %>%
  #   mutate(absolute_contribution_perc = round(100*additional_Cinput_total / additional_Cinput_total_farm)) %>%
  #   select(parcel_ID, farm_frac, additional_Cinput_per_ha, relative_increase, additional_Cinput_total, absolute_contribution_perc)
  # 
  # ## Calculation of total c inputs for the whole farm -- Fernando: NOT BEING USED FURTHER IN CODE!
  # # Sum over all parcels
  # yearly_Cinputs_farm = merge(x= parcel_Cinputs, 
  #                             y= inputs$parcel_inputs, 
  #                             by="parcel_ID") %>%
  #   group_by(scenario) %>%
  #   summarise(Cinputs_ha=sum(Cinputs_ha*area), 
  #             orgamendments_Cinputs=sum(area*orgamendments_Cinputs), 
  #             animal_Cinputs=sum(area*animal_Cinputs), 
  #             crop_Cinputs=sum(area*crop_Cinputs), 
  #             pasture_Cinputs=sum(area*pasture_Cinputs), 
  #             agroforestry_Cinputs=sum(area*agroforestry_Cinputs))
  # 
  
  
  ###### 4. SOC initialisation ######
  ## Initialise by making the model reach SOC of natural areas of the pedo-climatic area
  ## Pulling DPM/RPM ratios from different kinds of land use in corresponding pedo-climatic area 
  dr_ratio_woody = unique(factors$factors_natural_area$dr_ratio_woody)
  dr_ratio_grassland = unique(factors$factors_natural_area$dr_ratio_grassland)
  dr_ratio_annualcrops = unique(factors$factors_natural_area$dr_ratio_annualcrops)
  dr_ratio_farmyard_manure <- unique(factors$factors_natural_area$dr_ratio_farmyard_manure)
  
  ## Calculate the fraction of inputs that take different DR ratios
  # and use this to calculate the mean dr_ratio for each parcel
  # (note: this is only used if the setting dr_ratio_from_litter==TRUE)
  parcel_dr_ratios_biomass <- C_inputs_long %>%
    group_by(parcel_name, year, year_index, period, scenario) %>%
    summarise(frac_woody = sum(tC * frac_woody_dr) / sum(tC),
              frac_annualcrops = sum(tC * frac_annualcrops_dr) / sum(tC),
              frac_grassland = sum(tC * frac_grassland_dr) / sum(tC),
              frac_farmyard_manure = sum(tC * frac_farmyard_manure_dr) / sum(tC),
              .groups='drop') %>%
    mutate(dr_ratio = frac_woody*dr_ratio_woody + frac_annualcrops*dr_ratio_annualcrops + 
                      frac_grassland*dr_ratio_grassland + frac_farmyard_manure*dr_ratio_farmyard_manure,
           sum_of_fracs = frac_woody + frac_annualcrops + frac_grassland + frac_farmyard_manure,
           dr_ratio = if_else(is.na(dr_ratio), dr_ratio_annualcrops, dr_ratio)) # fill blanks (with tC=0)
  # check the fractions
  if (nrow(parcel_dr_ratios_biomass %>% filter(!is.na(sum_of_fracs) & round(sum_of_fracs,2)!=1)) > 0) {
    browser()
    stop("The sum of the fractions of the different DR ratios is not equal to 1.")
  }
  
  ## Building an object of inputs to feed RothC
  ## store as a list to allow for different lengths (e.g. monthly vs single values)
  soil_data <- list(
    # constant across all parcels and years
    run = NA,
    soil_thick = 30, # modelled for 30 cm depth as recommended in IPCC Guidelines 2006
    SOC = soilMapsData$SOC,
    clay = soilMapsData$clay,
    silt = soilMapsData$silt,
    bulk_density = soilMapsData$bulk_density,
    pE = 0.75, # mean potential transpiration to open-pan evap convertion rate
    # variable across parcels and/or years --> to be defined later
    tillage_factor = NA,  # annual (defined as maximum over all months)
    field_carbon_in = NA,  # annual
    dr_ratio = dr_ratio_grassland, # annual
    bare_months = logical(12)  # monthly
  )
  
  ## Get SOC steady state using baseline soil conditions
  # This section gets soil C close to steady state so spin-ups are not
  # required for every run.
  soil_pools <- c('DPM', 'RPM', 'BIO', 'HUM', 'IOM')
  SOC_start <- data.frame(
    parcel_name = inputs$inputs_parcel_fixed$parcel_name,
    parcel_id = inputs$inputs_parcel_fixed$parcel_id)
  SOC_start[,soil_pools] <- NA
  
  soil_data_init <- soil_data  # model inputs for the initialisation simulation
  
  for(p in 1:nrow(inputs$inputs_parcel_fixed)) {

    parcel <- SOC_start$parcel_name[p]
    
    ## Select and process values for the baseline scenario
    soil_data_init$field_carbon_in <- parcel_Cinputs %>% filter (parcel_name==parcel, year_index==0) %>% pull(Cinputs_ha)
    soil_data_init$bare_months <- get_mean_monthly_baresoil(inputs$inputs_baresoil %>% filter(parcel_name==parcel, year_index<0))
    soil_data_init$tillage_factor <- get_mean_annual_tillage_factor(inputs$inputs_tillage %>% filter(parcel_name==parcel, year_index<0))
    soil_data_init$dr_ratio <- ifelse(settings$dr_ratio_from_litter,
                                      parcel_dr_ratios_biomass %>% filter(parcel_name==parcel, year_index==0) %>% pull(dr_ratio),
                                      get_mean_dr_ratio(inputs$inputs_irrigation %>% filter(parcel_name==parcel, year_index<0),
                                                 dr_ratio_annualcrops, 
                                                 dr_ratio_grassland))
    
    # Get starting soil content
    starting_soil_content <- estimate_starting_soil_content(SOC=soil_data_init$SOC, clay=soil_data_init$clay) / 2
    
    # Run spinup to equilibrium using baseline data
    time_horizon = settings$spinup_years
    modelSOC_spinup <- calc_carbon_over_time(time_horizon, soil_data_init, starting_soil_content, spinup_climate, model_version)
    SOC_start[SOC_start$parcel_name==parcel, soil_pools] <- as.numeric(tail(modelSOC_spinup, 1)[soil_pools])
  
  }  # end parcel loop
  
  
  ###### 5. Model runs ######
  # Data frame for holding all results
  all_results <- tibble()
  project_years <- seq(1, max(parcel_Cinputs$year_index))
  
  # run multiple replications with stochastic inputs
  # "_n" indexes the data in this loop
  for (n in 1:settings$n_runs) {
    
    # generate a stochastic realisation of the inputs
    inputs_n <- generate_stochastic_inputs(n, settings, se_field_carbon_in, se_inputs_nonfarm, soil_data, spinup_climate, present_climate)
    soil_data_n <- inputs_n$soil_data_n
    coefs_n <- inputs_n$coefs_n
    spinup_climate_n <- inputs_n$spinup_climate_n
    present_climate_n <- inputs_n$present_climate_n
    
    # Dataframe for all results of one run
    results_n <- tibble()
    
    # loop over the parcels
    for(p in 1:nrow(inputs$inputs_parcel_fixed)) {

      if(settings$debug_mode) {print(paste("Initialising soil run", n, "for parcel", parcel))}

      # Define parcel fixed values
      soil_data_np_base <- soil_data_n  # copy the soil data to use for this parcel
      parcel <- inputs$inputs_parcel_fixed$parcel_name[p]
      parcel_Cinputs_np <- parcel_Cinputs %>% 
        filter(parcel_name==parcel) %>% 
        mutate(Cinputs_ha=Cinputs_ha * coefs_n$field_carbon_in)
      
      # Select values for the baseline scenario 
      # (i.e., year_index=-1 for discrete variables and year_index=0 for continuous variables)
      soil_data_np_base$field_carbon_in <- parcel_Cinputs_np %>% filter(year_index==0) %>% pull(Cinputs_ha)  # already modified by coefs_n
      soil_data_np_base$bare_months <- get_mean_monthly_baresoil(inputs$inputs_baresoil %>% filter(parcel_name==parcel, year_index<0))
      soil_data_np_base$tillage_factor <- get_mean_annual_tillage_factor(inputs$inputs_tillage %>% filter(parcel_name==parcel, year_index<0)) * coefs_n$tillage_factor
      soil_data_np_base$dr_ratio <- ifelse(settings$dr_ratio_from_litter,
                                           parcel_dr_ratios_biomass %>% filter(parcel_name==parcel, year_index==0) %>% pull(dr_ratio),
                                           get_mean_dr_ratio(inputs$inputs_irrigation %>% filter(parcel_name==parcel, year_index<0), 
                                                      dr_ratio_annualcrops, 
                                                      dr_ratio_grassland)
                                           ) * coefs_n$dr_ratio
      
      # Get starting soil content from the previous initialisation simulation
      starting_soil_content <- as.numeric(SOC_start %>% filter(parcel_name==parcel) %>% select(all_of(soil_pools)))

      # Run spinup to equilibrium using baseline data
      time_horizon = 20
      modelSOC_steadystate <- calc_carbon_over_time(time_horizon, soil_data_np_base, starting_soil_content, spinup_climate_n, model_version)
      steadystate_soil_content <- as.numeric(tail(modelSOC_steadystate, 1)[soil_pools])
      
      if (n==1 & p==1) {
        ## Print the last spinup to check for equilibrium ----
        graph <- ggplot(data = modelSOC_steadystate, aes(x=1:nrow(modelSOC_steadystate), y=TOT)) +
          geom_line() +
          theme(legend.position = "bottom") +
          # labs(title = "Model spinup for an example farm plot") +
          xlab("Months") +
          ylab("SOC (in tonnes per hectare)") +
          ylim(0, 30)
        # print(graph)
      }
      
      # Run a single year for historical baseline
      time_horizon = 1
      modelSOC_baseline <- calc_carbon_over_time(time_horizon, soil_data_np_base, steadystate_soil_content, present_climate_n, model_version)
      baseline_soil_content <- as.numeric(tail(modelSOC_baseline, 1)[soil_pools])
      
      # Loop over the project years (for the project and projected_baseline simulations)
      project_soil_content_y <- baseline_soil_content  # init soil content for project simulations
      baseline_soil_content_y <- baseline_soil_content  # ditto, for the projected baseline
      modelSOC_project <- modelSOC_baseline  # init the project SOC with the baseline year
      for (y in project_years) {
        ## PROJECT ##
        # define the inputs for this year
        soil_data_np_proj_y <- soil_data_np_base
        soil_data_np_proj_y$field_carbon_in <- parcel_Cinputs_np %>% filter (year_index==y, scenario=="project") %>% pull(Cinputs_ha)
        soil_data_np_proj_y$bare_months <- get_mean_monthly_baresoil(inputs$inputs_baresoil %>% filter(parcel_name==parcel, year_index==y))
        soil_data_np_proj_y$tillage_factor <- get_mean_annual_tillage_factor(inputs$inputs_tillage %>% filter(parcel_name==parcel & year_index==y)) * coefs_n$tillage_factor
        soil_data_np_proj_y$dr_ratio <- ifelse(settings$dr_ratio_from_litter,
                                               parcel_dr_ratios_biomass %>% filter(parcel_name==parcel, year_index==y, scenario=="project") %>% pull(dr_ratio),
                                               get_mean_dr_ratio(inputs$inputs_irrigation %>% filter(parcel_name==parcel & year_index==y), 
                                                          dr_ratio_annualcrops, 
                                                          dr_ratio_grassland)
                                               ) * coefs_n$dr_ratio
        
        # Run a single year of the project scenario
        
        time_horizon = 1
        modelSOC_project_yearly <- calc_carbon_over_time(time_horizon, soil_data_np_proj_y, project_soil_content_y, present_climate_n, 
                                                         model_version, year_index_start=y)
        project_soil_content_y <- as.numeric(tail(modelSOC_project_yearly , 1))[c(1:5)]  # update soil C iterator with the last month
        modelSOC_project <- rbind(modelSOC_project, modelSOC_project_yearly) 
        
        ## PROJECTED BASELINE ##
        # define the inputs for this year --> the dynamic baseline only impacts the carbon inputs
        soil_data_np_base_y <- soil_data_np_base
        soil_data_np_base_y$field_carbon_in <- parcel_Cinputs_np %>% filter(year_index==y, scenario=="projected_baseline") %>% pull(Cinputs_ha)
        soil_data_np_base$dr_ratio <- ifelse(settings$dr_ratio_from_litter,
                                             parcel_dr_ratios_biomass %>% filter(parcel_name==parcel, year_index==y, scenario=="projected_baseline") %>% pull(dr_ratio),
                                             get_mean_dr_ratio(inputs$inputs_irrigation %>% filter(parcel_name==parcel & year_index<0), 
                                                        dr_ratio_annualcrops, 
                                                        dr_ratio_grassland)
                                             ) * coefs_n$dr_ratio

        # Run a single year of the projected baseline scenario
        time_horizon = 1
        modelSOC_projected_baseline_yearly <- calc_carbon_over_time(time_horizon, soil_data_np_base_y, baseline_soil_content_y, present_climate_n, 
                                                         model_version, year_index_start=y)
        baseline_soil_content_y <- as.numeric(tail(modelSOC_projected_baseline_yearly , 1))[c(1:5)]  # update soil C iterator with the last month
        modelSOC_baseline <- rbind(modelSOC_baseline, modelSOC_projected_baseline_yearly) 
      
      }  # end loop over project years (y)
      
      # combine the projected baseline and project data
      modelSOC_baseline$scenario <- 'bl'  # projected baseline
      modelSOC_project$scenario <- 'pr'  # project
      results_np <- bind_rows(modelSOC_baseline, modelSOC_project) %>%
        mutate(run=n, parcel=parcel, SOC=TOT) %>%
        select(run, parcel, scenario, year_index, month, SOC)
      # combine with the other parcel results
      results_n <- bind_rows(results_n, results_np) 
      
    }  # end parcel loop (p)
    
    all_results <- bind_rows(all_results, results_n)
    print(paste("Run", n, "of", settings$n_runs, "done"))
    
  } # end of random replications loop (n)
  
  
  # finish
  return(list(all_results=all_results,
              parcel_Cinputs=parcel_Cinputs,
              C_inputs_long=C_inputs_long,
              parcel_dr_ratios_biomass=parcel_dr_ratios_biomass,
              present_climate=present_climate,
              soil_data=soilMapsData,
              model_version=model_version))
}


calculate_soil_ERRs <- function(soil_model_results, inputs) {
  ###### Calculations of CO2 ERR per year ######
  ## (a) Monthly farm-level SOC
  ## (b) Yearly parcel-level SOC and Cinputs
  ## (c) Yearly farm-level CO2 estimates (mean, sd, 95% CI)
  ## (d) Rename some variables from (c)
  
  ## Pre-processing
  # add parcel areas
  all_results <- left_join(soil_model_results$all_results, inputs$inputs_parcel_fixed[,c('parcel_name', 'area')], 
                           by=c('parcel'='parcel_name'))
  tot_area <- sum(inputs$inputs_parcel_fixed$area)
  # calculate SOC values at the end of each year and restructure 
  # so that the project and baseline values have their own columns
  soc_allruns_parcels <- all_results %>% 
    filter(month==12) %>% 
    mutate(SOC_abs = SOC * area) %>%
    rename(SOC_ha = SOC) %>%
    pivot_wider(id_cols = c(run, parcel, year_index, area), 
                names_from = scenario, 
                values_from = c(SOC_ha, SOC_abs)) %>%
    mutate(SOC_abs_pbdiff = SOC_abs_pr - SOC_abs_bl)
  
 
  ## (a) Calculate monthly SOC values at the farm level
  # first average over the runs
  soc_parcels_monthly <- all_results %>% 
    group_by(parcel, year_index, month, scenario, area) %>%
    summarise(SOC=mean(SOC, na.rm=T), .groups='keep')
  # now average over the parcels
  soc_monthly <- soc_parcels_monthly %>% 
    group_by(year_index, month, scenario) %>%
    summarise(SOC_ha = mean(SOC*area/tot_area, na.rm=T),  # mean SOC per hectare
              SOC_abs = SOC_ha * tot_area,  # total SOC over the whole farm
              has_na = any(is.na(SOC)),  # check for NAs
              .groups='keep') %>%
    mutate(cal_year = as.numeric(inputs$inputs_farm_fixed$project_start_year) + (year_index - 1))  # add calendar year
  
  
  ## (b) Calculate yearly values of SOC and C inputs at the parcel level
  # first need to format the parcel_Cinputs data to separate the project and projected_baseline
  parcel_Cinputs_proj <- soil_model_results$parcel_Cinputs %>% 
    filter(scenario %in% c('baseline_average', 'project')) %>%
    rename(Cinputs_ha_proj = Cinputs_ha)
  parcel_Cinputs_base <- soil_model_results$parcel_Cinputs %>% 
    filter(scenario %in% c('baseline_average', 'projected_baseline')) %>%
    rename(Cinputs_ha_base = Cinputs_ha)
  parcel_Cinputs_all <- left_join(parcel_Cinputs_proj, parcel_Cinputs_base, by=c('parcel_name', 'parcel_id', 'year_index')) %>%
    select(parcel_name, year_index, Cinputs_ha_proj, Cinputs_ha_base)
  # now summarise the SOC over the runs and add the Cinputs
  soc_parcels <- soc_allruns_parcels %>% 
    group_by(parcel, year_index) %>% 
    summarise_all(mean, .groups='drop') %>% select(-c(run))
  soc_parcels <- left_join(soc_parcels, parcel_Cinputs_all, by=c('parcel'='parcel_name', 'year_index')) %>%
    mutate(Cinputs_abs_proj = Cinputs_ha_proj * area,
           Cinputs_abs_base = Cinputs_ha_base * area,
           year = year_index + as.numeric(inputs$inputs_farm_fixed$project_start_year) - 1)

    
  ## (c) Calculate CO2 values at the farm level
  # Summarize over parcels and calculate total CO2 values for the farm (for each run separately)
  soc_allruns_farm <- soc_allruns_parcels %>% 
    group_by(run, year_index) %>% 
    summarise(SOC_sum_pr = sum(SOC_abs_pr, na.rm=T), 
              SOC_sum_bl = sum(SOC_abs_bl, na.rm=T), 
              has_na = any(is.na(SOC_abs_pr) | is.na(SOC_abs_bl)),
              .groups='keep') %>%
    mutate(SOC_abs_pbdiff = SOC_sum_pr - SOC_sum_bl, 
           CO2 = SOC_abs_pbdiff * 44/12)
  soc_allruns_farm$CO2_gain <- c(0, diff(soc_allruns_farm$CO2))
  soc_allruns_farm$CO2_gain[soc_allruns_farm$year_index == 0] <- 0
  # Summarize over runs and calculate mean, sd, 95% conf and yearly values
  soc_farm <- soc_allruns_farm %>% 
    group_by(year_index) %>% 
    summarise(SOC_pbdiff = mean(SOC_abs_pbdiff),
              CO2_cum = mean(CO2), 
              CO2_gain_mean = mean(CO2_gain), 
              CO2_gain_sd = sd(CO2_gain),
              has_na = any(has_na)) %>%
    mutate(CO2_gain_95conf = CO2_gain_mean - CO2_gain_sd * 1.96,
           year = year_index + as.numeric(inputs$inputs_farm_fixed$project_start_year) - 1)
  # Add farm-level C input variable
  farm_Cinput <- soc_parcels %>% 
    select(c(year, parcel, Cinputs_abs_proj, Cinputs_abs_base)) %>% 
    group_by(year) %>% 
    summarise(Cinputs_abs_proj = sum(Cinputs_abs_proj), 
              Cinputs_abs_base = sum(Cinputs_abs_base))
  soc_farm <- right_join(farm_Cinput, soc_farm, by='year')
  
  
  ## (d) Rename some variables from c
  yearly_results <- soc_farm %>%
    mutate(CO2eq_soil_gain_95conf = CO2_gain_95conf,
           CO2eq_soil_cum = CO2_cum,
           CO2eq_soil_gain_mean = CO2_gain_mean,
           CO2eq_soil_gain_sd = CO2_gain_sd,
           soil_has_na = has_na) %>% 
    select(year, year_index, CO2eq_soil_gain_95conf, CO2eq_soil_cum, CO2eq_soil_gain_mean, CO2eq_soil_gain_sd, soil_has_na)

  
  ## Return values ----
  return(list(soc_monthly=soc_monthly,      # (a)
              soc_parcels=soc_parcels,      # (b)
              soc_farm=soc_farm,            # (c)
              yearly_results=yearly_results # (d)
              ))
}


get_soil_data <- function(farmId, settings) {
  
  if(settings$use_test_soil) {  
    soilMapsData <- as.list(read.csv(file.path("example-farm-data", "soilMaps_data_Alves4.csv"))) # For testing only
  } else {
    # extract soil data from AWS (original source: ISRIC)
    SOC_df = s3read_using(FUN = read_csv, object = paste0("s3://soil-modelling/soil_variables/", farmId, "/ocs.csv"), show_col_types = F)
    clay_df = s3read_using(FUN = read_csv, object = paste0("s3://soil-modelling/soil_variables/", farmId, "/clay.csv"), show_col_types = F)
    silt_df = s3read_using(FUN = read_csv, object = paste0("s3://soil-modelling/soil_variables/", farmId, "/silt.csv"), show_col_types = F)
    bdod_df = s3read_using(FUN = read_csv, object = paste0("s3://soil-modelling/soil_variables/", farmId, "/bdod.csv"), show_col_types = F)
    
    # Fill soil maps data frame. Waited for values from soil maps
    # soilMapsData = data.frame(SOC=mean(SOC_df$`ocs_0-30cm_mean`), SOC_Q0.05=mean(SOC_df$`ocs_0-30cm_Q0.05`), SOC_Q0.95=mean(SOC_df$`ocs_0-30cm_Q0.95`), 
    #                           clay=mean(clay_df$`clay_5-15cm_mean`)/10, clay_Q0.05=mean(clay_df$`clay_5-15cm_Q0.05`)/10, clay_Q0.95=mean(clay_df$`clay_5-15cm_Q0.95`)/10, 
    #                           silt=mean(silt_df$`silt_5-15cm_mean`)/10, silt_Q0.05=mean(silt_df$`silt_5-15cm_Q0.05`)/10, silt_Q0.95=mean(silt_df$`silt_5-15cm_Q0.95`)/10, 
    #                           bulk_density=mean(bdod_df$`bdod_5-15cm_mean`)/100, bdod_Q0.05=mean(bdod_df$`bdod_5-15cm_Q0.05`)/100, bdod_Q0.95=mean(bdod_df$`bdod_5-15cm_Q0.95`)/100)# waiting for values from soil maps
    
    # Remove all 0 or missing values from SOC_df (e.g. from water surfaces or similar)
    SOC_df <- SOC_df %>% replace_na(list(`ocs_0-30cm_mean` = 0))
    SOC_0_30 <- SOC_df %>% filter(`ocs_0-30cm_mean` > 0) %>% select(`ocs_0-30cm_mean`) %>% pull()
    
    # Lower clay correlates with higher credits, so we take the 75th percentile of all values in the farm area as a conservative approach.
    # The SOC value should not be critical since a spinup is later done, but a conservative approach is taken here as well using the 25th percentile.
    soilMapsData = list(
      'SOC' = as.numeric(quantile(SOC_0_30, 0.05)),
      'clay' = as.numeric(quantile(clay_df$`clay_5-15cm_mean`/10, 0.75)),
      'silt' = mean(silt_df$`silt_5-15cm_mean`)/10, 
      'bulk_density' = mean(bdod_df$`bdod_5-15cm_mean`)/100
    )
  }
  
  return(soilMapsData)
}


generate_stochastic_inputs <- function(n, settings, se_field_carbon_in, se_inputs_nonfarm, soil_data, spinup_climate, present_climate) {
  ## sample the factors from normal distributions
  ## and create input objects for a single simulation run
  
  ## First, define the uncertainty factors
  # (note this is repeated each replication, but is not computational)
  if (settings$debug_mode) {
    se_inputs_nonfarm <- 0.0
    se_field_carbon_in <- 0.0
  } else {
    se_inputs_nonfarm <- settings$se_inputs_nonfarm
    se_field_carbon_in <- settings$se_field_carbon_in
  }
  
  se=data.frame(field_carbon_in=se_field_carbon_in, 
                dr_ratio = se_inputs_nonfarm, 
                temp = se_inputs_nonfarm, 
                precip = se_inputs_nonfarm, 
                evap = se_inputs_nonfarm, 
                soil_thick = se_inputs_nonfarm, 
                SOC = se_inputs_nonfarm,
                clay = se_inputs_nonfarm,
                silt = se_inputs_nonfarm,
                bulk_density = se_inputs_nonfarm,
                pE = se_inputs_nonfarm, 
                tillage_factor = se_inputs_nonfarm)
  
  # set the seed to control stochasticity
  set.seed(n)
  
  # Choice of a random factor to normally randomize input values
  coefs_n <- data.frame(field_carbon_in = rnorm(1, 1, se$field_carbon_in), 
                           dr_ratio = rnorm(1, 1, se$dr_ratio), 
                           temp = rnorm(1, 1, se$temp), 
                           precip = rnorm(1, 1, se$precip), 
                           evap = rnorm(1, 1, se$evap), 
                           soil_thick = rnorm(1, 1, se$soil_thick), 
                           SOC = rnorm(1, 1, se$SOC), 
                           clay = rnorm(1, 1, se$clay), 
                           pE = rnorm(1, 1, se$pE), 
                           tillage_factor = rnorm(1, 1, se$tillage_factor), 
                           silt = rnorm(1, 1, se$silt), 
                           bulk_density = rnorm(1, 1, se$bulk_density))
  
  # Apply factors to inputs average
  soil_data_n <- list(run=n, 
                  bare_months = soil_data$bare_months, 
                  soil_thick = soil_data$soil_thick * coefs_n$soil_thick, 
                  SOC = soil_data$SOC * coefs_n$SOC, 
                  clay = soil_data$clay * coefs_n$clay, 
                  silt = soil_data$silt * coefs_n$silt, 
                  bulk_density = soil_data$bulk_density * coefs_n$bulk_density, 
                  pE = soil_data$pE * coefs_n$pE, 
                  tillage_factor = soil_data$tillage_factor * coefs_n$tillage_factor
  )
  
  spinup_climate_n <- spinup_climate %>% 
    mutate(temperature = temperature * coefs_n$temp,
           precipitation = precipitation * coefs_n$precip,
           evap = evap * coefs_n$evap) %>%
    select(-c(pevap))
  
  present_climate_n <- present_climate %>% 
    mutate(temperature = temperature * coefs_n$temp,
           precipitation = precipitation * coefs_n$precip,
           evap = evap * coefs_n$evap) %>%
    select(-c(pevap))
  
  # return a list of the objects
  return(list(soil_data_n=soil_data_n, 
              coefs_n=coefs_n, 
              spinup_climate_n=spinup_climate_n, 
              present_climate_n=present_climate_n))
}
