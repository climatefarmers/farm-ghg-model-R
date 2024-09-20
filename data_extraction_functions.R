#MongoDB parameters extraction functions


### TOOL FUNCTIONS
## Helper function to convert inputs to numeric
missing_to_zero <- function(input){
  if(length(input)==0) return(0)
  for(i in c(1:length(input))){
    if(is.null(input[i])){
      input[i]=0
    } else if(is.na(input[i])){
      input[i]=0
    } else if(input[i]==""){
      input[i]=0
    }
  }
  return(as.numeric(sub(",", ".", input, fixed = TRUE)))
}


missing_to_na <- function(input){
  if(length(input)==0) return(NA)
  for(i in c(1:length(input))){
    if(is.null(input[i])){
      input[i]=NA
    } else if(is.na(input[i])){
      input[i]=NA
    } else if(input[i]==""){
      input[i]=NA
    }
  }
  return(as.numeric(sub(",", ".", input, fixed = TRUE)))
}


missing_to_value <- function(input, value=NULL){
  if(length(input)==0) return(NA)
  for(i in c(1:length(input))){
    if(is.null(input[i])){
      if(is.null(value)) {input[i]=NA} else {input[i]=value}
    } else if(is.na(input[i])){
      if(is.null(value)) {input[i]=NA} else {input[i]=value}
    } else if(input[i]==""){
      if(is.null(value)) {input[i]=NA} else {input[i]=value}
    }
  }
  return(as.numeric(sub(",", ".", input, fixed = TRUE)))
}


## Helper function to extract latitude
extract_latitude_landUseSummaryOrPractices <- function(landUseSummaryOrPractices, parcel_index = i){
  #takes a landUseSummaryOrPractices from farms collection and the inde corresping to the parcel.
  #extracts the mean latitude of parcel's corners
  latitudes = c()
  for (parcels in landUseSummaryOrPractices[[1]]$coordinates){
    for (i in c(1:nrow(parcels))){
      latitudes <- append(latitudes,parcels[[i,2]])
    }
  }
  return(mean(latitudes))
}


## Helper function to extract longitude
extract_longitude_landUseSummaryOrPractices <- function(landUseSummaryOrPractices, parcel_index = i){
  #takes a landUseSummaryOrPractices from farms collection and a parcel index
  #extracts the mean longitude of parcel's corners
  longitudes = c()
  for (parcels in landUseSummaryOrPractices[[1]]$coordinates){
    for (i in c(1:nrow(parcels))){
      longitudes <- append(longitudes,parcels[[i,1]])
    }
  }
  return(mean(longitudes))
}

get_mean_longitude <- function(coordinates) {
  return(mean(coordinates[,1]))
}

get_mean_latitude <- function(coordinates) {
 return(mean(coordinates[,2]))
}

get_livestock_inputs <- function(monitoringData, factors_animals, start_index) {
  years <- monitoringData$yearlyFarmData$year
  
  livestock_category <- tibble()
  livestock_species <- tibble()
  grazing_management <- tibble(
    year = years, 
    grazing_management = monitoringData$yearlyFarmData$livestock$grazingManagement
  )
  livestock_outfarm <- tibble()
  
  for (i in start_index:length(years)) {
    
    # Extract infarm livestock species data
    livestock_year <- as.data.frame(monitoringData$yearlyFarmData$livestock$inFarm[[i]])
    
    if (nrow(livestock_year) > 0) {
      # Create a data frame for livestock species data
      livestock_species_temp <- jsonlite::flatten(livestock_year) %>% 
        select(-c("category")) %>%
        rename(grazing_days = grazingDays) %>%
        rename(manure_treatment = manureTreatment) %>%
        rename(fertility_rate = fertilityRate)
      livestock_species_temp$manure_treatment <- as.character(livestock_species_temp$manure_treatment)
      livestock_species_temp$`_id.$oid` <- NULL
      livestock_species_temp$year <- years[i]
      livestock_species <- bind_rows(livestock_species, livestock_species_temp)
    
      # Create a data frame for livestock categories data
      for (j in 1:nrow(livestock_year)) {
        livestock_category_temp <- livestock_year$category[[j]]
        livestock_category_temp$species <- livestock_year$species[j]
        livestock_category_temp$grazing_days <- livestock_year$grazingDays[j]
        livestock_category_temp$year <- years[i]
        livestock_category <- bind_rows(livestock_category, livestock_category_temp)
      }
      
      # fill in missing values
      livestock_category <- livestock_category %>%
        mutate(amount = if_else (amount == -9999, 0, amount),
               grazing_days = if_else(grazing_days == -9999, 0, grazing_days))
      livestock_species <- livestock_species %>%
        mutate(fertility_rate = if_else(fertility_rate == -9999, 0, fertility_rate),
               grazing_days = if_else(grazing_days == -9999, 0, grazing_days))
    }
    
    # Recalculate young animals from fertility rate (because of mistake in platform equation)
    for (y in unique(livestock_category$year)) {
      # Calculate the number of young animals (after bug found in platform formula)
      species_y <- livestock_species %>% filter(year == y)
      category_y <- livestock_category %>% filter(year == y)
      
      # Calculate the number of young animals (after bug found in platform formula)
      n_cows <- category_y %>% filter(species %in% c("cattle", "dairy cows") &
                                        name %in% c("mature females (non-dairy)", "mature females (> 2 years)")) %>%
        pull(amount) %>% sum()
      cow_fert <- species_y$fertility_rate[species_y$species == "cattle"]
      n_calves <- n_cows * cow_fert
      livestock_category$amount[
        (livestock_category$name == "calves (0 - 6 months)") &
          (livestock_category$year == y)] <- n_calves
      
      sows <- category_y %>% filter(name == "sows") %>% pull(amount)
      sow_fert <- species_y$fertility_rate[species_y$species == "swine"]
      n_piglets <- sows * sow_fert
      livestock_category$amount[
        (livestock_category$name == "piglets") &
          (livestock_category$year == y)] <- n_piglets
      
      n_sheep <- category_y %>% filter(
        species == "sheep" & name == "mature females (> 1 year)") %>% pull(amount)
      sheep_fert <- species_y$fertility_rate[species_y$species == "sheep"]
      n_lambs <- n_sheep * sheep_fert
      livestock_category$amount[
        (livestock_category$name == "lambs (< 1 year)") &
          (livestock_category$year == y)] <- n_lambs
      
      n_goats <- category_y %>% filter(
        species == "goats" & name == "mature females (> 1 year)") %>% pull(amount)
      goat_fert <- species_y$fertility_rate[species_y$species == "goats"]
      n_kids <- n_goats * goat_fert
      livestock_category$amount[
        (livestock_category$name == "goat kids (< 1 year)") &
          (livestock_category$year == y)] <- n_kids
    }
    
    # Extract outfarm livestock data
    livestock_outfarm_temp <- as_tibble(monitoringData$yearlyFarmData$livestock$outFarm[[i]])
    if (nrow(livestock_outfarm_temp) > 0) {
      livestock_outfarm_temp <- livestock_outfarm_temp %>%
        rename(grazing_days = grazingDays)
      livestock_outfarm_temp$`_id` <- NULL
      livestock_outfarm_temp$year <- years[i]
      livestock_outfarm <- bind_rows(livestock_outfarm, livestock_outfarm_temp)
    }
  }
  
  ## Clean up livestock data
  # all species df to ensure that all species are included in the database
  all_species <- select(factors_animals, species, name = db_reference)
  if (start_index == 1) {
    all_species_years <- expand_grid(year = years, species = unique(factors_animals$species))
    all_categories_years <- expand_grid(year = years, all_species)
  } else {
    all_species_years <- expand_grid(year = years[-(1:(start_index-1))], species = unique(factors_animals$species))
    all_categories_years <- expand_grid(year = years[-(1:(start_index-1))], all_species)
  }
  
  # Infarm livestock data
  if (nrow(livestock_species) > 0) {
    livestock_species <- left_join(all_species_years, livestock_species, by = c("year", "species"))
    livestock_species$fertility_rate <- na_if(livestock_species$fertility_rate, -9999)
    livestock_species$grazing_days <- na_if(livestock_species$grazing_days, -9999)
    livestock_species$manure_treatment <- na_if(livestock_species$manure_treatment, "-")
    livestock_species <- replace_na(livestock_species, list(manure_treatment = NA, fertility_rate = 0, grazing_days = 0))
    livestock_species <- livestock_species %>%
      mutate(grazing_days = if_else(grazing_days == -9999, 0, grazing_days))
  } else {
    livestock_species <- all_species_years %>% mutate(manure_treatment = NA, fertility_rate = 0, grazing_days = 0)
  }
  
  if (nrow(livestock_category) > 0) {
    ## group any purchased young animals
    # remove " purchased" from the end of the "name" column
    livestock_category$name <- gsub(" purchased", "", livestock_category$name)
    # group by name and year --> summarise
    livestock_category <- livestock_category %>%
      group_by(name, year, species) %>%
      summarise(grazing_days = weighted.mean(grazing_days, amount, na.rm=T), # based on the original amounts
                grazing_days = if_else(is.na(grazing_days), 0, grazing_days), # NAs generated with weighted.mean(0,0)
                amount = sum(amount), # sum the non-purchased and purchased
                .groups='drop')
    
    # join with the full dataset
    livestock_category <- left_join(all_categories_years, livestock_category, by = c("year", "species","name"))
    livestock_category <- replace_na(livestock_category, list(amount = 0, grazing_days = 0))
    livestock_category <- livestock_category %>% rename(db_reference = name)
  } else {
    livestock_category <- all_categories_years %>% mutate(amount = 0, grazing_days = 0)
  }
  
  # Outfarm livestock data
  if (nrow(livestock_outfarm) > 0) {
    livestock_outfarm$amount <- na_if(livestock_outfarm$amount, -9999)
    livestock_outfarm$grazing_days <- na_if(livestock_outfarm$grazing_days, -9999)
    # To get a complete data frame with all species and years
    livestock_outfarm <- left_join(all_species_years, livestock_outfarm, by = c("year", "species"))
    livestock_outfarm <- replace_na(livestock_outfarm, list(amount = 0, grazing_days = 0))
  } else {
    livestock_outfarm <- all_species_years %>% mutate(amount = 0, grazing_days = 0)
    
  }
  
  return(list(
    livestock_category=livestock_category, 
    livestock_species=livestock_species, 
    grazing_management=grazing_management,
    livestock_outfarm=livestock_outfarm
  ))
}


## Function to extract expected grazing from livestock and fodder from the whole farm over all years
# Definitions:
# Fodder: hay and straw that was additionally applied to the parcel to feed animals
# Grazing: amount of biomass that is eaten by animals
# forage: amount of growing biomass (on the parcel) that is eaten by the animals
get_grazing_inputs_monthly <- function(monitoringData, start_index){
  
  years <- monitoringData$yearlyFarmData$year
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  month <- c(1:12)
  
  # Data frame for monthly grazing inputs
  inputs_grazing_monthly <- data.frame()
  
  # Loop through years and parcels to extract monthly grazing information
  for (i in start_index:length(years)) {
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[i]]
    for (j in 1:nrow(year_parcels)){
      inputs_grazing_monthly_temp <- data.frame(
        year = years[i],
        parcel_name = parcels$parcel_name[j],
        month = month,
        was_grazed = year_parcels$yearParcelData$grazingMonthlyEvent[[j]]
      )
      inputs_grazing_monthly <- bind_rows(inputs_grazing_monthly, inputs_grazing_monthly_temp)
    }
  }

  return (inputs_grazing_monthly)
}


# Helper function that extracts crop type per month per parcel:
# In case of crop rotation there can be two different cash crops within one year (cash crop 1 & cash crop 2)
get_monthly_cash_crop <- function(parcel_index = i, year_chosen){
  crop=rep(NA,12)
  for (k in c(1:12)){
    if(!is.na(year_chosen$cashCrop1MonthlyData[[parcel_index]][k]) &
       year_chosen$cashCrop1MonthlyData[[parcel_index]][k] != "-"){ # cash crop 1 checked for month k
      if(!is.na(year_chosen$cashCrop2MonthlyData[[parcel_index]][k]) &
         year_chosen$cashCrop2MonthlyData[[parcel_index]][k] != "-"){ # case of conflict, we assume that the correct is cash crop 1
        log4r::error(my_logger, paste('Two different cash crops checked for month ',k,' in parcel ',landUseSummaryOrPractices[[1]]$parcelName[i],'.',sep='')) # flag
      } else {
        crop[k] = year_chosen$cashCrop1MonthlyData[[parcel_index]][k]
      }
    } else if (!is.na(year_chosen$cashCrop2MonthlyData[[parcel_index]][k]) &
               year_chosen$cashCrop2MonthlyData[[parcel_index]][k] != "-"){
      crop[k] = year_chosen$cashCrop2MonthlyData[[parcel_index]][k]
    } else {
      crop[k] = NA # crop[k] stays NA
    }
  }
  return(crop)
}


# UNDER CONSTRUCTION:
detect_crop_rotations <- function(landUseSummaryOrPractices, parcel_index = i){
  ### Get landUse data for a parcel i
  ### Return a year where rotations starts 
  ### and a number of years of a cycle. 0 means no rotations? 1 means no change in management
  # Listing arablecrop years
  list_arablecrop_years = c()
  for (year in c(0:10)){
    year_str <- paste0('year', year)
    if(landUseSummaryOrPractices[[1]][[year_str]]$landUseType[i]=="Arablecrops"){
      list_arablecrop_years = c(list_arablecrop_years,year)
    }
  }
  # If occurrence of arable crops, looking for crop rotations
  if (length(list_arablecrop_years) > 0){
    df_crops = data.frame(year = c(), crops = c())
    for (year in c(0:10)){
      year_str <- paste0('year', year)
      year_chosen = landUseSummaryOrPractices[[1]][[year_str]]
      df_crops = rbind(df_crops, data.frame(
        year = year,
        crops = unique(na.omit(get_monthly_cash_crop(i, year_chosen)))
      ))
    }
  }
}


## Helper function to get clay content in %
# if soil samples available: farmer's input (%)
# else: soil maps
get_clay_content <- function(soilAnalysis, soilMapsData){
  if (is.null(soilAnalysis$clayContentPercent)){
    return(soilMapsData$clay)
  } else if (soilAnalysis$clayContentPercent==""){
    return(soilMapsData$clay)
  } else { # variable found and a value is provided
    if(5<missing_to_zero(soilAnalysis$clayContentPercent) & missing_to_zero(soilAnalysis$clayContentPercent)<80){ # assumed to be %
      return(missing_to_zero(soilAnalysis$clayContentPercent))
    } else {
      log4r::error(my_logger, paste("Clay content input = ", 
                                    missing_to_zero(soilAnalysis$clayContentPercent),
                                    "%. Check unit/values with farmer.", sep=""))
    }
  }
}


## Helper function to get silt content in %
# if soil samples available: farmer's input (%)
# else: soil maps
get_silt_content <- function(soilAnalysis, soilMapsData){
  if (is.null(soilAnalysis$siltContentPercent)){
    return(soilMapsData$silt)
  } else if (soilAnalysis$siltContentPercent==""){
    return(soilMapsData$silt)
  } else { # variable found and a value is provided
    if(5<missing_to_zero(soilAnalysis$siltContentPercent) & missing_to_zero(soilAnalysis$siltContentPercent)<80){ # assumed to be %
      return(missing_to_zero(soilAnalysis$siltContentPercent))
    } else {
      log4r::error(my_logger, paste("silt content input = ", 
                                    missing_to_zero(soilAnalysis$siltContentPercent),
                                    "%. Check unit/values with farmer.", sep=""))
    }
  }
}


## Helper function to get carbon content in kg/ha?
get_SOC_content <- function(soilAnalysis, soilMapsData){
  if (is.null(soilAnalysis$clayContentPercent) & is.null(soilAnalysis$organicMatterContent)){ #case that SOC & SOM variables weren't found
    return(soilMapsData$SOC)
  }
  if (!is.null(soilAnalysis$carbonContent) & is.null(soilAnalysis$organicMatterContent)){ 
    if (soilAnalysis$organicMatterContent==""){ # case that SOC variable wasn't found and SOM wasn't known
      return(soilMapsData$SOC)
    }
    if(8<missing_to_zero(soilAnalysis$organicMatterContent) & missing_to_zero(soilAnalysis$organicMatterContent)<80 & soilAnalysis$organicMatterContentMetric!="%"){ # SOC in t/ha = g/kg
      return(missing_to_zero(soilAnalysis$organicMatterContent)*0.55)
    } 
    if (0.7<missing_to_zero(soilAnalysis$organicMatterContent) & missing_to_zero(soilAnalysis$organicMatterContent)<8){ #SOC in %
      return(missing_to_zero(soilAnalysis$organicMatterContent)*5.5)
    } else {
      log4r::error(my_logger, paste("OM content input = ", missing_to_zero(soilAnalysis$organicMatterContent),
                                    soilAnalysis$organicMatterContentMetric,
                                    ". Check unit/values with farmer.", sep=""))
    }
  }
  if (soilAnalysis$carbonContent!=""){ # SOC variable exists and a value was entered
    if(4<missing_to_zero(soilAnalysis$carbonContent) & missing_to_zero(soilAnalysis$carbonContent)<40){ # SOC in t/ha = g/kg
      return(missing_to_zero(soilAnalysis$carbonContent))
    } 
    if (0.35<missing_to_zero(soilAnalysis$carbonContent) & missing_to_zero(soilAnalysis$carbonContent)<4){ #SOC in %
      return(missing_to_zero(soilAnalysis$carbonContent)*10)
    } else {
      log4r::error(my_logger, paste("SOC content input = ", missing_to_zero(soilAnalysis$carbonContent),
                                    soilAnalysis$carbonContentMetric,
                                    ". Check unit/values with farmer.", sep=""))
    }
  } else { # SOC variable exists and no value was entered
    return(soilMapsData$SOC)
  }
}


## Helper function to get bulk density
get_bulk_density <- function(soilAnalysis, soilMapsData){
  if (is.null(soilAnalysis$bulkDensity)){
    return(soilMapsData$bulk_density)
  } else if (soilAnalysis$bulkDensity==""){
    return(soilMapsData$bulk_density)
  } else { # variable found and a value is provided
    if(0.7<missing_to_zero(soilAnalysis$bulkDensity) & missing_to_zero(soilAnalysis$bulkDensity)<2){
      return(missing_to_zero(soilAnalysis$bulkDensity))
    } else if (700<missing_to_zero(soilAnalysis$bulkDensity) & missing_to_zero(soilAnalysis$bulkDensity)<2000){
      return(missing_to_zero(soilAnalysis$bulkDensity)*1e-3)
    } else {
      log4r::error(my_logger, paste("bulk density input = ", 
                                    missing_to_zero(soilAnalysis$bulkDensity),
                                    ". Check unit/values with farmer.", sep=""))
    }
  }
}


### GET INPUT FUNCTIONS

get_organicmatter_inputs = function(monitoringData, factors_om, start_index) {
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  
  # Extract the imported organic matter inputs ---
  inputs_om_imported <- tibble()
  for (i in start_index:length(years)) {
    inputs_om_imported_temp <- as.data.frame(monitoringData$yearlyFarmData$importedOrganicMatter[[i]])
    if (nrow(inputs_om_imported_temp) == 0) { next }
    inputs_om_imported_temp <- inputs_om_imported_temp %>%
      select(-"_id") %>%
      rename(percent_imported = percentImported)
    inputs_om_imported_temp$year <- years[i]
    inputs_om_imported <- bind_rows(inputs_om_imported, inputs_om_imported_temp)
  }

  if (nrow(inputs_om_imported)==0) {
    inputs_om_imported <- tibble(type=NA, percent_imported=NA, year=NA)
  } else {
    # Check for missing values and replace them with conservative assumption (100 = all imported)
    # also note that the left_join at the bottom of this function introduces NAs into percent_imported
    inputs_om_imported$percent_imported <- if_else(inputs_om_imported$percent_imported == -9999, 100, inputs_om_imported$percent_imported)
  }
  
  # Extract the added organic matter inputs ---

  # Extract added organic matter inputs
  inputs_om_added <- tibble()
  inputs_fodder <- tibble()
  
  for (y in start_index:length(years)) {
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    for (p in 1:nrow(year_parcels)) {
      organic_matter <- year_parcels$yearParcelData$addedOrganicMatter[[p]]
      if (is.null(organic_matter)) { next }
      
      for (o in 1:nrow(organic_matter)) {
        om <- organic_matter[o,]
        inputs_om_added_temp <- tibble(
          year        = years[y],
          parcel_name = year_parcels$parcelFixedValues$parcelName[p],
          parcel_id   = year_parcels$parcelFixedValues$parcelID[p],
          type        = om$type,
          sub_type    = om$subType,
          other       = om$other,
          amount_t = om$amount,
          units       = om$units
        )
        inputs_om_added <- bind_rows(inputs_om_added, inputs_om_added_temp)
      }
      
      fodder <- as_tibble(year_parcels$yearParcelData$animalFodder[p,])
      inputs_fodder_temp <- tibble(
        year        = years[y],
        parcel_name = year_parcels$parcelFixedValues$parcelName[p],
        parcel_id   = year_parcels$parcelFixedValues$parcelID[p],
        type        = "fodder",
        sub_type    = "generic fodder",
        other       = NA,
        amount_t = fodder$amount,
        units       = fodder$units
      )
      inputs_fodder <- bind_rows(inputs_fodder, inputs_fodder_temp)
    }
  }
  
  inputs_om_added <- bind_rows(inputs_om_added, inputs_fodder)
  inputs_om <- left_join(inputs_om_added, inputs_om_imported, by = c("year", "type"))
  
  # NOTE: Check if these clean ups are required:
  inputs_om$amount_t <- if_else(inputs_om$amount_t == -9999, 0, inputs_om$amount_t)
  inputs_om$amount_t <- if_else(is.na(inputs_om$amount_t), 0, inputs_om$amount_t)
  inputs_om$units <- if_else(inputs_om$units == "" & inputs_om$type == "slurry", "litres", inputs_om_added$units)
  inputs_om$units <- if_else(inputs_om$units == "" | is.na(inputs_om$units), "tonnes", inputs_om_added$units)
  inputs_om$sub_type <- if_else(inputs_om$sub_type == "", NA, inputs_om$sub_type)
  inputs_om$sub_type <- if_else(inputs_om$sub_type == "-", NA, inputs_om$sub_type)
  inputs_om$other <- if_else(inputs_om$other == "", NA, inputs_om$other)
  inputs_om$other <- if_else(inputs_om$other == "-", NA, inputs_om$other)
  
  # # Code if we want a complete data frame with all subtypes and years -  probably not!
  # omsubtypes <- factors_om %>% select(type, sub_type) %>% distinct()
  # all_om_subtypes <- expand_grid(parcel_name = year_parcels$parcelFixedValues$parcelName, year = years, omsubtypes)
  
  return(inputs_om)
}


# get_tree_inputs = function(landUseSummaryOrPractices){
#   # takes landUseSummaryOrPractices from farms collection
#   # extracts agroforestry inputs dataframe 
#   parcel_names <- landUseSummaryOrPractices[[1]]$parcelName
#   tree_inputs = tibble(parcel_ID = c(), scenario = c(), tree_species = c(), other_name = c(), dbh = c(),
#                            tree_density = c(), area = c())
#   for (i in c(1:length(parcel_names))){
#     for (j in c(0:10)){
#       year_str <- paste0('year', j)
#       row_index = 0
#       c = c()
#       for (tree in landUseSummaryOrPractices[[1]][[year_str]]$typeOfTrees[i][[1]]$treeName){
#         row_index = row_index + 1
#         if (!is.na(tree)){
#           if (tree!=""){ #filter out if no tree information given
#             c = append(c, row_index)
#           }
#         }
#       }
#       typeOfTrees = landUseSummaryOrPractices[[1]][[year_str]]$typeOfTrees[i][[1]][c,]
#       if(nrow(typeOfTrees)>0){
#         for (k in c(1:nrow(typeOfTrees))){
#           tree_inputs <- rbind(tree_inputs,data.frame(
#             parcel_ID = c(parcel_names[i]), 
#             scenario = c(year_str), 
#             tree_species = c(typeOfTrees$treeName[[k]]),
#             other_name = c(typeOfTrees$otherTreeName[[k]]),
#             dbh = c(missing_to_zero(typeOfTrees$treeAvgDBH[[k]])), 
#             tree_density = c(missing_to_zero(typeOfTrees$avgNoOfTrees[[k]])), 
#             area = c(missing_to_zero(landUseSummaryOrPractices[[1]]$area[i])/10000)))
#         }
#         if (j==0){ #baseline based on pre-project trees
#           for (k in c(1:nrow(typeOfTrees))){
#             tree_inputs <- rbind(tree_inputs,data.frame(
#               parcel_ID = c(parcel_names[i]), 
#               scenario = c("baseline"), 
#               tree_species = c(typeOfTrees$treeName[[k]]),
#               other_name = c(typeOfTrees$otherTreeName[[k]]),
#               dbh = c(missing_to_zero(typeOfTrees$treeAvgDBH[[k]])), 
#               tree_density = c(missing_to_zero(typeOfTrees$avgNoOfTrees[[k]])), 
#               area = c(missing_to_zero(landUseSummaryOrPractices[[1]]$area[i])/10000)))
#           }
#         }
#       }
#     }
#   }
#   NA_rows = nrow(tree_inputs)-nrow(na.omit(tree_inputs))
#   if(NA_rows>0){
#     log4r::error(my_logger, paste('WARNING: ',NA_rows,' rows contained NAs in tree_inputs.', paste=''))
#   }
#   return(na.omit(tree_inputs))
# }

get_annualcrops_fallow_inputs <- function(monitoringData, start_index){
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  
  parcels <- tibble(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  
  inputs_annual_crops <- tibble()
  inputs_bare_soil <- tibble()
  
  # Extract annual crop inputs and fallow period inputs
  for (y in start_index:length(years)) {
    
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    
    for (p in 1:nrow(year_parcels)) {
      
      crop_i <- 0 # crop index restarts for each parcel
      
      # Extract annual crop data
      annual_crops <- as_tibble(year_parcels$yearParcelData$annualCrops[[p]])
      
      if(nrow(annual_crops) > 0) {
        
        for (ac in 1:nrow(annual_crops)) {
          
          if (is.na(annual_crops$species[ac])) {
            # Check to see for what year and parcel this is true
            # log4r::warn(my_logger, "get_annualcrops_fallow_inputs (data_extraction_functions.R): Species is NA in annual crops. Check if front end is working correctly.",
            #             "\n Year: ", years[y]," Parcel: ", parcels$parcel_name[p])
            next
          }
          
          crop_i <- crop_i + 1
          
          # Extract harvest from annual crops
          harvests <- as_tibble(annual_crops$harvest[[ac]])
          
          harvests <- harvests %>% mutate(
            date = ifelse(date==-9999, NA, date),
            year = years[y],
            parcel_name = parcels$parcel_name[p],
            parcel_id = parcels$parcel_id[p],
            crop_index = crop_i,
            species = annual_crops$species[ac],
            other = annual_crops$other[ac],
            category = "harvest",
            start_date = annual_crops$sowingDate[ac],
            end_date = annual_crops$harvestDate[ac],
            dry = ifelse(units == "tonnes - dry", T, F)
          )
          
          # Extract residues from annual crops
          residues <- as_tibble(annual_crops$residue[[ac]])
          
          residues <- residues %>% mutate(
            date = ifelse(date==-9999, NA, date),
            year = years[y],
            parcel_name = parcels$parcel_name[p],
            parcel_id = parcels$parcel_id[p],
            crop_index = crop_i,
            species = annual_crops$species[ac],
            other = annual_crops$other[ac],
            category = "residue",
            start_date = annual_crops$sowingDate[ac],
            end_date = annual_crops$harvestDate[ac],
            dry = ifelse(units == "tonnes - dry", T, F)
          )
          
          crop_data_temp <- bind_rows(harvests, residues)
          inputs_annual_crops <- bind_rows(inputs_annual_crops, crop_data_temp)
        }
      }
      
    
      # Extract fallow periods data
      fallows <- as_tibble(year_parcels$yearParcelData$fallow[[p]])
      
      if(nrow(fallows) > 0) {

        for (f in 1:nrow(fallows)) {
          
          if (is.na(fallows$type[f]) | fallows$type[f]=="") {
            # log4r::warn(my_logger,"get_annualcrops_fallow_inputs (data_extraction_functions.R): Fallow is NA. Check if front end is working correctly.",
            #             "\n Year: ", years[y]," Parcel: ", parcels$parcel_name[p])
            next
          }
          
          if(fallows$type[f] == "productive fallow") {
            
            crop_i <- crop_i + 1
            
            # Extract harvest from annual crops
            harvests <- as_tibble(fallows$harvest[[f]])
            
            harvests <- harvests %>% mutate(
              date = ifelse(date==-9999, NA, date),
              year = years[y],
              parcel_name = parcels$parcel_name[p],
              parcel_id = parcels$parcel_id[p],
              crop_index = crop_i,
              species = "fallow",
              other = "",
              category = "harvest",
              start_date = fallows$startDate[f],
              end_date = fallows$endDate[f],
              dry = ifelse(units == "tonnes - dry", T, F)
            )
            
            # Extract residues from annual crops
            residues <- as_tibble(fallows$residue[[f]])
            
            residues <- residues %>% mutate(
              date = ifelse(date==-9999, NA, date),
              year = years[y],
              parcel_name = parcels$parcel_name[p],
              parcel_id = parcels$parcel_id[p],
              crop_index = crop_i,
              species = "fallow",
              other = "",
              category = "residue",
              start_date = fallows$startDate[f],
              end_date = fallows$endDate[f],
              dry = ifelse(units == "tonnes - dry", T, F)
            )
            
            crop_data_temp <- bind_rows(harvests, residues)
            inputs_annual_crops <- bind_rows(inputs_annual_crops, crop_data_temp)
            
          } else if(fallows$type[f] == "bare soil") {
              bare_soil_temp <- tibble(
                year = years[y],
                parcel_name = parcels$parcel_name[p],
                parcel_id = parcels$parcel_id[p],
                category = "bare soil",
                percent_area = fallows$percentArea[f],
                start_date = fallows$startDate[f],
                end_date = fallows$endDate[f]
              )
              inputs_bare_soil <- bind_rows(inputs_bare_soil, bare_soil_temp)
            } else {
              log4r::error(my_logger, paste("get_annnualcrops_fallow (data_extraction_functions.R): Fallow type not recognized for ", 
                                            fallows$type[f], "\n Year: ", years[y]," Parcel: ", parcels$parcel_name[p], 
                                            "\n Possible reason: empty data fields. Check front end is working correctly."))
            }
        }
      }
    }
  }
  
  # Check if any of the data frames is empty and if yes create minimal data frames.
  # Else format the NA values, then select and reorder columns.
  if(nrow(inputs_annual_crops) == 0) {
    category <- c("harvest", "residue")
    inputs_annual_crops <- tibble(year = NA, parcel_name = NA, parcel_id = NA, 
                                  crop_index = NA, start_date = NA, end_date = NA,
                                  species = NA, other = NA, type = NA, 
                                  amount = NA, units = NA, dry = NA
                                  )
    inputs_annual_crops <- expand_grid(inputs_annual_crops, category = category)
  } else {
    
    ## Cleaning data frame
    # Data frame should contain every species entered and at least one harvest and residue event per species.
    # Harvest and residue events that are empty: type = NA, amount = 0
    # This is to ensure all species are considered. 
    
    # Remove all empty species
    inputs_annual_crops$species <- na_if(inputs_annual_crops$species, "-")
    #inputs_annual_crops$end_date <- if_else(is.na(inputs_annual_crops$end_date), NA, inputs_annual_crops$end_date)  # issue: can be deleted? need to check that the date is NULL if it is not entered on the dashboard
    inputs_annual_crops <- inputs_annual_crops %>% filter(!is.na(species)) %>%  # IF IT HAS NA VALUES, CHECK IF FRONTEND IS WORKING CORRECTLY
      select(year, parcel_name, parcel_id, 
             crop_index, start_date, end_date,
             species, other, category, type, 
             amount, units, dry
      )
    
    
    # Replace -9999 with 0 values
    inputs_annual_crops$amount <- na_if(inputs_annual_crops$amount, -9999)
    inputs_annual_crops <- replace_na(inputs_annual_crops, list(amount = 0))
    
    # Replace "-" with NA
    inputs_annual_crops$type <- if_else(!is.na(inputs_annual_crops$type) & inputs_annual_crops$type == "-", NA, inputs_annual_crops$type)
    # Replace "" with NA
    inputs_annual_crops$type <- if_else(!is.na(inputs_annual_crops$type) & inputs_annual_crops$type == "", NA, inputs_annual_crops$type)
    inputs_annual_crops$species <- if_else(!is.na(inputs_annual_crops$species) & inputs_annual_crops$species == "", NA, inputs_annual_crops$species)
    # Fill empty units
    inputs_annual_crops$units <- if_else(inputs_annual_crops$units == "", "tonnes - fresh", inputs_annual_crops$units)
    # Fill empty  dates
    inputs_annual_crops$start_date <- if_else(inputs_annual_crops$start_date == "", NA, inputs_annual_crops$start_date)
    inputs_annual_crops$end_date <- if_else(inputs_annual_crops$end_date == "", NA, inputs_annual_crops$end_date)
  }
  
  if(nrow(inputs_bare_soil) == 0) {
    inputs_bare_soil <- tibble(year = NA, parcel_name = NA, parcel_id = NA, 
                               category = NA, percent_area = NA, 
                               start_date = NA, end_date = NA
                               )
  } else {
    inputs_bare_soil$percent_area <- na_if(inputs_bare_soil$percent_area, -9999)
    inputs_bare_soil$end_date <- if_else(is.na(inputs_bare_soil$end_date), NA, inputs_bare_soil$end_date)  # issue: need to check that the date is NULL if it is not entered on the dashboard
    inputs_bare_soil <- inputs_bare_soil %>% filter(!is.na(percent_area)) %>%
                                        select(year, parcel_name, parcel_id, 
                                               category, percent_area, 
                                               start_date, end_date
                                               )
  }
  
  # Return the data frames
  return(list(inputs_annual_crops = inputs_annual_crops, 
              inputs_bare_soil = inputs_bare_soil
              ))
}


get_perennials_inputs <- function(monitoringData, start_index) {
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )

  # Create empty data frames
  inputs_perennialcrops <- tibble() #DBH, age, lifespan
  inputs_perennialprod <- tibble() # Harvest and residue
  
  for (y in start_index:length(years)) {
    
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    
    for (p in 1:nrow(year_parcels)) {
      
      # Extract perennial crop data
      perennials <- year_parcels$yearParcelData$perennialCrops[[p]]

      tree_index <- 0  # tree index restarts for each parcel
      

      # skips parcels with no perennials
      if (!length(perennials)==0) {
        
        if (class(perennials)=="list") {
          next
        }
        
        for (pc in 1:nrow(perennials)) {
          
          tree_index <- tree_index + 1
        
          inputs_treecrops_temp <- tibble(
            year = years[y],
            parcel_name = parcels$parcel_name[p],
            parcel_id = parcels$parcel_id[p],
            species = perennials$species[pc],
            other = perennials$other[pc],
            cohort_id = perennials$cohortID[pc],
            tree_index = tree_index,
            planting_year = perennials$plantingYear[pc],
            tree_number = perennials$treeNumber[pc],
            avg_DBH = perennials$DBHmean$diameter[pc],
            units = perennials$DBHmean$units[pc],
            lifespan_years = perennials$rotationYears[pc]
          )
        
          inputs_perennialcrops <- bind_rows(inputs_perennialcrops, inputs_treecrops_temp)

          # Extract harvest from perennial crops
          harvests <- as_tibble(perennials$harvest[[pc]])
          if (nrow(harvests) > 0) {
            harvests <- harvests %>% mutate(
              year = years[y],
              parcel_name = parcels$parcel_name[p],
              parcel_id = parcels$parcel_id[p],
              species = perennials$species[pc],
              other = perennials$other[pc],
              cohort_id = perennials$cohortID[pc],
              tree_index = tree_index,
              category = "harvest",
              dry = ifelse(units == "tonnes - dry", T, F)
            )
          }
          
          inputs_perennialprod <- bind_rows(inputs_perennialprod, harvests)
          
          # Extract residues from perennial crops
          residues <- as_tibble(perennials$residue[[pc]])
          
          if (nrow(residues) > 0) {
            residues <- residues %>% mutate(
              year = years[y],
              parcel_name = parcels$parcel_name[p],
              parcel_id = parcels$parcel_id[p],
              species = perennials$species[pc],
              other = perennials$other[pc],
              cohort_id = perennials$cohortID[pc],
              tree_index = tree_index,
              category = "residue",
              dry = ifelse(units == "tonnes - dry", T, F)
            )
          }
          inputs_perennialprod <- bind_rows(inputs_perennialprod, residues)
        } # end loop over trees in parcel
      } # end if statement
    } # end loop over parcels
  } # end loop over years

  # Check if any of the data frames is empty and if yes create minimal data frames.
  # Else select and reorder columns.
  if(nrow(inputs_perennialcrops) == 0) {
    inputs_perennialcrops <- tibble(year = NA, parcel_name = NA, parcel_id = NA, 
                                    cohort_id = NA, tree_index = NA, species = NA, 
                                    other = NA, planting_year = NA, tree_number = NA, 
                                    avg_DBH = NA, units = NA, lifespan_years = NA)
  } else {
    # Replace -9999 values with NA
    inputs_perennialcrops$tree_number <- na_if(inputs_perennialcrops$tree_number, -9999)
    inputs_perennialcrops$avg_DBH <- na_if(inputs_perennialcrops$avg_DBH, -9999)
    inputs_perennialcrops$lifespan_years <- na_if(inputs_perennialcrops$lifespan_years, -9999)
    
    inputs_perennialcrops <- inputs_perennialcrops %>% 
      select(year, parcel_name, parcel_id, 
             cohort_id, tree_index, species, 
             other, planting_year, tree_number, 
             avg_DBH, units, lifespan_years)
  }
  
  if(nrow(inputs_perennialprod) == 0) {
    inputs_perennialprod <- tibble(year = NA, parcel_name = NA, parcel_id = NA, 
                                   cohort_id = NA, tree_index = NA, date = NA,
                                   species = NA, category = NA, type = NA, 
                                   amount = NA, units = NA, dry = NA)
  } else {
    
    # NAs are not removed here. Raw data should reflect the DB data
    inputs_perennialprod$amount <- na_if(inputs_perennialprod$amount, -9999)
    inputs_perennialprod <- inputs_perennialprod %>% 
      select(year, parcel_name, parcel_id, 
             cohort_id, tree_index, date,
             species, category, type, 
             amount, units, dry)
  }

  return(list(inputs_perennialprod = inputs_perennialprod,
              inputs_perennialcrops = inputs_perennialcrops 
              ))
}

get_felled_trees_inputs <- function(monitoringData, start_index) {
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  
  # Create empty data frames
  inputs_trees_felled <- tibble()
  
  for (y in start_index:length(years)) {
    
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    
    for (p in 1:nrow(year_parcels)) {
      # Extract perennial crop data
      felled_trees <- year_parcels$yearParcelData$treeFelling[[p]]
      if (!is.null(felled_trees)) {
        
        if (class(felled_trees)=="list" & length(felled_trees)==0) { 
          next # random blank entry
        }

        for (ft in 1:nrow(felled_trees)) {
          
          inputs_trees_felled_temp <- tibble(
            year = years[y],
            parcel_name = parcels$parcel_name[p],
            parcel_id = parcels$parcel_id[p],
            species = felled_trees$species[ft],
            other = felled_trees$other[ft],
            tree_number = felled_trees$treeNumber[ft],
            avg_DBH = felled_trees$DBHmean$diameter[ft],
            units = felled_trees$DBHmean$units[ft]
            )
          
          inputs_trees_felled <- bind_rows(inputs_trees_felled, inputs_trees_felled_temp)
        }
      }
    }
  }
  # Replace -9999 values with NA
  inputs_trees_felled$tree_number <- na_if(inputs_trees_felled$tree_number, -9999)
  inputs_trees_felled$avg_DBH <- na_if(inputs_trees_felled$avg_DBH, -9999)
  
  # Remove rows with NA values
  # IF IT HAS NA VALUES, CHECK IF FRONTEND IS WORKING CORRECTLY
  inputs_trees_felled <- inputs_trees_felled[!is.na(inputs_trees_felled$tree_number), ]
  
  # Check if any of the data frames is empty and if yes create minimal data frames.
  # Else select and reorder columns.
  if(nrow(inputs_trees_felled) == 0) {
    inputs_trees_felled <- tibble(year = NA, parcel_name = NA, parcel_id = NA,
                                  species = NA, other = NA, tree_number = NA,
                                  avg_DBH = NA, units = NA)
  }
  
  return(inputs_trees_felled)    
  
}




## The commented function below was written by Jeremie to set a baseline using common practices data.
## Not used because it was decided it introduces too much uncertainty.
## Keeping for potential use
# get_baseline_crop_inputs <- function(landUseSummaryOrPractices, crop_inputs, crop_data, my_logger, farm_EnZ){
#   
#   if (nrow(crop_inputs)==0){ # no crops previously found
#     return(crop_inputs) # so no crop baselines to be created, returned empty
#   }
#   for (i in c(1:length(landUseSummaryOrPractices[[1]]$parcelName))){
#     #ONLY ARABLECROPS LANDUSE TYPE IS CURRENTLY CONSIDERED IN get_crop_inputs
#     #SHOULD BE REFINED IN CASE OF CASH CROPS UNDER AGROFORESTRY
#     # if(landUseSummaryOrPractices[[1]][['year0']]$landUseType[i]=="Agroforestry" |
#     #    landUseSummaryOrPractices[[1]][['year0']]$landUseType[i]=="Forestry" ){
#     #   # We assume that for the above land uses soil cover management baseline is the current state
#     #   crop_inputs <- rbind(crop_inputs,crop_inputs%>%
#     #                          filter(parcel_ID==landUseSummaryOrPractices[[1]]$parcelName[i], scenario=='year0')%>%
#     #                          mutate(scenario='baseline')) # arable crop baseline is based on previous years
#     # }
#     if(landUseSummaryOrPractices[[1]][['year0']]$landUseType[i]=="Arablecrops"){
#       # AT THE MOMENT PERMANENT COVER CROPS ARE ALSO ASSOCIATED TO CEREAL-BASELINE
#       if(landUseSummaryOrPractices[[1]]$year0$applyingThesePracticesInYears[i]==""){
#         log4r::error(my_logger,"No value found for 'applyingThesePracticesInYears'.")
#         stop("No value found for 'applyingThesePracticesInYears'.")
#       } else if (missing_to_zero(landUseSummaryOrPractices[[1]]$year0$applyingThesePracticesInYears[i])>3){
#         # choice that if an arable crop has been run for more than 3 years in a way, this way must be the baseline
#         crop_inputs <- rbind(crop_inputs,crop_inputs%>%
#                                filter(parcel_ID==landUseSummaryOrPractices[[1]]$parcelName[i], scenario=='year0')%>%
#                                mutate(scenario='baseline')) # arable crop baseline is based on previous years
#       } else {
#         # If 3 years or less, assume common practices. Use provided wheat yield data if given. Else take from factors table.
#         if(nrow(crop_inputs %>% filter(crop=='Wheat' | crop=='Winter wheat' | crop=='Spring wheat'))>0){#if we have wheat data from the farmer
#           crop_inputs_temp <- crop_inputs %>% filter(crop=='Wheat' | crop=='Winter wheat' | crop=='Spring wheat') %>%
#             summarize(parcel_ID=landUseSummaryOrPractices[[1]]$parcelName[i], scenario='baseline',
#                       crop = 'Wheat',
#                       harvest=mean(dry_agb_peak)*0.95, fresh = mean(fresh_agb_peak)*0.95,
#                       dry_grazing=0, fresh_grazing=0,
#                       dry_residue=mean(dry_agb_peak)*0.05, fresh_residue=mean(fresh_agb_peak)*0.05, #assumption that only 5% of aboveground biomass  is left-on-site
#                       dry_agb_peak=mean(dry_agb_peak), fresh_agb_peak=mean(fresh_agb_peak))
#           crop_inputs <- rbind(crop_inputs, crop_inputs_temp)
#         } else {
#           # if no wheat yield data is provided by the farmer
#           dry_agb_peak = (crop_data %>% filter(pedo_climatic_area==farm_EnZ))$ag_dm_peak
#           crop_inputs_temp <- data.frame(parcel_ID=landUseSummaryOrPractices[[1]]$parcelName[i], scenario='baseline',
#                                          crop = 'Wheat',
#                                          harvest=mean(dry_agb_peak)*0.95, fresh = 0,
#                                          dry_grazing=0, fresh_grazing=0,
#                                          dry_residue=mean(dry_agb_peak)*0.05, fresh_residue=0, #assumption that only 5% of aboveground biomass is left-on-site
#                                          dry_agb_peak=mean(dry_agb_peak), fresh_agb_peak=0)
#           crop_inputs <- rbind(crop_inputs, crop_inputs_temp)
#         }
#       }
#     }
#   }
#   
#   return(crop_inputs)
# }


get_fertilizer_inputs = function(monitoringData, start_index) {

  # Data frame for fertilzer inputs
  fertilizer_inputs = tibble()
  years <- monitoringData$yearlyFarmData$year
  
  for (i in start_index:length(years)) {
    fertilizer_year <- monitoringData$yearlyFarmData$fertilizerUsage[i]
    fertilizer_inputs_temp <- jsonlite::flatten(as.data.frame(fertilizer_year))
    if (nrow(fertilizer_inputs_temp)==0) {next}  # skip if there is no data
    fertilizer_inputs_temp$year <- years[i]
    fertilizer_inputs <- bind_rows(fertilizer_inputs, fertilizer_inputs_temp)
  }
  # return empty if there is no data
  if (nrow(fertilizer_inputs)==0) {
    fertilizer_inputs <- tibble(amount = NA, unit = NA, percentN = NA, year = NA)
  } else {
    # Clean up the data
    fertilizer_inputs$percentN <- na_if(fertilizer_inputs$percentN, -9999)
    fertilizer_inputs$amount <- na_if(fertilizer_inputs$amount, -9999)
    fertilizer_inputs$units <- if_else(is.na(fertilizer_inputs$units), "tonnes", fertilizer_inputs$units)
    fertilizer_inputs$`X_id.$oid` <- NULL
  } 
  return (fertilizer_inputs)
}
  
  # for (i in c(1:length(landUseSummaryOrPractices[[1]]$parcelName))){
  #   for (j in c(0:10)){
  #     
  #     parcel_id <- landUseSummaryOrPractices[[1]]$parcelName[i]
  #     year_str <- paste0('year',j)
  #     
  #     # Determine the field area depending on input source
  #     use_manual_area <- landUseSummaryOrPractices[[1]]$usingManuallyEnteredArea[i]
  #     if(is.na(use_manual_area) |  is.null(use_manual_area) | !use_manual_area) {
  #       field_area <- missing_to_zero(landUseSummaryOrPractices[[1]]$area[i])/10000 # if no corrected value was provided by the farmer
  #     } else {
  #       field_area <- missing_to_zero(landUseSummaryOrPractices[[1]]$manuallyEnteredArea[i])/10000 # add a verification of consistency here?
  #     }
  #     
  #     year_chosen = landUseSummaryOrPractices[[1]][[year_str]]
  #     
  #     fertilizer_temp <- data.frame(
  #       parcel_ID = parcel_id, 
  #       field_area = field_area,
  #       scenario = year_str,
  #       usage_boolean = year_chosen$syntheticFertilizer$usage[i],
  #       fertilizer_type = "synthetic", # here gathering data from the synthetic fertilizer dashboard entry
  #       quantity_t_ha = ifelse(year_chosen$syntheticFertilizer$usage[i]==TRUE, missing_to_zero(year_chosen$syntheticFertilizer$tonsPerYear[i]),0),
  #       n_content_perc=ifelse(year_chosen$syntheticFertilizer$usage[i]==TRUE, missing_to_zero(year_chosen$syntheticFertilizer$percentOfNitrogen[i]),0)
  #     )
  #     fertilizer_inputs <- rbind(fertilizer_inputs, fertilizer_temp)
  #     
  #     if (j==0){
  #       fertilizer_inputs <- rbind(fertilizer_inputs,data.frame(
  #         parcel_ID = parcel_id, 
  #         field_area = ifelse(is.null(landUseSummaryOrPractices[[1]]$usingManuallyEnteredArea[i]),
  #                             c(missing_to_zero(landUseSummaryOrPractices[[1]]$area[i])/10000),
  #                             ifelse(is.na(landUseSummaryOrPractices[[1]]$usingManuallyEnteredArea[i]) |
  #                                      landUseSummaryOrPractices[[1]]$usingManuallyEnteredArea[i] == FALSE, # means that no corrected value was provided by the farmer
  #                                    c(missing_to_zero(landUseSummaryOrPractices[[1]]$area[i])/10000),
  #                                    c(missing_to_zero(landUseSummaryOrPractices[[1]]$manuallyEnteredArea[i])/10000))), # add a verification of consistence here
  #         scenario = c("baseline"),
  #         usage_boolean = year_chosen$syntheticFertilizer$usage[i],
  #         fertilizer_type = "synthetic", # here gathering data from the synthetic fertilizer dashboard entry
  #         quantity_t_ha = ifelse(year_chosen$syntheticFertilizer$usage[i]==TRUE, missing_to_zero(year_chosen$syntheticFertilizer$tonsPerYear[i]),0),
  #         n_content_perc=ifelse(year_chosen$syntheticFertilizer$usage[i]==TRUE, missing_to_zero(year_chosen$syntheticFertilizer$percentOfNitrogen[i]),0)))
  #     }
  #     last_index = nrow(fertilizer_inputs)
  #     if (fertilizer_inputs$usage_boolean[last_index]==TRUE){
  #       if (fertilizer_inputs$quantity_t_ha[last_index]==0){
  #         list_missing_data = c(list_missing_data,paste(fertilizer_inputs$parcel_ID[last_index],
  #                                                       ' (year',j,"): quantity_t_ha missing", sep=""))
  #       }
  #       if (fertilizer_inputs$n_content_perc[last_index]==0){
  #         list_missing_data = c(list_missing_data,paste(fertilizer_inputs$parcel_ID[last_index],
  #                                                       ' (year',j,"): n_content_perc missing", sep=""))
  #       }
  #     }
  #   }
  # }
  # if (length(list_missing_data)>0){
  #   log4r::error(my_logger, paste('WARNING: Fertilizer data: ',list(list_missing_data),'.', paste=''))
  # }
  # return(fertilizer_inputs)
# }


get_fuel_inputs_direct = function(monitoringData, start_index){
  
  # Data frame for direct fuel inputs
  # Data entry mandatory. That's why is_null request is not necessary.
  years <- monitoringData$yearlyFarmData$year
  
  fuel_inputs_direct <- jsonlite::flatten(as.data.frame(monitoringData$yearlyFarmData$fuelUsage$direct))
  fuel_inputs_direct$year <- years
  
  fuel_inputs_direct <- fuel_inputs_direct %>% rename(
    diesel_amount = diesel.amount,
    diesel_units = diesel.units,
    petrol_amount = petrol.amount,
    petrol_units = petrol.units
  )
  
  # convert -9999 to NA
  fuel_inputs_direct$diesel_amount <- na_if(fuel_inputs_direct$diesel_amount, -9999)
  fuel_inputs_direct$petrol_amount <- na_if(fuel_inputs_direct$petrol_amount, -9999)
  # filter for relevant years
  fuel_inputs_direct <- filter(fuel_inputs_direct, year >= years[start_index])
  
  return(fuel_inputs_direct)
}

get_fuel_inputs_indirect = function(monitoringData, start_index) {
  # Get indirect fuel inputs as data frame
  
  fuel_inputs_indirect <- tibble()
  years <- monitoringData$yearlyFarmData$year
  # For loop to extract fuel inputs
  for (i in start_index:length(years)) {
    fuel_year <- monitoringData$yearlyFarmData$fuelUsage$indirect[i]
    fuel_inputs_indirect_temp <- jsonlite::flatten(as.data.frame(fuel_year))
    if (nrow(fuel_inputs_indirect_temp)==0) {next}
    fuel_inputs_indirect_temp$year <- years[i]
    fuel_inputs_indirect <- bind_rows(fuel_inputs_indirect, fuel_inputs_indirect_temp)
  }
  
  if (nrow(fuel_inputs_indirect)==0) {
    # Create empty data frame if no data
    fuel_inputs_indirect <- tibble(
      year = NA,
      service = NA,
      service_category = NA,
      service_detail = NA,
      area_amount = NA,
      area_units = NA
    )
    
  } else {
    fuel_inputs_indirect$area.amount <- na_if(fuel_inputs_indirect$area.amount, -9999) # not mandatory
    
    # Reassign column names
    fuel_inputs_indirect <- fuel_inputs_indirect %>% rename(
      service_category = serviceCategory,
      service_detail = serviceDetail,
      area_amount = area.amount,
      area_units = area.units
    ) 
    fuel_inputs_indirect$`X_id.$oid` <- NULL
  }
  return(fuel_inputs_indirect)
}

get_landuse_inputs <- function(monitoringData, start_index) {
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  
  inputs_landuse <- tibble()
  
  for (y in start_index:length(years)) {
    
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    
    for (p in 1:nrow(year_parcels)) {
      
      primary <- year_parcels$yearParcelData$primaryLandUse[p]
      secondary <- year_parcels$yearParcelData$secondaryLandUse[p]
      
      inputs_landuse_temp <- tibble(
        year = years[y],
        parcel_name = parcels$parcel_name[p],
        parcel_id = parcels$parcel_id[p],
        primary_landuse = primary,
        secondary_landuse = secondary
      )
      
      inputs_landuse <- bind_rows(inputs_landuse, inputs_landuse_temp)
    }
  }
  
  return(inputs_landuse)
}


get_fixed_farm_inputs <- function(monitoringData, inputs_parcel_fixed) {
  
  farm_area <- as.numeric(monitoringData$farmInfo$farmSize)
  all_parcels_area <- sum(inputs_parcel_fixed$area)
  
  inputs_farm_fixed <- data.frame(farm_id            = monitoringData$farmId,
                                  pars_farmId        = monitoringData$farmInfo$farmId,
                                  email              = monitoringData$email,
                                  farm_id_cf         = ifelse(is.null(monitoringData$uniqueCfFarmId), NA, monitoringData$uniqueCfFarmId),
                                  project_start_year = monitoringData$projectStartYear,
                                  area_farm          = farm_area,
                                  area_parcels       = all_parcels_area)
  return(inputs_farm_fixed)
  
}

get_fixed_parcel_inputs <- function(monitoringData, start_index) {

  # Data frame for fixed parcel inputs
  
  # Extract values
  df_in <- as_tibble(monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues)
  fixed_parcel_inputs <- data.frame(
    parcel_name = df_in$parcelName,
    parcel_id = df_in$parcelID,
    area_geo = df_in$areaGeoFile$area,
    area_geo_units = df_in$areaGeoFile$units,
    area_manual = df_in$areaManualEntry$area,
    area_manual_units = df_in$areaManualEntry$units,
    coordinates = df_in['coordinates']
  )
  
  # Process values
  fixed_parcel_inputs$area_manual <- na_if(fixed_parcel_inputs$area_manual, -9999)
  fixed_parcel_inputs$area_units <- if_else(!is.na(fixed_parcel_inputs$area_manual), fixed_parcel_inputs$area_manual_units, fixed_parcel_inputs$area_geo_units)
  fixed_parcel_inputs$area <- if_else(!is.na(fixed_parcel_inputs$area_manual), fixed_parcel_inputs$area_manual, fixed_parcel_inputs$area_geo)
  fixed_parcel_inputs <- fixed_parcel_inputs %>% rowwise() %>% mutate(lon = get_mean_longitude(coordinates))
  fixed_parcel_inputs <- fixed_parcel_inputs %>% rowwise() %>% mutate(lat = get_mean_latitude(coordinates))
  

  return(fixed_parcel_inputs)
}

## Function extracts baseline and project years based on the project start year
get_periods <- function(monitoringData, project_start_year, start_index) {
  
  if (start_index == 1) {
    year <- sort(monitoringData$yearlyFarmData$year)
  } else {
    year <- sort(monitoringData$yearlyFarmData$year[-(1:(start_index-1))])
  }

  i <- which(year == project_start_year)
  
  # Check: is project start year in the data?
  if (length(i) == 0) {
    log4r::error(my_logger, "Project start year not found in the farm data.")
  }
  
  # Check: are there at least 3 years of data before the project start year?
  if (i < 4) {
    log4r::error(my_logger, "Three baseline years are required but data has less than 3 years before project start year.")
  }
  
  # Create vectors of project years and periods
  year_index <- seq(1:length(year)) - (i - 1)
  year_index[year_index < 1] <- year_index[year_index < 1] - 1  # Sets baseline years to negative values
  periods <- data.frame(
    year = year,
    year_index = year_index,
    period = ifelse(year < project_start_year, "baseline", "project")
  )
  
  # # Filter out past years not used as baseline? Maybe later when calculating baseline values.
  # periods <- periods %>% filter(year >= project_start_year-3)

  return(periods)
}


get_pasture_inputs <- function(monitoringData, start_index){
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  
  inputs_pasture <- tibble()

  for (y in start_index:length(years)) {
    
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    
    for (p in 1:nrow(year_parcels)) {
      
      pasture_i <- 0 # pasture index restarts for each parcel
      
      # Extract perennial crop data
      pastures <- year_parcels$yearParcelData$pastures[[p]]
      
      if(!is.null(pastures)) {
        
        for (pa in 1:nrow(pastures)) {
          
          # Extract pasture data
          pasture <- tibble(
            year = years[y],
            parcel_name = parcels$parcel_name[p],
            parcel_id = parcels$parcel_id[p],
            percent_area = pastures$percentArea[[pa]]
          )

          # Extract peak productivity from pasture
          productivity <- as_tibble(pastures$productivity[[pa]])
          productivity$category <- "productivity"
          productivity$`_id` <- NA
          inputs_pasture_productivity <- bind_cols(pasture, productivity)
          
          # Extract harvest from pasture
          harvest <- as_tibble(pastures$harvest[[pa]])
          harvest$category <- "harvest"
          harvest$`_id` <- NA
          harvest$date <- harvest$date # The database should not allow missing harvest dates.
          inputs_pasture_harvest <- bind_cols(pasture, harvest)
          
          inputs_pasture_temp <- bind_rows(inputs_pasture_productivity, inputs_pasture_harvest)
        
          inputs_pasture <- bind_rows(inputs_pasture, inputs_pasture_temp)
        }
      }
    }
  }
  
  ## Clean up
  # Replace -9999 values with NA
  inputs_pasture$percent_area <- na_if(inputs_pasture$percent_area, -9999)
  inputs_pasture$amount <- na_if(inputs_pasture$amount, -9999)
  
  inputs_pasture$date <- if_else (inputs_pasture$date == "", NA, inputs_pasture$date)
  inputs_pasture$date <- if_else (inputs_pasture$date == "-", NA, inputs_pasture$date)
  inputs_pasture$date <- if_else (inputs_pasture$date == -9999, NA, inputs_pasture$date)
  
  
  # Clean up units
  inputs_pasture$units <- if_else(inputs_pasture$units == "" & inputs_pasture$category == "harvest", "tonnes - fresh", inputs_pasture$units)
  inputs_pasture$units <- if_else(inputs_pasture$units == "" & inputs_pasture$category == "productivity", "kg/m² - fresh", inputs_pasture$units)
  
  # set the dry indicator
  if (nrow(inputs_pasture)>0) {
    inputs_pasture$dry <- ifelse(grepl("dry", inputs_pasture$units), T, F)  # units may be kg/m2 or tonnes
  }
  
  # Remove rows with NA values # IF IT HAS NA VALUES, CHECK IF FRONTEND IS WORKING CORRECTLY
  inputs_pasture <- replace_na(inputs_pasture, list(amount = 0, percent_area = NA))
  
  # Check if any of the data frames is empty and if yes create minimal data frames.
  if(nrow(inputs_pasture) == 0) {
    inputs_pasture <- tibble(year = NA, parcel_name = NA, parcel_id = NA, 
                                  percent_area = NA, category = NA,
                                  date = NA, amount = NA, units = NA, dry = NA
    )
  } else {
    inputs_pasture <- inputs_pasture %>% select(year, parcel_name, parcel_id, 
                                                percent_area, category,
                                                date, amount, units, dry)
  }
    
  # Return the data frame
  return(inputs_pasture)
}

# this function is not used, so commenting out for now
# get_soil_inputs = function(landUseSummaryOrPractices, soilAnalysis, soilMapsData){
#   # takes landUseSummaryOrPractices from farms collection
#   # extracts parcels input dataframe 
#   parcel_names <- landUseSummaryOrPractices[[1]]$parcelName
#   soil_inputs = data.frame(parcel_ID = c(), scenario = c(), clay = c(), irrigation=c())
#   for (i in c(1:length(parcel_names))){
#     for (j in c(0:10)){
#       year_str <- paste0('year', j)
#       soil_inputs <- rbind(soil_inputs,data.frame(
#         parcel_ID = c(parcel_names[i]),
#         scenario = c(year_str),
#         clay = c(get_clay_content(soilAnalysis, soilMapsData)),
#         silt = c(get_silt_content(soilAnalysis, soilMapsData)),
#         SOC = c(get_SOC_content(soilAnalysis, soilMapsData)),
#         bulk_density = c(get_bulk_density(soilAnalysis, soilMapsData)),
#         irrigation = c(ifelse(is.null(landUseSummaryOrPractices[[1]][[year_str]]$irrigation[i]),FALSE,
#                               ifelse(is.na(landUseSummaryOrPractices[[1]][[year_str]]$irrigation[i]),FALSE,
#                                      landUseSummaryOrPractices[[1]][[year_str]]$irrigation[i])))))
#       if (j==0){
#         soil_inputs <- rbind(soil_inputs,data.frame(
#           parcel_ID = c(parcel_names[i]),
#           scenario = c("baseline"),
#           clay = c(get_clay_content(soilAnalysis, soilMapsData)),
#           silt = c(get_silt_content(soilAnalysis, soilMapsData)),
#           SOC = c(get_SOC_content(soilAnalysis, soilMapsData)),
#           bulk_density = c(get_bulk_density(soilAnalysis, soilMapsData)),
#           irrigation = c(ifelse(is.null(landUseSummaryOrPractices[[1]][[year_str]]$irrigation[i]),FALSE,
#                                 ifelse(is.na(landUseSummaryOrPractices[[1]][[year_str]]$irrigation[i]),FALSE,
#                                        landUseSummaryOrPractices[[1]][[year_str]]$irrigation[i])))))
#       }
#     }
#   }
#   return(soil_inputs)
# }

get_tillage_inputs <- function(monitoringData, start_index) {
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  
  inputs_tillage <- tibble()

  for (y in start_index:length(years)) {
    
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    
    for (p in 1:nrow(year_parcels)) {
      
      tillage_events <- year_parcels$yearParcelData$tillage[[p]]
      
      if (!is.null(tillage_events)) {
      
        inputs_tillage_temp <- as_tibble(tillage_events) %>%
          mutate(year = years[y],
                 parcel_name = parcels$parcel_name[p],
                 parcel_id = parcels$parcel_id[p])
        
        inputs_tillage <- bind_rows(inputs_tillage, inputs_tillage_temp)
        
      }
    }
  }
  
  # Remove rows with NA values # IF IT HAS NA VALUES, CHECK IF FRONTEND IS WORKING CORRECTLY
  inputs_tillage <- inputs_tillage[!is.na(inputs_tillage$type), ]
  
  # Create minimal data frame if empty
  # Else reorder columns
  if (nrow(inputs_tillage) == 0) {
    inputs_tillage <- tibble(year = NA, parcel_name = NA, parcel_id = NA, 
                             date = NA, type = NA, percent = NA
    ) 
  } else {
    
    # Replace -9999 values with NA
      inputs_tillage$amount <- na_if(inputs_tillage$percent, -9999)
    
      inputs_tillage <- inputs_tillage %>% 
        select(year, parcel_name, parcel_id, type, date, percent)
  }
  
  return(inputs_tillage)
}

get_irrigation_inputs <- function(monitoringData, start_index) {
  
  # Prepare variables
  years <- monitoringData$yearlyFarmData$year
  
  parcels <- data.frame(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  
  inputs_irrigation <- tibble()

  for (y in start_index:length(years)) {
    
    year_parcels <- monitoringData$yearlyFarmData$parcelLevelData[[y]]
    
    for (p in 1:nrow(year_parcels)) {
      
      irrigation_events <- year_parcels$yearParcelData$irrigation[[p]]
      
      inputs_irrigation_temp <- as_tibble(irrigation_events) %>%
        mutate(year = years[y],
               parcel_name = parcels$parcel_name[p],
               parcel_id = parcels$parcel_id[p])
      
      inputs_irrigation <- bind_rows(inputs_irrigation, inputs_irrigation_temp)
        
    }
  }
  
  # Reorder columns
  # FYI: Irrigation can't be empty, mandatory field and value given for every year, Default: Yes = True
  inputs_irrigation <- inputs_irrigation %>% select(year, parcel_name, parcel_id, irrigation = value)
  
  # convert the format from character to TRUE/FALSE
  inputs_irrigation <- inputs_irrigation %>% 
    mutate(irrigation = ifelse(irrigation == "true", TRUE, FALSE))
  
  return(inputs_irrigation)
}

get_npp_inputs <- function(npp_data, start_index) {
  
  inputs_npp <- as_tibble(npp_data) %>%
    select(-farmId)
  
  return(inputs_npp)
  
}
