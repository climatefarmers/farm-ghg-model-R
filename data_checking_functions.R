#### Functions that conduct various sense-checks on the input data (both raw and processed)
#### Any warning/error messages are written in the logger file
#### Code execution is halted if there are any critical errors

#### Note: There is a utility function right at the bottom of the file
#### It is not executed within the model, but can be used to combine all of the sense check results into an XLSX file

check_all_inputs <- function(inputs_raw, inputs_processed, grazing_error_messages, save_dir, email) {
  ### MAIN FUNCTION -- conduct all tests ###
  
  ## definition of the color codes
  # blue: data suggests potential for negative credits
  # red: data is missing --> we cannot run the model
  # orange: data is invalid --> we can run the model, but the results can't be trusted
  # yellow: data is suspicious --> we can run the model, but the results might not make sense
  
  ## definition of the responsibility
  # science: incorrect data extraction or calculations within the model
  # farmer: missing or invalid data entry
  # platform: potential problem that must be resolved in the dashboard
  # science&farmer: first check by science, then decide if to communicate to the farmer
  # science&farmer (low priority): monthly landuse checks -- not critical for now
  
  log4r::info(my_logger, "Running sense checks.")
  
  all_years <- unique(inputs_raw$inputs_fertilizer$year)
  all_parcels <- inputs_raw$inputs_parcel_fixed$parcel_name
  
  test_pass <- list()
  test_pass$process_grazing_function <- grazing_error_messages
  test_pass$farm_fixed <- check_farm_fixed(inputs_raw$inputs_farm_fixed)
  test_pass$parcel_fixed <- check_parcel_fixed(inputs_raw$inputs_parcel_fixed)
  test_pass$direct_fuel <- check_direct_fuel(inputs_processed$inputs_fuel_direct)
  test_pass$fuel_indirect <- check_indirect_fuel(inputs_processed$inputs_fuel_indirect)
  test_pass$fertilizer <- check_fertilizer(inputs_processed$inputs_fertilizer, inputs_processed$inputs_parcel_fixed, factors$factors_fertilizer)
  test_pass$landuse <- check_landuse(inputs_processed$inputs_landuse, inputs_raw$inputs_perennialcrops, all_years, all_parcels)
  test_pass$annualcrops <- check_annualcrops(inputs_raw$inputs_annualcrops, inputs_processed$inputs_annualcrops)
  test_pass$pasture <- check_pasture(inputs_raw$inputs_pasture, inputs_processed$inputs_pasture)
  test_pass$org_amendments <- check_org_amendments(inputs_raw$inputs_organicmatter, inputs_processed$inputs_organicmatter)
  test_pass$fodder <- check_fodder(inputs_processed$inputs_fodder)
  test_pass$livestock <- check_livestock(inputs_processed$inputs_livestock)
  test_pass$productivity <- check_productivity(inputs_processed$inputs_productivity)
  test_pass$irrigation <- check_irrigation(inputs_processed$inputs_irrigation)
  test_pass$grazing_parcels <- check_grazing_parcels(inputs_processed$inputs_grazing_parcels)
  test_pass$grazing_cover <- check_grazing_cover(inputs_processed$inputs_grazing_cover)
  test_pass$trees <- check_trees(inputs_raw$inputs_perennialcrops, inputs_processed$inputs_perennials)
  test_pass$baresoil <- check_baresoil(inputs_raw$inputs_baresoil)
  
  ## non-implemented tests
  # tillage --> no tests necessary?
  # livestock_parcels --> no tests necessary?
  
  # extract and format the messages from the list
  df <- data.frame('input'=character(), 'code'=character(), 
                   'responsibility'=character(), 
                   'message'=character(), 'details'=character(), stringsAsFactors=F)
  for (i in 1:length(test_pass)) {
    for (j in 1:length(test_pass[[i]])) {
      text_i <- str_split(test_pass[[i]][j], pattern=" \\|\\| ")[[1]]
      df <- rbind(df, data.frame('input'=names(test_pass)[i], 
                                 'code'=text_i[1], 
                                 'responsibility'=text_i[2],
                                 'message'=text_i[3],
                                 'details'=text_i[4]))
    }
  }
  
  # sort by severity (red->orange->yellow)
  df <- df %>% mutate(code = factor(code, levels=c("blue", "red", "orange", "yellow")))
  df <- df[,c("responsibility", "code", "input", "message", "details")] %>%
    arrange(code, responsibility, input)
  
  # write to file and return
  log4r::info(my_logger, paste0("Sense checks completed. The following data frames have issues:",
                                "\n", paste0(unique(df$input), collapse=", "), "\n"))

  write.csv(df, paste0(save_dir, "/", email, ".csv"), row.names=F)
  log4r::info(my_logger, "Sense checks completed.")
  
  return(df)
}


## ----- individual checking functions ----- ##

check_annualcrops <- function(raw, processed, messages=c()) {
  # first, filter to valid crops (it is possible we have a blank data frame with single NA row)
  processed <- processed %>% filter(!is.na(parcel_name)) %>% filter(!is.na(species))

  # Check if start_date is missing
  if (any(is.na(processed$start_date))) {
    msg <- "red || farmer || Start date is missing for the following annual crops. || "
    msg2 <- paste0("\n Year: ", processed$year[is.na(processed$start_date)], 
                   " | Parcel: ", processed$parcel_name[is.na(processed$start_date)],
                   " | Species: ", processed$species[is.na(processed$start_date)])
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
    # Remove crops without start date from the database
    # inputs_annual_crops <- inputs_annual_crops %>% filter(!is.na(start_date))
  }
  
  # check end dates
  if (any(is.na(processed$end_date))) {
    msg <- "red || farmer || End date is missing for the following annual crops. || "
    msg2 <- paste0("\n Year: ", processed$year[is.na(processed$end_date)], 
                   " | Parcel: ", processed$parcel_name[is.na(processed$end_date)],
                   " | Species: ", processed$species[is.na(processed$end_date)])
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
    # Remove crops without end date from the database
    # inputs_annual_crops <- inputs_annual_crops %>% filter(!is.na(end_date))
  }
  
  # has any harvest/residue information been entered at all?
  sum_harvest <- sum(processed$harvest_t_dry)
  sum_residue <- sum(processed$residue_t_dry)
  if (sum_harvest == 0 & sum_residue == 0) {
    msg <- "yellow || farmer || No harvest or residue information has been entered for any annual crops."
    log4r::warn(my_logger, msg)
    messages <- c(messages, msg)
  }
  
  # flag "other" crops that have been entered
  others <- raw %>% filter(other != "") %>%
    select(species, other) %>%
    distinct()
  if (nrow(others) > 0) {
    msg <- "orange || science || Note: The following 'other' annual crops have been entered. || "
    msg2 <- paste0("\n Year: ", others$year, 
                   " | Parcel: ", others$parcel_name,
                   " | Species: ", others$species, 
                   " |  Other: ", others$other)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # maximum values
  processed <- processed %>% mutate(
    above_max = (harvest_t_ha_dry + residue_t_ha_dry) > (max_prod_ag_t_ha_fresh * dry)
    )
  above_max <- processed %>% filter(above_max)
  if (nrow(above_max)>0) {
    msg <- "orange || farmer || The following annual crops have (harvest + residue) exceeding the maximum production amount. || "
    msg2 <- paste0(
      "\n Year: ", above_max$year, 
      " | Parcel: ", above_max$parcel_name, 
      " | Species: ", above_max$species,
      ' | Harvest + Residue: ', above_max$harvest_t_ha_dry + above_max$residue_t_ha_dry,
      ' (Max: ', above_max$max_prod_ag_t_ha_fresh, ')')
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_pasture <- function(raw, processed, messages=c()) {
  # is percent_area>0 and productivity ("amount") 0?
  zero_prod <- raw %>% filter(category=="productivity", percent_area > 0) %>%
    group_by(parcel_name, year) %>% 
    summarise(amount = sum(amount), .groups='drop') %>%
    filter(amount == 0) %>%
    group_by(parcel_name) %>%
    summarise(years = paste0(unique(year), collapse=','))
  if (nrow(zero_prod)>0) {
    msg <- "orange || farmer || Productivity is 0 for the following pastures. Check data entry. || "
    msg2 <- paste0("\n Parcel: ", zero_prod$parcel_name, " | Year(s): ", zero_prod$years)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # harvest should not be greater than the total productivity
  over_harvest <- which(processed$harvest_t_ha_dry > processed$productivity_t_ha_dry)
  if (length(over_harvest) > 0) {
    msg <- paste0("orange || farmer || Harvest is greater than the productivity for the following pastures. Check data entry.",
                  "\nNote the values in this error message are calculated based on parcel area and differ from the directly entered values || ")
    msg2 <- paste0(
      "\n Parcel: ", processed$parcel_name[over_harvest], 
      " | Year: ", processed$year[over_harvest],
      ' | harvest_t_ha_dry: ', processed$harvest_t_ha_dry[over_harvest],
      ' | productivity_t_ha_dry: ', processed$productivity_t_ha_dry[over_harvest])
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # productivity should be smaller than the maximum possible
  processed <- processed %>% mutate(
    above_max_prod = productivity_t_ha_dry > (max_prod_ag_t_ha_fresh * dry),
    above_max_harvest = harvest_t_ha_dry > (max_prod_ag_t_ha_fresh * dry)
  )
  above_max_prod <- processed %>% filter(above_max_prod)
  above_max_harvest <- processed %>% filter(above_max_harvest)
  if (nrow(above_max_prod)>0) {
    msg <- "orange || farmer || The following pastures have productivity exceeding the maximum production amount. || "
    msg2 <- paste0(
      "\n Parcel: ", above_max_prod$parcel_name, 
      " | Year: ", above_max_prod$year, 
      " | Productivity: ", above_max_prod$productivity_t_ha_dry, 
      ' (Max: ', above_max_prod$max_prod_ag_t_ha_fresh, ')')
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  if (nrow(above_max_harvest)>0) {
    msg <- "orange || farmer || The following pastures have harvest exceeding the maximum production amount. || "
    msg2 <- paste0(
      "\n Parcel: ", above_max_harvest$parcel_name, 
      " | Year: ", above_max_harvest$year, 
      " | Harvest: ", above_max_harvest$harvest_t_ha_dry, 
      ' (Max: ', above_max_harvest$max_prod_ag_t_ha_fresh, ')')
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check how many productivity events have been reported in each year, and whether it is consistent
  n_per_year <- raw %>%
    filter(category=="productivity", amount>0) %>%
    group_by(parcel_name, year) %>%
    summarise(n=n(), .groups='drop') %>%
    group_by(parcel_name) %>%
    summarise(n_prod_events = paste0(n, collapse=','),
              uniques = length(unique(n)), .groups='drop') %>%
    filter(uniques > 1)
  if (nrow(n_per_year)>0) {
    msg <- "yellow || farmer || The number of productivity events is inconsistent across years for the following pastures. || "
    msg2 <- paste0("\n Parcel: ", n_per_year$parcel_name, " | Number of events: ", n_per_year$n_prod_events)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # yellow check: is only one productivity event reported?
  single_years <- raw %>%
    filter(category=="productivity", percent_area>0) %>%
    group_by(parcel_name, year) %>%
    summarise(n=n(), .groups='drop') %>%
    filter(n==1) %>%
    group_by(parcel_name) %>%
    summarise(years=paste0(year, collapse=','))
  if (nrow(single_years)>0) {
    msg <- "yellow || farmer || Only one productivity event is reported for the following pastures. || "
    msg2 <- paste0("\n Parcel: ", single_years$parcel_name, " | Year(s): ", single_years$years)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_grazing_parcels <- function(processed, messages=c()) {
  # check for negative calculated values
  neg <- processed %>% filter(forage_total_t_dry<0 | forage_eaten_t_dry<0 | forage_residue_t_dry<0 | 
                                fodder_eaten_t_dry<0 | grazing_t_dry<0)
  if (nrow(neg) > 0) {
    msg <- "red || science || Negative values have been calculated for grazing parcels. || "
    msg2 <- paste0('\n Year: ', neg$year, 
                   ' | Parcel: ', neg$parcel_name, 
                   ' | Forage Total: ', neg$forage_total_t_dry, 
                   ' | Forage Eaten: ', neg$forage_eaten_t_dry, 
                   ' | Forage Residue: ', neg$forage_residue_t_dry, 
                   ' | Fodder Eaten: ', neg$fodder_eaten_t_dry, 
                   ' | Grazing: ', neg$grazing_t_dry)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check that the grazing_frac sums to one each year
  grazing_frac <- processed %>% group_by(year) %>%
    summarise(sum = sum(grazing_frac)) %>%
    filter(sum < 0.99 | sum > 1.01)  # allow for some tolerance
  if (nrow(grazing_frac) > 0) {
    msg <- "red || science || The sum of the grazing fractions is not 1 for the following years. || "
    msg2 <- paste0('\n Year: ', grazing_frac$year,
                   ' | Total fraction: ', grazing_frac$sum)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages) 
}


check_grazing_cover <- function(processed, messages=c()) {
  # check if the forage_pot_frac sums to one each year
  yearly <- processed %>% group_by(year) %>%
    summarise(sum = sum(forage_pot_frac)) %>%
    filter(sum < 0.99 | sum > 1.01)  # allow for some tolerance
  if (nrow(yearly) > 0) {
    msg <- "red || science || The sum of the forage potential fractions is not 1 for the following years. || "
    msg2 <- paste0('\n Year: ', yearly$year,
                   ' | Total fraction: ', yearly$sum)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_landuse <- function(inputs_landuse, inputs_trees, all_years, all_parcels, messages=c()) {
  
  # special check: all landuse data is missing --> RETURN here
  if (all(is.na(inputs_landuse$landuse))) {
    msg <- "red || farmer || No land cover (crops, pastures, fallow, bare soil, or trees) is reported in any parcel or year! || "
    log4r::error(my_logger, msg)
    messages <- c(messages, msg)
    return(messages)
  }
  
  # Check if percent_area is 0 and there is harvest or residue
  if (any(inputs_landuse$percent_area == 0)) {
    
    lu_error <- inputs_landuse %>% 
      filter(percent_area == 0, harvest_t_ha_dry>0 | residue_t_ha_dry>0) %>%
      group_by(parcel_name) %>%
      summarise(missing=paste0(unique(year), collapse=','))
    
    if (nrow(lu_error)>0) {
      msg <- paste0("yellow || farmer || Land cover (annual crops, productive fallow, or trees) with non-zero harvest and/or residue are reported in the same year as 100% pasture.",
      " Does the pasture really take up 100% of the area? || ")
      msg2 <- paste0("\n Parcel: ", lu_error$parcel_name, " | Year(s): ", lu_error$missing)
      log4r::error(my_logger, msg)
      log4r::info(my_logger, msg2)
      messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
    }
    
  }
  
  check1 <- inputs_landuse$percent_area < 0
  check2 <- inputs_landuse$percent_area_check > 100
  if (any(check1, na.rm=T) | any(check2, na.rm=T)) {
    rowz <- check1 | check2
    rowz[is.na(rowz)] <- FALSE
    msg <- "orange || science || The sum of the stated area percentages for pasture and bare soil is greater than 100 %. Check land cover information (pasture and fallow periods) for the following years and parcels. || "
    msg2 <- paste0("\n Year: ", inputs_landuse$year[rowz], 
                   " | Parcel: ", inputs_landuse$parcel_name[rowz])
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # Check if land use information is available for all parcels, all years
  lu_all <- inputs_landuse %>%
    # expand to include all parcels and years
    expand(parcel_name = all_parcels, year = all_years)
  lu_parcelyear <- inputs_landuse %>% 
    filter(!is.na(landuse)) %>%
    group_by(parcel_name, year) %>% 
    summarise(n=n(), .groups='drop')
  lu_check <- left_join(lu_all, lu_parcelyear, by=c("parcel_name", "year")) %>%
    filter(is.na(n)) %>%
    group_by(parcel_name) %>% 
    summarise(years=paste0(year, collapse=','))
  if(nrow(lu_check)>0) {
    msg <- "red || farmer || No land cover information in the 'Crops' page is reported for the following years and parcels. Please add information about perennials, annual crops, pasture, or fallow. || "
    msg2 <- paste0("\n Parcel: ", lu_check$parcel_name, " | Year(s): ", lu_check$years)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  ## check that there is appropriate land-use indicated each month
  # given the start_date and end_date, create a new dataframe that shows the unique land-uses for each month/parcel combination
  first_date <- as.Date(paste0(min(inputs_landuse$year), '-01-01'))  # some crops might carry over from the previous year (we want to exclude this)
  all_months <- seq.Date(first_date, max(inputs_landuse$end_date, na.rm=T), by = "month")
  monthly <- inputs_landuse
  monthly[,as.character(all_months)] <- 0
  monthly_TF <- inputs_landuse
  monthly_TF[,as.character(all_months)] <- FALSE
  # set each month to true if it lies between start_date and end_date
  for (i in 1:nrow(inputs_landuse)) {
    if (is.na(inputs_landuse$landuse[i])) {
      # already checked above
      next
    }
    if (is.na(inputs_landuse$start_date[i]) | is.na(inputs_landuse$end_date[i])) {
      # already checked in the annualcrops section
      next
    }
    col_matches <- as.character(seq.Date(inputs_landuse$start_date[i], inputs_landuse$end_date[i], by = "month"))
    col_matches <- col_matches[col_matches >= first_date]
    monthly[i, col_matches] <- inputs_landuse$percent_area[i]
    monthly_TF[i, col_matches] <- TRUE
  }
  ## calculate the sum of area percentages in each month
  monthly_perc_sum <- monthly %>% group_by(parcel_name) %>%
    summarise_at(vars(as.character(all_months)), sum)
  # print the month/parcel combinations that do not add to 100%
  if (!all(monthly_perc_sum[,-1] == 100)) {
    msg <- "yellow || science&farmer (low priority) || The sum of the landuse area percentages is not 100 % for the following months and parcels. || "
    log4r::warn(my_logger, msg)
    message <- ""
    for (i in 1:nrow(monthly_perc_sum)) {
      if (!all(monthly_perc_sum[i, -1] == 100)) {
        missing_months <- all_months[which(monthly_perc_sum[i,as.character(all_months)] != 100)]
        # turn into a data frame with columns showing year / month
        missing_df <- data.frame(year=year(missing_months), month=month(missing_months))
        # group by year and summarise months
        missing_df <- missing_df %>% group_by(year) %>%
          summarise(months = paste0(month, collapse=',')) %>% 
          mutate(months = if_else(months == "1,2,3,4,5,6,7,8,9,10,11,12", "all months", months)) %>%
          mutate(string = paste0(year, ' (', months, ')'))
        print_text <- paste0(missing_df$string, collapse=', ')
        # add to the message
        message <- paste0(message, paste0("\n Parcel: ", monthly_perc_sum$parcel_name[i], " | ", print_text))
      }
    }
    log4r::info(my_logger, message)
    messages <- c(messages, paste0(msg, message))
  }
  
  ## check that there is a land-use for each month
  monthly_has_LU <- monthly_TF %>% group_by(parcel_name) %>%
    summarise_at(vars(as.character(all_months)), any)
  # print the month/parcel combinations that do not have a land-use
  if (!all(monthly_has_LU[,-1])) {
    msg <- "yellow || science&farmer (low priority) || There is no landuse in the following months and parcels. || "
    log4r::warn(my_logger, msg)
    message <- ""
    for (i in 1:nrow(monthly_has_LU)) {
      if (!all(monthly_has_LU[i, -1])) {
        missing_months <- all_months[which(monthly_has_LU[i,as.character(all_months)]==F)]
        # turn into a data frame with columns showing year / month
        missing_df <- data.frame(year=year(missing_months), month=month(missing_months))
        # group by year and summarise months
        missing_df <- missing_df %>% group_by(year) %>%
          summarise(months = paste0(month, collapse=',')) %>% 
          mutate(months = if_else(months == "1,2,3,4,5,6,7,8,9,10,11,12", "all months", months)) %>%
          mutate(string = paste0(year, ' (', months, ')'))
        print_text <- paste0(missing_df$string, collapse=', ')
        # add to the message
        message <- paste0(message, paste0("\n Parcel: ", monthly_has_LU$parcel_name[i], " | ", print_text))
      }
    }
    log4r::info(my_logger, message)
    messages <- c(messages, paste0(msg, message))
  }
  
  return(messages)
}


check_farm_fixed <- function(raw, messages=c()) {
  
  if (is.na(raw$area_farm)) {
    
    msg <- "red || farmer || Farm area is missing. || "
    log4r::error(my_logger, msg)
    messages <- c(messages, msg)
    return(messages)
    
  } else if (raw$area_farm < raw$area_parcels) {
    
    perc_overshoot <- (raw$area_parcels - raw$area_farm) / raw$area_farm * 100
    ha_overshoot <- raw$area_parcels - raw$area_farm
    
    if (perc_overshoot>10) {
      # red error -- maybe need to contact farmer
      msg <- "orange || farmer || The sum of the parcel areas is more than 10% larger than the area of the farm. Check data entry. || "
      msg2 <- paste0("Sum of parcel areas: ", raw$area_parcels, " ha | Farm area: ", raw$area_farm, " ha")
      log4r::error(my_logger, msg)
      log4r::info(my_logger, msg2)
      messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
    } else {
      # yellow error -- just a warning
      msg <- "yellow || farmer || The sum of the parcel areas is greater than the area of the farm. However, the difference is only minor. || "
      msg2 <- paste0("Sum of parcel areas: ", raw$area_parcels, " ha | Farm area: ", raw$area_farm, " ha")
      log4r::error(my_logger, msg)
      log4r::info(my_logger, msg2)
      messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
    }

    
  }
  
  return(messages)
}

check_parcel_fixed <- function(inputs_parcel_fixed, messages=c()) {
  # units
  if(any(inputs_parcel_fixed$area_units != "HA")) {
    msg <- "orange || science || Area units are not in hectares. Expected hectares. || "
    log4r::error(my_logger, msg)
    messages <- c(messages, msg)
  }
  # non-negative
  if (any(inputs_parcel_fixed$area <= 0)) {
    msg <- "red || platform || Parcel area is 0 or negative. || "
    msg2 <- paste0("Parcel(s): ", paste0(inputs_parcel_fixed$parcel_name[inputs_parcel_fixed$area <= 0], collapse=', '))
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  return(messages)
}


check_direct_fuel <- function(inputs_fuel_direct, messages=c()) {
  # Check units
  if (any(c(inputs_fuel_direct$petrol_units, inputs_fuel_direct$diesel_units) != "litres")){
    msg <- "orange || science || Fuel units for direct fuel are not all in litres. Expected litres. || "
    log4r::error(my_logger, msg)
    messages <- c(messages, msg)
  }
  
  # check for NA / missing values
  if (any(is.na(inputs_fuel_direct$petrol_amount)) | any(is.na(inputs_fuel_direct$diesel_amount))) {
    msg <- "red || farmer || Some direct fuel data is missing. || "
    log4r::error(my_logger, msg)
    missing <- inputs_fuel_direct %>% filter(is.na(petrol_amount) | is.na(diesel_amount))
    msg2 <- paste0('\n Year(s): ', paste0(unique(missing$year), collapse=', '))
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, msg2))
    
  } else {
    # Data sense checking -- only possible with complete data
    
    # Check if baseline and project data make sense
    bl_avg_data <- inputs_fuel_direct %>% filter(period == "baseline") %>%
      summarise(bl_avg_petrol = mean(petrol_amount), bl_avg_diesel = mean(diesel_amount))
    
    pr_avg_data <- inputs_fuel_direct %>% filter(period == "project") %>%
      summarise(pr_avg_petrol = mean(petrol_amount), pr_avg_diesel = mean(diesel_amount))
    
    if ((bl_avg_data$bl_avg_petrol == 0 & pr_avg_data$pr_avg_petrol > 0) | (bl_avg_data$bl_avg_diesel == 0 & pr_avg_data$pr_avg_diesel > 0)) {
      msg <- "blue || farmer || Direct fuel consumption is 0 in the baseline and larger than 0 in the project period. Potential for negative credits!! || "
      log4r::warn(my_logger, msg)
      messages <- c(messages, msg)
    }
    if (bl_avg_data$bl_avg_petrol < pr_avg_data$pr_avg_petrol | bl_avg_data$bl_avg_diesel < pr_avg_data$pr_avg_diesel) {
      msg <- "blue || farmer || Average direct fuel consumption (excluding indirect services) in the project period is higher than in the baseline period. Potential for negative credits!! || "
      log4r::warn(my_logger, msg)
      messages <- c(messages, msg)
    }
  }
  
  return(messages)
}


check_indirect_fuel <- function(processed, messages=c()) {
  # check that the matches worked in the processing function, where appropriate
  unmatched <- processed %>% filter(!is.na(area_amount) & is.na(diesel_l_ha))
  if (nrow(unmatched) > 0) {
    msg <- "red || farmer || The following indirect fuel data have not been matched with the factors table. || "
    msg2 <- paste0('\n Year: ', unmatched$year, 
                   ' | Service: ', unmatched$service, 
                   ' | Service detail: ', unmatched$service_detail)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  return(messages)
}


check_fertilizer <- function(processed, parcel_fixed, factors, messages=c()) {
  # Check units
  if (any(processed$units[!is.na(processed$amount)] != 'tonnes')) {
    msg <- "orange || science || Fertilizer amount units are not in tonnes. || "
    log4r::error(my_logger, msg)
    messages <- c(messages, msg)
  }
  
  # see if the data are complete
  missing <- processed %>% filter(is.na(amount))
  if (nrow(missing) > 0) {
    msg <- "red || farmer || Fertilizer data is missing. || "
    msg2 <- paste0('\n Year(s): ', paste0(unique(missing$year), collapse=', '))
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, msg2))
  }
  
  # check maximum values (all reported fertilizer in this table is synthetic)
  total_area <- sum(parcel_fixed$area)
  max_t_ha_synthetic <- factors %>% filter(fertilizer_type=='synthetic') %>% pull(max_t_ha)
  above_max <- processed %>% mutate(rate=amount/total_area) %>% 
    filter(rate>max_t_ha_synthetic)
  if (nrow(above_max)>0) {
    msg <- "yellow || farmer || Fertilizer rate(s) exceed the maximum allowed. || "
    msg2 <- paste0("Year(s): ", paste0(unique(above_max$year), collapse=', '))
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, msg2))
  }
  
  # is baseline higher than project?
  diff <- processed %>%
    group_by(period) %>%
    summarise(avg_amount = mean(amount, na.rm=T)) %>%
    pivot_wider(names_from = period, values_from = avg_amount, values_fill=0)
  if (ncol(diff)==2) {
    if ((!is.na(diff$project)) & (!is.na(diff$baseline))) {
      if (diff$project > diff$baseline) {
        msg <- "blue || farmer || Average fertilizer amount in the project period is higher than in the baseline period. Potential negative credits!! || "
        log4r::warn(my_logger, msg)
        messages <- c(messages, msg)
      }
    }
  }
  return(messages)
}


check_org_amendments <- function(raw, processed, messages=c()) {
  # check for instances of "other"
  others <- raw %>% filter(other != "") %>%
    select(type, sub_type, other) %>%
    distinct()
  if (nrow(others)>0) {
    msg <- "red || science || Note: The following 'other' organic amendments have been entered. || "
    msg2 <- paste0("\n Type: ", others$type, " | Sub-type: ", others$sub_type, " | Other: ", others$other)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check that all reported inputs have matched with the factors table
  no_match <- processed %>% filter(amount_t>0, is.na(carbon_content_fresh))
  if (nrow(no_match)>0) {
    msg <- "red || science || The following organic amendments have not been matched with the factors table. || "
    msg2 <- paste0('\n Type: ', no_match$type, ' | Sub-type: ', no_match$sub_type)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
    pass <- F
  }
  
  # check that all units are in tonnes, which is assumed in the soil model
  if (any(processed$units[processed$amount_t > 0] != 'tonnes')) {
    msg <- "orange || science || Organic amendment amount units are not all in tonnes. || "
    log4r::error(my_logger, msg)
    messages <- c(messages, msg)
  }
  
  # check the amounts against the maximum limits
  above_max <- processed %>% filter(amount_t_ha>max_t_ha, amount_t_ha>0)
  if (nrow(above_max) > 0) {
    msg <- "orange || farmer || Organic amendment amount(s) exceed the maximum allowed. || "
    msg2 <- paste0("\n Type: ", above_max$type, 
                   " | Sub-type: ", above_max$sub_type, 
                   " | Amount: ", above_max$amount_t_ha, 
                   " (Max: ", above_max$max_t_ha, ")")
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_fodder <- function(processed, messages=c()) {
  # check amount against maximum limit
  over_max <- processed %>% 
    mutate(fodder_t_ha = fodder_t_ha_dry / dry) %>%
    filter(fodder_t_ha > max_t_ha)
  if (nrow(over_max) > 0) {
    msg <- "orange || farmer || Fodder amount(s) exceed the maximum allowed. || "
    msg2 <- paste0('\n Year: ', over_max$year, 
                   ' | Parcel: ', over_max$parcel_name, 
                   ' | Fodder: ', over_max$fodder_t_ha, 
                   ' (Max: ', over_max$max_t_ha, ')')
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check for negative values
  neg <- processed %>%
    mutate(has_negative = fodder_t_dry<0 | fodder_available_t_dry<0 | 
             fodder_eaten_t_dry<0 | fodder_residue_t_dry<0) %>%
    filter(has_negative)
  if (nrow(neg) > 0) {
    msg <- "red || science || Negative values have been calculated for fodder. Issue with grazing algorithm? || "
    msg2 <- paste0('\n Year: ', neg$year, 
                   ' | Parcel: ', neg$parcel_name, 
                   ' | Fodder: ', neg$fodder_t_dry, 
                   ' | Available: ', neg$fodder_available_t_dry, 
                   ' | Eaten: ', neg$fodder_eaten_t_dry, 
                   ' | Residue: ', neg$fodder_residue_t_dry)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_livestock <- function(processed, factors, messages=c()) {
  # check maximum against limit
  above_max <- processed %>% filter(amount > max_head_per_farm)
  if (nrow(above_max) > 0) {
    msg <- "yellow || farmer || Livestock amount(s) exceed the maximum allowed. || "
    msg2 <- paste0('\n Year: ', above_max$year, 
                   ' | Species: ', above_max$species,
                   ' | Category: ', above_max$category,
                   ' | Amount: ', above_max$amount,
                   ' (Max: ', above_max$max_head_per_farm, ')')
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check that all matched with factors
  unmatched <- processed %>% filter(is.na(mass_kg_per_animal))
  if (nrow(unmatched) > 0) {
    msg <- "red || science || The following livestock have not been matched with the factors table. || "
    msg2 <- paste0('\n Species: ', unmatched$species, 
                   ' | Category: ', unmatched$category)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check stability of farm-level total livestock counts
  # test whether it changed more than 50% in a single year
  annual <- processed %>% 
    group_by(year) %>%
    summarise(total = sum(amount)) %>%
    mutate(diff = total - lag(total),
           perc_diff = diff / lag(total) * 100)
  unstable <- annual %>%
    filter(abs(perc_diff) > 50)
  if (nrow(unstable) > 0) {
    msg <- "yellow || farmer || The total livestock count changed by more than 50% in a single year. || "
    msg2 <- paste0('\n Year: ', unstable$year,
                   ' | Total: ', unstable$total,
                   ' | Previous year: ', unstable$total - unstable$diff)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # are there zero livestock in some years and livestock in others?
  has_zero <- any(annual$total == 0)
  has_nonzero <- any(annual$total > 0)
  if (has_zero & has_nonzero) {
    msg <- "yellow || farmer || There are years with zero livestock and years with livestock. || "
    log4r::warn(my_logger, msg)
    messages <- c(messages, msg)
  }
  
  # flag if there is an increase from baseline to project in livestock numbers (total and species)
  # (the livestock table is complete even with zero values, so this calculation is valid)
  species <- processed %>% 
    group_by(period, species) %>%
    summarise(total = mean(amount), .groups='drop') %>%
    pivot_wider(names_from=period, values_from=total, values_fill=0)
  species <- species %>%
    bind_rows(data.frame('species'='TOTAL', 
                        'baseline'=sum(species$baseline),
                        'project'=sum(species$project))) %>%
    mutate(diff=project-baseline) %>%
    filter(diff > 0)
  if (nrow(species) > 0) {
    msg <- paste0("blue || farmer || The following livestock numbers (summed over all sub-categories) have increased from baseline to project.",
                    " Potential for negative credits!! || ")
    msg2 <- paste0('\n Species: ', species$species, 
                   ' | Baseline (avg.): ', round(species$baseline, 1), 
                   ' | Project (avg.): ', round(species$project, 1))
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_productivity <- function(processed, messages=c()) {
  # forage total/eaten/residue are non-negative
  neg <- processed %>%
    filter(forage_total_t_dry < 0 | forage_eaten_t_dry < 0 | forage_residue_t_dry < 0)
  if (nrow(neg) > 0) {
    msg <- "red || science || Negative values have been calculated for forage. Issue with grazing algorithm? || "
    msg2 <- paste0('\n Year: ', neg$year, 
                   ' | Parcel: ', neg$parcel_name, 
                   ' | Total: ', neg$forage_total_t_dry, 
                   ' | Eaten: ', neg$forage_eaten_t_dry, 
                   ' | Residue: ', neg$forage_residue_t_dry)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_irrigation <- function(processed, messages=c()) {
  # check that all years have been filled for all parcels
  nas <- processed %>% 
    filter(is.na(irrigation)) %>%
    group_by(parcel_name) %>% 
    summarise(missing=paste0(year, collapse=','))
  if (nrow(nas) > 0) {
    msg <- "red || farmer || Irrigation information is missing for the following years and parcels. || "
    msg2 <- paste0("\n Parcel: ", nas$parcel_name, " | Year(s): ", nas$missing)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check for inconsistent irrigation for particular parcels between years
  # this is only a warning
  year_avgs <- processed %>% 
    mutate(irrigation = if_else(irrigation==TRUE, 1, 0)) %>%
    group_by(parcel_name) %>%
    summarise(avg = mean(irrigation, na.rm=T))
  mixed_irrig <- year_avgs %>% filter(avg > 0 & avg < 1)
  if (nrow(mixed_irrig) > 0) {
    msg <- "yellow || farmer || Irrigation practices changed between years for the following parcels. || "
    msg2 <- paste0("\n Parcel: ", mixed_irrig$parcel_name)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  return(messages)
}


check_trees <- function(raw, processed, messages=c()) {
  
  # remove invalid trees
  raw <- raw %>% filter(!is.na(species))
  
  if (all(is.na(raw$year))) {
    return(messages) # no trees
  }
  
  # data completeness check: tree amounts
  missing_number <- raw %>% 
    filter(is.na(tree_number)) %>%
    # no number for bushes/shrubs or vines or Herbaceous perennials
    filter(!grepl("bushes/shrubs", species, ignore.case=T)) %>%
    filter(!grepl("vine - ", species, ignore.case=T)) %>%
    filter(!grepl("herbaceous perennials", species, ignore.case=T)) %>%
    # combine the "other" type
    mutate(other = if_else(is.na(other),"",paste0(" - ", other)),
      species_comb = paste0(species, other)) %>%
    group_by(parcel_name, species_comb) %>%
    # summarise the missing years
    summarise(years = paste0(year, collapse=','), .groups="drop")
  if (nrow(missing_number) > 0) {
    msg <- "red || farmer || Some trees are missing tree number values. || "
    msg2 <- paste0("\n Parcel: ", missing_number$parcel_name,
                   " | Species: ", missing_number$species_comb, 
                   " | Year: ", missing_number$years)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  ### other checks ###
  # check for "other" trees
  others <- raw %>% filter(other != "") %>%
    select(parcel_name, species, other) %>%
    distinct()
  if (nrow(others)>0) {
    msg <- "orange || science || Note: The following 'other' trees have been entered. || "
    msg2 <- paste0("\n Parcel: ", others$parcel_name, 
                   " | Species: ", others$species, 
                   " | Other: ", others$other)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  
  ### tests based on the DBH values ###
  
  # raise warning if any DBH > 100
  big_trees <- raw %>% 
    filter(avg_DBH > 100)
  if (nrow(big_trees) > 0) {
    msg <- "yellow || farmer || Trees with a diameter greater than 100 cm have been reported. || "
    msg2 <- paste0("\n Parcel: ", big_trees$parcel_name,
                   " | Species: ", big_trees$species, 
                   " | DBH: ", big_trees$avg_DBH,
                   " | Year: ", big_trees$year)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # missing DBH values?
  missing_dbh <- raw %>% 
    filter(is.na(avg_DBH)) %>%
    # also no DBH for bushes/shrubs or vines or Herbaceous perennials
    filter(!grepl("bushes/shrubs", species, ignore.case=T)) %>%
    filter(!grepl("vine - ", species, ignore.case=T)) %>%
    filter(!grepl("herbaceous perennials", species, ignore.case=T)) %>%
    # combine the "other" type
    mutate(other = if_else(is.na(other),"",paste0(" - ", other)),
      species_comb = paste0(species, other)) %>%
    group_by(parcel_name, species_comb) %>%
    # summarise the missing years
    summarise(years = paste0(year, collapse=','), .groups="drop")
  if (nrow(missing_dbh) > 0) {
    msg <- "red || farmer || Some trees are missing DBH values. || "
    msg2 <- paste0("\n Parcel: ", missing_dbh$parcel_name,
                   " | Species: ", missing_dbh$species_comb, 
                   " | Year(s): ", missing_dbh$years)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # check for shrinking DBH
  dbh_t <- raw %>%
    filter(!grepl('bushes/shrubs', species)) %>%
    filter(!grepl('vine - ', species)) %>%
    pivot_wider(id_cols = c(parcel_name, species, planting_year, tree_index), names_from = year,
                values_from = avg_DBH, names_prefix="dbh_") %>%
    as.data.frame()
  has_shrunk <- rep(FALSE, nrow(dbh_t))
  nyrs <- ncol(dbh_t) - 4
  if (nyrs==1) {
    return(messages)  # no need to check if only one year
  }
  
  # check for DBH greater than expected maximum for that tree species
  above_mature_size <- processed %>%
    filter(avg_DBH > mature_tree_diam_cm) %>%
    group_by(species, mature_tree_diam_cm) %>%
    summarise(DBHs = paste0(unique(avg_DBH), collapse=', '), .groups='drop')
  if (nrow(above_mature_size) > 0) {
    msg <- "yellow || farmer || Some trees have a DBH greater than the expected maximum for that species. || "
    msg2 <- paste0("\n Species: ", above_mature_size$species,
                   " | Expected max DBH: ", above_mature_size$mature_tree_diam_cm,
                   " | Actual DBH: ", above_mature_size$DBHs)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  ## ... the following tests all rely on nyrs>1 ... ##
  for (yr_ix in 1:(nyrs-1)) {
    shrink_check <- (dbh_t[,5+yr_ix] < dbh_t[,4+yr_ix])
    shrink_check[is.na(shrink_check)] <- FALSE  # NA when trees are not planted yet
    has_shrunk <- has_shrunk | shrink_check
  }

  dbh_t$has_shrunk <- has_shrunk
  if (any(dbh_t$has_shrunk)) {
    msg <- "orange || farmer || Some trees have shrunk in diameter over time. || "
    msg2 <- paste0("\n Parcel: ", dbh_t$parcel_name[dbh_t$has_shrunk],
                   " | Species: ", dbh_t$species[dbh_t$has_shrunk])
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # DBH increasing surprisingly fast?
  max_growth_rate <- rep(0, nrow(dbh_t))
  for (yr_ix in 1:(nyrs-1)) {
    growth_rate <- dbh_t[,5+yr_ix] - dbh_t[,4+yr_ix]
    growth_rate[is.na(growth_rate)] <- 0  # NA when trees are not planted yet
    max_growth_rate <- pmax(max_growth_rate, as.numeric(growth_rate))
  }
  dbh_t$max_growth_rate <- max_growth_rate
  too_fast <- dbh_t %>% filter(max_growth_rate > 5)
  if (nrow(too_fast) > 0) {
    msg <- "orange || farmer || Some trees have grown in diameter at a rate greater than 5 cm/year. || "
    msg2 <- paste0("\n Parcel: ", too_fast$parcel_name,
                   " | Species: ", too_fast$species, 
                   " | Max growth rate (cm/yr): ", too_fast$max_growth_rate)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  # large DBH with plant year > 2018
  dbh_new <- dbh_t %>% filter(planting_year != "before 2018")
  dbh_new$max_DBH <- apply(dbh_new[,5:(4+nyrs)], 1, max, na.rm=T)
  big_new <- dbh_new %>% filter(max_DBH > 15)
  if (nrow(big_new)>0) {
    msg <- "orange || farmer || Some trees have a diameter greater than 15 cm and were planted after 2018. || "
    msg2 <- paste0("\n Parcel: ", big_new$parcel_name,
                   " | Species: ", big_new$species, 
                   " | Max DBH: ", big_new$max_DBH)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  
  ### tests based on the number of trees ###
  num <- raw %>%
    filter(!grepl('bushes/shrubs', species)) %>%
    filter(!grepl('vine - ', species)) %>%
    pivot_wider(id_cols = c(parcel_name, species, planting_year, tree_index), names_from = year,
                values_from = tree_number, names_prefix="num_") %>%
    as.data.frame()
  
  # should not have an increasing number in a particular cohort
  increasing <- rep(FALSE, nrow(num))
  for (yr_ix in 1:(nyrs-1)) {
    increase_check <- (num[,5+yr_ix] > num[,4+yr_ix])
    increase_check[is.na(increase_check)] <- FALSE  # NA when trees are not planted yet
    increasing <- increasing | increase_check
  }
  num$increasing <- increasing
  increased <- num %>% filter(increasing)
  if (nrow(increased) > 0) {
    msg <- "orange || farmer || The number of trees has increased in a particular cohort. || "
    msg2 <- paste0("\n Parcel: ", increased$parcel_name,
                   " | Species: ", increased$species)
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }

  return(messages)
}


check_baresoil <- function(raw, messages=c()) {
  # check for missing start and end dates
  nodate <- raw %>% filter(start_date=="" | end_date=="") %>%
    group_by(parcel_name) %>%
    summarise(year=paste0(unique(year), collapse=','))
  if (nrow(nodate)>0) {
    msg <- "red || farmer || Some bare soil data is missing start and/or end dates. || "
    msg2 <- paste0("\n Parcel: ", nodate$parcel_name,
                   " | Year(s): ", nodate$year)
    log4r::error(my_logger, msg)
    log4r::info(my_logger, msg2)
    messages <- c(messages, paste0(msg, paste0(msg2, collapse='')))
  }
  return(messages)
}

###### UTILITY FUNCTIONS ######
combine_sense_checks_to_xlsx <- function(farms_incl) {
  
  # settings
  translate <- TRUE
  
  # get all files
  dir_read <- 'output/data_check/errors/'
  files <- list.files(dir_read, pattern = "*.csv", full.names = FALSE)
  # remove those with "all_errors" in it
  files <- files[!grepl("all_errors", files)]
  # keep just those in farmers_incl
  files <- files[files %in% paste0(farms_incl, '.csv')]
  
  # remove the file if it already exists
  fn_out <- paste0(dir_read, "00_all_errors_", Sys.Date(), ".xlsx")
  fn_frmr <- paste0(dir_read, "00_farmer_errors_", Sys.Date(), ".xlsx")
  if (file.exists(fn_out)) file.remove(fn_out)
  if (file.exists(fn_frmr)) file.remove(fn_frmr)
  
  # create 2x new workbook: one with all, one with farmer-related issues only
  wb <- wb_workbook()
  wb_farmer <- wb_workbook()  # excluding the science issues
  # create a sheet with the overview
  wb$add_worksheet("Overview")
  wb_farmer$add_worksheet("Overview")
  wb$add_worksheet("all")
  wb_farmer$add_worksheet("all")
  overview_list <- list()
  overview_list_farmer <- list()
  frmr_responsibility <- c("farmer","science&farmer")
  
  ### first, translate all the unique error messages
  ### NOTE: THIS USES UP DEEPL ALLOWANCE, SO USE THIS VERY SPARINGLY
  if (translate) {
    # get the messages
    unique_messages <- c()
    for(fn in files) {
      check_results <- read_csv(paste0(dir_read, fn), show_col_types=F)
      unique_messages <- c(unique_messages, unique(check_results$message))
    }
    unique_messages <- unique(unique_messages)
    
    # read in the already-translated messages
    fn_translations <- 'translated_error_messages.csv'
    if (file.exists(fn_translations)) {
      translations_old <- tibble(read.csv(fn_translations))
      messages_to_translate <- unique_messages[!unique_messages %in% translations_old$EN]
    } else {
      messages_to_translate <- unique_messages
    }
    
    # translate
    if (length(messages_to_translate)>0) {
      library(deeplr)
      translations <- data.frame('EN'=messages_to_translate, 'ES'=NA, 'PT'=NA)
      init_file <- jsonlite::fromJSON('../sensitive-data/init_file.json')
      api_key <- init_file$deepl_api_key
      translations$ES <- translate2(messages_to_translate, 
                                   target_lang = "ES",
                                   auth_key = api_key)
      translations$PT <- translate2(messages_to_translate, 
                                   target_lang = "PT",
                                   auth_key = api_key)
      
      if (file.exists(fn_translations)) {
        # combine the new and old translations
        translations <- bind_rows(translations_old, translations)
      }
      # write to file
      write.csv(translations, fn_translations, row.names=F)
    } else {
      translations <- translations_old
    }
  }
  
  
  ### loop over the files
  row_count <- 1
  row_count_frmr <- 1
  all_errors <- list()
  for(fn in files) {
    # load the data
    farmer_email <- str_split(fn, ".csv")[[1]][1]
    check_results <- read_csv(paste0(dir_read, fn), show_col_types=F)
    
    # translate the messages to spanish and portugese
    if (translate) {
      check_results <- check_results %>%
        left_join(translations, by=c('message'='EN')) %>%
        select(responsibility, code, input, message, ES, PT, details)
    }
    all_errors[[fn]] <- check_results
    all_errors[[fn]]$farmer <- farmer_email
    
    # write data to a new sheet
    # all
    email_short <- str_split(farmer_email, "@")[[1]][1]
    wb$add_worksheet(email_short)
    wb$add_data_table(email_short, check_results, with_filter=F)
    # farmer only
    wb_farmer$add_worksheet(email_short)
    check_results_farmer <- check_results %>% filter(responsibility %in% frmr_responsibility)
    wb_farmer$add_data_table(email_short, check_results_farmer, with_filter=F)
    
    # save the workbook
    wb_save(wb, file = fn_out, overwrite = TRUE)
    wb_save(wb_farmer, file = fn_frmr, overwrite = TRUE)
    
    # add to the combined sheet at the start of the workbook
    wb$add_data_table("all", data.frame(email=farmer_email), with_filter=F, start_row=row_count, tableStyle = "TableStyleLight9")
    wb$add_data_table("all", check_results, with_filter=F, start_row=row_count+2)
    row_count <- row_count + nrow(check_results) + 7
    wb_farmer$add_data_table("all", data.frame(email=farmer_email), with_filter=F, start_row=row_count_frmr, tableStyle = "TableStyleLight9")
    wb_farmer$add_data_table("all", check_results_farmer, with_filter=F, start_row=row_count_frmr+2)
    row_count_frmr <- row_count_frmr + nrow(check_results_farmer) + 7
    
    # add to the overview data
    code_info <- check_results %>% group_by(code) %>% summarise(n=n()) %>% as.data.frame()
    resp_info <- check_results %>% group_by(responsibility) %>% summarise(n=n()) %>% as.data.frame()
    if (nrow(code_info) == 0) code_info <- data.frame('code'=c("blue","red","orange","yellow"), 'n'=c(0,0,0,0))
    if (nrow(resp_info) == 0) resp_info <- data.frame('responsibility'=c("farmer","science","science&farmer", "science&farmer (low priority)"), 'n'=c(0,0,0,0))
    combined <- data.frame('email'=farmer_email, 
                           'name'=c(code_info[,1], resp_info[,1]), 
                           'count'=c(code_info[,2], resp_info[,2]))
    overview_list[[farmer_email]] <- combined
    
    # same for farmers
    code_info <- check_results %>% filter(responsibility %in% frmr_responsibility) %>% group_by(code) %>% summarise(n=n()) %>% as.data.frame()
    resp_info <- check_results %>% filter(responsibility %in% frmr_responsibility) %>% group_by(responsibility) %>% summarise(n=n()) %>% as.data.frame()
    if (nrow(code_info) == 0) code_info <- data.frame('code'=c("blue","red","orange","yellow"), 'n'=c(0,0,0,0))
    if (nrow(resp_info) == 0) resp_info <- data.frame('responsibility'=c("farmer","science&farmer"), 'n'=c(0,0))
    combined <- data.frame('email'=farmer_email, 
                           'name'=c(code_info[,1], resp_info[,1]), 
                           'count'=c(code_info[,2], resp_info[,2]))
    overview_list_farmer[[farmer_email]] <- combined
    
    # create a farmer-specific workbook
    # with a sheet based on the current date
    wb_frmr_i <- wb_workbook()
    date_str <- as.character(Sys.Date())
    wb_frmr_i$add_worksheet(date_str)
    # add their data
    wb_frmr_i$add_data_table(date_str, check_results_farmer, with_filter=F)
    # write
    dir_frmr_i <- paste0('output/data_check/individual_farmer/', farmer_email, "/")
    if (!dir.exists(dir_frmr_i)) dir.create(dir_frmr_i, recursive = TRUE)
    fn_frmr_i <- paste0(dir_frmr_i, "errors_", farmer_email, ".xlsx")
    wb_save(wb_frmr_i, file = fn_frmr_i, overwrite = TRUE)
  }
  
  # load the data from Bilal's tracker
  perc_complete <- read_csv('../sensitive-data/farm_info_with_data_progress.csv', show_col_types=F)
  perc_complete$percent_complete <- as.numeric(perc_complete$percent_complete)
  perc_complete <- perc_complete %>% select(email_address, farm_id_monitoringdatas, percent_complete, sense_checking_complete)
  
  # format the overview data and add to the workbook
  overview_df <- do.call(rbind, overview_list) %>% as_tibble()
  overview_df$name <- factor(overview_df$name, levels=c("blue","red","orange","yellow","farmer","science","science&farmer", "science&farmer (low priority)"))
  overview_df <- overview_df %>% arrange(email, name)
  overview_wide <- overview_df %>%
    pivot_wider(names_from = name, values_from = count, names_prefix = "# ", values_fill=0)
  overview_wide$total <- overview_wide$`# blue` + overview_wide$`# red` + overview_wide$`# orange` + overview_wide$`# yellow`
  # reorder the columns so it is email, total, then the others
  overview_wide <- overview_wide[, c(1, ncol(overview_wide), 2:(ncol(overview_wide)-1))]
  # add the % complete
  overview_wide <- overview_wide %>% left_join(perc_complete, by=c("email"="email_address"))
  # write
  wb$add_data_table("Overview", overview_wide, with_filter=F)
  wb_save(wb, file = fn_out, overwrite = TRUE)
  
  # same thing for the farmer-only data
  overview_df_farmer <- do.call(rbind, overview_list_farmer) %>% as_tibble()
  overview_df_farmer$name <- factor(overview_df_farmer$name, levels=c("blue","red","orange","yellow","farmer","science&farmer"))
  overview_df_farmer <- overview_df_farmer %>% arrange(email, name)
  overview_wide_farmer <- overview_df_farmer %>%
    pivot_wider(names_from = name, values_from = count, names_prefix = "# ", values_fill=0)
  overview_wide_farmer$total <- overview_wide_farmer$`# blue` + overview_wide_farmer$`# red` + overview_wide_farmer$`# orange` + overview_wide_farmer$`# yellow`
  # reorder the columns so it is email, total, then the others
  overview_wide_farmer <- overview_wide_farmer[, c(1, ncol(overview_wide_farmer), 2:(ncol(overview_wide_farmer)-1))]
  # add the % complete
  overview_wide_farmer <- overview_wide_farmer %>% left_join(perc_complete, by=c("email"="email_address"))
  # write
  wb_farmer$add_data_table("Overview", overview_wide_farmer, with_filter=F)
  wb_save(wb_farmer, file = fn_frmr, overwrite = TRUE)
  
  # write a csv with all errors comb
  all_comb <- do.call(bind_rows, all_errors)
  all_comb <- all_comb %>% left_join(perc_complete, by=c("farmer"="email_address"))
  all_comb <- all_comb %>% arrange(desc(percent_complete), farmer)
  write.csv(all_comb, paste0(dir_read, "00_all_errors_single_sheet_", Sys.Date(), ".csv"), row.names=F)
 
  # group by error type and sum
  error_freq <- all_comb %>% group_by(
    responsibility, code, input, message, ES, PT) %>%
    summarise(n=n(), .groups='drop') %>%
    arrange(desc(n))
  write.csv(error_freq, paste0(dir_read, "00_all_errors_freq_", Sys.Date(), ".csv"), row.names=F)
  
  # get the errors about farm area
  farm_area_errors <- all_comb %>% filter(input=="farm_fixed")
  write.csv(farm_area_errors, paste0(dir_read, "00_all_errors_farm_area_", Sys.Date(), ".csv"), row.names=F)
  
}
