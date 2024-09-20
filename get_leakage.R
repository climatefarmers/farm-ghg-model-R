get_leakage <- function(inputs) {
  
  source(file.path("leakage_functions.R"), local = TRUE)
  # organic matter
  om_leakage <- imported_organic_matter_leakage(inputs$inputs_organicmatter)
  om_all_sources <- om_leakage$leakage_all_sources
  om_annual <- om_leakage$leakage_annual
  
  # productivity
  # ... to do ...
  
  # Integrate and calculate sums
  leakage_annual <- om_annual  # to do: left_join with productivity
  leakage_annual <- leakage_annual %>%
    mutate(CO2eq_leakage = organicmatter_t_c02eq) # + productivity_t_co2eq
  
  # calculate the average baseline value
  last_baseline_year <- max((leakage_annual %>%
    filter(period == "baseline"))$year)
  leakage_baseline <- leakage_annual %>% 
    filter(period == "baseline") %>% 
    group_by(period) %>%
    summarise_all(mean) %>%
    mutate(year = last_baseline_year,
           year_index = 0) %>%
    select(colnames(leakage_annual))
  leakage_annual <- bind_rows(leakage_annual, leakage_baseline) %>%
    arrange(year_index)
  
  # calculate the difference from the baseline
  # NOTE: this output is currently not used in the overall carbon calculations
  # (instead, we take just the absolute leakage in each project year)
  leakage_diffs <- leakage_annual %>%
    filter(year_index >= 0) %>% 
    mutate(CO2eq_leakage_diff = CO2eq_leakage - leakage_baseline$CO2eq_leakage)
  
  # to do: also return leakage by source
  return(list(leakage_annual=leakage_annual,
              leakage_diffs=leakage_diffs))
  
  # OLD CODE
  # amendments <- merge(filter(inputs$inputs_organicmatter, scenario==scenario_selected), factors$manure_factors, by = "source", all.x = TRUE)
  # amendments <- merge(filter(amendments, scenario==scenario_selected), inputs$parcel_inputs, by = "parcel_ID", all.x = TRUE)
  # 
  # leakage <- manure_leakage(amendments)
  # yearly_productivity <- productivity_crops(inputs$crop_inputs, scenario_selected, farm_EnZ, inputs$parcel_inputs)
  # productivity_table <- rbind(productivity_table,
  #                             get_yearly_productivity_table(productivity_table, inputs$crop_inputs, scenario_selected, farm_EnZ))
  
}