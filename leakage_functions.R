# leakage functions

imported_organic_matter_leakage <- function(organic_amendments){
  # leakage from imported organic matter (all types)
  if (nrow(organic_amendments) > 0) {
    # calculate the co2_eq leakage
    leakage_all_sources <- organic_amendments %>%
      replace_na(list(percent_imported = 0, amount_t = 0)) %>%
      mutate(organicmatter_t_c02eq = amount_t * percent_imported/100 * dry_c * .12 * (44/12)) %>%
      select(year, year_index, period, parcel_name, type, sub_type, other, organicmatter_t_c02eq)
    
    # calculate annual sums
    leakage_annual <- leakage_all_sources %>%
      group_by(year, year_index, period) %>%
      summarise(organicmatter_t_c02eq = sum(organicmatter_t_c02eq, na.rm=T))
    
    # raise warning for unknown dry_c
    if (sum(is.na(leakage_all_sources$organicmatter_t_c02eq)) > 0) {
      print("Warning: unknown dry_c in imported_manure_leakage()")
    }
  
  } else {
    # return empty data frames
    leakage_all_sources <- tibble(year = integer(), year_index = integer(), period = character(),
                                  parcel_name = character(), 
                                  type = character(), sub_type = character(), other = character(), 
                                  organicmatter_t_c02eq = numeric())
    leakage_annual <- tibble(year = integer(), year_index = integer(), period = character(),
                             organicmatter_t_c02eq = numeric())
    
  }
  
  return(list(leakage_all_sources = leakage_all_sources, 
              leakage_annual = leakage_annual))
}


productivity_crops <- function(crop_inputs, scenario_selected, farm_EnZ, parcel_inputs){
  
  return()
  #### OLD CODE ####
  # # TODO: Incorporate this as part of the monitoring report. 
  # # This should check the changes in production and check whether there has been a reduction in productivity. 
  # # This can only happen when there are results to compare to. 
  # ## Join crop data /ha and area in parcel_input
  # 
  # crops <- merge(x = filter(crop_inputs,scenario==scenario_selected), 
  #                y = parcel_inputs, by = "parcel_ID", all.x = TRUE)
  # crops <- merge(x = crops, 
  #                y = filter(factors_crops,pedo_climatic_area==farm_EnZ | is.na(pedo_climatic_area)==TRUE), by = "crop", all.x = TRUE)
  # crops <- crops %>% mutate(productivity = (dry_harvest + dry_grazing * area * dry_c))
  # farm_productivity = sum(crops$productivity) * 44/12
  # return(farm_productivity)
}

get_yearly_productivity_table <- function(productivity_table, crop_inputs, scenario_selected, farm_EnZ){
  
  return()
  #### OLD CODE ####
  # # TODO: Incorporate this as part of the monitoring report. 
  # # This should check the changes in production and check whether there has been a reduction in productivity. 
  # # This can only happen when there are results to compare to. 
  # ## Join crop data /ha and area in parcel_input
  # 
  # # Fernando: Issue: The code doesn't seem to make sense 
  # crops <- merge(x = filter(crop_inputs,scenario==scenario_selected), 
  #                y = parcel_inputs, by = "parcel_ID", all.x = TRUE)
  # crops <- merge(x = crops, 
  #                y = filter(factors_crops, pedo_climatic_area==farm_EnZ | is.na(pedo_climatic_area)==TRUE), by = "crop", all.x = TRUE)
  # if(nrow(crops)==0){
  #   productivity_table = data.frame(year = c(), crop = c(), productivity = c())
  #   return(productivity_table)
  #   }
  # crops$scenario=scenario_selected
  # productivity_table <- crops %>% group_by(scenario,crop) %>% summarise(productivity = 44/12 * sum(dry_harvest + dry_grazing * area * dry_c))
  # return(productivity_table)
}
