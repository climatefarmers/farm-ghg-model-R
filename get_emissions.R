#main in function form

#### TO DO: fix n2o_n_fixing & leakage functions so that it is calc and displayed
#### TO DO: include pastures and compst ?? to n2o_n_fixing

get_emissions <- function(init_data, farm_EnZ, inputs, factors, periods){

  ## Log start running messages
  log4r::info(my_logger, "Started calculations of farm emissions.")

  source("emissions_calc_functions.R", local = TRUE)

  fuel_direct <- get_fuel_direct_emissions(
    inputs$inputs_fuel_direct,
    inputs$inputs_farm_fixed,
    factors$factors_fuel_emissions
  )
  
  fuel_indirect <- get_fuel_indirect_emissions(
    inputs$inputs_fuel_indirect,
    inputs$inputs_farm_fixed,
    factors$factors_fuel_emissions
    )

  fertilizers <- get_fertilizer_emissions(
    inputs_fertilizer = inputs$inputs_fertilizer,
    inputs_farm_fixed = inputs$inputs_farm_fixed,
    factors_fertilizer = factors$factors_fertilizer,
    factors_n2o_emission = factors$factors_n2o_emission
    )
  
  livestock <- get_livestock_emissions(
    livestock = inputs$inputs_livestock,
    factors_livestock = factors$factors_livestock,
    factors_n2o_emission = factors$factors_n2o_emission,
    climate_wet_or_dry = factors$factors_natural_area$climate_wet_or_dry
    )

  n_fixing_species <- get_n_fixing_species_emissions(
    productivity = inputs$inputs_productivity,
    factors_n2o_emission = factors$factors_n2o_emission
    )
  
  
  ## convert all emission types to annual totals at the farm level
  fuel_direct_farmlevel <- fuel_direct %>% 
    group_by(year) %>% 
    summarise(co2_dfuel_kg_total = sum(co2_dfuel_kg_total, na.rm=T))
  fuel_indirect_farmlevel <- fuel_indirect %>% 
    group_by(year) %>% 
    summarise(co2_ifuel_kg_total = sum(co2_ifuel_kg_total, na.rm=T))
  fertilizer_farmlevel <- fertilizers %>% 
    group_by(year) %>% 
    summarise(n2o_fert_kg_total_farm = sum(n2o_fert_kg_total_farm, na.rm=T))
  livestock_farmlevel <- livestock %>% 
    group_by(year) %>% 
    summarise(ch4_kg_ent_ferm = sum(ch4_kg_ent_ferm, na.rm=T),
              ch4_kg_manure = sum(ch4_kg_manure, na.rm=T),
              n2o_kg_manure_direct = sum(n2o_kg_manure_direct, na.rm=T),
              n2o_kg_manure_indirect = sum(n2o_kg_manure_indirect, na.rm=T)
    )
  n_fixing_species_farmlevel <- n_fixing_species %>% 
    group_by(year, scenario, year_index) %>%  # with dynamic baseline, need to group by scenario also
    summarise(n2o_kg_n_fixing = sum(n2o_kg_n_fixing, na.rm=T), .groups='drop')
  # to make same format as the other emissions, we need to add rows for "project" and "projected_baseline" at year0
  n_fixing_species_year0 <- n_fixing_species_farmlevel %>% 
    filter(year_index == 0)
  n_fixing_species_farmlevel <- bind_rows(n_fixing_species_farmlevel, 
                                          n_fixing_species_year0 %>% mutate(scenario = "projected_baseline"),
                                          n_fixing_species_year0 %>% mutate(scenario = "project")) %>%
    arrange(year, year_index) %>%
    select(-year_index)
  
  ## Create a combined data frame including all emission sources
  
  ## A: first, those that are unaffected by the dynamic baseline
  periods$scenario <- NA
  df_list <- list(fuel_direct_farmlevel, fuel_indirect_farmlevel, fertilizer_farmlevel, livestock_farmlevel)
  emissions <- plyr::join_all(df_list, by = 'year', type = 'left')
  emissions <- right_join(periods, emissions, by='year') %>% 
    mutate(scenario = period) %>%
    as_tibble()
  
  # create the projected baseline for these emissions
  # this is the average of the baseline years
  bl_avg <- emissions %>% 
    filter(period == 'baseline') %>% 
    select(-c(year, year_index, scenario, period)) %>%
    summarise_all(mean, na.rm = T) %>% 
    mutate(period = 'project',
           scenario = 'projected_baseline')
  proj_bl <- left_join(periods %>% filter(period == 'project') %>% select(-scenario),
                       bl_avg,  # this will be replicated for each project year
                       by = 'period') %>%
    select(colnames(emissions))
  
  # add the projected baseline to the emissions data frame
  emissions <- bind_rows(emissions, proj_bl)
  
  # add rows for the average of the baseline (with year_index=0 and scenarios as project and projected_baseline)
  last_bl_yr <- periods$year[periods$year_index == -1]
  bl_avg_full <- bind_rows(bl_avg %>% mutate(year=last_bl_yr, year_index=0, period='baseline', scenario='projected_baseline'),
                       bl_avg %>% mutate(year=last_bl_yr, year_index=0, period='baseline', scenario='project')) %>%
    select(colnames(emissions))
  emissions <- bind_rows(emissions, bl_avg_full) %>%
    arrange(year_index)
  
  
  ## B: Now add the n_fixing_species, which have distinct values in the projected baseline
  emissions_all <- left_join(emissions, n_fixing_species_farmlevel, by = c('year', 'scenario')) %>%
    replace_na(list(n2o_kg_n_fixing = 0))
  
  
  ## Group by gas type and convert to CO2eq
  emissions_long <- emissions_all %>% 
    gather(key = 'source', value = 'value', -c(year, year_index, period, scenario)) %>% 
    mutate(gas = substr(source, 1, 3),
           source = substr(source, 5, nchar(source))) %>%
    left_join(factors$factors_co2eq, by = 'gas') %>% 
    mutate(kgCO2_eq = value * co2eq_factor) %>% 
    select(-c(value, co2eq_factor))
  
  
  ## Calculate differences between projected baseline and each project year
  # a) for each emission source
  emission_diffs_by_source <- emissions_long %>% 
    filter(year_index >= 0) %>% 
    group_by(year, year_index, source, gas) %>% 
    pivot_wider(names_from = 'scenario', names_prefix = 'kgCO2_eq_', values_from = 'kgCO2_eq') %>%
    mutate(kgCO2_eq_diff = kgCO2_eq_project - kgCO2_eq_projected_baseline)
  # b) for each gas type
  emission_diffs_by_gas <- emission_diffs_by_source %>% 
    group_by(year, year_index, period, gas) %>% 
    summarise(kgCO2_eq_projected_baseline = sum(kgCO2_eq_projected_baseline, na.rm = T),
              kgCO2_eq_project = sum(kgCO2_eq_project, na.rm = T),
              kgCO2_eq_diff = sum(kgCO2_eq_diff, na.rm = T),
              .groups='keep')
  # c) totals
  emission_diffs_total <- emission_diffs_by_gas %>% 
    group_by(year, year_index, period) %>% 
    summarise(kgCO2_eq_projected_baseline = sum(kgCO2_eq_projected_baseline, na.rm = T),
              kgCO2_eq_project = sum(kgCO2_eq_project, na.rm = T),
              kgCO2_eq_diff = sum(kgCO2_eq_diff, na.rm = T),
              .groups='keep')
  
  ## return
  return(list(
    emissions = emissions,
    emissions_long = emissions_long,
    emission_diffs_by_source = emission_diffs_by_source,
    emission_diffs_by_gas = emission_diffs_by_gas,
    emission_diffs_total = emission_diffs_total
  ))
  
  
  ## FERNANDO'S COMMENTS.
  ## Create parcel level data frame including all emission sources
  
  # parcel_emissions_detailed <- tibble()
  
  ## Get yearly sums at parcel level
  
  # parcel_emissions_average <- parcel_emissions_detailed %>% group_by(parcel_id) %>% summarise(
  # n2o_n_fixing = sum(n2o_n_fixing),
  # ...
  # )
  
  ## Get yearly sums of emissions at farm level
  
  ## Get average of baseline
  
  ## Get difference between baseline and each project year
  
  ## Create data frame aggregating and summing up emissions for each gas type
  
  ## Return data frames with the different levels of detail/aggregation
  
}
  
# OLD CODE ----
  
#   ## Calculation of yearly results
#   # Preparation of data frames
#   emissions_yearly_sources = data.frame(scenario_selected = c(), source = c(), value = c(),
#                                         gas = c(), co2eq_factor = c(), kgCO2_eq = c())
# 
#   emissions_parcels_yearly_livestock <- create_empty_dataframe(c('parcel_ID', 'year', 'ch4_manure', 'ch4_ent_ferm', 'n2o_manure'))
# 
#   productivity_table = data.frame(year = c(), crop = c(), productivity = c())
#   
#   for (i in years) {
# 
#     # Results by parcels
#     parcel_emissions_livestock_sum <- parcel_emissions_livestock %>% group_by(parcel_ID) %>%
#       summarise(year = i,
#                 ch4_manure = sum(ch4_manure),
#                 ch4_ent_ferm = sum(ch4_ent_ferm),
#                 n2o_manure_direct = sum(n2o_manure_direct),
#                 n2o_manure_indirect = sum(n2o_manure_indirect)
#       )
# 
#     emissions_parcels_yearly_livestock <- rbind(emissions_parcels_yearly_livestock, parcel_emissions_livestock_sum)
# 
#     parcel_emissions_crop_sum <- parcel_emissions_crop %>% group_by(parcel_ID) %>% summarise(n2o_n_fixing = sum(n2o_n_fixing))
# 
#     # Total by source
#     emissions_fertilizer_total <- parcel_emissions_fertilizer %>% summarise(n2o_fertilizer = sum(n2o_fertilizer))
# 
#     emissions_livestock_total <- parcel_emissions_livestock %>% summarise(ch4_manure = sum(ch4_manure),
#                                                                       ch4_ent_ferm = sum(ch4_ent_ferm),
#                                                                       n2o_manure_direct = sum(n2o_manure_direct),
#                                                                       n2o_manure_indirect = sum(n2o_manure_indirect)
#                                                                       )
# 
#     emissions_fuel_total <- emissions_fuel %>% summarise(co2_fuel = sum(co2_fuel))
# 
#     emissions_crop_total <- parcel_emissions_crop %>% summarise(n2o_n_fixing = sum(n2o_n_fixing))
# 
#     # pasture_results_sum <- # not using pasture n fixation # pasture_results %>% summarise(n2o_n_fixing = sum(n2o_n_fixing))
#     #n_fixing_sum <- parcel_emissions_crop_sum + pasture_results_sum
#     leakage_sum <- leakage %>% summarise(co2_leakage = sum(co2_leakage))
#     crops_productivity <- data.frame("co2_crops_productivity_tCO2eq"=c(yearly_productivity))
#     # Summarise Results
#     all_results <- bind_rows(emissions_fertilizer_total, emissions_livestock_total, emissions_fuel_total, emissions_crop_total, leakage_sum) %>% #, crops_productivity) %>%
#       pivot_longer(cols = everything(), names_to = "source") %>%
#       filter(!is.na(value)) %>%
#       mutate(gas = substr(source,1,3),
#              source = substr(source, 5, nchar(source))) %>%
#       left_join(factors$factors_co2eq, by = "gas") %>%
#       mutate(kgCO2_eq = co2eq_factor * value)
#     all_results$scenario_selected=scenario_selected
#     all_results$year = i
#     emissions_yearly_sources = rbind(emissions_yearly_sources,
#                                      all_results)
#   }
# 
#   emissions_yearly_sources <- emissions_yearly_sources %>%
#     mutate(tCO2_eq = kgCO2_eq/1000) %>% select(!c(kgCO2_eq))
# 
#   emissions_yearly_total = emissions_yearly_sources %>% group_by(year) %>%
#     filter(source != "leakage" & source != "crops_productivity_tCO2eq") %>%
#     summarise(emissions_tCO2_eq=sum(tCO2_eq))
# 
#   emissions_yearly_total$leakage_tCO2_eq <- round((emissions_yearly_sources %>% group_by(year) %>%
#                                                      filter(source=="leakage") %>%
#                                                      summarise(leakage_tCO2_eq=tCO2_eq))$leakage_tCO2_eq)
# 
#   # Fernando: The lines below are not working!!!
#   # summarise(total_emissions_without_leakage_tCO2_eq=sum(kgCO2_eq)*1e-3) # Fernando: what is this line doing here!
#   # emissions_yearly_total$crops_productivity_tCO2eq = (emissions_yearly_sources %>% group_by(scenario_selected) %>%
#   #                                                filter(source=="crops_productivity_tCO2eq")%>%
#   #                                                summarise(leakage_tCO2_eq=kgCO2_eq))$leakage_tCO2_eq
# 
#   emissions_yearly_total$emissions_diff_tCO2_eq <-
#     round(emissions_yearly_total$emissions_tCO2_eq - emissions_yearly_total$emissions_tCO2_eq[1])
# 
#   return(
#     list(emissions_yearly_total = emissions_yearly_total,
#          emissions_yearly_sources = emissions_yearly_sources,
#          productivity_table = productivity_table,
#          emissions_parcels_yearly_livestock= emissions_parcels_yearly_livestock)
#   )
# }
# 
# 
# create_empty_dataframe <- function(column_names = c()){
# 
#   df = matrix(ncol = length(column_names), nrow = 0)
#   colnames(df)  <- column_names
#   df <- as.data.frame(df)
# 
#   return(df)


