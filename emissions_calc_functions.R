# Initial calculations all in kg -> convert to tonnes later

## Calculation: CO2 from direct fuel consumption
get_fuel_direct_emissions <- function(inputs_fuel_direct, inputs_farm_fixed, factors_fuel_emissions) {
  # Function calculates the CO2 emissions from fuel consumption, based on the direct and indirect fuel usage.
  
  # Extract the emission factors for diesel and petrol
  ef_diesel <- factors_fuel_emissions$value[factors_fuel_emissions$variable == "ef_diesel_kg_l"]
  ef_petrol <- factors_fuel_emissions$value[factors_fuel_emissions$variable == "ef_petrol_kg_l"]

  # Calculate the emissions from direct fuel consumption
  inputs_fuel_direct$petrol_co2 <- inputs_fuel_direct$petrol_amount * ef_petrol
  inputs_fuel_direct$diesel_co2 <- inputs_fuel_direct$diesel_amount * ef_diesel
  inputs_fuel_direct$co2_dfuel_kg_total <- inputs_fuel_direct$diesel_co2 + inputs_fuel_direct$petrol_co2  # Get emissions in kg
  
  # Replace NA values with 0
  inputs_fuel_direct$co2_dfuel_kg_total <- replace_na(inputs_fuel_direct$co2_dfuel_kg_total, 0)
  
  # Calculate the emissions per hectare
  inputs_fuel_direct$co2_dfuel_kg_ha <- inputs_fuel_direct$co2_dfuel_kg_total / inputs_farm_fixed$area_farm
  
  # Return a list with the direct and indirect fuel emissions
  return(inputs_fuel_direct)
}

## Calculation: CO2 from indirect fuel consumption
get_fuel_indirect_emissions <- function(inputs_fuel_indirect, inputs_farm_fixed, factors_fuel_emissions) {
  # Function calculates the CO2 emissions from fuel consumption, based on the direct and indirect fuel usage.
  
  # Extract the emission factors for diesel and petrol
  ef_diesel <- factors_fuel_emissions$value[factors_fuel_emissions$variable == "ef_diesel_kg_l"]
  ef_petrol <- factors_fuel_emissions$value[factors_fuel_emissions$variable == "ef_petrol_kg_l"]
  
  # Calculate the emissions from indirect fuel consumption using a lookup table with the fuel consumption per field operation/service
  inputs_fuel_indirect$diesel_l <- inputs_fuel_indirect$area_amount * inputs_fuel_indirect$diesel_l_ha
  inputs_fuel_indirect$co2_ifuel_kg_total <- inputs_fuel_indirect$diesel_l * ef_diesel  # Get emissions in kg
  
  # Replace NA values with 0
  inputs_fuel_indirect$co2_ifuel_kg_total <- replace_na(inputs_fuel_indirect$co2_ifuel_kg_total, 0)

  # Calculate the emissions per hectare
  inputs_fuel_indirect$co2_ifuel_kg_ha <- inputs_fuel_indirect$co2_ifuel_kg_total / inputs_farm_fixed$area_farm

  # Return a list with the direct and indirect fuel emissions
  return(inputs_fuel_indirect)
}

## Calculation CH4 and N2O emissions from livestock
get_livestock_emissions <- function(livestock,
                                    factors_livestock,
                                    factors_n2o_emission,
                                    climate_wet_or_dry) {
  
  if(nrow(livestock) == 0) {
    return(livestock)
  } else {
    
    # Calculate the CH4 emissions from enteric fermentation and manure
    livestock <- livestock %>% mutate(
      ch4_kg_ent_ferm = amount * ef_enteric_fermentation_kg_head / 365 * days_on_farm_per_year,
      ch4_kg_manure = amount * vs_kg_per_tonne_per_day * days_on_farm_per_year * (mass_kg_per_animal/1000) * ef_methane_manure / 1000
      )
    
    # Calculate the direct and indirect N2O emissions from manure
    if (climate_wet_or_dry == "wet"){
      ef_4 <- factors_n2o_emission$value[factors_n2o_emission$name == 'ef_4_n2o_wet']
      ef_3_pasture <- factors_n2o_emission$value[factors_n2o_emission$name == 'ef_3_n2o_wet']
    }
    if (climate_wet_or_dry == "dry"){
      ef_4 <- factors_n2o_emission$value[factors_n2o_emission$name == 'ef_4_n2o_dry']
      ef_3_pasture <- factors_n2o_emission$value[factors_n2o_emission$name == 'ef_3_n2o_dry']
    }
    frac_gasm <- factors_n2o_emission$value[factors_n2o_emission$name == 'n2o_frac_gasm']
    frac_leach <- factors_n2o_emission$value[factors_n2o_emission$name == 'n2o_frac_leach']
    ef_5 <- factors_n2o_emission$value[factors_n2o_emission$name == 'ef_5_n2o']
    ef_3_deep_bedding <- factors_n2o_emission$value[factors_n2o_emission$name == 'ef_3_deep_bedding']
    
    livestock <- livestock %>% mutate(
      n2o_kg_manure_indirect = amount * n_excretion_rate_kg_1000am * grazing_days * mass_kg_per_animal / 1000 *
        (frac_gasm * ef_4 + frac_leach * ef_5) * (44/28),
      n2o_kg_manure_direct = amount * n_excretion_rate_kg_1000am * mass_kg_per_animal / 1000 * 
        (grazing_days * ef_3_pasture + (days_on_farm_per_year - grazing_days) * ef_3_deep_bedding) * (44/28)
    )
    
    return(livestock) 
  }
}

get_n_fixing_species_emissions <- function(productivity, factors_n2o_emission) {
  
  ef_n <- factors_n2o_emission$value[factors_n2o_emission$name == 'ef_n_fixing']
    
  # For crops
  productivity <- productivity %>% 
    mutate(n2o_kg_n_fixing = n_fixing_frac * 
             (
               (residue_t_dry + forage_residue_t_dry) * n_ag + prod_bg * n_bg) *
                ef_n * (44/28) * 1000 # 1000 converts t to kg
             )
  # replace NA with zero
  productivity$n2o_kg_n_fixing <- replace_na(productivity$n2o_kg_n_fixing, 0)

  return(productivity)
}


get_fertilizer_emissions <- function(inputs_fertilizer, inputs_farm_fixed, factors_fertilizer, factors_n2o_emissions) {
  
  f_type <- 'synthetic'
  ef_fertilizer <- factors_fertilizer$ef_fertilizer[factors_fertilizer$fertilizer_type == f_type]
  volatile_fraction <- factors_fertilizer$volatile_fraction[factors_fertilizer$fertilizer_type == f_type]
  n_to_n2o <- factors_n2o_emissions$value[factors_n2o_emissions$name == 'n_n2o']
  
  # Calculate the N2O emissions from fertilizer application
  inputs_fertilizer <- inputs_fertilizer %>% 
    # Spelled out: N2O = (tonnes to kg) x fertilizeer N fraction x non-volatile fraction x emission factor x N to N2O conversion factor
    mutate(n2o_fert_kg_total_farm = (amount * 1000) * (percentN / 100) * (1 - volatile_fraction) * ef_fertilizer * n_to_n2o) # units: kg of N2O-N

  # Replace NA values with 0
  inputs_fertilizer$n2o_fert_kg_total_farm <- replace_na(inputs_fertilizer$n2o_fert_kg_total_farm, 0)
  inputs_fertilizer$n2o_fert_kg_ha_farm <- inputs_fertilizer$n2o_fert_kg_total_farm / inputs_farm_fixed$area_farm

  return(inputs_fertilizer)
}
