#### TREE EMISSION CALCUALTION FUNCTIONS ---------------------------------------
#### This script contains functions to calculate tree biomass and related carbon
#### stocks and emissions

get_tree_carbon <- function(
    perennials_raw, 
    periods,
    inputs_parcels_fixed,
    trees_felled_raw,
    email
) {
  ## MAIN FUNCTION
  # This function calculates tree carbon stocks and litter amounts
  # Currently it does not include other perennials (e.g. shrubs)
  # Inputs: perennials tables with factors already merged
  # Outputs: 
  # - Farm level table with biomass carbon stocks and C sequestration/emissions (CO2 equivalents)
  # - Parcel level table with soil C inputs from trees
  
  # Get the project start year
  start_year <- periods$year[periods$year_index==1]
  
  # Define data frame with all years and parcels
  all_year_parcels <- expand_grid(periods %>% select(year, year_index, period), 
                                  parcel_name = inputs_parcels_fixed$parcel_name)
  
  ### Calculate the C balance and credits earned from managed trees biomass -------------
  
  # If empty, return empty data frames
  if(nrow(perennials_raw)==0) {
    
    managed_biomass_year <- tibble(
      year= periods$year,
      year_index = periods$year_index,
      period = periods$period,
      co2_trees_managed = 0)
    
    trees_soil_parcel <- all_year_parcels %>% mutate(
      c_litt_bg_t = 0,
      c_litt_bg_t_lt = 0,
      harvest_t_dry = 0,
      harvest_t_ha_dry = 0,
      residue_t_dry = 0,
      residue_t_ha_dry = 0,
      c_litt_ag_t = 0,
      c_litt_bg_t_baseline_hist_max = 0,
      c_litt_bg_t_lt_baseline = 0,
      c_litt_bg_t_lt_diff = 0,
      c_litt_bg_t_baseline_min = 0,
      c_litt_bg_t_baseline = 0,
      c_litt_ag_t_baseline = 0
    )
    
  } else {
    
    # select only trees
    trees_managed <- as_tibble(perennials_raw) %>% 
      filter(type == "tree")
    
    # limit lifespan to max lifespan
    # define management
    trees_managed <- trees_managed %>% mutate(
      lifespan_years_reported = lifespan_years,
      lifespan_years = ifelse(
        is.na(lifespan_years) | lifespan_years == 0,
        max_lifespan, 
        pmin(lifespan_years, max_lifespan)
      ),
      management = ifelse(lifespan_years >= 100, "agroforest", "tree crop")
    )
    
    # Change planting_year to integer
    trees_managed$planting_year[trees_managed$planting_year=="before 2018"] <- 0
    trees_managed$planting_year <- as.integer(trees_managed$planting_year)
    
    ## Calculate values for each tree cohort -----
    
    # Calculation of above and below-ground biomass
    trees_managed <- trees_managed %>% mutate(
      # calculate kilograms per tree
      agb_actual = get_agb(avg_DBH, a, b, c, d, e, f, j, k, cyp, cork, exp, poly),
      bgb_actual = get_bgb(avg_DBH, agb_actual, b_a, b_b, b_c, b_d, r_s_ratio, b_cork,
                           b_poly, b_others, b_common, b_exp, b_citrus),
      agb_mature = get_agb(mature_tree_diam_cm, a, b, c, d, e, f, j, k, cyp, cork, exp, poly),
      bgb_mature = get_bgb(mature_tree_diam_cm, agb_mature, b_a, b_b, b_c, b_d, r_s_ratio, b_cork,
                           b_poly, b_others, b_common, b_exp, b_citrus),
      # limit to max biomass
      overshoot_ratio_actual = pmax(1, (agb_actual+bgb_actual) / max_biomass_kg),
      overshoot_ratio_mature = pmax(1, (agb_mature+bgb_mature) / max_biomass_kg),
      agb_actual = agb_actual / overshoot_ratio_actual,
      bgb_actual = bgb_actual / overshoot_ratio_actual,
      agb_mature = agb_mature / overshoot_ratio_mature,
      bgb_mature = bgb_mature / overshoot_ratio_mature,
      # total carbon per tree cohort: multiply by the number of trees and convert to tonnes of dry matter
      total_actual_c_t_dry = (agb_actual + bgb_actual) * dry_c * tree_number / 1000,
      total_mature_c_t_dry = (agb_mature + bgb_mature) * dry_c * tree_number / 1000
    ) %>%
      select(-c(a,b,c,d,e,f,j,k,exp,poly,quad,cyp,cork,b_poly,b_exp,b_common,
                b_others,b_citrus,b_cork,b_a,b_b,b_c,b_d,comment,equation_type,
                max_biomass_kg, overshoot_ratio_actual, overshoot_ratio_mature))
    
    # Round values
    trees_managed <- trees_managed %>% mutate_at(
      vars(agb_actual, bgb_actual, agb_mature, bgb_mature, total_actual_c_t_dry,
           total_mature_c_t_dry), .funs = ~round(., 3)
    )
    
    # Calculate long term carbon stocks for each tree cohort
    trees_managed <- trees_managed %>%
      mutate(
        # intermediate variables
        time_growing = pmin(lifespan_years, mature_age_years),
        time_at_max = pmax(lifespan_years - mature_age_years, 0),
        frac_of_max_achieved = time_growing / mature_age_years,
        # calculation
        c_longterm_t = (1/lifespan_years) * (
          0.5 * time_growing * frac_of_max_achieved * total_mature_c_t_dry + 
            time_at_max * total_mature_c_t_dry),
        c_longterm_t = round(c_longterm_t, 3)
      )
    
    
    ### Calculate the C balance and credits earned from tree biomass -------------
    
    # select variables biomass calculations
    trees_managed_biomass <- trees_managed %>%
      rename(c_actual = total_actual_c_t_dry, c_longterm = c_longterm_t) %>%
      select(parcel_name, year, year_index, period, management, species, 
             planting_year, tree_index, tree_number, avg_DBH, c_actual, c_longterm)
    
    # Create a data frame with aggregated yearly sums
    # Note: aggregation is done at farm level (not parcel level) so parcels can 
    # compensate for each other when computing the long term C differences
    managed_biomass_year <- trees_managed_biomass %>%
      group_by(year, year_index, period) %>%
      summarise(
        c_actual = sum(c_actual),
        c_longterm = sum(c_longterm),
        .groups='drop')
    
    # Calculate the sum of actual C only for trees planted during the project years: this serves as a limit for credits
    managed_biomass_year_proj <- trees_managed_biomass %>% filter(planting_year >= start_year) %>%
      group_by(year, year_index, period) %>%
      summarise(c_actual_proj = sum(c_actual), .groups='drop')
    managed_biomass_year <- left_join(
      managed_biomass_year, managed_biomass_year_proj, by=c('year', 'year_index', 'period')
    )
    
    # Ensure all years are present and set any missing values to 0.
    managed_biomass_year <- left_join(
      periods %>% select(year, year_index, period), 
      managed_biomass_year, by=c('year', 'year_index', 'period')
    )
    managed_biomass_year <- managed_biomass_year %>% mutate(
      c_actual = if_else(is.na(c_actual), 0, c_actual),
      c_actual_proj = if_else(is.na(c_actual_proj), 0, c_actual_proj),
      c_longterm = if_else(is.na(c_longterm), 0, c_longterm)
    )

    # Set long-term projected baseline equal to the highest long-term value of historical baseline years (most conservative)
    managed_biomass_year <- managed_biomass_year %>% mutate(
      c_longterm_baseline = max(c_longterm[year_index<0])
    ) 
    
    # Keep only project years
    managed_biomass_year <- managed_biomass_year %>% filter(year_index>0)
    
    # Calculate the difference between baseline long term and 1. project long term and 2. project actual C stocks.
    # Limit any positive difference with actual to the actual from project year trees.
    managed_biomass_year <- managed_biomass_year %>% mutate(
      c_longterm_diff = c_longterm - c_longterm_baseline,
      c_actual_diff = pmin(c_actual_proj, c_actual - c_longterm_baseline))
    
    
    ## Calculate the project year's balance following methodology rules:
    managed_biomass_year$c_year_balance <- NA
    # For each year determine the difference between the long-term C stocks of the project and the baseline:
    # If the lt diff is zero or negative (baseline is higher), this is the current balance
    managed_biomass_year$c_year_balance[managed_biomass_year$c_longterm_diff <= 0] <- 
      managed_biomass_year$c_longterm_diff[managed_biomass_year$c_longterm_diff <= 0]
    # if the lt diff is positive, the balance is equal to the max of 0 and the c_actual_diff
    # Note: this does not limit the balance to the long term project stocks estimate, so credits can be generated beyond that value. Revise?
    managed_biomass_year$c_year_balance[managed_biomass_year$c_longterm_diff > 0] <- 
      pmax(0, managed_biomass_year$c_actual_diff[managed_biomass_year$c_longterm_diff > 0])
    # The realized removals or emissions are the difference between the balance in the current year and the previous year
    managed_biomass_year$c_longterm_diff_realized <- 
      c(managed_biomass_year$c_year_balance[1], diff(managed_biomass_year$c_year_balance))
    managed_biomass_year$co2_trees_managed <- managed_biomass_year$c_longterm_diff_realized * 44/12
    
    
    ### Calculate the soil C inputs for project and projected baseline ------
    
    # Debug code
    # trees_managed <- trees_managed %>% filter(parcel_name=="Barrada")
    
    # Create a data frame with variables for soil C calculations
    trees_managed_soil <- trees_managed %>% 
      rename(c_actual = total_actual_c_t_dry, c_longterm = c_longterm_t,
      ) %>%
      select(year, year_index, period, parcel_name, parcel_id, management, species,
             mature_tree_diam_cm, lifespan_years, r_s_ratio,
             planting_year, tree_number, avg_DBH, agb_actual, bgb_actual, 
             harvest_t_dry, harvest_t_ha_dry, residue_t_dry, residue_t_ha_dry, 
             dry_c, bg_turnover, leaflitter_factor, c_actual, c_longterm)
    
    ## Belowground Inputs
    # Calculate the long term root C stocks and litter inputs using the root to shoot ratios
    trees_managed_soil <- trees_managed_soil %>% mutate(
      c_roots = c_actual * (r_s_ratio/(r_s_ratio+1)),
      c_roots_longterm = c_longterm * (r_s_ratio/(r_s_ratio+1)),
      c_litt_bg_t = round(c_roots * bg_turnover, 3),
      c_litt_bg_t_lt = round(c_roots_longterm * bg_turnover, 3),
      c_litt_bg_t_lt = pmax(c_litt_bg_t, c_litt_bg_t_lt), # If the long term is lower than the actual, set equal to actual
      c_litt_ag_t = residue_t_dry * dry_c,
    ) %>% mutate_at(vars(c_roots, c_roots_longterm), .funs = ~round(., 3))

    # Aggregate at parcel level
    trees_soil_parcel <- trees_managed_soil %>% 
      group_by(parcel_name, year, year_index, period) %>%
      summarise(across(
        c("c_litt_bg_t", "c_litt_bg_t_lt", "harvest_t_dry", "harvest_t_ha_dry", 
          "residue_t_dry", "residue_t_ha_dry", "c_litt_ag_t"), 
        ~ sum(.x, na.rm = TRUE)
      ), .groups='drop')
    
    # Get litter from project year trees and merge
    trees_soil_parcel_proj <- trees_managed_soil %>% filter(planting_year >= start_year) %>%
      group_by(parcel_name, year, year_index, period) %>%
      summarise(c_litt_bg_t_p = sum(c_litt_bg_t), .groups='drop')
    trees_soil_parcel <- left_join(
      trees_soil_parcel, trees_soil_parcel_proj, by=c('parcel_name', 'year', 'year_index', 'period')
    )
    
    # Complete data frame with all years and parcels, replacing missing values with 0
    trees_soil_parcel <- left_join(
      all_year_parcels, trees_soil_parcel, 
      by=c('year', 'year_index', 'period', 'parcel_name')
    ) %>% replace_na(list(
      c_litt_bg_t = 0, c_litt_bg_t_lt = 0, harvest_t_dry = 0, harvest_t_ha_dry = 0,
      residue_t_dry = 0, residue_t_ha_dry = 0, c_litt_ag_t = 0, c_litt_bg_t_p = 0
    ))
    
    ## Determine the projected baseline litter inputs ----
    # First step: get litter inputs of the historical and long-term baseline.
    # Choosing the maximum of the baseline years is a conservative approach.
    # If the historical is larger, set the long-term baseline equal to the historical.
    trees_soil_parcel <- trees_soil_parcel %>% group_by(parcel_name) %>%
      mutate(
        c_litt_bg_t_baseline_hist_max = max(c_litt_bg_t[year_index<0]),
        c_litt_bg_t_lt_baseline = max(c_litt_bg_t_lt[year_index<0]),
        c_litt_bg_t_lt_baseline = max(c_litt_bg_t_lt_baseline, c_litt_bg_t_baseline_hist_max)  # This is probably redundant, given pmax setting above
      )
    # Second step: get the difference in long term values between baseline and project
    trees_soil_parcel <- trees_soil_parcel %>% mutate(
      c_litt_bg_t_lt_diff = c_litt_bg_t_lt - c_litt_bg_t_lt_baseline
    )
    # Third step, set baseline acccording to the case:
    #   1. difference is 0 or positive and the actual is below or equal to the baseline longterm -> set to actual
    #   2. difference is 0 or positive and the actual is above the baseline longterm -> set to actual minus project litter
    #   3. difference is negative -> set to actual plus a fraction proportional to the reduction in long term average
    trees_soil_parcel <- trees_soil_parcel %>% mutate(
      c_litt_bg_t_baseline = case_when(
        (c_litt_bg_t_lt_diff >= 0 & c_litt_bg_t <= c_litt_bg_t_lt_baseline) ~ c_litt_bg_t,
        (c_litt_bg_t_lt_diff >= 0 & c_litt_bg_t >  c_litt_bg_t_lt_baseline) ~ c_litt_bg_t - c_litt_bg_t_p,
        (c_litt_bg_t_lt_diff < 0) ~ c_litt_bg_t + c_litt_bg_t_baseline_hist_max * (-c_litt_bg_t_lt_diff/c_litt_bg_t_lt_baseline)
      ))

    # reset the projected baseline for the baseline years, so that it reflects the actual values
    trees_soil_parcel <- trees_soil_parcel %>% 
      mutate(c_litt_bg_t_baseline = if_else(year_index<0, c_litt_bg_t, c_litt_bg_t_baseline))
    
    ## Aboveground Inputs
    
    # No special treatment is given to the above-ground inputs, so the actual values are used
    # The assumption is made that reported values include all above-ground residues
    trees_soil_parcel <- trees_soil_parcel %>% mutate(
      c_litt_ag_t_baseline = mean(c_litt_ag_t[year_index<0])
    )
    
  }

  ### Calculate the C balance from felled trees biomass -------------
  
  if(nrow(trees_felled_raw)==0 | sum(!is.na(trees_felled_raw$species))==0) {
    
    felled_biomass_year <- tibble(
      year= periods$year, 
      year_index = periods$year_index,
      period = periods$period,
      c_felled_t = 0,
      co2_trees_felled = 0)
    
  } else {
    
    # Select only trees (always the case?) amd only project years (felled during baseline do not affect credits)
    trees_felled <- trees_felled_raw %>% filter(type == "tree" & year_index > 0)
    
    # Define data frame with all years and parcels
    all_year_parcels <- expand_grid(periods %>% select(year, year_index, period), 
                                    parcel_name = inputs_parcels_fixed$parcel_name)
    
    ## Calculate values for each tree cohort -----
    
    # Calculation of above and below-ground biomass
    trees_felled <- trees_felled %>% mutate(
      # calculate kilograms per tree
      agb_felled = get_agb(avg_DBH, a, b, c, d, e, f, j, k, cyp, cork, exp, poly),
      bgb_felled = get_bgb(avg_DBH, agb_felled, b_a, b_b, b_c, b_d, r_s_ratio, b_cork,
                           b_poly, b_others, b_common, b_exp, b_citrus),
      # restrict to maximum
      overshoot_ratio_felled = pmax(1, (agb_felled+bgb_felled) / max_biomass_kg),
      agb_felled = agb_felled / overshoot_ratio_felled,
      bgb_felled = bgb_felled / overshoot_ratio_felled,
      # total carbon per tree cohort: multiply by the number of trees and convert to tonnes of dry matter
      total_felled_c_t_dry = (agb_felled + bgb_felled) * dry_c * tree_number / 1000,
    ) %>%
      select(-c(a,b,c,d,e,f,j,k,exp,poly,quad,cyp,cork,b_poly,b_exp,b_common,
                b_others,b_citrus,b_cork,b_a,b_b,b_c,b_d,comment,equation_type,
                max_biomass_kg, overshoot_ratio_felled))
    # Round values
    trees_felled <- trees_felled %>% mutate_at(
      vars(agb_felled, bgb_felled, total_felled_c_t_dry), .funs = ~round(., 3)
    )
    
    # Calculate yearly total CO2 equivalents of felled trees
    felled_biomass_year <- trees_felled %>%
      group_by(year, year_index, period) %>%
      summarise(
        c_felled_t = sum(total_felled_c_t_dry),
        co2_trees_felled = c_felled_t * 44/12 * (-1),  # Convert C to CO2 and make negative (negative CO2 removals)
        .groups = 'drop'
      ) %>%
      mutate(
        c_felled_t = round(c_felled_t, 3),
        co2_trees_felled = round(co2_trees_felled, 3)
      )
  }
  
  ### Merge felled tree data with managed tree data ----
  tree_biomass_year <- full_join(
    managed_biomass_year,
    felled_biomass_year
  ) %>%
    mutate(
      c_felled_t = if_else(is.na(c_felled_t), 0, c_felled_t),
      co2_trees_felled = if_else(is.na(co2_trees_felled), 0, co2_trees_felled),
      co2_trees = co2_trees_managed + co2_trees_felled
    )
  
  # Add an uncertainty effect that reduces credits: equals a 10% reduction to CO2 values
  tree_biomass_year <- tree_biomass_year %>% mutate(
    co2_trees_uncert = co2_trees - abs(0.1 * co2_trees)
  )
  
  ### Return the results ----
  return(list(tree_biomass=tree_biomass_year, tree_soil_inputs=trees_soil_parcel))
}

# Functions to calculate above and belowground biomass
get_agb <- function(diam, a, b, c, d, e, f, j, k, use_cyp, use_cork, use_exp, use_poly) {
  
  agb_cyp <- (((exp(a + b * log(diam))) + (exp(c + d * diam^0.5)) + exp(e + f * diam^0.5)) * 1000) * use_cyp
  agb_cork <- ((exp(a)* diam ^ b) + (( exp(c) * diam^d ) * 2 ) + (exp(e)  * diam^f) + (exp(j) * diam^k)) * use_cork
  agb_exp <- ((exp(a + b * log(diam)^c ) * d)) * use_exp
  agb_poly <- ((a + b * (diam^j)^c) * (d^e) + f * diam) * use_poly
  
  agb <- agb_cyp + agb_cork + agb_exp + agb_poly
  
  agb <- if_else(is.na(agb), 0, agb)
  agb <- if_else(agb < 0, 0, agb)
  agb <- if_else(diam==0, 0, agb) # some equations might have non-zero intercepts
  
  return(agb)
}

get_bgb <- function(diam, agb, b_a, b_b, b_c, b_d, r_s_ratio, use_cork,
                    use_poly, use_others, use_common, use_exp, use_citrus) {
  
  bgb_cork <- (exp(b_b) * diam^b_c) * use_cork
  bgb_citrus <- (exp(b_a + b_b * log(agb)) * b_c ) * use_citrus
  bgb_exp <- ((exp(b_a + b_b * log(diam))^b_c + b_d)) * use_exp
  bgb_poly <- (b_a + b_b * diam ^ b_c + b_d) * use_poly 
  bgb_other <- (b_a + b_b * (agb^b_c) + b_d) * use_others
  bgb_common <- (agb * r_s_ratio) * use_common
  
  bgb_common <- if_else(is.na(bgb_common) & use_common==0, 0, bgb_common) # happens if r_s_ratio==NA
  
  bgb <- bgb_cork + bgb_citrus + bgb_exp + bgb_poly + bgb_other + bgb_common
  
  bgb <- if_else(is.na(bgb), 0, bgb)
  bgb <- if_else(bgb < 0, 0, bgb)
  bgb <- if_else(diam==0, 0, bgb) # some equations might have non-zero intercepts
  
  return(bgb)
}
