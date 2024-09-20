library(tidyr)

get_Cinputs_organic_amendments <- function (inputs_organicmatter, periods, all_parcel_names) {
  ### Calculation of added organic amendments input: Carbon input due to manure/compost/hay daily spreading over a grazed field
  # total C inputs = sum(tonnes * carbon content)
  # also calculate the fraction of inputs relating to each DR ratio type (woody, annual crops, grassland, farmyard manure)
  C_inputs <- inputs_organicmatter %>%
    filter(amount_t>0) %>%
    mutate(tC = amount_t * carbon_content_fresh) %>%
    group_by(parcel_name, year, year_index, period) %>%
    summarise(tC_total = sum(tC, na.rm=T),
              frac_woody_dr = sum(tC[dr_ratio_type=="woody"], na.rm=T) / tC_total,
              frac_annualcrops_dr = sum(tC[dr_ratio_type=="annualcrops"], na.rm=T) / tC_total,
              frac_grassland_dr = sum(tC[dr_ratio_type=="grassland"], na.rm=T) / tC_total,
              frac_farmyard_manure_dr = sum(tC[dr_ratio_type=="farmyard_manure"], na.rm=T) / tC_total,
              source = 'organic_amendments', 
              .groups='keep') %>%
    rename(tC = tC_total)
  # to do: check that all "amount" is in tonnes
  log4r::warn(my_logger, 'issue in calc_functions_soil_modelling: what to do with organic amendments with unknown carbon content? Print a warning?')
  
  # expand to include all year/period combinations
  all_parcel_years <- expand_grid(parcel_name=all_parcel_names, periods)
  C_inputs <- left_join(all_parcel_years, C_inputs, by=c('parcel_name', 'year', 'year_index', 'period')) %>%
    mutate_at(c('tC', 'frac_woody_dr', 'frac_annualcrops_dr', 'frac_grassland_dr', 'frac_farmyard_manure_dr'), ~replace_na(.,0)) %>%
    mutate(source = 'organic_amendments')
  
  return(C_inputs)
}


get_Cinputs_livestock <- function (animal_inputs) {
  ### Calculation of animal input: Carbon input due to manure daily spreading over a grazed field
  C_inputs <- animal_inputs %>% 
    mutate(tC = amount_livestock_parcel * c_kg_per_year_per_animal * grazing_days / 365) %>%
    group_by(parcel_name, year, year_index, period) %>%
    summarise(tC = sum(tC, na.rm=T) * 1e-3,
              frac_woody_dr = 0,
              frac_annualcrops_dr = 0,
              frac_grassland_dr = 0,
              frac_farmyard_manure_dr = 1,
              source = 'livestock',
              .groups='keep')
  
  return(C_inputs)
}


get_Cinputs_trees <- function(tree_soil_inputs, include_tree_inputs){
  
  # if no trees: make zero-row dataframe
  if (nrow(tree_soil_inputs)==0 | !include_tree_inputs) {
    C_inputs <- tibble(.rows = 0)
    return(C_inputs)
  }
  
  # Aggregate tree inputs to parcel-year level
  C_inputs_wide <- tree_soil_inputs %>% 
    group_by(parcel_name, year, year_index, period) %>% 
    summarise(
      tC_ag_pr = sum(c_litt_ag_t),
      tC_bg_pr = sum(c_litt_bg_t),
      tC_ag_bl = sum(c_litt_ag_t_baseline),
      tC_bg_bl = sum(c_litt_bg_t_baseline),
      .groups='drop'
    )
  
  # Reshape to long format and calculate total C and fraction woody
  # assuming that the above-ground inputs are woody and the below-ground inputs have the "grassland" DR ratio
  C_inputs_pr <- C_inputs_wide %>% 
    select(parcel_name, year, year_index, period, tC_ag_pr, tC_bg_pr) %>%
    rename(tC_ag = tC_ag_pr, tC_bg = tC_bg_pr) %>%
    mutate(scenario = if_else(year_index > 0, 'project', 'baseline'))
  C_inputs_bl <- C_inputs_wide %>% 
    select(parcel_name, year, year_index, period, tC_ag_bl, tC_bg_bl) %>%
    rename(tC_ag = tC_ag_bl, tC_bg = tC_bg_bl) %>%
    mutate(scenario = 'projected_baseline') %>%
    filter(year_index>0)
  C_inputs <- bind_rows(C_inputs_pr, C_inputs_bl) %>% select(
    parcel_name, year, year_index, period, scenario, tC_ag, tC_bg
  ) %>% mutate(
    tC = tC_ag + tC_bg,
    frac_woody_dr = tC_ag / tC,
    frac_annualcrops_dr = 0,
    frac_grassland_dr = tC_bg / tC,
    frac_farmyard_manure_dr = 0,
    source = 'trees'
  ) %>% arrange(parcel_name, year_index) %>% select(-c(tC_bg, tC_ag))
  
  # Add rows for the average baseline year
  max_allow_na <- function(x) ifelse( !all(is.na(x)), max(x, na.rm=T), NA)
  bl_avg <- C_inputs %>%
    filter(year_index < 0) %>%
    group_by(parcel_name) %>%
    summarise(tC_mean = mean(tC, na.rm=T),
              frac_woody_dr = weighted.mean(frac_woody_dr, tC, na.rm=T),
              frac_annualcrops_dr = 0,
              frac_grassland_dr = weighted.mean(frac_grassland_dr, tC, na.rm=T),
              frac_farmyard_manure_dr = 0,
              source = 'trees',
              .groups='keep') %>%
    rename(tC = tC_mean) %>%
    mutate(year = max_allow_na(C_inputs$year[C_inputs$year_index<0]),
           year_index = 0,
           period = 'baseline',
           scenario = 'baseline_average') %>%
    select(colnames(C_inputs))
  C_inputs <- bind_rows(C_inputs, bl_avg) %>%
    arrange(parcel_name, year_index) %>%
    replace_na(list(frac_woody_dr = 0, frac_grassland_dr = 0))
  
  return(C_inputs)
}


# # YEARLY
# get_monthly_Cinputs_pasture <- function (pasture_inputs, factors_pastures, scenario_chosen, parcel, year, settings){
# 
#   pasture <- pasture_inputs %>% filter(scenario==scenario_chosen & parcel_ID==parcel)
#   
#   if(nrow(pasture)==0){ return(0) }
# 
#   annual_factors <- factors_pastures %>% filter(grass == 'Generic grasses', pasture_type == "annual")
#   perennial_factors <- factors_pastures %>% filter(grass == 'Generic grasses', pasture_type == "perennial")
# 
#   if(year > settings$curr_monit_year & settings$predict_amp_effect) {
#     annual_pastures <- 
#       merge(x = pasture, y = annual_factors, by = "grass", all.x = TRUE) %>% 
#       mutate(c_input_shoot = (dry_residue + dry_grazing * 0.15) * pasture_efficiency * AMP_baseline_factor * dry_c) %>%
#       mutate(c_input_root = dry_agb_peak * pasture_efficiency * AMP_baseline_factor * r_s_ratio * dry_c * bg_turnover) %>%
#       mutate(c_inputs = c_input_shoot + c_input_root)
#     
#     perennial_pastures <- 
#       merge(x = pasture, y = perennial_factors, by = "grass", all.x = TRUE) %>% 
#       mutate(c_input_shoot = (dry_grazing * 0.15 + dry_agb_peak * pasture_efficiency * AMP_baseline_factor * ag_turnover) * dry_c) %>%
#       mutate(c_input_root = dry_agb_peak * pasture_efficiency * AMP_baseline_factor * r_s_ratio * dry_c * bg_turnover) %>%
#       mutate(c_inputs = c_input_shoot + c_input_root)
#   } else {
#     annual_pastures <- 
#       merge(x = pasture, y = annual_factors, by = "grass", all.x = TRUE) %>% 
#       mutate(c_input_shoot = (dry_residue + dry_grazing * 0.15) * dry_c) %>%
#       mutate(c_input_root = dry_agb_peak * r_s_ratio * dry_c * bg_turnover) %>%
#       mutate(c_inputs = c_input_shoot + c_input_root)
#     
#     perennial_pastures <- 
#       merge(x = pasture, y = perennial_factors, by = "grass", all.x = TRUE) %>% 
#       mutate(c_input_shoot = (dry_grazing * 0.15 + dry_agb_peak * ag_turnover) * dry_c) %>%
#       mutate(c_input_root = dry_agb_peak * r_s_ratio * dry_c * bg_turnover) %>%
#       mutate(c_inputs = c_input_shoot + c_input_root)
#   }
# 
#   tC_inputs_per_ha_per_year <- sum(perennial_pastures$c_inputs * perennial_pastures$perennial_frac, na.rm=T) +
#     sum(annual_pastures$c_inputs * (1 - annual_pastures$perennial_frac), na.rm=T)
#   
#   return(tC_inputs_per_ha_per_year)
# }


get_Cinputs_crops_and_pasture <- function (inputs_productivity, inputs_fodder) {
  ### Calculation of crop input: Carbon input from cash crops and cover crops biomass turnover rates
  ### Calculation of pasture input: Carbon input from pasture biomass turnover
  # process the crops and pasture inputs (crop-level)
  C_inputs <- inputs_productivity %>% 
    mutate(c_shoot = (residue_t_dry + forage_residue_t_dry) * dry_c,
           c_root = prod_bg * dry_c,
           c_inputs = c_shoot*ag_turnover + c_root*bg_turnover) %>%
    # add the DR ratio information
    mutate(dr_ratio = case_when(
      landuse %in% c('annual crop', 'fallow') ~ 'annualcrops',
      landuse %in% c('pasture', 'shrub') ~ 'grassland'
    )) %>%
    # summarise from crop-level to parcel-level
    group_by(parcel_name, year, year_index, period, scenario) %>%
    summarise(tC = sum(c_inputs, na.rm=T), 
              frac_woody_dr = 0, # all crop/pasture inputs assumed non-woody
              frac_annualcrops_dr = sum(c_inputs[dr_ratio=="annualcrops"], na.rm=T) / tC,
              frac_grassland_dr = sum(c_inputs[dr_ratio=="grassland"], na.rm=T) / tC,
              frac_farmyard_manure_dr = 0,
              source = 'crops_and_pasture',
              .groups='keep')
  
  # add the fodder residues
  # calculate baseline averages for fodder, keep dry c
  fodder_bl_avg <- inputs_fodder %>%
    filter(year_index < 0) %>%
    group_by(parcel_name) %>%
    summarise(fodder_residue_t_dry = mean(fodder_residue_t_dry, na.rm=T),
              dry_c = mean(dry_c, na.rm=T))
  
  # join fodder residues to the C_inputs
  C_inputs <- left_join(C_inputs,
                        inputs_fodder %>% select(parcel_name, year, year_index, fodder_residue_t_dry, dry_c),
                        by = c('parcel_name', 'year', 'year_index')) %>%
    mutate(fodder_residue_t_dry = ifelse(scenario == "baseline_average", fodder_bl_avg$fodder_residue_t_dry[fodder_bl_avg$parcel_name == parcel_name], fodder_residue_t_dry),
           dry_c = ifelse(scenario == "baseline_average", fodder_bl_avg$dry_c[fodder_bl_avg$parcel_name == parcel_name], dry_c)) %>%
    replace_na(list(fodder_residue_t_dry = 0, dry_c = 0)) %>%
    mutate(tC_fodder = fodder_residue_t_dry * dry_c) %>%   # ag_turnover for fodder is assumed to be 1
    # modify the dr_ratio fractions, assuming that fodder has the "grassland" DR ratio
    mutate(frac_annualcrops_dr = (frac_annualcrops_dr*tC) / (tC + tC_fodder),
           frac_grassland_dr = (frac_grassland_dr*tC + 1*tC_fodder) / (tC + tC_fodder)) %>%
    mutate(tC = tC + tC_fodder) %>%
    select(-c(fodder_residue_t_dry, dry_c, tC_fodder))
  
  
  
  # OLD CALCULATIONS
  # crops <- crops %>%
  #   mutate(c_shoot = dry_residue * dry_c) %>%
  #   mutate(c_root = dry_agb_peak * dry_c * r_s_ratio) %>%
  #   mutate(c_inputs = c_shoot * s_turnover + c_root * r_turnover)
  # tC_inputs_per_ha_per_year = sum(crops$c_inputs)
  # annual_pastures <- 
  #   merge(x = pasture, y = annual_factors, by = "grass", all.x = TRUE) %>% 
  #   mutate(c_input_shoot = (dry_residue + dry_grazing * 0.15) * dry_c) %>%
  #   mutate(c_input_root = dry_agb_peak * r_s_ratio * dry_c * bg_turnover) %>%
  #   mutate(c_inputs = c_input_shoot + c_input_root)
  # perennial_pastures <- 
  #   merge(x = pasture, y = perennial_factors, by = "grass", all.x = TRUE) %>% 
  #   mutate(c_input_shoot = (dry_grazing * 0.15 + dry_agb_peak * ag_turnover) * dry_c) %>%
  #   mutate(c_input_root = dry_agb_peak * r_s_ratio * dry_c * bg_turnover) %>%
  #   mutate(c_inputs = c_input_shoot + c_input_root)
  
  return(C_inputs)
}

# get_Cinputs_perennials_trees <- function(inputs_perennials_trees) {
#   
# }

add_projected_baseline <- function(df_list, periods) {
  ## calculate projected baselines for the inputs that are not affected by the dynamic baseline
  C_inputs <- bind_rows(df_list) %>%
    mutate(scenario = period)
  # average the baseline years
  base <- C_inputs %>%
    filter(year_index < 0) %>%
    group_by(parcel_name, source) %>%
    summarise(tC_mean = mean(tC, na.rm=T), 
              frac_woody_dr = weighted.mean(frac_woody_dr, tC, na.rm=T),
              frac_annualcrops_dr = weighted.mean(frac_annualcrops_dr, tC, na.rm=T),
              frac_grassland_dr = weighted.mean(frac_grassland_dr, tC, na.rm=T),
              frac_farmyard_manure_dr = weighted.mean(frac_farmyard_manure_dr, tC, na.rm=T),
              .groups='keep') %>%
    rename(tC = tC_mean) %>%
    mutate(year = periods$year[periods$year_index==-1],
           year_index = 0,
           period = 'baseline',
           scenario = 'baseline_average') %>%
    select(colnames(C_inputs)) %>%
    # Remove NaNs from 0 division
    replace_na(list(frac_woody_dr = 0, frac_annualcrops_dr = 0, frac_grassland_dr = 0, frac_farmyard_manure_dr = 0))
  
  # then, copy this into all project years
  proj_base <- left_join(periods %>% filter(year_index>0), 
                         base %>% select(-c(year, year_index)) %>% mutate(period='project'),
                         by='period', relationship='many-to-many') %>%
    mutate(scenario = 'projected_baseline') %>%
    select(colnames(C_inputs)) %>%
    filter(!is.na(parcel_name)) # in case there is no baseline data, we end up with 3 blank rows
  
  # combine all three together
  C_inputs_long <- bind_rows(C_inputs, base, proj_base) %>% 
    arrange(parcel_name, year_index)
  
  return(C_inputs_long)
}


#### FUNCTIONS TO EXTRACT INFORMATION FOR THE SOIL MODEL FROM MODEL INPUTS ####

get_mean_dr_ratio <- function(inputs_irrigation, dr_ratio_annualcrops, dr_ratio_grassland) {
  # get the mean dr_ratio across all years in the data frame
  irrig_extract <- inputs_irrigation %>% pull(irrigation)
  mean_factor <- mean(ifelse(irrig_extract, dr_ratio_annualcrops, dr_ratio_grassland))
  
  return(mean_factor)  
}

get_mean_monthly_baresoil <- function(inputs_baresoil) {
  # take the monthly averages across the years in the dataframe, and set to TRUE if the majority were bare
  mean_bareground <- inputs_baresoil %>%
    group_by(month) %>% 
    summarise(bareground=mean(bareground)>0.5) %>%
    pull(bareground)
  
  return(mean_bareground)
}

get_mean_annual_tillage_factor <- function(inputs_tillage) {
  # take maximum tillage factor in each year, then average it
  mean_tillage <- inputs_tillage %>% 
    group_by(year) %>% 
    summarise(tillage_factor=max(tillage_factor)) %>%
    pull(tillage_factor) %>%
    mean()
  
  return(mean_tillage)
}

