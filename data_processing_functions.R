### Data processing functions
## Prepares data for cacluations. This includes:
# 1. Conversion of units
# 2. Merge with parameter data
# 3. Necessary pre-calculatons

process_livestock_inputs <- function(inputs_livestock_category, 
                                     inputs_livestock_outfarm, 
                                     factors_livestock,
                                     factors_methane) {
  
  factors_methane <-  factors_methane %>% select(species, ef_methane_manure)
  
  inputs_livestock_category <- left_join(inputs_livestock_category, factors_livestock, by = c("species", "db_reference")) %>%
    mutate(grazing_days = if_else(grazing_days > days_on_farm_per_year, days_on_farm_per_year, grazing_days),
           origin = "infarm")
  
  factors_livestock_outfarm <- factors_livestock %>% filter(is_generic)
  inputs_livestock_outfarm <- left_join(inputs_livestock_outfarm, factors_livestock_outfarm, by = "species") %>%
    mutate(grazing_days = if_else(grazing_days > days_on_farm_per_year, days_on_farm_per_year, grazing_days),
           origin = "outfarm") %>%
    mutate(days_on_farm_per_year = grazing_days)
  
  inputs_livestock <- bind_rows(inputs_livestock_category, inputs_livestock_outfarm) %>%
    left_join(factors_methane, by = "species") %>%
    select(year, year_index, period, origin, species, category, amount, grazing_days, mass_kg_per_animal, ef_enteric_fermentation_kg_head, n_excretion_rate_kg_1000am,     
           c_kg_per_year_per_animal, vs_kg_per_tonne_per_day, days_on_farm_per_year, dmi_kg_per_kg_per_day, ef_methane_manure, max_head_per_farm)

  return(inputs_livestock)
}


process_organicmatter_inputs <- function(inputs_organicmatter, inputs_parcel_fixed, factors_organicmatter, factors_area_calc) {
  
  inputs_organicmatter$sub_type <- if_else(inputs_organicmatter$type == "biochar", "generic biochar", inputs_organicmatter$sub_type)
  
  # change slurry from litres to tonnes
  inputs_organicmatter <- inputs_organicmatter %>%
    mutate(amount_t = if_else(type == "slurry", amount_t / 1000, amount_t),
           units = if_else(type == "slurry", "tonnes", units))
  
  # Scale amount by area factor
  inputs_organicmatter <- inputs_organicmatter %>%
    mutate(amount_t = amount_t * factors_area_calc$factor)
  
  #Values per hectare
  inputs_organicmatter <- inputs_organicmatter %>%
    left_join(select(inputs_parcel_fixed, c("parcel_name", "area")), by = c("parcel_name")) %>%
    mutate(amount_t_ha = amount_t / area)
  
  inputs_organicmatter <- left_join(inputs_organicmatter, factors_organicmatter, by = c("type", "sub_type"))
  
  return(inputs_organicmatter)
  
}

#IN-PROGRESS: baresoil and perennials still need to be added
#Implement checks and warnings in cases when land use data doesn't make sense, i.e. bare soil + pasture
process_landuse_inputs <- function(
    inputs_parcel_fixed,
    inputs_landuse,
    inputs_annualcrops,
    inputs_baresoil,
    inputs_pasture,
    inputs_perennials,
    periods) {
  
  # Function that combines all land use data, including:
  # - Arable crops and fallow
  # - Pasture
  # - Bare soil
  # - Perennials
  
  # The function also calculates the percent area for every land use type 
  # based on the given percent area of pasture and bare soil for each parcel

  # Add land use variable to each data frame
  inputs_annualcrops <- inputs_annualcrops %>% mutate(landuse = "annual crop")
  inputs_pasture <- inputs_pasture %>% mutate(landuse = "pasture")
  inputs_baresoil <- inputs_baresoil %>% mutate(landuse = "bare soil")
  inputs_perennials <- inputs_perennials %>% mutate(landuse = ifelse(type == "tree", "tree", subtype))
  
  # Select only the necessary columns
  inputs_annualcrops <- inputs_annualcrops %>% select(
    year, year_index, period, parcel_name, parcel_id, area, landuse,
    start_date, end_date, species, harvest_t_ha_dry, residue_t_ha_dry, 
    harvest_t_dry, residue_t_dry, crop_index, dry_c, r_s_ratio, 
    n_fixing_frac, n_ag, n_bg, ag_turnover, bg_turnover
  ) %>% ungroup()
  
  inputs_pasture <- inputs_pasture %>% select(
    year, year_index, period, parcel_name, parcel_id, area, landuse,
    start_date, end_date, species, percent_area, productivity_t_ha_dry, 
    harvest_t_ha_dry, productivity_t_dry, harvest_t_dry, dry_c, r_s_ratio,
    n_ag, n_bg, ag_turnover, bg_turnover, n_fixing_frac
  ) %>% ungroup()
  
  inputs_perennials <- inputs_perennials %>% select(
    year, year_index, period, parcel_name, parcel_id, area, landuse,
    start_date, end_date, tree_index, species, harvest_t_ha_dry, residue_t_ha_dry, harvest_t_dry, residue_t_dry,
    dry_c, r_s_ratio, leaflitter_factor, bg_turnover
  ) %>% ungroup() %>%
    mutate(landuse=as.character(landuse))

  # Prepare bare soil df
  inputs_baresoil <- left_join(
    inputs_baresoil, 
    select(inputs_parcel_fixed, c("parcel_name", "area")),  
    by = "parcel_name" 
    ) %>% select(-category) %>%
    # Convert string dates to date format
    mutate(start_date = as.Date(start_date, format = "%Y-%m-%d"),
           end_date = as.Date(end_date, format = "%Y-%m-%d"),
           # change end date to the last day of each month
           end_date = end_date + as.numeric(days_in_month(month(end_date))) - 1
           )

  ## Create meta data frame that combines all land use data
  # Bind all land use data
  inputs_landuse_temp <- bind_rows(
    inputs_annualcrops, inputs_pasture, inputs_perennials, inputs_baresoil
    ) %>% mutate(landuse = case_when(
      species == "fallow" ~ "fallow",
      .default = landuse
      ))

  # Join with land use data
  inputs_landuse_temp <- left_join(inputs_landuse, inputs_landuse_temp, by = c("year", "parcel_name"), suffix = c("", ".y")) %>%
    select(-ends_with(".y")) %>%
    mutate(primary_landuse = tolower(primary_landuse),
           secondary_landuse = tolower(secondary_landuse))
  
  ## Calculate percent area for all landuse types
  ## Prepare variables and necessary data frames
  years <- periods$year
  parcels <- inputs_parcel_fixed %>% select(parcel_name, parcel_id)
  years_parcels <- expand_grid(year = years, parcel_name = parcels) %>% jsonlite::flatten() %>% 
    rename(parcel_name=parcel_name.parcel_name, parcel_id = parcel_name.parcel_id)
  
  pasture_area_temp <- years_parcels %>% left_join(
    select(inputs_pasture, year, parcel_name, percent_pasture_area = percent_area),
    by = c("year", "parcel_name")) %>%
    mutate(percent_pasture_area = replace_na(percent_pasture_area, 0))
  
  # integrate the percent_pasture_area to calculate the percent_area for each land use type
  inputs_landuse_temp <- inputs_landuse_temp %>% 
    # Join pasture areas
    left_join(pasture_area_temp, by = c("year", "parcel_name", "parcel_id")) %>%
    # Calculate percent area for all land-use types
    mutate(
      percent_area = if_else(
        landuse == "pasture",
        percent_pasture_area, 
        100 - percent_pasture_area),
      frac_area = percent_area / 100,  # to use in the sense-checking later
      percent_area_check = if_else(
        landuse=="pasture", 
        percent_pasture_area, 
        percent_area),
      green = if_else(
        rowSums(
          across(c(harvest_t_ha_dry, residue_t_ha_dry, productivity_t_ha_dry)),
          na.rm = TRUE) > 0, TRUE, FALSE)
    ) %>% 
    select(-percent_pasture_area)
  
  ## Clean up
  # Remove empty rows from the data frame
  # if completely empty data frame (for i.e. bare soil) is extracted, it is removed from the data frame
  inputs_landuse_temp <- inputs_landuse_temp %>% filter(
    !is.na(parcel_name), !(is.na(species) & (landuse != "bare soil"))
    )

  inputs_landuse <- inputs_landuse_temp
  
  return(inputs_landuse)
}

process_grazing_inputs <- function(
    inputs_grazing_monthly, 
    inputs_parcel_fixed, 
    inputs_livestock, 
    inputs_organicmatter,
    inputs_landuse,
    inputs_npp,
    factors_others
) {
  
  ## Function that:
  # 1. calculates the grazing per parcel and year based on livestock numbers
  # 2. calculates the fodder and forage grazed and grazing residues per parcel
  
  # Definitions:
  # Fodder: hay and straw that was additionally applied to the parcel to feed livestock
  # Grazing: amount of biomass that is eaten by livestock
  # Forage: amount of growing biomass (on the parcel) that is eaten by the livestock
  # Grazing residues: amount of biomass that is left on the parcel after grazing
  
  # Constants
  fodder_res_frac <- factors_others$value[factors_others$name=='frf']
  forage_res_frac <- factors_others$value[factors_others$name=='grf']
  npp_max_frac <- factors_others$value[factors_others$name == "npp_max_frac"]
  avg_C_frac <- factors_others$value[factors_others$name == "avg_C_frac"]
  
  # initiate error messages
  error_messages <- c()
  
  ### Preparation of necessary data frames
  ## Extract parcel information and fodder information from organamendment inputs and fixed parcel inputs
  # Create final data frame for fodder inputs
  inputs_fodder_parcels_temp <- inputs_organicmatter %>%
    filter(type == "fodder") %>%
    rename(fodder_t = amount_t) %>%
    mutate(frac_imported = percent_imported / 100) %>%
    mutate(fodder_t_dry = fodder_t * dry) %>%
    mutate(fodder_available_t_dry = (1 - fodder_res_frac) * fodder_t_dry,
           fodder_residue_t_dry = fodder_t_dry - fodder_available_t_dry)
  
  ## Calculation of yearly amounts for grazing, fodder and forage
  ## Calculation is based on the yearly grazing amount and fodder amounts
  # Calculate total yearly grazing for in and out farm livestock based on livestock amounts and grazing days
  grazing_total_yearly <- inputs_livestock %>%
    select(year, category, amount, grazing_days, mass_kg_per_animal, dmi_kg_per_kg_per_day) %>%
    mutate(grazing_category_yearly_t_dry = amount * mass_kg_per_animal / 1000 * dmi_kg_per_kg_per_day * grazing_days) %>%
    group_by(year) %>%
    summarise(grazing_yearly_t_dry = sum(grazing_category_yearly_t_dry))
  
  # Calculate total yearly fodder based on amounts of fodder per parcel
  fodder_available_total_yearly <- inputs_fodder_parcels_temp %>%
    group_by(year) %>%
    summarise(fodder_available_yearly_t_dry = sum(fodder_available_t_dry))
  
  # Calculate total yearly forage that need to be distribute across parcels
  forage_total_yearly <- grazing_total_yearly %>% 
    left_join(fodder_available_total_yearly, by = "year") %>%
    mutate(forage_eaten_yearly_t_dry = if_else(grazing_yearly_t_dry >= fodder_available_yearly_t_dry, grazing_yearly_t_dry - fodder_available_yearly_t_dry, 0),
           forage_yearly_t_dry = forage_eaten_yearly_t_dry/(1-fodder_res_frac))
  
  # Calculate total ADDITIONAL fodder residues that need to be distributed across parcels
  fodder_yearly <- grazing_total_yearly %>% 
    left_join(fodder_available_total_yearly, by = "year") %>%
    mutate(fodder_eaten_yearly_t_dry = if_else(grazing_yearly_t_dry >= fodder_available_yearly_t_dry, fodder_available_yearly_t_dry, fodder_available_yearly_t_dry - grazing_yearly_t_dry)) %>%
    mutate(fodder_eaten_yearly_t_dry = if_else(fodder_eaten_yearly_t_dry < 0, 0, fodder_eaten_yearly_t_dry)) %>% 
    mutate(fodder_residues_add_yearly_t_dry = if_else(grazing_yearly_t_dry >= fodder_available_yearly_t_dry, 0, fodder_available_yearly_t_dry - grazing_yearly_t_dry))
 
   ### FODDER
  ## Create final data frame with all fodder information
  # Distribution of fodder eaten and left over fodder residues proportionally to the total fodder amount applied per year
  inputs_fodder_parcels <- inputs_fodder_parcels_temp %>%
    group_by(year) %>%
    mutate(fodder_sum = sum(fodder_t_dry)) %>%
    mutate(frac_fodder_residues = if_else(fodder_sum != 0, fodder_t_dry/fodder_sum, 0)) %>%
    select(-fodder_sum) %>%
    ungroup() %>%
    left_join(fodder_yearly, by = c("year")) %>%
    mutate(fodder_eaten_t_dry = fodder_eaten_yearly_t_dry * frac_fodder_residues) %>%
    mutate(fodder_residue_t_dry = fodder_residue_t_dry + fodder_residues_add_yearly_t_dry * frac_fodder_residues) %>%
    # Values per hectare
    mutate(fodder_t_ha_dry = fodder_t_dry/area,
           fodder_available_t_ha_dry = fodder_available_t_dry/area,
           fodder_eaten_t_ha_dry = fodder_eaten_t_dry/area,
           fodder_residue_t_ha_dry = fodder_residue_t_dry/area) %>%
    select(year, year_index, period, parcel_name, area, 
           fodder_t_dry, fodder_available_t_dry, fodder_eaten_t_dry, fodder_residue_t_dry, 
           fodder_t_ha_dry, fodder_available_t_ha_dry, fodder_eaten_t_ha_dry, fodder_residue_t_ha_dry, 
           percent_imported, frac_imported, carbon_content_fresh, dry, dry_c, max_t_ha)
  
  ### GRAZING & FORAGE
  ## Preparation of yearly and monthly grazing information for parcels and crops
  # Determine months that are grazed
  months_grazed <- inputs_grazing_monthly %>%
    mutate(grazing_date = as.Date(paste(year,"-", month,"-01", sep = ""), format = "%Y-%m-%d")) %>%
    select(year, parcel_name, was_grazed, grazing_date) %>%
    filter(was_grazed == TRUE)
  
  # Determine parcels that are grazed as well as the number of months grazed
  parcels_grazed <- inputs_grazing_monthly %>%
    group_by(year, parcel_name) %>%
    summarise(months_grazed = sum(was_grazed)) %>%
    mutate(was_grazed_parcel = if_else (months_grazed > 0, TRUE, FALSE))

  # Determine crops/pastures that are grazed
  cover_grazed <- left_join(inputs_landuse, parcels_grazed, by = c("year", "parcel_name")) %>%
    filter(was_grazed_parcel == TRUE, landuse %in% c("fallow", "annual crop", "pasture", "shrub")) %>%
    merge(months_grazed, by = c("year", "parcel_name")) %>%
    mutate(was_grazed_cover = if_else (grazing_date >= start_date & grazing_date <= end_date, TRUE, FALSE)) %>%
    group_by(year, year_index, period, parcel_name, landuse, area, months_grazed, crop_index, tree_index, species, 
             productivity_t_ha_dry, productivity_t_dry, harvest_t_dry, residue_t_dry, 
             frac_area, dry_c, r_s_ratio) %>%
    summarise(months_grazed_cover = sum(was_grazed_cover), .groups = "drop") %>%
    mutate(was_grazed_cover = if_else (months_grazed_cover > 0, 1, 0))
  
  # Raise error when grazing month are not covered
  # ISSUE: the following returns a warning about Returning more (or less) than 1 row per `summarise()`.... fix when there is time
  check_month_grazed <- cover_grazed %>%
    group_by(year, parcel_name, months_grazed) %>%
    reframe(months_grazed_cover = if_else(sum(months_grazed_cover) > months_grazed, months_grazed, sum(months_grazed_cover))) %>%
    mutate(months_missing = months_grazed - months_grazed_cover) %>%
    mutate(months_covered =  months_grazed - months_missing) %>%
    mutate(warn_category = if_else(months_covered == 0, "orange", if_else(months_missing > 0, "yellow", "green")))
  
  if (any(check_month_grazed$warn_category == "orange")) {
    msg1 <- paste0("yellow || science&farmer || Livestock grazing was reported in the following parcels, ",
                   "but the reported grazing months do not overlap with any valid landuse type (pasture, fallow, or annual crops). ",
                   "If livestock grazed on crop residues only, you can ignore this warning.",
                   "Otherwise, have you entered the correct grazing months, or not added pasture or annual crop or productive fallow periods? || ")
    msg2 <- paste0('\n Year: ', check_month_grazed$year[check_month_grazed$warn_category == "orange"], 
                   ' | Parcel: ', check_month_grazed$parcel_name[check_month_grazed$warn_category == "orange"])
    log4r::warn(my_logger, msg1)
    log4r::info(my_logger, msg2)
    error_messages <- c(error_messages, paste0(msg1, paste0(msg2, collapse='')))
  }
  if (any(check_month_grazed$warn_category == "yellow")) {
    
    msg1 <- paste0("yellow || science&farmer (low priority) || Livestock grazing was reported in the following parcels, ",
                   "but not all of the reported grazing months overlap with a valid landuse type (pasture, fallow, or annual crops) || ")
    msg2 <- paste0('\n Year: ', check_month_grazed$year[check_month_grazed$warn_category == "yellow"], 
                   ' | Parcel: ', check_month_grazed$parcel_name[check_month_grazed$warn_category == "yellow"])
    log4r::warn(my_logger, msg1)
    log4r::info(my_logger, msg2)
    error_messages <- c(error_messages, paste0(msg1, paste0(msg2, collapse='')))
  }
  # return error message if there is no grazable landuse in a year with reported grazing
  has_grazable_lu <- inputs_landuse %>%
    filter(landuse %in% c("pasture", "fallow", "annual crop", "shrub")) %>%
    group_by(parcel_name, year) %>%
    summarise(n=n(), .groups='drop')
  grazing_year_match <- parcels_grazed %>% 
    left_join(has_grazable_lu, by=c('parcel_name', 'year')) %>%
    filter(is.na(n), was_grazed_parcel)
  if (nrow(grazing_year_match>0)) {
    msg1 <- paste0("orange || farmer || Livestock grazing was reported in the following parcels, ",
                   "but there is no pasture, fallow, or annual crop in the same year. ",
                   "Please check the landuse data. || ")
    msg2 <- paste0('\n Year: ', grazing_year_match$year, 
                   ' | Parcel: ', grazing_year_match$parcel_name)
    log4r::warn(my_logger, msg1)
    log4r::info(my_logger, msg2)
    error_messages <- c(error_messages, paste0(msg1, paste0(msg2, collapse='')))
  }
  

  ## # FORAGE
  ## Distribution of FORAGE eaten across parcels and crops/pastures
  ## Distribute the yearly calculated forage to the different parcels and crops ---------
  # The forage per parcel is distributed proportionally to the expected productivity of the respective crop or pasture
  
  # Merge with NPP data to get the total NPP per year
  inputs_grazing_cover_temp <- as_tibble(cover_grazed) %>%
    filter(was_grazed_cover == TRUE) %>%
    left_join(select(inputs_npp, year, parcel_name, npp_total_tC_ha), by = c("year", "parcel_name"))

  # Get the number of grazable crops per parcel and year
  nr_crops_df <- inputs_landuse %>%
    filter(landuse %in% c("fallow", "annual crop", "shrub")) %>%
    group_by(year, parcel_name) %>%
    summarise(nr_crops = n())

  # Calculate expected total above-ground biomass production per crop/pasture
  # For pastures: total productivity entry for pastures, for crops: based on npp
  # Multiply by the area of the respective land use type (to account for several land use types in the same parcel)
  inputs_grazing_cover_temp <- inputs_grazing_cover_temp %>%
    left_join(nr_crops_df, by = c("year", "parcel_name")) %>%
    mutate(nr_crops = if_else(landuse == "pasture", 1, nr_crops)) %>%
    mutate(npp_crop_tC_ha = pmin(npp_total_tC_ha * npp_max_frac, npp_total_tC_ha/nr_crops)) %>%
    mutate(npp_ag_t_cover_dry = if_else(landuse == "pasture", 
                                        productivity_t_dry,
                                        npp_crop_tC_ha/avg_C_frac/(1 + r_s_ratio) * (area * frac_area)))

  # Calculate the potential forage per crop/pasture
  inputs_grazing_cover_temp <- inputs_grazing_cover_temp %>%
    # filter crops with perc area = 0
    filter(frac_area != 0) %>%
    mutate(forage_pot = if_else(landuse == "pasture", 
                                npp_ag_t_cover_dry - harvest_t_dry,
                                (npp_ag_t_cover_dry - harvest_t_dry - residue_t_dry) * was_grazed_cover))
  
  # Check if forage potential is negative and raise error
  if (any(inputs_grazing_cover_temp$forage_pot < 0)) {
    # if negative, sets forage potential to 0.05 * npp_ag_t_cover_dry
    msg <- paste0("yellow || science || harvest and/or residue amounts are greater than the (estimated) above-ground productivity.",
                    "Forage potential is set to 5% of the above-ground productivity. Check the input data. || ")
    msg2 <- paste0('\n Year: ', inputs_grazing_cover_temp$year[inputs_grazing_cover_temp$forage_pot < 0], 
                   ' | Parcel: ', inputs_grazing_cover_temp$parcel_name[inputs_grazing_cover_temp$forage_pot < 0], 
                   ' | Landuse: ', inputs_grazing_cover_temp$landuse[inputs_grazing_cover_temp$forage_pot < 0])
    log4r::warn(my_logger, msg)
    log4r::info(my_logger, msg2)
    error_messages <- c(error_messages, paste0(msg, paste0(msg2, collapse='')))
    
    inputs_grazing_cover_temp <- inputs_grazing_cover_temp %>%
      mutate(forage_pot = if_else(forage_pot < 0, 0.05 * npp_ag_t_cover_dry, forage_pot))
  } 

  # Group by year to calculate the fraction of forage potential per year
  inputs_grazing_cover_temp <- inputs_grazing_cover_temp %>%
    group_by(year) %>%
    mutate(forage_pot_frac = forage_pot/sum(forage_pot)) %>%  # issue: what if forage_pot is 0 in a given year?
    ungroup()

    # Check if total forage potential per year can cover the yearly forage required
  forage_potential_total_yearly <- inputs_grazing_cover_temp %>%
    group_by(year) %>%
    summarise(forage_pot_total = sum(forage_pot)) %>%
    ungroup() %>%
    left_join(select(forage_total_yearly, year, forage_yearly_t_dry), by = c("year")) %>%
    mutate(forage_covered = if_else(forage_pot_total < forage_yearly_t_dry, FALSE, TRUE))
  
  if(any(forage_potential_total_yearly$forage_covered == FALSE)) {
    msg <- "orange || science || The total forage potential per year is not sufficient to cover the yearly forage required. || "
    log4r::warn(my_logger, msg)
    error_messages <- c(error_messages, msg)
  }

  # Join with the forage
    inputs_grazing_cover_temp <- inputs_grazing_cover_temp %>%
      left_join(select(forage_total_yearly, year, forage_yearly_t_dry), by = c("year"))

    
  # Distribute forage across parcels and crops depending on the forage potential
  # Calculate forage residue and forage eaten
  inputs_grazing_cover_temp <- inputs_grazing_cover_temp %>%
    mutate(forage_necessary_t_dry = forage_yearly_t_dry * forage_pot_frac) %>%
    # Limit forage eaten to the forage potential
    mutate(forage_total_t_dry = if_else(forage_necessary_t_dry > forage_pot, forage_pot, forage_necessary_t_dry)) %>%
    mutate(forage_necessary_pot_diff_t_dry = forage_necessary_t_dry - forage_pot) %>%
    # Calculate forage residues, defined as a fraction of the TOTAL forage (ie forage_residue = forage_res_frac * forage_total)
    # need to rearrange this to be a function of forage eaten..
    mutate(forage_residue_t_dry = forage_total_t_dry * forage_res_frac) %>%
    # NOTE: "Forage" is misleading terminology in my opinion; it is actually the estimated above-ground biomass that comes on top of harvest and residues
    mutate(forage_eaten_t_dry = forage_total_t_dry - forage_residue_t_dry)
    
  # Values per hectare
  inputs_grazing_cover_temp <- inputs_grazing_cover_temp %>%
    mutate(forage_total_t_ha_dry = forage_total_t_dry/area,
           forage_residue_t_ha_dry = forage_residue_t_dry/area,
           forage_eaten_t_ha_dry = forage_eaten_t_dry/area)
    
  # Final data frame
  # Provides amount of forage for every crop/pasture grazed in every parcel
  inputs_grazing_cover <- inputs_grazing_cover_temp

  ## # GRAZING  
  ## Distribution of GRAZING across parcels
  inputs_grazing_parcels_temp <- inputs_grazing_cover %>%
    group_by(year, year_index, period, parcel_name) %>%
    # Sum of forage across all crops/pastures per parcel
    summarise(forage_total_t_dry = sum(forage_total_t_dry),
              forage_eaten_t_dry = sum(forage_eaten_t_dry),
              forage_residue_t_dry = sum(forage_residue_t_dry),
              .groups = 'keep') %>%
    ungroup() %>%
    # Join with the fodder inputs
    left_join(select(inputs_fodder_parcels, c("year", "parcel_name", "area", "fodder_eaten_t_dry")), by = c("year", "parcel_name")) %>%
    mutate(fodder_eaten_t_dry = if_else(is.na(fodder_eaten_t_dry), 0, fodder_eaten_t_dry)) %>%
    mutate(grazing_t_dry = forage_eaten_t_dry + fodder_eaten_t_dry) %>%
    # calculate the fraction of grazing per year
    group_by(year) %>%
    mutate(grazing_frac = grazing_t_dry/sum(grazing_t_dry)) %>%
    ungroup()
  
  # Final data frame
  # Provides forage and grazing amount per parcel to be used in the livestock function
  inputs_grazing_parcels <- inputs_grazing_parcels_temp
  

  # NOTE: To-Decide --> Do we still need to distribute grazing across months? 
  # ## Distribute yearly grazing per hectare to the respective months per parcel per year
  # inputs_grazing_monthly <- inputs_grazing_monthly %>%
  #   group_by(year, parcel_name) %>%
  #   mutate (number_grazing_months = sum(was_grazed)) %>%
  #   merge(grazing_parcels_yearly %>% select(parcel_name, year, grazing_ha, fodder_residue_ha, forage_total_ha, forage_residue_ha), by = c("parcel_name", "year")) %>%
  #   mutate(grazing_monthly_ha = ifelse(was_grazed == 1, grazing_ha / number_grazing_months, 0)) %>%
  #   mutate(fodder_residue_monthly_ha = ifelse(was_grazed == 1, fodder_residue_ha / number_grazing_months, 0)) %>%
  #   mutate(forage_monthly_ha = ifelse(was_grazed == 1, forage_total_ha / number_grazing_months, 0)) %>%
  #   mutate(forage_residue_monthly_ha = ifelse(was_grazed == 1, forage_residue_ha / number_grazing_months, 0)) %>%
  #   select(-grazing_ha,-forage_residue_ha,-fodder_residue_ha,-forage_total_ha) # maybe keep columns for understanding
  # 
  
  return(list(
    inputs_grazing_cover = inputs_grazing_cover,
    inputs_grazing_parcels = inputs_grazing_parcels,
    inputs_fodder_parcels = inputs_fodder_parcels,
    error_messages = error_messages
    #inputs_grazing_parcels_monthly = inputs_grazing_parcels_monthly
  ))
}


## Function to process fuel inputs
process_indirect_fuel_inputs <- function(inputs_fuel_indirect, factors_fuel_operations) {

  # Change missing area values to 0
  inputs_fuel_indirect <- inputs_fuel_indirect %>%
    mutate(area_amount = replace_na(area_amount, 0))
  
  # Merge service category with fuel operations parameters
  # just match with the service_category, as it is possible that the same service category is used for different services
  # (e.g., entered by agronomists for conservativeness)
  inputs_fuel_indirect <- left_join(inputs_fuel_indirect, factors_fuel_operations %>% select(-service), by = "service_category")
  
  # remove invalid area values
  inputs_fuel_indirect <- inputs_fuel_indirect %>%
    filter(!is.na(area_amount), area_amount>0)
  
  # Return a list with the direct and indirect fuel emissions
  return(inputs_fuel_indirect)
}

process_productivity_inputs <- function(inputs_landuse, inputs_grazing_cover, inputs_grazing_parcels) {

  ## Join land use data frame with forage data
  # we don't want the perennials to match with the grazing data (they have crop_index=NA, same as pasture),
  # so add a dummy column called "grazing_relevant" to exclude the perennials from matching
  inputs_landuse_temp <- inputs_landuse %>% 
    filter(landuse %in% c("pasture", "fallow", "annual crop", "shrub")) %>%
    left_join(inputs_grazing_cover %>% select(
                     c("year", "parcel_name", "species", "crop_index", "tree_index", "was_grazed_cover",
                       "forage_total_t_dry", "forage_eaten_t_dry", "forage_residue_t_dry",
                       "forage_total_t_ha_dry", "forage_eaten_t_ha_dry", "forage_residue_t_ha_dry")),
              by = c("year", "parcel_name", "crop_index", "tree_index", "species")) %>%
    # Replace NA values with 0
    mutate(forage_total_t_dry = ifelse(is.na(forage_total_t_dry), 0, forage_total_t_dry),
           forage_eaten_t_dry = ifelse(is.na(forage_eaten_t_dry), 0, forage_eaten_t_dry),
           forage_residue_t_dry = ifelse(is.na(forage_residue_t_dry), 0, forage_residue_t_dry),
           forage_total_t_ha_dry = ifelse(is.na(forage_total_t_ha_dry), 0, forage_total_t_ha_dry),
           forage_eaten_t_ha_dry = ifelse(is.na(forage_eaten_t_ha_dry), 0, forage_eaten_t_ha_dry),
           forage_residue_t_ha_dry = ifelse(is.na(forage_residue_t_ha_dry), 0, forage_residue_t_ha_dry))
  
  # Calculate above-ground productivity for all crops
  inputs_productivity_temp <- inputs_landuse_temp %>%
    filter(landuse != "bare soil") %>%
    # Above-ground productivity
    mutate(prod_ag = ifelse(landuse == "pasture", productivity_t_dry, harvest_t_dry + residue_t_dry + forage_total_t_dry)) %>%
    # Below-ground productivity
    mutate(prod_bg = prod_ag * r_s_ratio) %>%
    # Pasture residues
    mutate(residue_t_dry = if_else(landuse == "pasture", productivity_t_dry - harvest_t_dry - forage_total_t_dry, residue_t_dry),
           residue_t_dry = if_else (residue_t_dry <= 0, 0, residue_t_dry)) %>%
    # Values per hectare
    mutate(prod_ag_ha = prod_ag/area,
           prod_bg_ha = prod_bg/area,
           residue_t_ha_dry = residue_t_dry/area)

  # # create a unique land cover indicator (either crop/pasture/tree)
  # inputs_productivity_temp$land_cover_ix <- ""
  # crop_ix <- inputs_productivity_temp$landuse %in% c("annual crop", "fallow")
  # inputs_productivity_temp$land_cover_ix[crop_ix] <- paste0("crop", inputs_productivity_temp$crop_index[crop_ix])
  # pasture_ix <- inputs_productivity_temp$landuse=="pasture"
  # inputs_productivity_temp$land_cover_ix[pasture_ix] <- "pasture"
  # tree_ix <- inputs_productivity_temp$landuse=="perennials"
  # inputs_productivity_temp$land_cover_ix[tree_ix] <- paste0("tree", inputs_productivity_temp$tree_index[tree_ix])
  # if (sum(inputs_productivity_temp$land_cover_ix=="") > 0) {
  #   log4r::error(my_logger, "ERROR: unmatched land use types in productivity data")
  #   stop("ERROR: unmatched land use types in productivity data")
  # }
  
  inputs_productivity <- inputs_productivity_temp

  # Return the productivity inputs
  return(inputs_productivity)
}

process_livestock_parcels <- function(inputs_grazing_parcels, inputs_livestock, inputs_livestock_species) {
  
  # Calculate the amount of livestock per parcel based on the grazing fraction
  inputs_livestock_parcels_temp <- as_tibble(inputs_livestock) %>%
    merge(select(inputs_grazing_parcels, c("year", "parcel_name", "area", "grazing_frac")), by = "year") %>%
    # Number of livestock weighted by grazing per parcel
    mutate(amount_livestock_parcel = amount * grazing_frac, origin = origin) %>% 
    select(year, year_index, period, parcel_name, area, origin, species, category, amount_livestock_total = amount, grazing_frac, amount_livestock_parcel,
           grazing_days, c_kg_per_year_per_animal, vs_kg_per_tonne_per_day, n_excretion_rate_kg_1000am)

  
  # Combine in farm and out farm livestock inputs and add manure treatment information
  inputs_livestock_parcels <- inputs_livestock_parcels_temp %>%
    left_join(select(inputs_livestock_species, c("year","species", "manure_treatment")), by = c("species", "year")) %>%
    # Remove parcels with no livestock
    filter(amount_livestock_parcel > 0) 
  
  return(inputs_livestock_parcels)
}

## Process annual crops inputs
process_annualcrops_inputs <- function(inputs_annualcrops, inputs_parcel_fixed, factors_crops, factors_area_calc) {
  
  # Convert dates to date format, and change end_date to the last day of the month
  inputs_annualcrops_temp <- inputs_annualcrops %>%
    mutate(start_date = as.Date(start_date),
           end_date = as.Date(end_date),
           end_date = end_date + as.numeric(days_in_month(month(end_date))) - 1
           ) %>%
    rename(is_dry = dry)
  
  # Merge with crop factors
  # "generic plant mixture" is used for fallow and any kind of cover crops
  inputs_annualcrops_temp <- inputs_annualcrops_temp %>%
    mutate(db_reference = if_else(species == "fallow", "generic plant mixture", species)) %>%
    left_join(select(factors_crops, -crop, -type), by = "db_reference")
  
  # Convert amount fresh to dry matter
  inputs_annualcrops_temp <- inputs_annualcrops_temp %>%
    mutate(amount_t_dry = if_else(is_dry, amount, amount * dry))

  # Scale amount by area factor
  inputs_annualcrops_temp <- inputs_annualcrops_temp %>%
    mutate(amount_t_dry = amount_t_dry * factors_area_calc$factor)
  
  # Calculate amount per hectares
  inputs_annualcrops_temp <- inputs_annualcrops_temp %>%
    left_join(inputs_parcel_fixed, by = c("parcel_name", "parcel_id")) %>%
    mutate(amount_t_ha_dry = amount_t_dry/area)
  
  # Calculate total harvest and residue amounts
  inputs_annualcrops_temp <- inputs_annualcrops_temp %>%
    group_by(year, year_index, period, parcel_name, parcel_id, area, start_date, end_date, crop_index, species, category,
             n_fixing_frac, n_ag, n_bg, dry, r_s_ratio, ag_turnover, bg_turnover, dry_c, max_prod_ag_t_ha_fresh) %>%
    summarise(amount_t_dry = sum(amount_t_dry),
              amount_t_ha_dry = sum(amount_t_ha_dry), .groups="drop")
  
  # Build data frame in wide format
  inputs_annualcrops <- inputs_annualcrops_temp %>%
    pivot_wider(names_from = category, values_from = c(amount_t_dry, amount_t_ha_dry)) %>%
    select(year, year_index, period, parcel_name, parcel_id, area, start_date, end_date, crop_index, species, 
           harvest_t_dry = amount_t_dry_harvest, harvest_t_ha_dry = amount_t_ha_dry_harvest, 
           residue_t_dry = amount_t_dry_residue, residue_t_ha_dry = amount_t_ha_dry_residue, 
           n_fixing_frac, n_ag, n_bg, dry, r_s_ratio, ag_turnover, bg_turnover, dry_c, max_prod_ag_t_ha_fresh)
  
  # Replace NA residues with 0
  inputs_annualcrops <- inputs_annualcrops %>%
    mutate(harvest_t_dry = if_else(is.na(harvest_t_dry), 0, harvest_t_dry),
           harvest_t_ha_dry = if_else(is.na(harvest_t_ha_dry), 0, harvest_t_ha_dry),
           residue_t_ha_dry = if_else(is.na(residue_t_ha_dry), 0, residue_t_ha_dry),
           residue_t_dry = if_else(is.na(residue_t_dry), 0, residue_t_dry))
  
  # filter out invalid crop entries (these cannot be processed)
  # if no crops at all, we will have a single row with NA values
  if (nrow(inputs_annualcrops) > 1) {
    inputs_annualcrops <- inputs_annualcrops %>% 
      filter(!is.na(species))
  }
  
  return(inputs_annualcrops)
  
  
  #OLD FUNCTION FOR REFERENCE
  # get_crop_inputs <- function(monitoringData, parcel_inputs, factors_crops, grazing_yearly, grazing_monthly, periods){
  #   
  #   year_strings <- paste0("year", 0:10)
  #   parcel_names <- parcel_inputs$parcel_ID
  #   
  #   crop_inputs <- data.frame()
  #   
  #   for (j in c(0:10)){ # cycle through years
  #     
  #     year_str <- paste0('year', j)
  #     year_chosen = landUseSummaryOrPractices[[1]][[year_str]]
  #     
  #     for (i in c(1:length(parcel_names))){
  #       
  #       parcel <- parcel_names[i]
  #       
  #       # Excluding non-arable parcels: no project grazing compatible land-uses (no pasture efficiency coef will be used in crop inputs)
  #       if (!year_chosen$landUseType[i]=="Arablecrops") { next }
  #       
  #       # Creating the data frame storing monthly yield and residue
  #       monthly_harvest = data.frame(
  #         crop = rep(NA, 12), 
  #         coverCrop = rep(NA, 12), 
  #         productiveFallow = rep(NA, 12),
  #         harvest = rep(NA, 12),
  #         residue = rep(NA, 12)
  #       )
  #       
  #       # Getting input data (grazing was calculated previously)
  #       monthly_harvest$crop <- get_monthly_cash_crop(parcel_index = i, year_chosen)
  #       monthly_harvest$coverCrop <- year_chosen$coverCropMonthlyData[[i]]
  #       monthly_harvest$productiveFallow <- year_chosen$productiveFallow[[i]]
  #       monthly_harvest$grazing <- grazing_monthly$grazing_final[grazing_monthly$parcel == parcel & grazing_monthly$year == j]
  #       monthly_harvest$harvest <- missing_to_zero(year_chosen$harvestYield[[i]])
  #       monthly_harvest$residue <- missing_to_zero(year_chosen$estimationAfterResidueGrazingHarvest[[i]])
  #       
  #       # Check if input values are fresh or dry.
  #       # Fresh is interpreted as weight at harvest. Dry as fully dry.
  #       dryOrFresh <- year_chosen$yieldsResiduesDryOrFresh[i]
  #       if(is.null(dryOrFresh)) dryOrFresh <- NA
  #       if (!(dryOrFresh %in% c("Dry", "Fresh"))){
  #         if(i == 1) log4r::info(my_logger,  paste0("WARNING: dryOrFresh value not found for year ", j,". Assuming: Fresh."))
  #         dryOrFresh <- "Fresh"
  #       }
  #       
  #       # case of cash crop with no grazing
  #       for (crop_chosen in unique(monthly_harvest$crop)){
  #         
  #         if(is.na(crop_chosen)) {
  #           crop_monthly <- monthly_harvest %>% filter(is.na(crop))
  #           crop_chosen <- "Generic Plant Mixture"
  #         } else {
  #           crop_monthly <- monthly_harvest %>% filter(crop==crop_chosen)
  #         }
  #         
  #         yield_sums <- crop_monthly$harvest + crop_monthly$grazing + crop_monthly$residue
  #         harvest <- sum(crop_monthly$harvest)
  #         grazing <- sum(missing_to_zero(crop_monthly$grazing))
  #         residue_general <- sum(crop_monthly$residue)
  #         residue_grazing <- grazing * 0.15  # 15% considered dropped during grazing
  #         
  #         crop_inputs_temp <- data.frame(scenario = year_str,
  #                                        parcel_ID = parcel_names[i],
  #                                        crop = crop_chosen,
  #                                        harvest = harvest, 
  #                                        grazing = grazing, 
  #                                        residue = residue_general + residue_grazing,
  #                                        agb_peak = max(yield_sums),
  #                                        dry_fresh = dryOrFresh
  #         )
  #         
  #         # Merge crop with previous crops 
  #         crop_inputs <- rbind(crop_inputs, crop_inputs_temp)
  #       }
  #     }
  #   }
  #   
  #   # If arable parcels found, correct for dry weight
  #   if(length(crop_inputs) > 0) {
  #     crop_inputs <- merge(x = crop_inputs, y = factors_crops %>% select(crop, dw_fresh), by = "crop", all.x = TRUE)
  #     crop_inputs$dw_fresh[is.na(crop_inputs$dw_fresh)] <- factors_crops$dw_fresh[factors_crops$crop == "Other"] # Using option 'Other' if no crop match was found
  #     # Select the dw correction according to dry or fresh variable.
  #     crop_inputs$dry_weight <- 1
  #     ind <- crop_inputs$dry_fresh == "Fresh"
  #     crop_inputs$dry_weight[ind] <- crop_inputs$dw_fresh[ind]
  #     crop_inputs <- crop_inputs %>% mutate(
  #       dry_harvest = harvest * dry_weight,
  #       dry_grazing = grazing * dry_weight,
  #       dry_residue = residue * dry_weight,
  #       dry_agb_peak = agb_peak * dry_weight
  #     )
  #   } else { # In the case there weren't any arable parcels
  #     crop_inputs <- expand_grid(
  #       scenario = year_strings,
  #       parcel_ID = parcel_names,
  #       crop = "Generic Plant Mixture",
  #       harvest = 0, 
  #       grazing = 0, 
  #       residue = 0,  
  #       agb_peak = 0,
  #       dry_harvest = 0,
  #       dry_grazing = 0,
  #       dry_residue = 0,
  #       dry_agb_peak = 0
  #     )
  #   }
  #   
  #   # Set baseline to be equal to year0
  #   crop_inputs <- rbind(crop_inputs, crop_inputs %>% filter(scenario=='year0') %>% mutate(scenario='baseline'))
  #   
  #   return(crop_inputs)
  }

process_perennials_inputs <- function(inputs_perennialcrops,
                                      inputs_perennialprod,
                                      factors_perennials_trees,
                                      inputs_parcel_fixed,
                                      factors_area_calc) {

  ## Process perennial productivity inputs

  # Remove cases with no species information (normally should not be allowed in the DB)
  inputs_perennialprod_proc <- inputs_perennialprod %>%
    filter(!is.na(species))
  
  # Rename variables
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    rename(is_dry = dry, event_date = date)

  # Replace missing values with NA
  inputs_perennialprod_proc$amount <- if_else(inputs_perennialprod_proc$amount == -9999, 0, inputs_perennialprod_proc$amount)
  inputs_perennialprod_proc$event_date <- if_else(inputs_perennialprod_proc$event_date %in% c("", "-"), NA, inputs_perennialprod_proc$event_date)
  inputs_perennialprod_proc$type <- if_else(inputs_perennialprod_proc$type %in% c("", "-"), NA, inputs_perennialprod_proc$type)
  inputs_perennialprod_proc$units <- if_else(inputs_perennialprod_proc$units %in% c("", "-"), NA, inputs_perennialprod_proc$units)
  inputs_perennialprod_proc$species <- if_else(inputs_perennialprod_proc$species %in% c("", "-"), NA, inputs_perennialprod_proc$species)
  
  # Convert dates to date format
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>% mutate(event_date = as.Date(event_date))

  # Merge with factors
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    mutate(species = as.character(species)) %>% # convert "species" column to character (needed when nrow=0)
    left_join(factors_perennials_trees, join_by("species" == "db_reference")) %>%
    rename(type = type.x, plant_type = type.y, plant_subtype = subtype) 
  
  # Discard unnecessary columns
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>% 
    select(year, year_index, period, parcel_name, parcel_id, cohort_id, tree_index, event_date, species,
           category, type, amount, units,  is_dry,  tree_species, plant_type, 
           plant_subtype, dry, dry_c)

  # Convert amount fresh to dry matter
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    mutate(amount_t_dry = if_else(is_dry, amount, amount * dry))
  
  # Scale amount by area factor
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    mutate(amount_t_dry = amount_t_dry * factors_area_calc$factor)

  # Calculate amount per hectares
  inputs_perennialprod_proc$parcel_name <- as.character(inputs_perennialprod_proc$parcel_name)
  inputs_perennialprod_proc$parcel_id <- as.character(inputs_perennialprod_proc$parcel_id)
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    left_join(inputs_parcel_fixed, by = c("parcel_name", "parcel_id")) %>%
    mutate(amount_t_ha_dry = amount_t_dry/area)

  # Calculate total harvest and residue amounts
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    group_by(
      year, year_index, period, parcel_name, parcel_id, area, cohort_id, tree_index, tree_species, 
      species, category, plant_type, plant_subtype, dry, dry_c
      ) %>%
    summarise(amount_t_dry = sum(amount_t_dry),
              amount_t_ha_dry = sum(amount_t_ha_dry), .groups="drop")

  # Build data frame in wide format
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    mutate(category = factor(category, levels = c("harvest", "residue"))) %>% # to ensure harvest and residue columns are created
    pivot_wider(names_from = category, values_from = c(amount_t_dry, amount_t_ha_dry), names_expand=TRUE) %>%
    select(year, year_index, period, parcel_name, parcel_id, area, tree_index, cohort_id, species, 
           harvest_t_dry = amount_t_dry_harvest, harvest_t_ha_dry = amount_t_ha_dry_harvest, 
           residue_t_dry = amount_t_dry_residue, residue_t_ha_dry = amount_t_ha_dry_residue, 
           dry, dry_c)

  # Replace NA residues with 0
  inputs_perennialprod_proc <- inputs_perennialprod_proc %>%
    mutate(harvest_t_dry = if_else(is.na(harvest_t_dry), 0, harvest_t_dry),
           harvest_t_ha_dry = if_else(is.na(harvest_t_ha_dry), 0, harvest_t_ha_dry),
           residue_t_ha_dry = if_else(is.na(residue_t_ha_dry), 0, residue_t_ha_dry),
           residue_t_dry = if_else(is.na(residue_t_dry), 0, residue_t_dry))
  
  ## Process perennial crops inputs

  # Replace missing values with NA
  inputs_perennialcrops <- inputs_perennialcrops %>%
    mutate(species = if_else(species %in% c("", "-"), NA, species))
  
  inputs_perennialcrops_proc <- inputs_perennialcrops %>%
    filter(!is.na(species))

  # Scale amount by area factor
  inputs_perennialcrops_proc <- inputs_perennialcrops_proc %>%
    mutate(tree_number = round(tree_number * factors_area_calc$factor))

  # Merge with factors
  inputs_perennialcrops_proc <- inputs_perennialcrops_proc %>%
    mutate(species = as.character(species)) %>% # convert "species" column to character (needed when nrow=0)
    left_join(factors_perennials_trees, join_by("species" == "db_reference"))
  
  # Add start and end dates to cover entire year
  if (nrow(inputs_perennialcrops_proc) > 0) {
    inputs_perennialcrops_proc <- inputs_perennialcrops_proc %>%
      mutate(start_date = as.Date(paste(year, "-01-01", sep = "")),
             end_date = as.Date(paste(year, "-12-31", sep = "")))
  } else {
    inputs_perennialcrops_proc$start_date <- Date()
    inputs_perennialcrops_proc$end_date <- Date()
    inputs_perennialcrops_proc$parcel_name <- character()
    inputs_perennialcrops_proc$parcel_id <- character()
  }

  # Limit the tree diameter reported by the literature mature tree value
  inputs_perennialcrops_proc <- inputs_perennialcrops_proc %>% mutate(
    avg_DBH = pmin(avg_DBH, mature_tree_diam_cm),
  )
  
  ## Join the two perennials data frames
  inputs_perennials <- inputs_perennialcrops_proc %>% 
    full_join(inputs_perennialprod_proc)
  # For later cleanup, remove the columns that are not needed instead of selecting the ones that are needed
  # This avoids missing columns whenever something changes upstream
  # %>%
  #   select(
  #     year, year_index, period, parcel_name, parcel_id, 
  #     start_date, end_date, area, 
  #     cohort_id, tree_index, species, other, tree_species, type, subtype, 
  #     planting_year, tree_number, avg_DBH, lifespan_years, 
  #     harvest_t_dry, harvest_t_ha_dry, residue_t_dry, residue_t_ha_dry,
  #     r_s_ratio, ag_turnover, bg_turnover, dry, dry_c, 
  #     mature_tree_diam, mature_age_years, 
  #     equation_type, a, b, c, d, e, f, j, k, exp, poly, quad, 
  #     cyp, cork, b_poly, b_exp, b_common, b_others, 
  #     b_citrus, b_a, b_b, b_c, b_d 
  #   )
  
  inputs_perennials <- adjust_tree_index(inputs_perennials)
  ## Return results
  return(inputs_perennials)
}

process_trees_felled_inputs <- function(inputs_trees_felled, factors_perennials_trees) {

  # Check if inputs_trees_felled is empty
  if (nrow(inputs_trees_felled) == 0) {
    return(inputs_trees_felled)
  }

  # Merge with factors
  inputs_trees_felled_proc <- inputs_trees_felled %>%
    mutate(species = as.character(species)) %>%
    left_join(factors_perennials_trees, by = c("species" = "db_reference"))

  return(inputs_trees_felled_proc)
}
  
  ## Process pasture inputs
process_pasture_inputs <- function (inputs_pasture, inputs_parcel_fixed, factors_pasture, factors_others, factors_area_calc, new_grass_mixture) {
  
  if(new_grass_mixture){
    factors_pasture <- factors_pasture %>% filter(pasture_type == "annuals42%_perennials58%")
  } else {
    factors_pasture <- factors_pasture %>% filter(pasture_type == "annual")
  }
  
  # Merge with pasture factors
  # NOTE: TO-DISCUSS: Only perennial pastures?
  inputs_pasture_temp <- inputs_pasture %>%
    rename(is_dry = dry) %>%
    bind_cols(factors_pasture)
  
  # Convert to t/ha
  # Join wih parcel info and converst kg/m2 in t/ha; conversion factor: 10
  kgsqm2tha <- factors_others$value[factors_others$name == "kgsqm2tha"]
  inputs_pasture_temp <- inputs_pasture_temp %>%
    left_join(inputs_parcel_fixed, by = c("parcel_name", "parcel_id")) %>%
    mutate(amount_t = if_else(grepl("kg/m²", units), amount * kgsqm2tha * area * percent_area/100, amount))
  
  # Convert fresh to dry matter and calculate total productivity and harvest in case of multiple productivity or harvest entries
  inputs_pasture_temp <- inputs_pasture_temp %>%
    mutate(amount_t_dry = if_else(is_dry, amount_t, amount_t * dry))
  
  # Add up values in same category
  inputs_pasture_temp <- inputs_pasture_temp %>%
    select(-date, - units, - is_dry, -amount, -amount_t) %>%
    group_by(year, parcel_name, parcel_id, category) %>%
    mutate(amount_t_dry = sum(amount_t_dry)) %>%
    distinct(.keep_all = TRUE)
  
  inputs_pasture_temp <- inputs_pasture_temp %>%
    mutate(amount_t_ha_dry = amount_t_dry / area)
  
  # Create final data frame in wide format
  inputs_pasture_temp <- inputs_pasture_temp %>%
    mutate(start_date = as.Date(paste(year, "-01-01", sep = "")),
           end_date = as.Date(paste(year, "-12-01", sep = ""))) %>%
    pivot_wider(id_cols = c(year, year_index, period, parcel_name, parcel_id, area, percent_area, start_date, end_date, grass,
                            n_ag, n_bg, dry, r_s_ratio, dry_c, ag_turnover, bg_turnover, max_prod_ag_t_ha_fresh, n_fixing_frac), 
                names_from = category, values_from = c(amount_t_dry, amount_t_ha_dry)) %>%
    select(year, year_index, period, parcel_name, parcel_id, area,
           start_date, end_date, species = grass, percent_area,
           productivity_t_dry = amount_t_dry_productivity, productivity_t_ha_dry = amount_t_ha_dry_productivity,
           harvest_t_dry = amount_t_dry_harvest, harvest_t_ha_dry = amount_t_ha_dry_harvest,
           n_ag, n_bg, dry, r_s_ratio, dry_c, ag_turnover, bg_turnover, max_prod_ag_t_ha_fresh, n_fixing_frac)

  
  # Scale harvest amount by area factor
  inputs_pasture_temp$harvest_t_dry <- inputs_pasture_temp$harvest_t_dry * factors_area_calc$factor 
  inputs_pasture_temp$harvest_t_ha_dry <- inputs_pasture_temp$harvest_t_ha_dry * factors_area_calc$factor
  
  # Remove entries with productivity = 0
  inputs_pasture_temp <- inputs_pasture_temp %>%
    filter(productivity_t_dry > 0)
  
  # change the end dates to the last day of the month
  inputs_pasture_temp <- inputs_pasture_temp %>%
    mutate(end_date = end_date + as.numeric(days_in_month(month(end_date))) - 1)
  
  inputs_pasture <- inputs_pasture_temp
  
  return(inputs_pasture)
  
  # Takes landUseSummaryOrPractices from farms collection
  # Extracts yield and residues left on site when grazing happened
  
  # pasture_efficiency_potential_difference = unique(
  #   (grazing_factors %>% filter(pedo_climatic_area==farm_EnZ))$pasture_efficiency_potential_difference
  # )
  # 
  # # Dry weights
  # dw_dry <- 1  # dry fraction of fully dehydrated plant material
  # dw_fresh <- (factors_pastures %>% filter(grass == 'Generic annual grasses') %>% select(dw_fresh))[[1]]  # dry fraction of harvest weight
  # 
  # pasture_inputs = data.frame()
  # 
  # parcel_names <- parcel_inputs$parcel_ID
  # 
  # year_strings <- paste0('year', 0:10)
  # 
  # for (i in c(1:length(parcel_names))){
  #   
  #   parcel <- parcel_names[i]
  #   
  #   year0_is_AMP <- landUseSummaryOrPractices[[1]][['year0']]$adaptiveMultiPaddockGrazing[i]
  #   if(is.na(year0_is_AMP)) {year0_is_AMP <- FALSE} # Workaround if value is missing. Should not be allowed. To be enforced at data collection.
  #   
  #   year0_since_years <- landUseSummaryOrPractices[[1]][['year0']]$applyingThesePracticesInYears[i]
  #   
  #   if(year0_since_years==""){
  #     log4r::error(my_logger, "Number of years that practices have been applied until now is NOT entered.")
  #   } else {
  #     baseline_since_years = missing_to_zero(year0_since_years)
  #   }
  #   if(year0_is_AMP){
  #     AMP_years_baseline <- baseline_since_years
  #     AMP_years_current <- AMP_years_baseline
  #   } else {
  #     AMP_years_baseline <- 0
  #     AMP_years_current <- 0
  #   }
  #   
  #   years_lost_by_till = 3
  #   years_lost_by_mintill = 1
  #   
  #   for (j in c(0:10)){
  #     year_str <- year_strings[j+1]
  #     year_chosen <- landUseSummaryOrPractices[[1]][[year_str]]
  #     year_is_AMP <- year_chosen$adaptiveMultiPaddockGrazing[i]
  #     if(is.na(year_is_AMP)) {year_is_AMP <- FALSE} # Workaround if value is missing. Should not be allowed. To be enforced at data collection.
  #     
  #     if(j>0) { # Only apply to project years
  #       # Counting AMP years to calculate related efficiency
  #       # Efficiency is assumed to be reversible. If AMP is not happening, efficiency will go backward.
  #       # AMP related productivity benefits get penalized in case of till or minimum till
  #       till=unlist(year_chosen$tillingEvent[i])
  #       minTill=unlist(year_chosen$minimumTillingEvent[i])
  #       if(sum(till) > 0) { # in case of conventional tillage over AMP grassland 
  #         AMP_years_current <- AMP_years_current - years_lost_by_till
  #       } else if(sum(minTill) > 0) { # in case of minimum tillage over AMP grassland
  #         AMP_years_current <- AMP_years_current - years_lost_by_mintill
  #       } else {
  #         if(year_is_AMP) {
  #           AMP_years_current <- AMP_years_current + 1
  #         } else {
  #           AMP_years_current <- AMP_years_current - 1
  #         }
  #       }
  #       if(AMP_years_current < 0) {AMP_years_current <- 0}
  #     }
  #     
  #     # Calculation of pasture_efficiency: an index of enhanced productivity due to AMP grazing
  #     # Efficiency increases with time towards a plateau.
  #     # 0.36 factor allows to reach 2/3 of potential efficiency after 3 years of AMP
  #     pasture_efficiency <- 1 + pasture_efficiency_potential_difference *
  #       (1-exp(-0.36*AMP_years_current))
  #     AMP_baseline_factor <- 1 /  # This is used later to get the baseline productivity if AMP started in the past.
  #       (1 + pasture_efficiency_potential_difference * (1-exp(-0.36*AMP_years_baseline)))
  #     
  #     # Selecting the type of land use where grazing management affects pasture efficiency the most
  #     # Monthly yield and residue (to avoid double-counting we will only look at grasslands) Fernando: the condition below includes also woody crops.
  #     if (year_chosen$landUseType[i] == 'Arablecrops') { next }
  #     
  #     # monthly yield and residue
  #     monthly_nonarables <- data.frame(grazing=rep(NA,12), residue=NA, harvest=NA)
  #     monthly_nonarables$grazing <- grazing_monthly$grazing_final[grazing_monthly$parcel == parcel & grazing_monthly$year == j]
  #     monthly_nonarables$residue <- missing_to_zero(year_chosen$estimationAfterResidueGrazingHarvest[i][[1]])
  #     monthly_nonarables$harvest <- missing_to_zero(year_chosen$harvestYield[i][[1]])
  #     
  #     ### building df for C inputs calculation
  #     
  #     yield_sums <- monthly_nonarables$grazing+monthly_nonarables$residue+monthly_nonarables$harvest
  #     
  #     if (farm_EnZ == "Mediterranean north" | farm_EnZ == "Mediterranean south"){ # env zones with 2 grass growing seasons
  #       winter_months = c(1,2,3,4,5,11,12) # month index
  #       summer_months = c(6,7,8,9,10) # month index
  #       agb_peak <- max(yield_sums[winter_months]) + max(yield_sums[summer_months])
  #     } else { # assuming a single growing season
  #       agb_peak <- max(yield_sums)
  #     }
  #     
  #     # Get fresh or dry value.
  #     dryOrFresh <- year_chosen$yieldsResiduesDryOrFresh[i]
  #     if(is.null(dryOrFresh)) dryOrFresh <- NA
  #     if (!(dryOrFresh %in% c("Dry", "Fresh"))){
  #       if(i == 1) log4r::info(my_logger, paste0("WARNING: dryOrFresh value not found for year ", j,". Assuming Fresh."))
  #       dryOrFresh <- "Fresh"
  #     }
  #     if(dryOrFresh == 'Fresh') { dw <- dw_fresh } else { dw <- dw_dry }
  #     
  #     pasture_temp <- data.frame(scenario = c(paste0('year',j)),
  #                                parcel_ID = parcel_names[i], 
  #                                grass = "Generic grasses",
  #                                perennial_frac = AMP_years_current * 0.02,
  #                                n_fixing_frac = 0, # WARNING: TO BE AUTOMATED FOR CO2-EMISSION BALANCE 
  #                                grazing = sum(monthly_nonarables$grazing), 
  #                                residue = sum(monthly_nonarables$residue), 
  #                                harvest = sum(monthly_nonarables$harvest),
  #                                agb_peak = agb_peak, 
  #                                pasture_efficiency = pasture_efficiency,
  #                                AMP_baseline_factor = AMP_baseline_factor,
  #                                dry_weight = dw
  #     )
  #     
  #     pasture_inputs <- rbind(pasture_inputs, pasture_temp)
  #   }
  # }
  # 
  # # Calculate the dry weights
  # pasture_inputs <- pasture_inputs %>% mutate(
  #   dry_harvest = harvest * dry_weight,
  #   dry_grazing = grazing * dry_weight,
  #   dry_residue = residue * dry_weight,
  #   dry_agb_peak = agb_peak * dry_weight
  # )
  # 
  # if(length(pasture_inputs)==0) {
  #   pasture_inputs <- expand_grid(
  #     scenario = year_strings, parcel_ID = parcel_names, grass = "Generic grasses", perennial_frac = 0, 
  #     n_fixing_frac = 0, grazing = 0, residue = 0,
  #     harvest = 0, agb_peak = 0, dry_grazing = 0, dry_residue = 0,
  #     dry_harvest = 0, dry_agb_peak = 0, pasture_efficiency = 0, dry_weight =0
  #   )
  # }
  # 
  # # Set baseline to be equal to year0
  # pasture_inputs <- rbind(pasture_inputs, pasture_inputs %>% filter(scenario=='year0') %>% mutate(scenario='baseline'))
}

process_tillage_inputs <- function(inputs_tillage, factors_tillage, monitoringData, farm_EnZ, start_index, periods) {
  # create a dataframe displaying the monthly tillage events
  # based on the farmer-reported tillage events
  # note: this is very similar to the process_baresoil_inputs() function
  
  years <- monitoringData$yearlyFarmData$year
  parcels <- tibble(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  months <- seq(1,12)
  
  # create a dataframe with all possible combinations of parcels, years and months
  
  if (start_index == 1) {
    tillage <- tibble(expand_grid(parcel_id = parcels$parcel_id, year = years, month = months))
  } else {
    tillage <- tibble(expand_grid(parcel_id = parcels$parcel_id, year = years[-(1:(start_index-1))], month = months))
  }
  tillage$parcel_name <- parcels$parcel_name[match(tillage$parcel_id, parcels$parcel_id)]
  
  # add the tillage data
  tillage$tillage <- 'none'
  tillage$percent_tilled <- NA
  for (i in 1:nrow(inputs_tillage)) {
    till_event <- inputs_tillage[i,]
    
    # set the tillage info to TRUE for this month
    row_match <- which(tillage$parcel_id == till_event$parcel_id & 
                         tillage$year == as.numeric(substr(till_event$date, 1, 4)) & 
                         tillage$month ==  as.numeric(substr(till_event$date, 6, 7)))
    tillage$tillage[row_match] <- ifelse(till_event$type=='tillage', 'full', 'minimum')  # this assumes it is either full or minimum..
    tillage$percent_tilled[row_match] <- till_event$percent
  }
  
  # set the monthly tillage factors
  factors_enz <- factors_tillage %>% filter(pedo_climatic_area == farm_EnZ)
  tillage$tillage_factor <- 1
  tillage$tillage_factor[tillage$tillage=='full'] <- factors_enz$full_tillage_factor
  tillage$tillage_factor[tillage$tillage=='minimum'] <- factors_enz$minimum_tillage_factor
  
  # re-format the data frame
  tillage <- tillage %>% select(parcel_id, parcel_name, year, month, tillage, percent_tilled, tillage_factor) %>% 
    arrange(parcel_id, year, month)
  tillage <- left_join(tillage, periods, by='year')  # add the period info
  
  return(tillage)
}


process_baresoil_inputs <- function(inputs_baresoil, monitoringData, start_index, periods) {
  # create a dataframe displaying the soil cover in each month
  # based on the farmer-reported bare soil events
  # this is needed for the rothC model
  
  years <- monitoringData$yearlyFarmData$year
  parcels <- tibble(
    parcel_name = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelName,
    parcel_id   = monitoringData$yearlyFarmData$parcelLevelData[[start_index]]$parcelFixedValues$parcelID
  )
  months <- seq(1,12)
  
  # create a dataframe with all possible combinations of parcels, years and months
  if (start_index == 1) {
    bare_soil <- tibble(expand_grid(parcel_id = parcels$parcel_id, year = years, month = months))
  } else {
    bare_soil <- tibble(expand_grid(parcel_id = parcels$parcel_id, year = years[-(1:(start_index-1))], month = months))
  }
  bare_soil$parcel_name <- parcels$parcel_name[match(bare_soil$parcel_id, parcels$parcel_id)]
  
  # add the bareground periods
  bare_soil$bareground <- FALSE
  bare_soil$percent_area <- 0
  if (all(!is.na(inputs_baresoil))) {
    # remove those with no start/end date -- we cannot process these
    inputs_baresoil <- inputs_baresoil %>% 
      filter(!is.na(start_date) & !is.na(end_date)) %>%
      filter(start_date != "" & end_date != "")
    
    for (i in 1:nrow(inputs_baresoil)) {
      bare_event <- inputs_baresoil[i,]
      
      # extract the year-month pairs from this bare ground event (ie between the start and end dates)
      months_bare <- seq(as.Date(bare_event$start_date), as.Date(bare_event$end_date), by="month")
      months_bare <- tibble(
        'year'=as.numeric(format(months_bare, "%Y")),
        'month'=as.numeric(format(months_bare, "%m"))
      )
      
      # set the bare ground to TRUE for these months
      for (m in 1:nrow(months_bare)) {
        row_match <- which(bare_soil$parcel_id == bare_event$parcel_id & 
                             bare_soil$year == months_bare$year[m] & 
                             bare_soil$month == months_bare$month[m])
        bare_soil$bareground[row_match] <- TRUE
        bare_soil$percent_area[row_match] <- bare_event$percent_area
      }
    }
  }
  
  # re-format the data frame
  bare_soil <- bare_soil %>% select(parcel_id, parcel_name, year, month, bareground, percent_area) %>% 
    arrange(parcel_id, year, month)
  bare_soil <- left_join(bare_soil, periods, by='year')  # add the period info

  return(bare_soil)
}

process_npp_inputs <- function (inputs_npp) {
  
  # re-scale so 1 is the mean value
  inputs_npp$npp_index <- inputs_npp$npp_index / 100
  inputs_npp$npp_total_tC_ha <- inputs_npp$npp_total_kgC_ha/1000

  return(inputs_npp)
  
}

process_area_inputs <- function (inputs_parcel_fixed, inputs_farm_fixed) {

  area_diff <- inputs_farm_fixed$area_parcels - inputs_farm_fixed$area_farm
  if (area_diff > 0) {
    inputs_parcel_fixed_temp <- inputs_parcel_fixed %>% 
      mutate(area_fraction = area/inputs_farm_fixed$area_parcels,
             area_sub = area_fraction * area_diff,
             area_old = area,
             area = area_old - area_sub)
    
    inputs_parcel_fixed <- inputs_parcel_fixed_temp
    
    area_factor = tibble(factor = inputs_farm_fixed$area_farm / inputs_farm_fixed$area_parcels)

  } else {
    area_factor = tibble(factor = 1)
  }
  
  return(list(inputs_parcel_fixed = inputs_parcel_fixed, area_factor = area_factor))
}

adjust_tree_index <- function(perennials) {
  
  if(nrow(perennials) == 0) {
    return(perennials)
  }

  # Create an index unique for each tree cohort across the farm
  perennials <- perennials %>% 
    mutate(tree_index = NA)

  perennials <- perennials %>% 
    group_by(parcel_name, species, other, planting_year) %>% 
    mutate(cohort_id = cur_group_id()) %>% 
    ungroup()

  # Check if there are years where an index appears more than once (e.g. case when two cohorts have same species and planting_year)
  tree_index_count <- perennials %>% group_by(year, parcel_name, cohort_id) %>% summarise(n = n()) %>% filter(n > 1)
  if(nrow(tree_index_count > 0)) {
    log4r::warn(my_logger, paste0("The tree cohort ID " , tree_index_count$cohort_id, 
                                 " appears more than once in parcel ", 
                                 tree_index_count$parcel_name, " and year ",
                                 tree_index_count$year, "."))
  }
  
  return(perennials)
}

# Delete below commented code after testing above replacement function
# adjust_tree_index <- function(perennials) {
#   
#   if(nrow(perennials) == 0) {
#     return(perennials)
#   }
#   browser()
#   # adjust the tree index so that it is unique within each parcel
#   perennials <- perennials %>% mutate(tree_index = NA)
#   years <- unique(perennials$year)
#   parcels <- unique(perennials$parcel_name)
#   
#   # counter for the tree index
#   parcels_cohort_counter <- tibble(parcel_name = parcels, nr_cohorts = 0)
#   
#   for (y in 1:length(years)) {
#     perennials_y <- perennials %>% filter(year == years[y])
#     parcels_y <- unique(perennials_y$parcel_name)
#     
#     for (p in 1:length(parcels_y)) {
#       perennials_p <- perennials_y %>% filter(parcel_name == parcels_y[p])
#       
#       for (pc in 1:nrow(perennials_p)) {
#         # if(parcels_y[p] == "MP Montado SW_23") {
#         #   browser()
#         # }
#         if (y == 1) {
#           #if year is the first year
#           parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] <- parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] + 1
#           tree_index <- parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] # what is the "which" doing?
# 
#           # assing tree index
#           #trees
#           if(!is.na(perennials_p$tree_number[pc])){
#             if(!is.na(perennials_p$other[pc])){
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$species == perennials_p$species[pc] & 
#                                       perennials$planting_year == perennials_p$planting_year[pc] &
#                                       perennials$tree_number == perennials_p$tree_number[pc] & 
#                                       perennials$other == perennials_p$other[pc]] <- tree_index
#             } else {
#               if (parcels_y[p] == "MP Montado SW_23") {
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] &
#                                         perennials$tree_number == perennials_p$tree_number[pc] & 
#                                         perennials$lifespan_years == perennials_p$lifespan_years[pc]] <- tree_index
#               } else {
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] &
#                                         perennials$tree_number == perennials_p$tree_number[pc]] <- tree_index
#                 
#               }
#             }
#             
#             # non-trees
#           } else {
#             if(!is.na(perennials_p$other[pc])){
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$other == perennials_p$other[pc]] <- tree_index
#             } else {
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$species == perennials_p$species[pc]] <- tree_index
#             }
#           }
#           
#         } else {
#           
#           ## find matches based on the tree number 
#           # if "other" trees
#           if (!is.na(perennials_p$other[pc])) {
#             
#             # match 1: match based on tree number
#             # match 2: match not based on tree number
#             # mach 2: match non-trees
#             
#             # other trees
#             # trees with match via tree number
#             match1 <- filter(perennials,
#                              year == years[y-1],
#                              parcel_name == parcels_y[p],
#                              species == perennials_p$species[pc],
#                              type == "tree",
#                              other == perennials_p$other[pc],
#                              planting_year == perennials_p$planting_year[pc],
#                              tree_number == perennials_p$tree_number[pc]
#             )
#             
#             # trees with no match via tree number
#             match2 <- filter(perennials, 
#                              year == years[y-1], 
#                              parcel_name == parcels_y[p], 
#                              species == perennials_p$species[pc],
#                              type == "tree",
#                              other == perennials_p$other[pc],
#                              planting_year == perennials_p$planting_year[pc]
#             )
#             
#             # non-trees
#             match3 <- filter(perennials, 
#                              year == years[y-1], 
#                              parcel_name == parcels_y[p], 
#                              species == perennials_p$species[pc],
#                              type == "non-tree",
#                              other == perennials_p$other[pc]
#             )
#             
#             # if no "other"
#           } else {
#             
#             # trees with match via tree number
#             match1 <- filter(perennials, 
#                              year == years[y-1], 
#                              parcel_name == parcels_y[p], 
#                              species == perennials_p$species[pc],
#                              type == "tree",
#                              planting_year == perennials_p$planting_year[pc],
#                              tree_number == perennials_p$tree_number[pc]
#             )
#             
#             # trees with no match via tree number
#             match2 <- filter(perennials, 
#                              year == years[y-1], 
#                              parcel_name == parcels_y[p], 
#                              species == perennials_p$species[pc],
#                              type == "tree",
#                              planting_year == perennials_p$planting_year[pc]
#             )
#             
#             # non-trees
#             match3 <- filter(perennials, 
#                              year == years[y-1], 
#                              parcel_name == parcels_y[p], 
#                              species == perennials_p$species[pc],
#                              type == "non-tree"
#             )
#           }
#           
#           ## assigne tree index based on the match found
#           
#           # if no cohort was found in previous years
#           if (nrow(match1) == 0 & nrow(match2) == 0 & nrow(match3) == 0) {
#             
#             # increase nr_cohorts by 1 & set tree index
#             parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] <- parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] + 1
#             
#             tree_index <- parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]]
#             
#             # assign tree index
#             if(!is.na(perennials_p$tree_number[pc])) {
#               if(!is.na(perennials_p$other[pc])) {
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$tree_number == perennials_p$tree_number[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] & 
#                                         perennials$other == perennials_p$other[pc]] <- tree_index
#               } else {
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] & 
#                                         perennials$tree_number == perennials_p$tree_number[pc]] <- tree_index
#               }
#             } else {
#               if(!is.na(perennials_p$other[pc])) {
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$other == perennials_p$other[pc]] <- tree_index
#               } else {
#                 perennials$tree_index[perennials$year == years[y] &
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc]] <- tree_index
#               }
#             }
#             
#             # if tree match (match 1) was found
#           } else if (nrow(match1) == 1) {
#             
#             tree_index <- match1$tree_index
#             
#             # assign tree index
#             if(!is.na(perennials_p$other[pc])){
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$species == perennials_p$species[pc] & 
#                                       perennials$planting_year == perennials_p$planting_year[pc] &
#                                       perennials$tree_number == perennials_p$tree_number[pc] & 
#                                       perennials$other == perennials_p$other[pc]] <- tree_index
#             } else {
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$species == perennials_p$species[pc] & 
#                                       perennials$planting_year == perennials_p$planting_year[pc] &
#                                       perennials$tree_number == perennials_p$tree_number[pc]] <- tree_index
#             }
#             
#             # if other perennial plant match (match 3) was found
#           } else if (nrow(match3) == 1) {
#             tree_index <- match3$tree_index
#             if(!is.na(perennials_p$other[pc])){
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$species == perennials_p$species[pc] & 
#                                       perennials$other == perennials_p$other[pc]] <- tree_index
#             } else {
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$species == perennials_p$species[pc]] <- tree_index
#             }
#             
#             # if no tree match based on tree number was found
#             # possible reasons: decreased tree number
#           } else if (nrow(match1) == 0 & nrow(match2) == 1) {
#             # if dbh values do not differ more than 5 cm --> same tree index
#             if (abs(perennials_p$avg_DBH[pc] - match2$avg_DBH) < 5){
#               tree_index <- match2$tree_index
#               dbh <- perennials_p$avg_DBH[pc]
#               
#               if(!is.na(perennials_p$other[pc])){
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] &
#                                         perennials$avg_DBH == dbh & 
#                                         perennials$other == perennials_p$other[pc]] <- tree_index
#               } else {
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] &
#                                         perennials$avg_DBH == dbh] <- tree_index
#               }
#               
#             } else {
#               # else new tree index
#               parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] <- parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] + 1
#               tree_index <- parcels_cohort_counter$nr_cohorts[parcels_cohort_counter$parcel_name == parcels_y[p]] 
#               
#               if(!is.na(perennials$other[pc])){
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] &
#                                         perennials$tree_number == perennials_p$tree_number[pc] & 
#                                         perennials$other == perennials_p$other[pc]] <- tree_index
#               } else {
#                 perennials$tree_index[perennials$year == years[y] & 
#                                         perennials$parcel_name == parcels_y[p] & 
#                                         perennials$species == perennials_p$species[pc] & 
#                                         perennials$planting_year == perennials_p$planting_year[pc] &
#                                         perennials$tree_number == perennials_p$tree_number[pc]] <- tree_index
#               }
#             }
#             
#             # if two many matches were found, stop   
#           } else if (nrow(match1) > 1 | (nrow(match1) == 0 & nrow(match2) > 1) | nrow(match3) > 1) {
#             if (parcels_y[p] == "MP Montado SW_23") {
#               perennials$tree_index[perennials$year == years[y] & 
#                                       perennials$parcel_name == parcels_y[p] & 
#                                       perennials$species == perennials_p$species[pc] & 
#                                       perennials$planting_year == perennials_p$planting_year[pc] &
#                                       perennials$tree_number == perennials_p$tree_number[pc] & 
#                                       perennials$lifespan_years == perennials_p$lifespan_years] <- match1$tree_index
#             } else {
#               browser()
#             }
#           } else {
#             browser()
#           }
#         }
#       }
#     }
#   }
#   browser()
#   return(perennials)
# }