# functions relating to the dynamic baseline
library(dplyr)


scale_inputs_for_dynamic_baseline <- function(inputs_productivity, inputs_npp, inputs_farm_fixed, project_years) {
  ##### use the dynamic baseline indexes to modify the productivity values
  ##### Return the inputs object with modified inputs_productivity
  ##### This object contains
  #####   (a) a new "scenario" column, which indicates whether the measurement corresponds to {baseline, project, projected_baseline}
  #####   (b) modified productivity values in the baseline 
  #####   (c) extra rows representing the projected baseline scenario

  ### If the dynamic baseline is not activated, ste the index to 1 to keep original data
  if (!settings$dynamic_baseline) {
    inputs_npp$npp_index <- 1
  }
  
  ### 0. Prepare the data and define the columns that are impacted by the dynamic baseline
  # not all of these values are used, but here we amend everything that is conceptually affected
  prod <- left_join(inputs_productivity, inputs_npp[,c('year','npp_index','parcel_name')], by=c('year', 'parcel_name')) %>%
    select(-tree_index, -percent_area, -frac_area, -percent_area_check, -green, -was_grazed_cover)
  prod$scenario <- prod$period  # initialise the scenario column
  cols_to_amend <- c('harvest_t_dry', 'harvest_t_ha_dry',
                     'residue_t_dry', 'residue_t_ha_dry', 
                     'productivity_t_dry', 'productivity_t_ha_dry',
                     'prod_ag', 'prod_ag_ha',
                     'prod_bg', 'prod_bg_ha',
                     'forage_total_t_dry', 'forage_total_t_ha_dry',
                     'forage_eaten_t_dry', 'forage_eaten_t_ha_dry',
                     'forage_residue_t_dry', 'forage_residue_t_ha_dry'
                     )  

  ### 1. normalize the measured baseline data
  # --> divide by the productivity modifier 
  # (e.g. if the measured data was a wet year, the normalized value will be lower)
  # extracts all baseline data
  ix_baseline <- prod$period == 'baseline'
  # normalizes all the predefined productivity values (columns) 
  # that are affected by the dynamic baseline in all baseline years
  prod[ix_baseline, cols_to_amend] <- prod[ix_baseline, cols_to_amend] / prod$npp_index[ix_baseline]
  

  ### 2. create a projected baseline
  # --> multiply by the productivity modifiers in each year
  # (e.g. if the project experiences a wet year, the normalized projected baseline will be higher)

  # a) create a baseline average dataframe
  # to do this, we stack together all of the landcover instances in the baseline years
  # and divide each of the productivity values by the number of baseline years (e.g. 3)
  n_baseline_yrs <- 3
  bl_avg <- prod %>%
    filter(period == "baseline") %>%
    mutate_at(cols_to_amend, list(~./n_baseline_yrs)) %>%
    group_by(parcel_name, parcel_id, area, primary_landuse, secondary_landuse, landuse, species, start_date, end_date, 
             dry_c, r_s_ratio, n_fixing_frac, n_ag, n_bg, ag_turnover, bg_turnover) %>%
    summarise_at(cols_to_amend, ~sum(.)) %>%
    mutate(scenario = 'baseline_average',
           period = 'baseline',
           year_index = 0,
           year = as.numeric(inputs_farm_fixed$project_start_year) - 1)

  # create a unique crop index for the stacked baseline average data (incl. pasture) for each parcel
  bl_avg <- bl_avg %>% group_by(parcel_name) %>% mutate(crop_index = row_number())
  
  # b) create a projected baseline dataframe
  # to do this, we loop over all project years
  # and for each project year, multiply the productivity values by the productivity index
  
  proj_bl <- list()
  for (proj_yr in project_years) {
    npp_yr <- inputs_npp %>% filter(year==proj_yr) %>% select(parcel_name, npp_index)
    proj_bl[[toString(proj_yr)]] <- bl_avg %>%
      left_join(npp_yr, by='parcel_name') %>%
      mutate(year = proj_yr,
             period = "project",
             year_index = proj_yr - as.numeric(inputs_farm_fixed$project_start_year) + 1,
             scenario = 'projected_baseline') %>%
      mutate_at(cols_to_amend, list(~.*npp_index))
  }
  # combine all years
  proj_bl <- bind_rows(proj_bl)
  

  ### 3. combine the projected baseline and average baseline with the original data
  prod <- bind_rows(prod, proj_bl, bl_avg) %>%
    arrange(parcel_name, year_index, scenario, crop_index)
  
  # return
  return(prod)
}

# Commented out as assumed not needed: move data consistency checks to data processing step
# perennials_projected_baseline <- function(perennials, inputs_farm_fixed, project_years) {
#   ## create a projected baseline for perennial crops that are present in the baseline
#   ## assume that their DBH matches the DBH of the same trees in the project years
#   
#   # add a scenario column
#   perennials$scenario <- perennials$period
#   
#   # only operate on perennials with valid DBH (i.e., exclude herbs etc)
#   # as these are the only ones that affect soil C inputs
#   perennials <- perennials %>% filter(!is.na(species), !is.na(avg_DBH))
#   
#   # identify the tree cohorts in the final year of the baseline
#   bl <- perennials %>%
#     filter(year_index == -1)
#   
#   # create a projected baseline for each project year
#   comb <- list()
#   for (proj_yr in project_years) {
#     # find the project trees in this year
#     proj_trees <- perennials %>%
#       filter(year == proj_yr) %>%
#       select(parcel_name, species, tree_index, avg_DBH, tree_number, start_date, end_date) %>%
#       rename(DBH_project = avg_DBH)
# 
#     # join with the baseline trees
#     proj_bl <- bl %>% 
#       select(-c(start_date, end_date)) %>%
#       left_join(proj_trees %>% select(-tree_index), 
#                 by=c('parcel_name', 'species', 'tree_number')) # 'tree_index' <-- excluding this, to allow for changing order of entry in the dashboard...
# 
#     # check for unmatched trees (with DBH>0 in baseline but not project)
#     failed_checks <- is.na(proj_bl$DBH_project) & !is.na(proj_bl$avg_DBH) & proj_bl$avg_DBH>0
#     if (sum(failed_checks) > 0) {
#       # stop("Error: Trees in the baseline were not found in the project years")
#       # do a less strict match with the species only (e.g., if the farmer has felled trees so the number changes)
#       proj_bl2 <- bl[failed_checks,] %>%
#         select(-c(start_date, end_date)) %>%
#         left_join(proj_trees %>% select(-c(tree_index, tree_number)), by=c('parcel_name', 'species'))
#       proj_bl[failed_checks,] <- proj_bl2 # join into the main one
#       # run the checks again
#       failed_checks2 <- is.na(proj_bl$DBH_project) & !is.na(proj_bl$avg_DBH) & proj_bl$avg_DBH>0
#       if (sum(failed_checks2) > 0) {
#         # still no match? Trees were felled.
#         # --> set the DBH for the projected baseline as the most recent DBH
#         needs_a_match <- proj_bl[failed_checks2,]
#         most_recent_dbh <- perennials %>%
#           filter(!is.na(avg_DBH)) %>%
#           filter(parcel_name %in% needs_a_match$parcel_name,
#                  species %in% needs_a_match$species,
#                  tree_number %in% needs_a_match$tree_number) %>%
#           filter(year < proj_yr) %>%
#           group_by(parcel_name, species, tree_number) %>%
#           summarise(most_recent_year = max(year), # find the most recent year with a dbh
#                     .groups = 'drop')
#         # add the DBH from these years
#         most_recent_dbh <- left_join(most_recent_dbh, 
#                                      perennials %>% select(parcel_name, species, tree_number, avg_DBH, year), 
#                                      by=c('parcel_name', 'species', 'tree_number', 'most_recent_year'='year')) %>%
#           rename(DBH_project = avg_DBH)
#         # create new row(s) for the failed data
#         failed_data <- proj_bl[failed_checks2,]
#         failed_data$DBH_project <- NULL
#         failed_data <- left_join(failed_data,
#                                  most_recent_dbh %>% select(-most_recent_year),
#                                  by=c('parcel_name', 'species', 'tree_number')) %>%
#           select(colnames(proj_bl))
#         proj_bl[failed_checks2,] <- failed_data
#         
#         # final check -- this should not fail
#         failed_checks3 <- is.na(proj_bl$DBH_project) & !is.na(proj_bl$avg_DBH) & proj_bl$avg_DBH>0
#         if (sum(failed_checks3) > 0) {
#            browser()
#         }
#       }
#     }
#     
#     # set the DBH to the project DBH and reformat
#     proj_bl <- proj_bl %>%
#       select(-avg_DBH) %>%
#       mutate(year = proj_yr,
#              year_index = proj_yr - as.numeric(inputs_farm_fixed$project_start_year) + 1,
#              period="project",
#              scenario = "projected_baseline") %>%
#       rename(avg_DBH = DBH_project) %>%
#       select(colnames(perennials))
#     
#     # add to the overall list
#     comb[[toString(proj_yr)]] <- proj_bl
#   }
#   
#   # add the projected baseline to the overall perennials data frame
#   proj_bl_all <- bind_rows(comb) %>% as_tibble()
#   perennials_comb <- bind_rows(perennials, proj_bl_all)
# 
#   return(perennials_comb)
# }