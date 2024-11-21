## UTILITY FUNCTIONS


#Functions for extracting LB2 Data from soil model results

#Project activity: Residue management
get_residue_management_data <- function(pasture_inputs,crop_inputs, parcel_inputs) {
  
  # Needs to be adapted depending on considering dry or fresh residues
  grass_residues = pasture_inputs$dry_residual
  grass_residues_input <- left_join(pasture_inputs, parcel_inputs, by = "parcel_ID")
  grass_residues_input <- grass_residues_input %>% mutate(residues_parcel = grass_residues*area) %>% group_by(scenario) %>% summarise(sum_grass_residues = sum(residues_parcel))
  # crop_inputs_new <- crop_inputs %>% group_by(scenario) %>% mutate(sum_year_residues = sum(dry_residual))
  # 
  crop_residues = crop_inputs$dry_residue
  crop_residues_input <- left_join(crop_inputs, parcel_inputs, by = "parcel_ID")
  crop_residues_input <- crop_residues_input %>% mutate(residues_parcel = crop_residues*area) %>% group_by(scenario) %>% summarise(sum_crop_residues = sum(residues_parcel))
  
  total_residues_input <- left_join(grass_residues_input,crop_residues_input, by = "scenario") %>% mutate(sum_residues = sum_grass_residues + sum_crop_residues)
  return(total_residues_input)
  
}

#residues = get_residue_management_data(pasture_inputs,crop_inputs, parcel_inputs)

#Project activity: Add manures
get_manure_data <- function(inputs_organicmatter, parcel_inputs) {
  
  # Needs to be adapted depending on considering dry or fresh residues
  total_orgamendments_table <- left_join(inputs_organicmatter, parcel_inputs, by = "parcel_ID")
  
  total_inputs_organicmatter <- total_orgamendments_table %>% filter(quantity_t_ha != 0) %>% group_by(scenario,source) %>% summarise(total_orgamendments = sum(quantity_t_ha*area/sum(area)))
  total_orgamendments_area <- total_orgamendments_table %>% filter(quantity_t_ha != 0) %>% group_by(scenario,source) %>% summarise(total_area = sum(area))
  #total_inputs_organicmatter <- total_inputs_organicmatter %>% mutate(quantity_parcel = quantity_t_ha*area) %>% group_by(scenario,source) %>% summarise(total_orgamendments = sum(quantity_t_ha))
  total_orgamendments_import_frac <- total_orgamendments_table %>% filter(quantity_t_ha != 0) %>% group_by(scenario,source) %>% summarise(average_imported_fraction= mean(imported_frac))
  
  total_inputs_organicmatter <- pivot_wider(total_inputs_organicmatter, names_from = source, values_from = total_orgamendments)
  total_orgamendments_area <- pivot_wider(total_orgamendments_area, names_from = source, values_from = total_area)
  total_orgamendments_import_frac <- pivot_wider(total_orgamendments_import_frac, names_from = source, values_from = average_imported_fraction)
  
  #total_orgamendments_area <- pivot_wider(total_orgamendments_area,names_from = source, values_from = total_area)
  total_orgamendments_table <- left_join(total_inputs_organicmatter,total_orgamendments_area, by = "scenario")
  total_orgamendments_table <- left_join(total_orgamendments_table,total_orgamendments_import_frac, by = "scenario")
  return(total_orgamendments_table)

}

#Project activity: Cover crops
get_cc_data <- function(crop_inputs, parcel_inputs) {
  total_cc_input <- left_join(crop_inputs, parcel_inputs, by = "parcel_ID")
  total_cc_input <- total_cc_input %>% filter(crop == "Generic Plant Mixture") %>% mutate(cc_parcel = dry_yield*area) %>% group_by(scenario) %>% summarise(total_cc = sum(cc_parcel))
}


# Function to get mean cover crops yields for a list of farmIds
get_cc_yield_list <- function(farmId_list){
  
  cc_yield_list = c()
  connection_string = init_data$connection_string_prod
  farms_collection = mongo(collection="farms", db="carbonplus_production_db", url=connection_string)
  for(farmId in farmId_list){
    farms_everything = farms_collection$find(paste('{"farmInfo.farmId":"',farmId,'"}',sep=""))
    landUseSummaryOrPractices = farms_everything$landUse$landUseSummaryOrPractices
    crop_inputs = data.frame(scenario = c(), parcel_ID = c(), crop = c(), dry_yield = c(), 
                             fresh_yield = c(), dry_grazing_yield = c(), fresh_grazing_yield = c(),
                             dry_residue = c(), fresh_residue = c(), 
                             dry_agb_peak = c(), fresh_agb_peak = c() )
    for (j in c(0:1)){ #years
      year_chosen = landUseSummaryOrPractices[[1]][[paste('year',j,sep="")]]
      for (i in c(1:length(landUseSummaryOrPractices[[1]]$parcelName))){
        if (year_chosen$landUseType[i]=="Arablecrops"){
          monthly_harvesting_yield = data.frame(crop=logical(12), 
                                                coverCrop=logical(12), 
                                                productiveFallow=logical(12),
                                                harvesting_yield=logical(12),
                                                residue_left=logical(12))
          # getting actual data
          monthly_harvesting_yield$crop = get_monthly_cash_crop(parcel_index = i, year_chosen)
          monthly_harvesting_yield$coverCrop = year_chosen$coverCropMonthlyData[[i]]
          monthly_harvesting_yield$productiveFallow = year_chosen$productiveFallow[[i]]
          monthly_harvesting_yield$grazing_yield = missing_to_zero(year_chosen$grazingYield[[i]])
          monthly_harvesting_yield$harvesting_yield = missing_to_zero(year_chosen$harvestYield[[i]])
          monthly_harvesting_yield$residue_left = missing_to_zero(year_chosen$estimationAfterResidueGrazingHarvest[[i]])
          # fresh or dry tOM/ha
          if (is.na(year_chosen$yieldsResiduesDryOrFresh[i])==TRUE){
            dryOrFresh = "Dry"
            log4r::info(my_logger, paste("CAUTION: dryOrFresh is NA in parcel ",landUseSummaryOrPractices[[1]]$parcelName[i],
                                         " for year ",j,". Was ASSUMED to be dry.", sep=""))
          } else {
            dryOrFresh = year_chosen$yieldsResiduesDryOrFresh[i]
          }
          # case of cash crop with no grazing
          for (crop_chosen in unique(monthly_harvesting_yield$crop)){
            if(is.na(crop_chosen)==TRUE){ # crop_chosen = NA, meaning no cash crop
              harvesting_yield = sum((monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$harvesting_yield)
              grazing_yield = sum(missing_to_zero((monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$grazing_yield))
              residue_left = sum((monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$residue_left)
              crop_inputs <- rbind(crop_inputs, 
                                   data.frame(scenario = c(paste('year',j,sep="")),
                                              parcel_ID = c(landUseSummaryOrPractices[[1]]$parcelName[i]),
                                              # assumed to be "generic grass" if no cash crop
                                              crop = "Non-N-fixing dry forages",# SHOULD WE DIFFERENTIATE PRODUCTIVE FALLOW AND COVER CROPS ?
                                              dry_yield = c(ifelse(dryOrFresh=="Dry", harvesting_yield,0)), 
                                              fresh_yield = c(ifelse(dryOrFresh=="Fresh", harvesting_yield,0)), 
                                              dry_grazing_yield = c(ifelse(dryOrFresh=="Dry", grazing_yield,0)), 
                                              fresh_grazing_yield = c(ifelse(dryOrFresh=="Fresh", grazing_yield,0)), 
                                              dry_residue = c(ifelse(dryOrFresh=="Dry", residue_left+grazing_yield*0.15,0)), 
                                              fresh_residue = c(ifelse(dryOrFresh=="Fresh", residue_left+grazing_yield*0.15,0)), 
                                              dry_agb_peak = c(ifelse(dryOrFresh=="Dry", max((monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$harvesting_yield+
                                                                                               missing_to_zero((monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$grazing_yield)+
                                                                                               (monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$residue_left),0)), 
                                              fresh_agb_peak = c(ifelse(dryOrFresh=="Fresh",  max((monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$harvesting_yield+
                                                                                                    missing_to_zero((monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$grazing_yield)+
                                                                                                    (monthly_harvesting_yield %>% filter(is.na(crop)==TRUE))$residue_left),0))))
            }
          }
        }
      }
    }
    if(nrow(crop_inputs)>0){
      crop_inputs = crop_inputs %>% mutate(cc_prod=dry_yield+dry_grazing_yield+dry_residue)
      cc_yield_list_farm = crop_inputs %>% filter(cc_prod>0)
      if(nrow(cc_yield_list_farm)>0){
        cc_yield_list = c(cc_yield_list,mean(na.omit(cc_yield_list_farm$cc_prod))) 
      }
    }
  }
  return(cc_yield_list)
}


# Funtion to write data frames in a list to csv files
write_list_to_csvs <- function(list, path = "output", prefix = "", simplify_names=F) {
  
  if (!dir.exists(here(path))) dir.create(here(path), recursive = TRUE)
  
  for (i in 1:length(list)) {
    out <- list[[i]]
    if(is.data.frame(out)) {
      name <- names(list)[i]
      if(simplify_names) {
        name <- gsub("inputs_", "", name)
      }
      write_csv(x = out, file = here(path, paste0(prefix, name, ".csv")))
    }
  }
}

# Function to write data frames in a list to a single xlsx file
write_list_to_xlsx <- function(data, path = "output", prefix = "combined", sideways=FALSE, indiv_sheets=TRUE, combined_workbook=FALSE) {
  # remove the file if it already exists
  fn_out <- here(path, paste0(prefix, '.xlsx'))
  if (file.exists(fn_out)) file.remove(fn_out)
  
  if (combined_workbook) {
    # create a file with today's date
    fn_comb <- here(path, paste0("../all_farm_data_", format(Sys.Date(), "%Y-%m-%d"), ".xlsx"))  # store it one directory higher
    if (!file.exists(fn_comb)) {
      wb_comb <- wb_workbook()
    } else {
      wb_comb <- wb_load(fn_comb)  # note this might have problems when running the code in parallel...
    }
    wb_comb$add_worksheet(prefix)
  }
  
  wb <- wb_workbook()
  wb$add_worksheet("all")
  ix_count <- 1
  for(name in names(data)) {
    out <- data[[name]]
    # if(nrow(out) == 0) next
    # take the "inputs_" off the start of the name (if necessary)
    name_short <- ifelse(grepl("inputs_", name), 
                         strsplit(name, "inputs_")[[1]][2], 
                         name)
    # remove any list-type columns from out (e.g. coordinates)
    out <- out %>% select_if(~!is.list(.))
    
    # write data to a new sheet
    if (indiv_sheets) {
      wb$add_worksheet(name_short)
      wb$add_data_table(name_short, out, with_filter=F)
      wb_save(wb, file = fn_out, overwrite = TRUE)
    }
    
    # add to the combined sheet at the start of the workbook
    if (sideways) {
      # for use in the excel model
      if (nrow(out) == 0) {
        out <- out[1,]  # initialise an NA row
      }
      out$input_type <- name_short
      out <- out %>% select(input_type, everything())
      wb$add_data_table("all", out, with_filter=F, start_row=1, start_col=ix_count, tableStyle = "TableStyleLight9", table_name=name_short)
      if (combined_workbook) {
        wb_comb$add_data(prefix, out, with_filter=F, start_row=1, start_col=ix_count)
      }
      ix_count <- ix_count + ncol(out) + 1
    } else {
      wb$add_data_table("all", data.frame(name=name_short), with_filter=F, start_row=ix_count, tableStyle = "TableStyleLight9")
      wb$add_data_table("all", out, with_filter=F, start_row=ix_count+2)
      ix_count <- ix_count + nrow(out) + 7
    }
    
    # save the workbook
    wb_save(wb, fn_out)
  }
  
  if (combined_workbook) {
    wb_save(wb_comb, fn_comb)
  }
  
}


clean_raw_inputs <- function(inputs_raw) {
  # Fertilizer
  inputs_raw$inputs_fertilizer <- inputs_raw$inputs_fertilizer %>% distinct() %>%
    mutate(farm_area = inputs_raw$inputs_farm_fixed$area_parcels,
      amount_per_ha = amount/farm_area)
  # Livestock
  inputs_raw$inputs_livestock_category <- inputs_raw$inputs_livestock_category %>%
    filter(amount > 0)
  inputs_raw$inputs_livestock_species <- inputs_raw$inputs_livestock_species %>% filter(grazing_days>0)
  inputs_raw$inputs_livestock_outfarm <- inputs_raw$inputs_livestock_outfarm %>% filter(amount>0)
  # Organic matter
  inputs_raw$inputs_organicmatter <- inputs_raw$inputs_organicmatter %>% filter(amount_t>0) %>%
    left_join(inputs_raw$inputs_parcel_fixed %>% select(parcel_name, area), by="parcel_name") %>%
    mutate(amount_per_ha = amount_t/area)
  # Monthly grazing
  inputs_raw$inputs_grazing_monthly <- inputs_raw$inputs_grazing_monthly %>% 
    group_by(parcel_name, year) %>% 
    summarise(months=paste0(month[was_grazed==TRUE], collapse=",")) %>%
    filter(months!="") %>%
    pivot_wider(id_cols = "parcel_name", names_from="year", values_from="months", names_prefix='y')
  # Tillage
  inputs_raw$inputs_tillage <- inputs_raw$inputs_tillage %>%
    mutate(month  = as.numeric(substr(date, 6, 7))) %>%
    select(-c(parcel_id, date))
  if (sum(!is.na(inputs_raw$inputs_tillage$month)) > 0 ) {
    inputs_raw$inputs_tillage <- inputs_raw$inputs_tillage %>% 
      filter(!is.na(month)) %>% 
      group_by(year, parcel_name, type) %>%
      summarise(months=paste0(month, collapse=",")) %>%
      filter(months!="") %>% 
      pivot_wider(id_cols=c('year', 'parcel_name'), names_from='type', values_from='months')
  }
  # irrigation
  inputs_raw$inputs_irrigation <- inputs_raw$inputs_irrigation %>%
    pivot_wider(id_cols = "parcel_name", names_from="year", values_from = "irrigation", names_prefix='y')
  
  # Annual crops & fallow
  inputs_raw$inputs_annualcrops <- inputs_raw$inputs_annualcrops %>% 
    select(-c(parcel_id, units))
  inputs_raw$inputs_annualcrops <- inputs_raw$inputs_annualcrops %>% 
    distinct() %>%
    left_join(inputs_raw$inputs_parcel_fixed %>% select(parcel_name, area), by="parcel_name") %>%
    mutate(amount_per_ha = amount/area) %>%
    pivot_wider(names_from="category", values_from = c("amount", "amount_per_ha", "type", "dry"), id_cols=c("year","parcel_name","crop_index","start_date","end_date","species","other"))
  # Trees & perennialcrops
  if (!(nrow(inputs_raw$inputs_perennialcrops) == 1 & sum(is.na(inputs_raw$inputs_perennialcrops$parcel_name))==1)) {
    inputs_raw$inputs_perennialcrops <- inputs_raw$inputs_perennialcrops %>% filter(!is.na(species)) %>% select(-c(parcel_id)) %>%
      left_join(inputs_raw$inputs_parcel_fixed %>% select(parcel_name, area), by="parcel_name") %>%
      mutate(trees_per_ha = tree_number/area)
  }
  if (!(nrow(inputs_raw$inputs_perennialprod) == 1 & sum(is.na(inputs_raw$inputs_perennialprod$parcel_name))==1)) {
    inputs_raw$inputs_perennialprod <- inputs_raw$inputs_perennialprod %>% filter(amount>0) %>%
      left_join(inputs_raw$inputs_parcel_fixed %>% select(parcel_name, area), by="parcel_name") %>%
      mutate(amount_per_ha = amount/area)
  }
  inputs_raw$inputs_trees_felled <- inputs_raw$inputs_trees_felled %>% filter(tree_number>0)
  # Pasture
  inputs_raw$inputs_pasture <- inputs_raw$inputs_pasture %>% filter(amount>0)
  # landuse
  inputs_raw$inputs_landuse <- inputs_raw$inputs_landuse %>% 
    select(parcel_name, year, primary_landuse) %>%
    pivot_wider(id_cols="parcel_name", names_from="year", values_from = "primary_landuse", names_prefix='y')
  
  # sort by parcel name (and year) in all data frames
  for (inp in names(inputs_raw)) {
    if ('parcel_name' %in% colnames(inputs_raw[[inp]])) {
      if ('year' %in% colnames(inputs_raw[[inp]])) {
        inputs_raw[[inp]] <- inputs_raw[[inp]] %>% arrange(parcel_name, year)
      } else {
        inputs_raw[[inp]] <- inputs_raw[[inp]] %>% arrange(parcel_name)
      }
    }
  }
  
  return(inputs_raw)
  
}

get_climate_periods <- function(climate_data, proj_start_year) {
  
  # Average of all past climate data since start for every month
  mean_past_climate <- climate_data %>% group_by(month) %>% 
    summarise(temperature=mean(temperature),
              precipitation=mean(precipitation),
              evap=mean(evap),
              pevap=mean(pevap))
  
  # Averaged recent climate (10 last years of data) for every month
  nr_cd <- nrow(climate_data) 
  i_cd <- nr_cd - (10*12)  # index for last 10 years of data
  
  mean_recent_climate <- climate_data[i_cd:nr_cd, ] %>% group_by(month) %>% 
    summarise(temperature=mean(temperature),
              precipitation=mean(precipitation),
              evap=mean(evap),
              pevap=mean(pevap))
  
  return(
    list(
      mean_past_climate = mean_past_climate,
      mean_recent_climate = mean_recent_climate
    )
  )
}
