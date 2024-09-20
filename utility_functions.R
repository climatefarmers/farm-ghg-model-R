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
write_list_to_xlsx <- function(data, path = "output", prefix = "combined", sideways=FALSE) {
  # remove the file if it already exists
  fn_out <- here(path, paste0(prefix, '.xlsx'))
  if (file.exists(fn_out)) file.remove(fn_out)
  
  wb <- wb_workbook()
  wb$add_worksheet("all")
  ix_count <- 1
  for(name in names(data)) {
    out <- data[[name]]
    # if(nrow(out) == 0) next
    # take the "inputs_" off the start of the name (if necessary)
    name_short <- ifelse(grepl("inputs", name), 
                         strsplit(name, "inputs_")[[1]][2], 
                         name)
    # remove any list-type columns from out (e.g. coordinates)
    out <- out %>% select_if(~!is.list(.))
    
    # write data to a new sheet
    wb$add_worksheet(name_short)
    wb$add_data_table(name_short, out, with_filter=F)
    wb_save(wb, file = fn_out, overwrite = TRUE)
    
    # add to the combined sheet at the start of the workbook
    if (sideways) {
      # for use in the excel model
      if (nrow(out) == 0) {
        out <- out[1,]  # initialise an NA row
      }
      out$input_type <- name_short
      out <- out %>% select(input_type, everything())
      wb$add_data_table("all", out, with_filter=F, start_row=1, start_col=ix_count, tableStyle = "TableStyleLight9", table_name=name_short)
      ix_count <- ix_count + ncol(out) + 1
    } else {
      wb$add_data_table("all", data.frame(name=name_short), with_filter=F, start_row=ix_count, tableStyle = "TableStyleLight9")
      wb$add_data_table("all", out, with_filter=F, start_row=ix_count+2)
      ix_count <- ix_count + nrow(out) + 7
    }
    
    # save the workbook
    wb_save(wb, fn_out)
  }
}

write_simple_productivity <- function(inputs_productivity, landuse, save_dir, save_dir2, farm_id) {
  # write simplified landuse table for remote sensing
  prod_simple <- inputs_productivity %>% 
    mutate(farm_id = db_farmId) %>%
    select(farm_id, year, parcel_name, area, primary_landuse, secondary_landuse, start_date, end_date,
           crop_index, species, prod_ag, prod_ag_ha, landuse, percent_area)
  write.csv(prod_simple, paste0(save_dir, farm_id, '_productivity_for_remote_sensing.csv'))
  
  # integrate landuses with the same start/end dates
  lu <- left_join(landuse, 
              inputs_productivity %>% select(parcel_name, parcel_id, year, start_date, end_date, species, prod_ag, prod_bg), 
              by=c("parcel_name", "parcel_id", "year", "start_date", "end_date", "species")) %>%
    group_by(parcel_name, parcel_id, year, start_date, end_date) %>%
    summarise(landuse = paste(unique(landuse), collapse = " & "), 
              prod_ag =sum(prod_ag, na.rm=T),
              prod_bg =sum(prod_bg, na.rm=T),
              .groups='drop') %>%
    mutate(green = if_else(prod_ag>0 | grepl("perennials", landuse), TRUE, FALSE)) %>%
    arrange(parcel_name, year)
  write.csv(lu, paste0(save_dir2, farm_id, '_landuse_for_remote_sensing.csv'))
}


plot_landuse_timeline <- function(inputs_landuse, inputs_trees, inputs_parcel_fixed, save_dir, save_dir_comb_plot, email) {
  
  log4r::info(my_logger, "Plotting land-use timeline...")
  
  lu_plot <- inputs_landuse %>% 
    select(parcel_name, year, landuse, species, start_date, end_date, percent_area, crop_index) %>%
    filter(!landuse %in% c("perennials", "tree", "shrub"))

  # adding tree DBH and number (not in the landuse table)
  tree_plot <- inputs_trees %>%
    filter(!is.na(species))
  if (nrow(tree_plot)>0) {
    tree_plot <- tree_plot %>%
      mutate(start_date = as.Date(paste0(year, "-01-01")),
             end_date = as.Date(paste0(year, "-12-31")),
             percent_area = avg_DBH,  # note the inconsistency
             landuse="perennials",
             crop_index = tree_index) %>%
      select(parcel_name, year, landuse, species, start_date, end_date, percent_area, crop_index)

    # add the trees to landuse
    lu_plot <- bind_rows(lu_plot, tree_plot)
  }
  
  all_parcels <- unique(inputs_parcel_fixed %>% pull(parcel_name))
  
  # cannot have anything with missing start/end dates
  lu_plot <- lu_plot %>% filter(!is.na(start_date) & !is.na(end_date))
  
  if (nrow(lu_plot)==0) {
    log4r::warn(my_logger, "No land-use data to plot for ", email)
    return()
  }
  
  # read in translations of crop types
  translations <- read_csv(here("data", "plant_translations.csv"), col_types = cols())
  # convert the columns to all lower case
  translations <- translations %>%
    mutate(en = tolower(en), es = tolower(es), pt = tolower(pt))
  translations$en <- iconv(translations$en, from = 'UTF-8', to = 'ASCII//TRANSLIT') # change encoding..
  # change "generic grasses" to pasture and match
  lu_plot <- lu_plot %>% 
    mutate(species = ifelse(species=="generic grasses", "pasture", species)) %>%
    left_join(translations, by=c("species"="en"))
  
  # note if there are any missing parcels
  plotted_parcels <- unique(lu_plot %>% pull(parcel_name))
  missing_parcels <- setdiff(all_parcels, plotted_parcels)
  if (length(missing_parcels)>0) {
    # create a text file with a list of the parcels
    missing_txt <- paste0(missing_parcels, collapse='\n')
    write(missing_txt, file=paste0(save_dir, '/00_parcels_not_plotted.txt'))
  }
  
  # slightly offset the start/end dates, so we can see gaps between data better
  lu_plot$start_date <- as.Date(lu_plot$start_date) + 2
  lu_plot$end_date <- as.Date(lu_plot$end_date) - 2
  
  parcels <- unique(lu_plot$parcel_name)
  plots <- list()
  plots_es <- list()
  plots_pt <- list()
  
  # loop over the languages
  for (lang in c("en","es","pt")) {
    for (parcel in parcels) {
      
      lu_plot_parcel <- lu_plot %>% filter(parcel_name == parcel)
    
      # plot line-by-line (each line is a landuse type)
      p <- ggplot()
      for (i in 1:nrow(lu_plot_parcel)) {
        # format the data for ggplot
        df_tidy_i <- lu_plot_parcel[i,] %>%
          pivot_longer(cols = c(start_date, end_date), names_to = 'date_type', values_to = 'date')
        df_tidy_i$date <- as.Date(df_tidy_i$date)
        if (lang == "en") {
          df_tidy_i$label <- df_tidy_i$species
          df_tidy_i$label[is.na(df_tidy_i$species)] <- df_tidy_i$landuse[is.na(df_tidy_i$species)]
        } else if (lang == "es") {
          df_tidy_i$label <- df_tidy_i$es
          df_tidy_i$label[is.na(df_tidy_i$es)] <- df_tidy_i$landuse[is.na(df_tidy_i$es)]
          df_tidy_i$label[df_tidy_i$label=="bare soil"] <- "suelo desnudo"
          df_tidy_i$label[df_tidy_i$label=="fallow"] <- "barbecho"
        } else if (lang == "pt") {
          df_tidy_i$label <- df_tidy_i$pt
          df_tidy_i$label[is.na(df_tidy_i$pt)] <- df_tidy_i$landuse[is.na(df_tidy_i$pt)]
          df_tidy_i$label[df_tidy_i$label=="bare soil"] <- "solo nu"
          df_tidy_i$label[df_tidy_i$label=="fallow"] <- "pousio"
        }
        # optional: modify the labels to include the indexes
        if (!is.na(df_tidy_i$crop_index[1])) {
          df_tidy_i$label <- paste0(df_tidy_i$label, " (", df_tidy_i$crop_index, ")")
        }
        
        # plot it as a line
        p <- p + geom_line(data=df_tidy_i, mapping=aes(x=label, y=date, color=factor(landuse)), linewidth=2)
  
        # annotate with the percent area (for bare soil and pasture) and DBH (for trees)
        lu_i <- df_tidy_i$landuse[1]
        if (lu_i %in% c("bare soil","pasture","perennials")) {
          unit <- ifelse(lu_i=="perennials", "cm", "%")
          detail_text <- data.frame(date=mean(df_tidy_i$date), text=paste(df_tidy_i$percent_area[1], unit), label=df_tidy_i$label[1])
          p <- p + geom_text(data=detail_text, mapping=aes(x=label, y=date, label=text), vjust=-0.5, hjust=0.5, color="black", size=3)
        }
      }
      plots[[parcel]] <- p + 
        coord_flip() + 
        labs(title=parcel, y='', x='') + 
        scale_y_date(date_breaks = "1 year", date_labels = "%Y", date_minor_breaks = "1 month",
                     limits=c(min(as.Date(lu_plot$start_date)), max(as.Date(lu_plot$end_date)))) +
        scale_color_manual(values=c('fallow'='brown', 'pasture'='darkgreen', 'annual crop'='black', 'bare soil'='grey', 'trees'='lightgreen')) +
        theme_bw() + 
        theme(legend.position='none')
      
      # write this individual plot to a file
      parcel_no_slash <- gsub('/', '_', parcel)
      ggsave(paste0(save_dir, '/', lang, '_timeline_', parcel_no_slash, '.png'), plots[[parcel]], width=10, height=2.5, units='in', dpi=100)
  
    
    } # end parcel loop
  
    # arrange the plots in a grid
    pg <- plot_grid(plotlist=plots, ncol=4, align='hv')
    ggsave(paste0(save_dir, '/00_', lang, '_timeline_all.png'), pg, width=25, height=2.5*length(parcels)/4, units='in', dpi=100)
    # also save the combined one in a common directory
    ggsave(paste0(save_dir_comb_plot, email, '_', lang, '.png'), pg, width=25, height=2.5*length(parcels)/4, units='in', dpi=100)
  } # end language loop
}


delete_grouped_parcels <- function(inputs_raw, db_farmId, grouped_parcels) {
  
  farmer_parcels <- grouped_parcels %>% filter(farmer_id == db_farmId)

  if (nrow(farmer_parcels)>0) {
    log4r::info(my_logger, "Deleting the farmer's grouped parcels...")
    # delete the ones that do not appear in the main_parcel column
    parcels_to_delete <- farmer_parcels$sub_parcel[! farmer_parcels$sub_parcel %in% unique(farmer_parcels$main_parcel)]
    N_main <- length(unique(farmer_parcels$main_parcel)) - sum(is.na(unique(farmer_parcels$main_parcel)))
    if (length(parcels_to_delete) != (nrow(farmer_parcels) - N_main)) {
      stop("Not all grouped parcels have a main_parcel. Check the data.")
    }
    
    # merge the inputs data with the grouped parcels - and check whether all matches are found
    merged <- inputs_raw$inputs_parcel_fixed %>% 
      full_join(farmer_parcels, by=c("parcel_name"="sub_parcel"))
    if (any(is.na(merged$parcel_id))) {
      # temp code : making list of mismatches for Maria
      mismatches <- merged # %>% filter(is.na(parcel_id) | is.na(farmer_id))
      mismatches$in_dashboard <- !is.na(mismatches$parcel_id)
      mismatches$in_google_sheet <- !is.na(mismatches$farmer_id)
      Encoding(mismatches$parcel_name) <- 'UTF-8'
      # write.csv(mismatches %>% select(-coordinates), 'test.csv')#, fileEncoding='UTF-8')
      stop("Not all grouped sub-parcels were found in the inputs data. Check the data.")
    }
    
    # special case: override the dashboard areas for the main parcels
    if (sum(!is.na(merged$manual_area_override))>0) {
      merged <- merged %>% 
        mutate(area = if_else(is.na(manual_area_override), area, manual_area_override))
      tmp <- left_join(inputs_raw$inputs_parcel_fixed, 
                       merged %>% select(parcel_name, manual_area_override), 
                       by='parcel_name')
      tmp <- tmp %>% 
        mutate(area = if_else(is.na(manual_area_override), area, manual_area_override))
      inputs_raw$inputs_parcel_fixed <- tmp
    }
    
    
    # set the area of the super-parcel to the sum of the sub-parcels
    # use the final "area" column from the inputs_parcel_fixed
    areas_main <- merged %>%
      group_by(main_parcel) %>%
      summarise(tot_area = sum(area)) %>%
      filter(!is.na(main_parcel))
    
    # update the NPP as the weighted average over all sub-parcels
    tmp_npp <- inputs_raw$inputs_npp %>% 
      left_join(farmer_parcels, by=c("parcel_name"="sub_parcel")) %>%
      left_join(inputs_raw$inputs_parcel_fixed %>% select(parcel_name, area), by="parcel_name") %>%
                group_by(main_parcel, year) %>%
                  summarise(npp_index = sum(npp_index*area)/sum(area), .groups='drop')
    inputs_raw$inputs_npp <- inputs_raw$inputs_npp %>% 
      mutate(npp_orig = npp_index) %>%
      select(-npp_index) %>%
      left_join(tmp_npp, by=c("year"="year", "parcel_name"="main_parcel")) %>%
      select(-npp_orig)
    
    # update the areas in the inputs_parcel_fixed dataset
    inputs_raw$inputs_parcel_fixed <- inputs_raw$inputs_parcel_fixed %>% 
      left_join(areas_main, by=c("parcel_name"="main_parcel")) %>% 
      mutate(area = if_else(is.na(tot_area), area, tot_area)) %>%
      select(-tot_area)
    original_n_parcels <- nrow(inputs_raw$inputs_parcel_fixed)
    
    # loop over the inputs items and remove these parcels' info (where relevant)
    inputs_changed <- c()
    inputs_not_changed <- c()
    for (i in 1:length(inputs_raw)) {
      if ('parcel_name' %in% colnames(inputs_raw[[i]])) {
        inputs_raw[[i]] <- inputs_raw[[i]] %>% filter(!parcel_name %in% parcels_to_delete)
        inputs_changed <- c(inputs_changed, names(inputs_raw)[i])
      } else {
        inputs_not_changed <- c(inputs_not_changed, names(inputs_raw)[i])
      }
      
      # check that all were deleted
      final_n_parcels <- nrow(inputs_raw$inputs_parcel_fixed)
      if (final_n_parcels != (original_n_parcels - length(parcels_to_delete))) {
        stop("ERROR: Not all grouped parcels were removed. Parcel name might not match exactly?")
      }
      
    }  # end loop over inputs items

    log4r::info(my_logger, " --> Removed ", length(parcels_to_delete), " parcels. Total # of parcels is now ", nrow(inputs_raw$inputs_parcel_fixed))
    log4r::info(my_logger, " --> Removed grouped parcels in: ", paste0(inputs_changed, collapse=", "))
    log4r::info(my_logger, " --> Info: Not removed grouped parcels in (b/c no parcel_name column): ", paste0(inputs_not_changed, collapse=", "))
    
  }  # end if statement
  
  return(inputs_raw)
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


clean_raw_inputs_for_excel_model <- function(inputs_raw, perennial_factors, inputs_grazing_cover, inputs_grazing_parcels) {
  # similar to above, but only remove the empty values.. no joins or reformatting
  # Fertilizer
  # inputs_raw$inputs_fertilizer <- inputs_raw$inputs_fertilizer
  # Livestock
  inputs_raw$inputs_livestock_category <- inputs_raw$inputs_livestock_category %>%
    filter(amount > 0)
  inputs_raw$inputs_livestock_species <- inputs_raw$inputs_livestock_species %>% filter(grazing_days>0)
  inputs_raw$inputs_livestock_outfarm <- inputs_raw$inputs_livestock_outfarm %>% filter(amount>0)
  # Organic matter
  inputs_raw$inputs_organicmatter <- inputs_raw$inputs_organicmatter %>% filter(amount_t>0)
  # Monthly grazing
  inputs_raw$inputs_grazing_monthly <- inputs_raw$inputs_grazing_monthly %>% 
    filter(was_grazed)
  # ^^ and create a second one that is condensed
  inputs_raw$inputs_grazing_monthly_wide <- inputs_raw$inputs_grazing_monthly %>% 
    group_by(parcel_name, year) %>% 
    summarise(months=paste0(month[was_grazed==TRUE], collapse=","), .groups='drop') %>%
    filter(months!="") %>%
    pivot_wider(id_cols = "parcel_name", names_from="year", values_from="months")
  # Tillage
  inputs_raw$inputs_tillage <- inputs_raw$inputs_tillage %>%
    filter(!is.na(date))
  # irrigation
  # Annual crops & fallow -- do some actual processing here to make it easier for excel (put the harvest and residues side-by-side)
  inputs_raw$inputs_annualcrops <- inputs_raw$inputs_annualcrops %>% 
    distinct() %>%  # some duplicate zero rows
    pivot_wider(names_from="category", values_from = c("amount", "type", "dry"), id_cols=c("year","parcel_name","crop_index","start_date","end_date","species","other"))
  # Trees & perennialcrops
  inputs_raw$inputs_perennialcrops <- inputs_raw$inputs_perennialcrops %>% filter(!is.na(species)) 
  # inputs_raw$inputs_perennialprod <- inputs_raw$inputs_perennialprod %>% filter(amount>0) 
  inputs_raw$inputs_trees_felled <- inputs_raw$inputs_trees_felled %>% filter(tree_number>0)
  # ...add the harvest and residue amounts to the tree dataframe
  # (these are difficult to combine in excel)
  if (nrow(inputs_raw$inputs_perennialcrops) == 0) {
    inputs_raw$inputs_perennialcrops[,c('harvest_t_dry', 'residue_t_dry')] <- NA
  } else {
    tmp <- inputs_raw$inputs_perennialprod %>% 
      select(-dry) %>%
      left_join(perennial_factors, by=c('species'='db_reference')) %>%
      mutate(amount_dry = if_else(units=="tonnes - fresh", amount*dry, amount)) %>%
      pivot_wider(id_cols=c(parcel_name, year, tree_index, species), names_from="category", values_from="amount_dry") %>%
      replace_na(list(harvest=0, residue=0)) %>%
      rename(harvest_t_dry=harvest, residue_t_dry=residue)
    tmp2 <- left_join(inputs_raw$inputs_perennialcrops, 
                     tmp, 
                     by=c("parcel_name", "year", "species", "tree_index"))
    inputs_raw$inputs_perennialcrops <- tmp2
  }
  inputs_raw$inputs_perennialprod <- NULL # remove the production/residue dataframe
  # Pasture
  inputs_raw$inputs_pasture <- inputs_raw$inputs_pasture %>% filter(amount>0)
  # landuse
  
  inputs_forage_potential <- inputs_grazing_cover %>%
    select(parcel_name, year, year_index, period, landuse, crop_index, tree_index, species, forage_pot)
  
  inputs_grazing_parcels <- inputs_grazing_parcels %>%
    select(parcel_name, year, year_index, period, grazing_t_dry)
  
  inputs_raw$inputs_forage_potential <- inputs_forage_potential
  inputs_raw$inputs_grazing_parcels <- inputs_grazing_parcels

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
