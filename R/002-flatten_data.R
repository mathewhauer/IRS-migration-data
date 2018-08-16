######################################################
##
## IRS County-to-County Migration Data
##       Flatten and Export Data
##
## Authors: Mathew Hauer, mehauer@fsu.edu
##          James Byars, jmbyars@uga.edu
## Last updated: 08/16/2018
##
######################################################


#### 1990 - 1991 
# Specify Years 
editions <- c("1990to1991", "1991to1992")

# Open Data
migration_19901991 <- foreach(i = 1:length(editions), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
  
  # Specify Directory
  directory <- paste0("./MigData/",editions[i],"/", editions[i],"CountyMigration")
  directory2 <- paste0("MigData/",editions[i],"/", editions[i],"CountyMigration/")
  state_files <- list.files(directory, pattern = "*.txt$", recursive = TRUE) # List Files
  state_files <- paste0(directory2, state_files)
  
  # Loop Data #
  foreach(j = 1:length(state_files), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "reshape2", "stringi", "stringr", "zoo", "tidyverse")) %dopar% {
    
    # Specify Flows #
    in_out_flow <- state_files[j] # Specify Flow
    in_out_flow <- unlist(strsplit(in_out_flow, "\\."))[[1]] # String Split
    in_out_flow <- toupper(in_out_flow) # Upper Case
    in_out_flow <- str_sub(in_out_flow, -1, -1) # Select Last Character
    
    if (in_out_flow == "I") {
      
      # Inflows #
      data_table <- fread(state_files[j], header = FALSE, sep = "\n") # Open Data
      data_table2 <- data_table[grepl("County Non-Migrants|County Non-migrants", V1) == TRUE] # Selecting non-migrants
      data_table <- data_table[grepl("Same State|Same Region|Different Region|County Non-Migrants|County Non-migrants|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", V1) == FALSE] # Remove Elements
      data_table <- data_table[, V1 := toupper(V1)] # Upper Case
      data_table <- data_table[, test := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}", "", V1)] # Extract Pattern
      data_table <- data_table[, test := str_trim(test)] # Remove Leading/Lagging Space
      data_table <- data_table[, test := numb_spaces(test)] # Remove Double Spaces
      data_table <- data_table[, lengths := length(unlist(strsplit(test, " "))), by = "test"] # Length of Splits
      data_table <- data_table[lengths == 1, des := gsub("[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}.*", "", V1)] # Extract Pattern
      data_table <- data_table[, des := na.locf(des, na.rm = TRUE, fromLast = FALSE)] # Roll Forward Values
      data_table <- data_table[, des_state := substr(des, 1, 2)] # Origin State
      data_table <- data_table[, des_county := substr(des, 4, 6)] # Origin County
      data_table3 <- data_table[lengths == 1] # Select Totals
      data_table <- data_table[lengths != 1] # Remove Totals
      data_table <- data_table[, org_state := substr(V1, 1, 2)] # Origin State
      data_table <- data_table[, org_county := substr(V1, 4, 6)] # Origin County
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, numbers := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}", "", V1)] # Extract Pattern
      data_table <- data_table[, numbers := str_trim(numbers)] # Remove Leading/Lagging Space
      data_table <- data_table[, numbers := numb_spaces(numbers)] # Remove Double Spaces
      data_table <- data_table[, c("returns", "number_1", "exemptions", "number_2") := tstrsplit(numbers, " ", fixed = TRUE), by = "numbers"] # Split Data
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      data_table2 <- data_table2[, V1 := toupper(V1)] # Upper Case
      data_table2 <- data_table2[, test := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}", "", V1)] # Extract Pattern
      data_table2 <- data_table2[, test := str_trim(test)] # Remove Leading/Lagging Space
      data_table2 <- data_table2[, test := numb_spaces(test)] # Remove Double Spaces
      data_table2 <- data_table2 %>%
        separate(test, into = c('a','b','c','d','e','f'), sep = " ") %>% # separating out the test variable into its constituents
        select("des_state" = a, "des_county" = b, "returns" = e, "exemptions" = "f") %>% # renaming the separated test variable
        mutate(org_state = des_state, # Origin state
               org_county = des_county, # Origin county
               year = as.integer(substr(editions[i], 1, 4)), # Year
               file_name = state_files[j]) # File name
      
      data_table3 <- data_table3[, V1 := toupper(V1)] # Upper Case
      data_table3 <- data_table3[, test2 := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}", "", V1)] # Extract Pattern
      data_table3 <- data_table3[, V1 := str_trim(V1)] # Remove Leading/Lagging Space
      data_table3 <- data_table3[, V1 := numb_spaces(V1)] # Remove Double Spaces
      data_table3 <- data_table3[, numbers := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}", "", V1)] # Extract Pattern
      data_table3 <- data_table3[, numbers := str_trim(numbers)] # Remove Leading/Lagging Space
      data_table3 <- data_table3[, numbers := numb_spaces(numbers)] # Remove Double Spaces
      data_table3 <- data_table3[, c("returns", "exemptions") := tstrsplit(numbers, " ", fixed = TRUE), by = "numbers"] # Split Data
      data_table3 <- data_table3[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table3 <- data_table3[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table3 <- data_table3[, org_state := 99] # Origin State
      data_table3 <- data_table3[, org_county := 999] # Origin County
      data_table3 <- data_table3[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table3 <- data_table3[, year := as.integer(year)] # Specify Integer Class
      data_table3 <- data_table3[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions"), with = FALSE] # Keep Columns
      data_table3 <- data_table3[, file_name := state_files[j]] # File Name
      
      tots <- data_table %>% # Summing to create the totals
        group_by(des_state, des_county) %>% # grouping by State and County
        summarise(tot_returns = sum(returns), # summing the total returns
                  tot_exemps = sum(exemptions)) # summing the total exemptions
      data_table3 <- left_join(data_table3, tots) %>% # joining the totals with the flows
        mutate(returns = returns - tot_returns, # creating the number of returns as the total returns minus the sum
               exemptions = exemptions - tot_exemps) %>%  # creating the number of exemptions as the total returns minus the sum
        select(-tot_returns, -tot_exemps) # deselecting the summed returns and exemptions
      
      data_table = rbind(data_table, data_table2, data_table3) # putting the three files together.
      
      
    } else {
      
      # Outflows #
      data_table <- fread(state_files[j], header = FALSE, sep = "\n") # Open Data
      data_table2 <- data_table[grepl("County Non-Migrants|County Non-migrants", V1) == TRUE] # Keep non-migrants
      data_table <- data_table[grepl("Same State|Same Region|Different Region|County Non-Migrants|County Non-migrants|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", V1) == FALSE] # Remove Elements
      data_table <- data_table[, V1 := toupper(V1)] # Upper Case
      data_table <- data_table[, test := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}", "", V1)] # Extract Pattern
      data_table <- data_table[, test := str_trim(test)] # Remove Leading/Lagging Space
      data_table <- data_table[, test := numb_spaces(test)] # Remove Double Spaces
      data_table <- data_table[, lengths := length(unlist(strsplit(test, " "))), by = "test"] # Length of Splits
      data_table <- data_table[lengths == 1, org := gsub("[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}.*", "", V1)] # Extract Pattern
      data_table <- data_table[, org := na.locf(org, na.rm = TRUE, fromLast = FALSE)] # Roll Forward Values
      data_table <- data_table[, org_state := substr(org, 1, 2)] # Origin State
      data_table <- data_table[, org_county := substr(org, 4, 6)] # Origin County
      data_table3 <- data_table[lengths == 1] # Select Totals
      data_table <- data_table[lengths != 1] # Remove Totals
      data_table <- data_table[, des_state := substr(V1, 1, 2)] # Origin State
      data_table <- data_table[, des_county := substr(V1, 4, 6)] # Origin County
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, numbers := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}", "", V1)] # Extract Pattern
      data_table <- data_table[, numbers := str_trim(numbers)] # Remove Leading/Lagging Space
      data_table <- data_table[, numbers := numb_spaces(numbers)] # Remove Double Spaces
      data_table <- data_table[, c("returns", "number_1", "exemptions", "number_2") := tstrsplit(numbers, " ", fixed = TRUE), by = "numbers"] # Split Data
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      
      data_table2 <- data_table2[, V1 := toupper(V1)] # Upper Case
      data_table2 <- data_table2[, test := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}", "", V1)] # Extract Pattern
      data_table2 <- data_table2[, test := str_trim(test)] # Remove Leading/Lagging Space
      data_table2 <- data_table2[, test := numb_spaces(test)] # Remove Double Spaces
      data_table2 <- data_table2 %>%
        separate(test, into = c('a','b','c','d','e','f'), sep = " ") %>% # separating out the test variable into its constituents
        select("des_state" = a, "des_county" = b, "returns" = e, "exemptions" = "f") %>%
        mutate(org_state = des_state, # Origin state
               org_county = des_county, # Origin county
               year = as.integer(substr(editions[i], 1, 4)), # Year
               file_name = state_files[j]) # File name
      
      data_table3 <- data_table3[, V1 := toupper(V1)] # Upper Case
      data_table3 <- data_table3[, test2 := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}[0-9]{1,}", "", V1)] # Extract Pattern
      data_table3 <- data_table3[, V1 := str_trim(V1)] # Remove Leading/Lagging Space
      data_table3 <- data_table3[, V1 := numb_spaces(V1)] # Remove Double Spaces
      data_table3 <- data_table3[, numbers := gsub(".*[[:space:]][A-Z]{2}[[:space:]]{1,}", "", V1)] # Extract Pattern
      data_table3 <- data_table3[, numbers := str_trim(numbers)] # Remove Leading/Lagging Space
      data_table3 <- data_table3[, numbers := numb_spaces(numbers)] # Remove Double Spaces
      data_table3 <- data_table3[, c("returns", "exemptions") := tstrsplit(numbers, " ", fixed = TRUE), by = "numbers"] # Split Data
      data_table3 <- data_table3[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table3 <- data_table3[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table3 <- data_table3[, des_state := 99] # Origin State
      data_table3 <- data_table3[, des_county := 999] # Origin County
      data_table3 <- data_table3[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table3 <- data_table3[, year := as.integer(year)] # Specify Integer Class
      data_table3 <- data_table3[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions"), with = FALSE] # Keep Columns
      data_table3 <- data_table3[, file_name := state_files[j]] # File Name
      tots <- data_table %>%
        group_by(org_state, org_county) %>%
        summarise(tot_returns = sum(returns),
                  tot_exemps = sum(exemptions))
      data_table3 <- left_join(data_table3, tots) %>%
        mutate(returns = returns - tot_returns,
               exemptions = exemptions - tot_exemps) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table = rbind(data_table, data_table2, data_table3) # putting the three files together.
    }
  }
}

# Order Observations
migration_19901991 <- migration_19901991[order(org_state, org_county, des_state, des_county)]
migration_19901991 <- migration_19901991[org_state %in% us_states$FIPS_CODE] # Recognized State
migration_19901991 <- migration_19901991[des_state %in% us_states$FIPS_CODE] # Recognized State

########## 1993 ##########

##### Specify Years #####
editions <- c("1993to1994")

##### Open Data #####
migration_1993 <- foreach(i = 1:length(editions), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
  
  # Specify Directory #
  directory <- paste0("./MigData/",editions[i],"/", editions[i],"CountyMigration")
  directory2 <- paste0("MigData/",editions[i],"/", editions[i],"CountyMigration/")
  state_files <- list.files(directory, pattern = "*.xls$", recursive = TRUE) # List Files
  state_files <- paste0(directory2, state_files)
  
  foreach(j = 1:length(state_files), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo", "tidyverse")) %dopar% {
    
    # Specify Flows #
    in_out_flow <- state_files[j] # Specify Flow
    in_out_flow <- unlist(strsplit(in_out_flow, "\\."))[[1]] # String Split
    in_out_flow <- toupper(in_out_flow) # Upper Case
    in_out_flow <- str_sub(in_out_flow, -1, -1) # Select Last Character
    
    if (in_out_flow == "I") {
      
      # Inflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[, X__1 := as.integer(X__1)] # Specify Integer Class
      data_table <- data_table[, X__1 := as.character(X__1)] # Specify Character Class
      data_table <- data_table[, X__2 := as.integer(X__2)] # Specify Integer Class
      data_table <- data_table[, X__2 := as.character(X__2)] # Specify Character Class
      data_table <- data_table[, X__3 := as.integer(X__3)] # Specify Integer Class
      data_table <- data_table[, X__3 := as.character(X__3)] # Specify Character Class
      data_table <- data_table[, X__4 := as.integer(X__4)] # Specify Integer Class
      data_table <- data_table[, X__4 := as.character(X__4)] # Specify Character Class
      data_table <- data_table[, X__1 := numb_digits_F(X__1, 2), by = "X__1"] # Specify Two Digit State Code
      data_table <- data_table[, X__2 := numb_digits_F(X__2, 3), by = "X__2"] # Specify Three Digit County Code
      data_table <- data_table[, X__3 := numb_digits_F(X__3, 2), by = "X__3"] # Specify Two Digit State Code
      data_table <- data_table[, X__4 := numb_digits_F(X__4, 3), by = "X__4"] # Specify Three Digit County Code
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "des_state") # Specify Column Names
      setnames(data_table, "X__2", "des_county") # Specify Column Names
      setnames(data_table, "X__3", "org_state") # Specify Column Names
      setnames(data_table, "X__4", "org_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      data_table2 <- data_table[grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>% # Keep non-migrants 	
        mutate(org_state = des_state, # Origin state
               org_county = des_county) # Origin county  
      data_table3 <- data_table[which(org_state == "00"),] %>% # Keep non-migrants 	
        mutate(org_state = 99, # Origin state
               org_county = 999)
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Total Migrant|Total migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      
      tots <- data_table %>%
        filter(org_state %in% us_states$FIPS_CODE) %>%
        group_by(des_state, des_county) %>%
        summarise(tot_returns = sum(returns),
                  tot_exemps = sum(exemptions))
      data_table3 <- left_join(data_table3, tots)
      data_table3[is.na(data_table3)] <- 0
      data_table3 <- data_table3 %>% 
        mutate(returns = returns - tot_returns,
               exemptions = exemptions - tot_exemps) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Total Migrant|Total migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE]%>%
        filter(org_state %in% us_states$FIPS_CODE) # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, data_table3))
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    } else {
      
      # Outflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[, X__1 := as.integer(X__1)] # Specify Integer Class
      data_table <- data_table[, X__1 := as.character(X__1)] # Specify Character Class
      data_table <- data_table[, X__2 := as.integer(X__2)] # Specify Integer Class
      data_table <- data_table[, X__2 := as.character(X__2)] # Specify Character Class
      data_table <- data_table[, X__3 := as.integer(X__3)] # Specify Integer Class
      data_table <- data_table[, X__3 := as.character(X__3)] # Specify Character Class
      data_table <- data_table[, X__4 := as.integer(X__4)] # Specify Integer Class
      data_table <- data_table[, X__4 := as.character(X__4)] # Specify Character Class
      data_table <- data_table[, X__1 := numb_digits_F(X__1, 2), by = "X__1"] # Specify Two Digit State Code
      data_table <- data_table[, X__2 := numb_digits_F(X__2, 3), by = "X__2"] # Specify Three Digit County Code
      data_table <- data_table[, X__3 := numb_digits_F(X__3, 2), by = "X__3"] # Specify Two Digit State Code
      data_table <- data_table[, X__4 := numb_digits_F(X__4, 3), by = "X__4"] # Specify Three Digit County Code
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "org_state") # Specify Column Names
      setnames(data_table, "X__2", "org_county") # Specify Column Names
      setnames(data_table, "X__3", "des_state") # Specify Column Names
      setnames(data_table, "X__4", "des_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      data_table2 <- data_table[grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>% # Keep non-migrants 	
        mutate(des_state = org_state, # Origin state
               des_county = org_county) # Origin county 
      data_table3 <- data_table[which(des_state == "00"),] %>% # Keep non-migrants 	
        mutate(des_state = "99", # Origin state
               des_county = "999")
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Total Migrant|Total migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      
      tots <- data_table %>%
        filter(des_state %in% us_states$FIPS_CODE) %>%
        group_by(org_state, org_county) %>%
        summarise(tot_returns = sum(returns),
                  tot_exemps = sum(exemptions))
      data_table3 <- left_join(data_table3, tots)
      data_table3[is.na(data_table3)] <- 0
      data_table3 <- data_table3 %>% 
        mutate(returns = returns - tot_returns,
               exemptions = exemptions - tot_exemps) %>%
        select(-tot_returns, -tot_exemps)
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, data_table3))
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    }
  }
}

##### Order Observations #####
migration_1993 <- migration_1993[order(org_state, org_county, des_state, des_county)]

########## 1992 - 1994 (Minus 1993) ##########

##### Specify Years #####
editions <- c("1992to1993", "1994to1995")

##### Open Data #####
migration_19921994 <- foreach(i = 1:length(editions), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
  
  # Specify Directory #
  directory <- paste0("./MigData/",editions[i],"/", editions[i],"CountyMigration")
  directory2 <- paste0("MigData/",editions[i],"/", editions[i],"CountyMigration/")
  state_files <- list.files(directory, pattern = "*.xls$", recursive = TRUE) # List Files
  state_files <- paste0(directory2, state_files)
  
  foreach(j = 1:length(state_files), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo", "tidyverse")) %dopar% {
    
    # Specify Flows #
    in_out_flow <- state_files[j] # Specify Flow
    in_out_flow <- unlist(strsplit(in_out_flow, "\\."))[[1]] # String Split
    in_out_flow <- toupper(in_out_flow) # Upper Case
    if (substr(editions[i], 1, 4) %in% c("1995","2000")) {in_out_flow <- str_sub(in_out_flow, -2, -2)} else {in_out_flow <- str_sub(in_out_flow, -1, -1)} # Select Last Character
    
    if (in_out_flow == "I") {
      
      # Inflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "des_state") # Specify Column Names
      setnames(data_table, "X__2", "des_county") # Specify Column Names
      setnames(data_table, "X__3", "org_state") # Specify Column Names
      setnames(data_table, "X__4", "org_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      data_table2 <- filter(data_table, org_state == des_state & org_county == des_county| grepl("Non-Migrant|Non-migrant", X__6))  %>% # Keep non-migrants 	
        mutate(org_state = des_state, # Origin state
               org_county = des_county) # Origin county  
      data_table3 <- filter(data_table, org_state == "00") %>% # Keep non-migrants 	
        mutate(org_state = 99, # Origin state
               org_county = 999)
      tots <- data_table %>%
        filter(org_state %in% us_states$FIPS_CODE) %>%
        group_by(des_state, des_county) %>%
        summarise(tot_returns = sum(returns),
                  tot_exemps = sum(exemptions))
      data_table3 <- left_join(data_table3, tots)
      data_table3[is.na(data_table3)] <- 0
      data_table3 <- data_table3 %>% 
        mutate(returns = returns - tot_returns,
               exemptions = exemptions - tot_exemps) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, data_table3))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
      
      
    } else {
      
      # Outflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "org_state") # Specify Column Names
      setnames(data_table, "X__2", "org_county") # Specify Column Names
      setnames(data_table, "X__3", "des_state") # Specify Column Names
      setnames(data_table, "X__4", "des_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      data_table2 <- filter(data_table, org_state == des_state & org_county == des_county| grepl("Non-Migrant|Non-migrant", X__6))  %>% # Keep non-migrants 	
        mutate(des_state = org_state, # Origin state
               des_county = org_county) # Origin county 
      data_table3 <- filter(data_table, des_state == "00") %>% # Keep non-migrants 	
        mutate(des_state = 99, # Origin state
               des_county = 999)
      tots <- data_table %>%
        filter(des_state %in% us_states$FIPS_CODE) %>%
        group_by(org_state, org_county) %>%
        summarise(tot_returns = sum(returns),
                  tot_exemps = sum(exemptions)) # Keep non-migrants 	
      data_table3 <- left_join(data_table3, tots)
      data_table3[is.na(data_table3)] <- 0
      data_table3 <- data_table3 %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        # mutate(org_state = 99, # Origin state
        #        org_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, data_table3))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    }
  }
}

##### Order Observations #####
migration_19921994 <- migration_19921994[order(org_state, org_county, des_state, des_county, year)]


########## 1995 - 2003  ##########

##### Specify Years #####
editions <- c("1995to1996", "1996to1997", "1997to1998", "1998to1999", "1999to2000", "2000to2001", "2001to2002", "2002to2003", "2003to2004")

##### Open Data #####
migration_19952003 <- foreach(i = 1:length(editions), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
  
  # Specify Directory #
  directory <- paste0("./MigData/",editions[i],"/", editions[i],"CountyMigration")
  directory2 <- paste0("MigData/",editions[i],"/", editions[i],"CountyMigration/")
  state_files <- list.files(directory, pattern = "*.xls$", recursive = TRUE) # List Files
  state_files <- paste0(directory2, state_files)
  
  foreach(j = 1:length(state_files), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo", "tidyverse")) %dopar% {
    
    # Specify Flows #
    in_out_flow <- state_files[j] # Specify Flow
    in_out_flow <- unlist(strsplit(in_out_flow, "\\."))[[1]] # String Split
    in_out_flow <- toupper(in_out_flow) # Upper Case
    if (substr(editions[i], 1, 4) %in% c("1995","2000")) {in_out_flow <- str_sub(in_out_flow, -2, -2)} else {in_out_flow <- str_sub(in_out_flow, -1, -1)} # Select Last Character
    
    if (in_out_flow == "I") {
      
      # Inflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "des_state") # Specify Column Names
      setnames(data_table, "X__2", "des_county") # Specify Column Names
      setnames(data_table, "X__3", "org_state") # Specify Column Names
      setnames(data_table, "X__4", "org_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      # data_table2 <- data_table[grepl("Non-Migr|Non-migr", X__6) == TRUE]  %>% # Keep non-migrants 	
      data_table2 <-   filter(data_table, paste0(des_state, des_county) == paste0(org_state, org_county))  %>%
        mutate(org_state = des_state, # Origin state
               org_county = des_county) # Origin county          
      data_table3 <-  filter(data_table, !paste0(des_state, des_county) == paste0(org_state, org_county)) %>%
        filter(org_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(des_state, des_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, org_state == "96", org_county == "000", !des_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(org_state = 99, # Origin state
               org_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    } else {
      
      # Outflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "org_state") # Specify Column Names
      setnames(data_table, "X__2", "org_county") # Specify Column Names
      setnames(data_table, "X__3", "des_state") # Specify Column Names
      setnames(data_table, "X__4", "des_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      data_table2 <- filter(data_table, paste0(des_state, des_county) == paste0(org_state, org_county))  %>% # Keep non-migrants 	
        mutate(des_state = org_state, # Origin state
               des_county = org_county) # Origin county          
      data_table3 <- filter(data_table, !paste0(des_state, des_county) == paste0(org_state, org_county)) %>%
        filter(des_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(org_state, org_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, des_state == "96", des_county == "000", !org_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(des_state = 99, # Origin state
               des_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    }
  }
}

##### Order Observations #####
migration_19952003 <- migration_19952003[order(org_state, org_county, des_state, des_county, year)]


##### Specify Years #####
editions <- c("0405", "0506", "0607")

##### Open Data #####
migration_20042006 <- foreach(i = 1:length(editions), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
  
  # Specify Directory #
  directory <- paste0("./MigData/",editions[i])
  directory2 <- paste0("MigData/",editions[i],"/")
  state_files <- list.files(directory, pattern = "*.xls$", recursive = TRUE) # List Files
  state_files <- paste0(directory2, state_files)
  
  foreach(j = 1:length(state_files), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
    
    # Specify Flows #
    in_out_flow <- state_files[j] # Specify Flow
    in_out_flow <- unlist(strsplit(in_out_flow, "\\."))[[1]] # String Split
    in_out_flow <- toupper(in_out_flow) # Upper Case
    if (substr(editions[i], 1, 4) %in% c("1995","2000")) {in_out_flow <- str_sub(in_out_flow, -2, -2)} else {in_out_flow <- str_sub(in_out_flow, -1, -1)} # Select Last Character
    
    if (in_out_flow == "I") {
      
      # Inflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "des_state") # Specify Column Names
      setnames(data_table, "X__2", "des_county") # Specify Column Names
      setnames(data_table, "X__3", "org_state") # Specify Column Names
      setnames(data_table, "X__4", "org_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      data_table2 <- data_table[grepl("Non-Migrant|Non-migrant", X__6) == TRUE]  %>% # Keep non-migrants 	
        mutate(org_state = des_state, # Origin state
               org_county = des_county) # Origin county          
      data_table3 <- data_table[!grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>%
        filter(org_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(des_state, des_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, org_state == "96", org_county == "000", !des_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(org_state = 99, # Origin state
               org_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    } else {
      
      # Outflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "org_state") # Specify Column Names
      setnames(data_table, "X__2", "org_county") # Specify Column Names
      setnames(data_table, "X__3", "des_state") # Specify Column Names
      setnames(data_table, "X__4", "des_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      data_table2 <- data_table[grepl("Non-Migrant|Non-migrant", X__6) == TRUE]  %>% # Keep non-migrants 	
        mutate(des_state = org_state, # Origin state
               des_county = org_county) # Origin county          
      data_table3 <- data_table[!grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>%
        filter(des_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(org_state, org_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, des_state == "96", des_county == "000", !org_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(des_state = 99, # Origin state
               des_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    }
  }
}

##### Order Observations #####
migration_20042006 <- migration_20042006[order(org_state, org_county, des_state, des_county, year)]

########## 2007 - 2010 ##########

##### Specify Years #####
editions <- c("0708", "0809")

##### Open Data #####
migration_20072008 <- foreach(i = 1:length(editions), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
  
  # Specify Directory #
  directory <- paste0("./MigData/",editions[i])
  directory2 <- paste0("MigData/",editions[i],"/")
  state_files <- list.files(directory, pattern = "*.xls$", recursive = TRUE) # List Files
  state_files <- paste0(directory2, state_files)
  
  foreach(j = 1:length(state_files), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
    
    # Specify Flows #
    in_out_flow <- state_files[j] # Specify Flow
    in_out_flow <- unlist(strsplit(in_out_flow, "\\."))[[1]] # String Split
    in_out_flow <- toupper(in_out_flow) # Upper Case
    in_out_flow <- str_sub(in_out_flow, -3, -3) # Select Last Character
    
    if (in_out_flow == "I") {
      
      # Inflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "des_state") # Specify Column Names
      setnames(data_table, "X__2", "des_county") # Specify Column Names
      setnames(data_table, "X__3", "org_state") # Specify Column Names
      setnames(data_table, "X__4", "org_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      data_table2 <- data_table[grepl("Non-Migrant|Non-migrant", X__6) == TRUE]  %>% # Keep non-migrants 	
        mutate(org_state = des_state, # Origin state
               org_county = des_county) # Origin county          
      data_table3 <- data_table[!grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>%
        filter(org_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(des_state, des_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, org_state == "96", org_county == "000", !des_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(org_state = 99, # Origin state
               org_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    } else {
      
      # Outflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 8, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[nchar(X__1) == 2 & is.na(X__1) == FALSE] # Remove Headings
      setnames(data_table, "X__1", "org_state") # Specify Column Names
      setnames(data_table, "X__2", "org_county") # Specify Column Names
      setnames(data_table, "X__3", "des_state") # Specify Column Names
      setnames(data_table, "X__4", "des_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      
      data_table2 <- data_table[grepl("Non-Migrant|Non-migrant", X__6) == TRUE]  %>% # Keep non-migrants 	
        mutate(des_state = org_state, # Origin state
               des_county = org_county) # Origin county          
      data_table3 <- data_table[!grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>%
        filter(des_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(org_state, org_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, des_state == "96", des_county == "000", !org_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(des_state = 99, # Origin state
               des_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
    }
  }
}

##### Order Observations #####
migration_20072008 <- migration_20072008[order(org_state, org_county, des_state, des_county)]

########## 2009 - 2010 ##########

##### Specify Years #####
editions <- c("0910", "1011")

##### Open Data #####
migration_20092010 <- foreach(i = 1:length(editions), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
  
  # Specify Directory #
  directory <- paste0("./MigData/",editions[i])
  directory2 <- paste0("MigData/",editions[i],"/")
  state_files <- list.files(directory, pattern = "*.xls$", recursive = TRUE) # List Files
  state_files <- paste0(directory2, state_files)
  
  foreach(j = 1:length(state_files), .combine = rbind, .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "readxl", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
    
    # Specify Flows #
    in_out_flow <- state_files[j] # Specify Flow
    in_out_flow <- unlist(strsplit(in_out_flow, "\\."))[[1]] # String Split
    in_out_flow <- toupper(in_out_flow) # Upper Case
    in_out_flow <- str_sub(in_out_flow, -3, -3) # Select Last Character
    
    if (in_out_flow == "I") {
      
      # Inflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 7, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[, X__1 := as.integer(X__1)] # Specify Integer Class
      data_table <- data_table[, X__1 := as.character(X__1)] # Specify Character Class
      data_table <- data_table[, X__2 := as.integer(X__2)] # Specify Integer Class
      data_table <- data_table[, X__2 := as.character(X__2)] # Specify Character Class
      data_table <- data_table[, X__3 := as.integer(X__3)] # Specify Integer Class
      data_table <- data_table[, X__3 := as.character(X__3)] # Specify Character Class
      data_table <- data_table[, X__4 := as.integer(X__4)] # Specify Integer Class
      data_table <- data_table[, X__4 := as.character(X__4)] # Specify Character Class
      data_table <- data_table[is.na(X__1) == FALSE & is.na(X__3) == FALSE] # Remove Blanks
      data_table <- data_table[, X__1 := numb_digits_F(X__1, 2), by = "X__1"] # Specify Two Digit State Code
      data_table <- data_table[, X__2 := numb_digits_F(X__2, 3), by = "X__2"] # Specify Three Digit County Code
      data_table <- data_table[, X__3 := numb_digits_F(X__3, 2), by = "X__3"] # Specify Two Digit State Code
      data_table <- data_table[, X__4 := numb_digits_F(X__4, 3), by = "X__4"] # Specify Three Digit County Code
      setnames(data_table, "X__1", "des_state") # Specify Column Names
      setnames(data_table, "X__2", "des_county") # Specify Column Names
      setnames(data_table, "X__3", "org_state") # Specify Column Names
      setnames(data_table, "X__4", "org_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      data_table2 <- filter(data_table, org_state == des_state, org_county == des_county)
      
      data_table3 <- data_table[!grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>%
        filter(org_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(des_state, des_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, org_state == "96", org_county == "000", !des_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(org_state = 99, # Origin state
               org_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[org_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
      
      
    } else {
      
      # Outflows #
      data_table <- read_excel(state_files[j], sheet = 1, skip = 7, col_names = FALSE) # Open Data
      data_table <- data.table(data_table) # Specify Data Table Object
      data_table <- data_table[, X__1 := as.integer(X__1)] # Specify Integer Class
      data_table <- data_table[, X__1 := as.character(X__1)] # Specify Character Class
      data_table <- data_table[, X__2 := as.integer(X__2)] # Specify Integer Class
      data_table <- data_table[, X__2 := as.character(X__2)] # Specify Character Class
      data_table <- data_table[, X__3 := as.integer(X__3)] # Specify Integer Class
      data_table <- data_table[, X__3 := as.character(X__3)] # Specify Character Class
      data_table <- data_table[, X__4 := as.integer(X__4)] # Specify Integer Class
      data_table <- data_table[, X__4 := as.character(X__4)] # Specify Character Class
      data_table <- data_table[is.na(X__1) == FALSE & is.na(X__3) == FALSE] # Remove Blanks
      data_table <- data_table[, X__1 := numb_digits_F(X__1, 2), by = "X__1"] # Specify Two Digit State Code
      data_table <- data_table[, X__2 := numb_digits_F(X__2, 3), by = "X__2"] # Specify Three Digit County Code
      data_table <- data_table[, X__3 := numb_digits_F(X__3, 2), by = "X__3"] # Specify Two Digit State Code
      data_table <- data_table[, X__4 := numb_digits_F(X__4, 3), by = "X__4"] # Specify Three Digit County Code
      setnames(data_table, "X__1", "org_state") # Specify Column Names
      setnames(data_table, "X__2", "org_county") # Specify Column Names
      setnames(data_table, "X__3", "des_state") # Specify Column Names
      setnames(data_table, "X__4", "des_county") # Specify Column Names
      setnames(data_table, "X__7", "returns") # Specify Column Names
      setnames(data_table, "X__8", "exemptions") # Specify Column Names
      data_table <- data_table[, year := substr(editions[i], 1, 4)] # Specify Year
      data_table <- data_table[, year := as.integer(year)] # Specify Integer Class
      data_table <- data_table[, returns := as.numeric(returns)] # Specify Numeric Class
      data_table <- data_table[, exemptions := as.numeric(exemptions)] # Specify Numeric Class
      data_table <- data_table[, file_name := state_files[j]] # File Name
      data_table2 <- filter(data_table, org_state == des_state, org_county == des_county)
      
      data_table2 <- data_table[grepl("Non-Migrant|Non-migrant", X__6) == TRUE]  %>% # Keep non-migrants 	
        mutate(des_state = org_state, # Origin state
               des_county = org_county) # Origin county          
      data_table3 <- data_table[!grepl("Non-Migrant|Non-migrant", X__6) == TRUE] %>%
        filter(des_state %in% us_states$FIPS_CODE) %>%  # Keep non-migrants 
        group_by(org_state, org_county) %>%
        summarise(tot_returns = sum(as.numeric(returns)),
                  tot_exemps = sum(as.numeric(exemptions)))
      tots <- filter(data_table, des_state == "96", des_county == "000", !org_county == "000" )  %>% # Keep non-migrants 	
        left_join(.,data_table3)
      tots[is.na(tots)] <- 0
      tots <- tots %>% 
        mutate(returns = as.numeric(returns) - tot_returns,
               exemptions = as.numeric(exemptions) - tot_exemps) %>%
        mutate(des_state = 99, # Origin state
               des_county = 999) %>%
        select(-tot_returns, -tot_exemps)
      
      data_table <- data_table[grepl("Same State|Same Region|Different Region|Non-Migrant|Non-migrant|Other Flows|Region[[:space:]][0-9]{1}|All Migration Flows|Foreign|Overseas", X__6) == FALSE] # Remove Elements	
      data_table <- as.data.table(rbind(data_table, data_table2, tots))
      data_table <- data_table[, c("org_state", "org_county", "des_state", "des_county", "year", "returns", "exemptions", "file_name"), with = FALSE] # Keep Columns
      data_table <- data_table[des_state %in% us_states$FIPS_CODE] # Recognized State
      data_table <- data_table[order(org_state, org_county, des_state, des_county)] # Order Observations
    }
  }
}

##### Order Observations #####
migration_20092010 <- migration_20092010[order(org_state, org_county, des_state, des_county)]


#################### Prepare Data ####################

########## Merge Data ##########
county_migration_data <- list(migration_19901991, migration_1993, migration_19921994, migration_19952003, migration_20042006, migration_20072008, migration_20092010) # List Data Tables
county_migration_data <- rbindlist(county_migration_data) # Row Bind Data

########## Clean Workspace ##########
rm(migration_19901991) # Remove From Workspace
rm(migration_19921994) # Remove From Workspace
rm(migration_19952003) # Remove From Workspace
rm(migration_20042006) # Remove From Workspace
rm(migration_1993) # Remove From Workspace
rm(migration_20072008) # Remove From Workspace
rm(migration_20092010) # Remove From Workspace
gc() # Garbage Collection

########## Prepare Data ##########

##### Create Variables #####
county_migration_data <- county_migration_data[, origin := paste0(org_state, org_county)] # Origin Code
county_migration_data <- county_migration_data[, destination := paste0(des_state, des_county)] # Origin Code

##### Specify Column Class #####
county_migration_data <- county_migration_data[, origin := as.character(origin)] # Character Class
county_migration_data <- county_migration_data[, destination := as.character(destination)] # Character Class
county_migration_data <- county_migration_data[, year := as.integer(year)] # Integer Class
county_migration_data <- county_migration_data[, returns := as.numeric(returns)] # Numeric Class
county_migration_data <- county_migration_data[, exemptions := as.numeric(exemptions)] # Numeric Class

##### Finalize Dataset #####
county_migration_data <- county_migration_data[, c("origin", "destination", "year", "exemptions"), with = FALSE] # Keep Columns
county_migration_data <- county_migration_data[order(origin, destination, year)] # Order Observations
county_migration_data <- unique(county_migration_data) %>% # Remove Duplicates
  mutate(year = case_when( # fixing the dates
    year == 405 ~ 2004,
    year == 506 ~ 2005,
    year == 607 ~ 2006,
    year == 708 ~ 2007,
    year == 809 ~ 2008,
    year == 910 ~ 2009,
    year == 1011 ~ 2010,
    TRUE ~ as.numeric(year))
  )
county_migration_data <- as.data.table(county_migration_data) # converting to data table

##### Max Reported Returns #####
county_migration_data <- county_migration_data[, .SD[which.max(exemptions), ], by = c("origin", "destination", "year")] # Remove Less Reported Values

##### Case Dataset #####
county_migration_data <- dcast.data.table(county_migration_data, origin + destination ~ year, value.var = "exemptions", fill = 0) # Case Dataset
county_migration_data <- county_migration_data[order(origin, destination)] # Order Observations

##### Proportions Dataset #####
county_migration_probs <- gather(county_migration_data, Year, mig, `1990`:`2010`) %>% # going from wide to tall
  group_by(origin, Year) %>% # grouping by origin and year
  mutate(freq = mig/sum(mig)) %>% # calculating the relative frequency
  dplyr::select(-mig) %>% # deselecting the count variable
  spread(Year, freq) # going from tall to wide

########## Export Dataset ##########
write.table(county_migration_data, "DATA-PROCESSED/county_migration_data.txt", sep = "\t", row.names = FALSE) # Tab Delimited

