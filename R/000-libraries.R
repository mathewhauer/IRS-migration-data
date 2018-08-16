######################################################
##
## IRS County-to-County Migration Data
##       Prepare R Session
##
## Authors: Mathew Hauer, mehauer@fsu.edu
##          James Byars, jmbyars@uga.edu
## Last updated: 08/16/2018
##
######################################################


#Clean Up Workspace 
rm(list = ls()) # Remove Previous Workspace
gc(reset = TRUE) # Garbage Collection


# R Workspace Options
options(scipen = 12) # Scientific Notation
options(digits = 6) # Specify Digits
options(java.parameters = "-Xmx1000m") # Increase Java Heap Size


#Functions, Libraries, & Parallel Computing 
## Functions 
# Specify Number of Digits (Forward)
numb_digits_F <- function(x,y){
  numb_digits_F <- do.call("paste0", list(paste0(rep(0, y - nchar(x)), collapse = ""), x))
  numb_digits_F <- ifelse(nchar(x) < y, numb_digits_F, x)
}

# Remove Double Space 
numb_spaces <- function(x) gsub("[[:space:]]{2,}", " ", x)


# Install and load pacman for easier package loading and installation
if (!require("pacman", character.only = TRUE)){
  install.packages("pacman", dep = TRUE)
  if (!require("pacman", character.only = TRUE))
    stop("Package not found")
}


# Libraries
pkgs <- c(
  "tidyverse",   # Tidyverse
  "data.table",  # Data Management/Manipulation
  "doParallel",  # Parallel Computing
  "foreach",     # Parallel Computing
  "openxlsx",    # Microsoft Excel Files
  "stringi",     # Character/String Editor
  "stringr",     # Character/String Editor
  "zoo",         # Time Series
  "reshape2"     # Data Management/Manipulation
)

# Install missing packages
# Will only run if at least one package is missing

if(!sum(!p_isinstalled(pkgs))==0){
  p_install(
    package = pkgs[!p_isinstalled(pkgs)], 
    character.only = TRUE
  )
}

# load the packages
p_load(pkgs, character.only = TRUE)


##Parallel Computing 
# Establish Parallel Computing Cluster
clusters <- makeCluster(detectCores() - 1) # Create Cluster with Specified Number of Cores
registerDoParallel(clusters) # Register Cluster
# Parallel Computing Details
getDoParWorkers() # Determine Number of Utilized Clusters
getDoParName() #  Name of the Currently Registered Parallel Computing Backend
getDoParVersion() #  Version of the Currently Registered Parallel Computing Backend

## Reference Data 
# Open & Prepare Data 
us_states <- read_tsv("./MigData/ref_state.tsv") # Open Data
us_states <- us_states %>%
  filter(!(STATE_ABBREV %in% c("AS","FM","GU","MH","MP","PR","PW","VI","XX","ZZ")))%>% # Remove Non States
  na.omit %>%
  add_row(STATE_ABBREV = "UN", STATE_DESCR = "Unknown", FIPS_CODE = "99") # add the unknown destination FIPS