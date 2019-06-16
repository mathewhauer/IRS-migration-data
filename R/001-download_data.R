######################################################
##
## IRS County-to-County Migration Data
##       Download Data
##
## Authors: Mathew Hauer, mehauer@fsu.edu
##          James Byars, jmbyars@uga.edu
## Last updated: 08/16/2018
##
######################################################


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


######################### Download & Unzip IRS Migration Data ##########

# Setting the first set of years to download. 1990-2003 are in the same format. 2004- are in a second format
years <- c('1990to1991',
           '1991to1992',
           '1992to1993',
           '1993to1994',
           '1994to1995',
           '1995to1996',
           '1996to1997',
           '1997to1998',
           '1998to1999',
           '1999to2000',
           '2000to2001',
           '2001to2002',
           '2002to2003',
           '2003to2004'
)

for (year in years){
  download.file(paste0("https://www.irs.gov/pub/irs-soi/",year,"countymigration.zip"), # download the IRS migration data
                dest=paste0("MigData/",year,"countymigration.zip"), # destination is MigData/
                mode="wb")
  unzip(paste0("MigData/", year, "countymigration.zip"), exdir = paste0("./MigData/", year))
}

# Setting the second set of years to download.
years <- c('0405',
           '0506',
           '0607',
           '0708',
           '0809',
           '0910',
           '1011'
)
for (year in years){
  download.file(paste0("https://www.irs.gov/pub/irs-soi/county",year,".zip"), # download the IRS migration data
                dest=paste0("MigData/",year,"countymigration.zip") # destination is MigData/
                , mode="wb")
  unzip(paste0("MigData/", year, "countymigration.zip"), exdir = paste0("./MigData/", year))
}

## The following IRS files do not contain data and cannot be read.
unlink("MigData/1998to1999/1998to1999CountyMigration/1998to1999CountyMigrationInflow/co989usi.xls") # file cannot be read and contains no information
unlink("MigData/1998to1999/1998to1999CountyMigration/1998to1999CountyMigrationOutflow/co989uso.xls")
unlink("MigData/1999to2000/1999to2000CountyMigration/1999to2000CountyMigrationInflow/co990usi.xls")
unlink("MigData/1999to2000/1999to2000CountyMigration/1999to2000CountyMigrationOutflow/co990uso.xls")
unlink("MigData/2000to2001/2000to2001CountyMigration/2000to2001CountyMigrationInflow/co001usir.xls")
unlink("MigData/2000to2001/2000to2001CountyMigration/2000to2001CountyMigrationOutflow/co001usor.xls")
unlink("MigData/2001to2002/2001to2002CountyMigration/2001to2002CountyMigrationInflow/co102usi.xls")
unlink("MigData/2001to2002/2001to2002CountyMigration/2001to2002CountyMigrationOutflow/co102uso.xls")
## These files are the only years that contain foreign migration. It's a double accounting.
unlink("MigData/1990to1991/1990to1991CountyMigration/1990to1991CountyMigrationInflow/C9091fri.txt") 
unlink("MigData/1990to1991/1990to1991CountyMigration/1990to1991CountyMigrationOutflow/C9091fro.txt")
unlink("MigData/1991to1992/1991to1992CountyMigration/1991to1992CountyMigrationInflow/C9192fri.txt")
unlink("MigData/1991to1992/1991to1992CountyMigration/1991to1992CountyMigrationOutflow/C9192fro.txt")