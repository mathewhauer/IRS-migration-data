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
  # foreach(i = 1:length(years),  
  #         .errorhandling = "stop", 
  #         .packages = c("data.table", "doParallel", "foreach", "reshape2", "stringi", "stringr", "zoo")
  #         ) %dopar% {
            download.file(paste0("https://www.irs.gov/pub/irs-soi/",year,"countymigration.zip"), 
                          dest=paste0("MigData/",year,"countymigration.zip"),
                          mode="wb") 
            unzip(paste0("MigData/", 
                         year, 
                         "countymigration.zip"), 
                  exdir = paste0("./MigData/", 
                             year))
} 

  years <- c('0405',
             '0506',
             '0607',
             '0708',
             '0809',
             '0910',
             '1011'
  )
  for (year in years){  
  # foreach(i = 1:length(years),  .errorhandling = "stop", .packages = c("data.table", "doParallel", "foreach", "reshape2", "stringi", "stringr", "zoo")) %dopar% {
    download.file(paste0("https://www.irs.gov/pub/irs-soi/county",year,".zip"), dest=paste0("MigData/",year,"countymigration.zip")
                  , mode="wb") 
    unzip(paste0("MigData/", year, "countymigration.zip"), exdir = paste0("./MigData/", year))
  } 
  
unzip(paste0("MigData/", years[i], "countymigration.zip"), exdir = paste0("./MigData/", years[i]))


directory <- paste0("./MigData/",editions[i],"/", editions[i],"CountyMigration")
directory2 <- paste0("MigData/",editions[i],"/")
state_files <- list.files(directory, pattern = "*.txt$", recursive = TRUE) # List Files
state_files <- paste0(directory2, state_files)