
migdat<-read_tsv("DATA-PROCESSED/county_migration_data.txt")

inmig <- filter(migdat, !origin == destination) %>%
  group_by(destination) %>%
  dplyr::select(-origin) %>%
  summarise_all(sum) %>%
  mutate(direction = "inmigrants") %>%
  gather(Years, Migrants, `1990`:`2010`) %>%
  dplyr::select(location = destination, everything())

outmig <- filter(migdat, !origin == destination) %>%
  group_by(origin) %>%
  dplyr::select(-destination) %>%
  summarise_all(sum)  %>%
  mutate(direction = "outmigrants")%>%
  gather(Years, Migrants, `1990`:`2010`) %>%
  dplyr::select(location = origin, everything())

migrants <- rbind(inmig, outmig) 

dat <- data.frame(origin = as.character(unique(migdat$origin))) %>%
  mutate(origin = as.character(origin))

for(i in 3:(ncol(migdat)-1)){
  func <- function(data){return(COR = cor(data[i], data[i+1]))}
  # z[,cor(z[i],z[i+1]),by="origin"]
  a<-ddply(migdat, .(origin), func)
  dat <- full_join(dat, a)
}

fips <- "16015"
a<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Boise ID")) +
  NULL

fips <- "13125"
b<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Glascock GA")) +
  NULL

fips <- "22087"
c<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " St. Barnard LA")) +
  NULL

fips <- "20061"
d<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Geary KS")) +
  NULL

fips <- "02016"
e<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips), " Aleutians W AK") +
  NULL

fips <- "16033"
f<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Clark ID")) +
  NULL

fips <- "22071"
g<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Orleans LA")) +
  NULL

fips <- "51069"
h<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Frederick VA")) +
  NULL

fips <- "22115"
i<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Vernon LA")) +
  NULL

fips <- "51660"
j<- ggplot(data=migrants) +
  geom_line(data=migrants[which(migrants$location %in% fips),], aes(x = Years, y = Migrants, group=direction, color = direction)) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
  labs(title = paste0("FIPS: ", fips, " Harrisonburg VA")) +
  NULL



plot_grid(a,b,c,
          d,e,f,
          g,h,i,
          j,
          ncol = 2,
          labels = "AUTO")