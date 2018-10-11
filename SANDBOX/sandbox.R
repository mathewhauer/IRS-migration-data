library(plyr)
library(tidyverse)
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

figure <- function(this.fips, this.name){
  a<- ggplot(data=migrants) +
    geom_line(data=migrants[which(migrants$location %in% this.fips),], aes(x = Years, y = Migrants, group=direction, color = direction, linetype=direction)) +
    theme_bw() +
    theme(axis.text.x = element_text(angle=70, vjust=0.5)) +
    scale_linetype(name = "",
                   labels = c("In Migrants", "Out Migrants")) +
    scale_color_discrete(name = "",
                         labels = c("In Migrants", "Out Migrants")) +
    labs(title = paste0("FIPS: ", this.fips, this.name),
         x = "Year",
         y = "Exemptions/Migrants") +
    NULL
  return(a)
}
a<-figure("22071", " Orleans LA") + annotate("text", x = "2002", y = 100000, label = "Hurricane Katrina →")
b<-figure("12011", " Broward FL") + 
  annotate("Rect",xmin="2006", xmax="2009", ymin=0, ymax=100000, alpha = 0.3) +
  annotate("text", x = "2007", y = 60000, label = "Housing Bubble Burst\n and \nGreat Recession")

c<-figure("32003", " Clark NV") +
  annotate("Rect",xmin="2006", xmax="2009", ymin=0, ymax=100000, alpha = 0.3) +
  annotate("text", x = "2007", y = 30000, label = "Housing Bubble Burst\n and \nGreat Recession")

d<-figure("36047", " Kings NY") 
e <- get_legend(d+ theme(legend.position="bottom"))

top <- plot_grid(a+ theme(legend.position="none"),
          b+ theme(legend.position="none"),
          c+ theme(legend.position="none"),
          d+ theme(legend.position="none"),
          ncol=2)
plot_grid(top, e, ncol=1, rel_heights = c(1,0.1))


a <- figure("16015", " Boise ID")
b <- figure("13125", " Glascock GA")
c <- figure("22087", " St. Barnard LA")
d <- figure("20061", " Geary KS")
e <- figure("02016", " Aleut W AK")
f <- figure("16033", " Clark ID")
g <- figure("22071", " Orleans LA")
h <- figure("51069", " Frederick VA")
i <- figure("22115", " Vernon LA")
j <- figure("51660", " Harrisonburg VA")




plot_grid(a,b,c,
          d,e,f,
          g,h,i,
          j,
          ncol = 2,
          labels = "AUTO")