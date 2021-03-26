library(dplyr)
library(datapkg)

##################################################################
#
# Processing Script for Student-Enrollment-by-Special-Education-Status
# Created by Jenna Daly
# On 03/22/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
data_location <- grep("Special", sub_folders, value=T)
path_to_top_level <- (paste0(getwd(), "/", data_location))
path_to_raw_data <- (paste0(getwd(), "/", data_location, "/", "raw"))
all_csvs <- dir(path_to_raw_data, recursive=T, pattern = ".csv") 
all_state_csvs <- dir(path_to_raw_data, recursive=T, pattern = "ct.csv") 
all_dist_csvs <- all_csvs[!all_csvs %in% all_state_csvs]

student_enrollment_dist <- data.frame(stringsAsFactors = F)
student_enrollment_dist_noTrend <- grep("trend", all_dist_csvs, value=T, invert=T)
for (i in 1:length(student_enrollment_dist_noTrend)) {
  current_file <- read.csv(paste0(path_to_raw_data, "/", student_enrollment_dist_noTrend[i]), stringsAsFactors=F, header=F )
  #remove first 4 rows
  current_file <- current_file[-c(1:4),]
  colnames(current_file) = current_file[1, ]
  current_file = current_file[-1, ] 
  current_file <- current_file[, !(names(current_file) == "District Code")]
  get_year <- as.numeric(substr(unique(unlist(gsub("[^0-9]", "", unlist(student_enrollment_dist_noTrend[i])), "")), 1, 4))
  get_year <- paste0(get_year, "-", get_year + 1) 
  current_file$Year <- get_year
  student_enrollment_dist <- rbind(student_enrollment_dist, current_file)
}

#Rename statewide data...
student_enrollment_dist[["District"]][student_enrollment_dist$"District" == "Total"]<- "Connecticut"
student_enrollment_dist$Total <- NULL

#backfill Districts
district_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-school-district-list/master/datapackage.json'
district_dp <- datapkg_read(path = district_dp_URL)
districts <- (district_dp$data[[1]])

student_enrollment_fips <- merge(student_enrollment_dist, districts, by.x = "District", by.y = "District", all=T)

student_enrollment_fips$District <- NULL

student_enrollment_fips<-student_enrollment_fips[!duplicated(student_enrollment_fips), ]

#backfill year
years <- c("2007-2008",
           "2008-2009",
           "2009-2010",
           "2010-2011",
           "2011-2012",
           "2012-2013",
           "2013-2014",
           "2014-2015",
           "2015-2016", 
           "2016-2017", 
           "2017-2018",
           "2018-2019",
           "2019-2020",
           "2020-2021")

backfill_years <- expand.grid(
  `FixedDistrict` = unique(districts$`FixedDistrict`),
  `Year` = years
)

backfill_years$FixedDistrict <- as.character(backfill_years$FixedDistrict)
backfill_years$Year <- as.character(backfill_years$Year)

backfill_years <- arrange(backfill_years, FixedDistrict)

complete_student_enrollment <- merge(student_enrollment_fips, backfill_years, all=T)

#remove duplicated Year rows
complete_student_enrollment <- complete_student_enrollment[!with(complete_student_enrollment, is.na(complete_student_enrollment$Year)),]

#Stack category columns
cols_to_stack <- c("No", "Yes")

long_row_count = nrow(complete_student_enrollment) * length(cols_to_stack)

complete_student_enrollment_long <- reshape(complete_student_enrollment,
                                            varying = cols_to_stack,
                                            v.names = "Value",
                                            timevar = "Special Education Status",
                                            times = cols_to_stack,
                                            new.row.names = 1:long_row_count,
                                            direction = "long"
)

#Rename FixedDistrict to District
names(complete_student_enrollment_long)[names(complete_student_enrollment_long) == 'FixedDistrict'] <- 'District'


#reorder columns and remove ID column
complete_student_enrollment_long <- complete_student_enrollment_long[order(complete_student_enrollment_long$District, complete_student_enrollment_long$Year),]
complete_student_enrollment_long$id <- NULL

complete_student_enrollment_long$`Special Education Status`[complete_student_enrollment_long$`Special Education Status` == "Yes"] <- "Special Education"
complete_student_enrollment_long$`Special Education Status`[complete_student_enrollment_long$`Special Education Status` == "No"] <- "Non-Special Education"

#return blank in FIPS if not reported
complete_student_enrollment_long$FIPS <- as.character(complete_student_enrollment_long$FIPS)
complete_student_enrollment_long[["FIPS"]][is.na(complete_student_enrollment_long[["FIPS"]])] <- ""

#recode missing data with -6666
complete_student_enrollment_long$Value[is.na(complete_student_enrollment_long$Value)] <- -6666

#recode suppressed data with -9999
complete_student_enrollment_long$Value[complete_student_enrollment_long$Value == "*"]<- -9999

#setup Measure Type 
complete_student_enrollment_long$"Measure Type" <- "Number"

#set Variable
complete_student_enrollment_long$"Variable" <- "Student Enrollment"

#Order columns
complete_student_enrollment_long <- complete_student_enrollment_long %>% 
  select(`District`, `FIPS`, `Year`, `Special Education Status`, `Variable`, `Measure Type`, `Value`)

#Use this to find if there are any duplicate entries for a given district
test <- complete_student_enrollment_long[,c("District", "Year", "Special Education Status")]
test2<-test[duplicated(test), ]

#Write CSV
write.table(
  complete_student_enrollment_long,
  file.path(path_to_top_level, "data", "student_enrollment_by_special_education_status_2008-2021.csv"),
  sep = ",",
  row.names = F
)