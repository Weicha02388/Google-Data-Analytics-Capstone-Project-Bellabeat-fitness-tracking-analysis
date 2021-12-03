#load libraries
library("dplyr")                                                
library("plyr")                                                 
library("readr")
library("tidyverse")
library("tibble")
library("lubridate")
library("janitor")
library("sqldf")

#Step1: Import all csv files
setwd('C:/Users/mypath....')

daily_activities_merged <- read.csv("dailyActivity_merged.csv")
daily_steps <- read.csv("dailySteps_merged.csv")

hourly_calories <- read.csv("hourlyCalories_merged.csv")
hourly_intensities <- read.csv("hourlyIntensities_merged.csv")
hourly_steps <- read.csv("hourlySteps_merged.csv")

minute_calories <- read.csv("minuteCaloriesNarrow_merged.csv") #minute calories NARROW has the same dataset as minute calories WIDE (only difference is table formatting)
minute_intensities <- read.csv("minuteIntensitiesNarrow_merged.csv") #minute intensities NARROW has the same dataset as minute intensities WIDE (only difference is table formatting)
minute_METs <- read.csv("minuteMETsNarrow_merged.csv")
minute_sleep <- read.csv("minuteSleep_merged.csv")
minute_steps <- read.csv("minuteStepsNarrow_merged.csv")

day_sleep <- read.csv("sleepDay_merged.csv")
weight_info <- read.csv("weightLogInfo_merged.csv")

seconds_heart_rate <- read.csv("heartrate_seconds_merged.csv")

#Step 2: check the minutes and hourly CALORIES, INTENSITIES and STEPS are consistent):

#Calories
colnames(hourly_calories)
head(hourly_calories)
n_distinct(hourly_calories$Id) #Finding: no. of unique customers are 33
sum(hourly_calories$Calories) #finding: 2152150

colnames(minute_calories)
head(minute_calories)
n_distinct(minute_calories$Id) #Finding: no. of unique customers are 33
sum(minute_calories$Calories) #finding: 2151588 (about the same - could be rounding off issues)
#Check if unique values are the same in both columns:
hourly_calories$Id[!(hourly_calories$Id %in% minute_calories$Id)] #all IDs are the same. 

#Intensities
colnames(hourly_intensities)
head(hourly_intensities)
n_distinct(hourly_intensities$Id) #Finding: no. of unique customers are 33
sum(hourly_intensities$TotalIntensity) #finding: 265969

colnames(minute_intensities)
head(minute_intensities)
n_distinct(minute_intensities$Id) #Finding: no. of unique customers are 33
sum(minute_intensities$Intensity) #finding: 265903 (about the same - could be rounding off issues)

#Check if unique values are the same in both columns:
hourly_intensities$Id[!(hourly_intensities$Id %in% minute_intensities$Id)] #all IDs are the same. 

#Steps
colnames(hourly_steps)
head(hourly_steps)
n_distinct(hourly_steps$Id) #Finding: no. of unique customers are 33
sum(hourly_steps$StepTotal) #finding: 7075356

colnames(minute_steps)
head(minute_steps)
n_distinct(minute_steps$Id) #Finding: no. of unique customers are 33
sum(minute_steps$Steps) #finding: 7073549 (about the same - could be rounding off issues)

#Check if unique values are the same in both columns:
hourly_steps$Id[!(hourly_steps$Id %in% minute_steps$Id)] #all IDs are the same. 

#Compare against Daily Steps dataset:
colnames(daily_steps)
head(daily_steps)
n_distinct(daily_steps$Id) #Finding: no. of unique customers are 33
sum(daily_steps$StepTotal) #finding: 7075356

##Conclusion, we can safely remove MINUTES data for CALORIES, INTENSITIES and STEPS given that they are exactly the same. Hourly for STEPS is also not needed as we have daily STEP figures.

#Step 3: Check data on Sleep:
colnames(day_sleep)
head(day_sleep)
n_distinct(day_sleep$Id) #Finding: no. of unique customers are 33
sum(day_sleep$TotalMinutesAsleep) #finding: 173240

colnames(minute_sleep)
head(minute_sleep)
n_distinct(minute_sleep$Id) #Finding: no. of unique customers are 24
sum(minute_sleep$value) #finding: 7073549 (about the same - could be rounding off issues)

##there is misalignment in sleep minutes. No need to include minute sleep.  Let's check the day_sleep dataset against daily activities merged dataset.

colnames(daily_activities_merged)
head(daily_activities_merged)
n_distinct(daily_activities_merged$Id) #Finding: no. of unique customers are 33
sum(daily_activities_merged$SedentaryMinutes) #finding: 931738

#Check if unique values are the same in both columns:
day_sleep$Id[!(day_sleep$Id %in% daily_activities_merged$Id)] #all IDs are the same. 

#Conclusion, we can remove minutes sleep and add a column to add daily sleep dataset.

#Step 4: Join Daily Sleep with Daily Activities Merged. Add a new column.
day_sleep_summarized <- day_sleep %>% 
  separate(SleepDay, into=c('sleep_day','sleep_time'), sep = ' ', remove = TRUE) %>% subset(select = -c(sleep_time))

sum(day_sleep_summarized$TotalMinutesAsleep) #double check against day_sleep dataset to ensure they are consistent
sum(day_sleep$TotalMinutesAsleep)

daily_activities_including_sleep <- left_join(daily_activities_merged,day_sleep_summarized,
                                              by=c("Id"="Id", "ActivityDate" = "sleep_day"),all.x=TRUE)
#check records to ensure no duplicates
count(daily_activities_merged, "Id")
count(daily_activities_including_sleep, "Id")


#Step 5: Fact check minute_METS and Weight Info
colnames(minute_METs)
head(minute_METs)
n_distinct(minute_METs$Id) #Finding: no. of unique customers are 33
sum(minute_METs$METs) #finding: 19472785

#Check if unique values are the same in both columns:
minute_METs$Id[!(minute_METs$Id %in% daily_activities_including_sleep$Id)] #all IDs are the same. 

colnames(weight_info)
head(weight_info)
n_distinct(weight_info$Id) #Finding: no. of unique customers are 8
#Conclusion: insufficient weight_info provided as only 8 people contributed to the dataset. Omit dataset avoid bias in the data.

#Step 6: Factcheck the heartbeat seconds dataset
colnames(seconds_heart_rate)
head(seconds_heart_rate)
n_distinct(seconds_heart_rate$Id) #Finding: no. of unique customers are 14
sum(seconds_heart_rate$Value) #finding: 19472785

#conclusion to omit seconds heartbeat dataset as there are insufficient dataset.

#Step 7: Join Hourly Intensity, Minute MET information with daily_activities_including_sleep dataset. Omit daily steps as data is included in merged file. 

#to join successfully, we need to summarize Intensity and METs data to Daily.
daily_intensity <- hourly_intensities %>% 
  separate(ActivityHour, into=c('activity_day','activity_hour'), sep = ' ', remove = TRUE) %>% subset(select = -c(activity_hour,AverageIntensity)) %>% 
  group_by(Id, activity_day) %>% summarise_each(funs(sum))

daily_METs <- minute_METs %>% 
  separate(ActivityMinute, into=c('activity_day','activity_time'), sep = ' ', remove = TRUE) %>% subset(select = -c(activity_time)) %>% 
  group_by(Id, activity_day) %>% summarise_each(funs(sum))


daily_activities_overall_1 <- left_join(daily_activities_including_sleep,daily_intensity,
                                        by=c("Id"="Id", "ActivityDate" = "activity_day"),all.x=TRUE)

daily_activities_overall_final <-left_join(daily_activities_overall_1,daily_METs,
                                           by=c("Id"="Id", "ActivityDate" = "activity_day"),all.x=TRUE)

#check records
count(daily_activities_including_sleep, "Id")
count(daily_activities_overall_1, "Id")
count(daily_activities_overall_2, "Id")
count(daily_activities_overall_final, "Id")

tibble(daily_activities_overall_final)
tibble(seconds_heart_rate)
n_distinct(daily_activities_overall_final$ActivityDate)