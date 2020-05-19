library(dplyr)
setwd("D:/DATA/Dunnhumby/")
# import sales data
sales<-read.csv("sales.csv")

# import csv file with relevant UPCs
UPC<-read.csv("UPC.csv")
# keep only these UPCs in sales data frame
sales<-sales[sales$UPC %in% unique(UPC$UPC),]
# add short UPCs to sales data frame y merging 2 data frames 
sales<-merge(sales, UPC[,c(1,ncol(UPC))], by="UPC",all.x=TRUE)

# Count number of days for which valid data is available for each store-UPC combination 
UPCs<-sales%>%group_by(UPC_SHORT,STORE_NUM)%>%summarise(n=n())

# Select popular stores with the maximum number of UPCs for which data is available for all 156 weeks.
POPULAR_STORES<-UPCs%>%
  group_by(STORE_NUM)%>%
  summarise(n_UPCs=sum(n>=156, na.rm=TRUE))%>%
  arrange(-n_UPCs)%>%top_n(10)

# Figure out UPCs that were available for 156 weeks in all the popular stores. 
COMPLETE_UPCs<-UPCs%>%
  filter(STORE_NUM%in%POPULAR_STORES$STORE_NUM)%>%
  group_by(UPC_SHORT)%>% 
  summarise(n_stores=sum(n>=156, na.rm=TRUE))%>% 
  filter(n_stores==nrow(POPULAR_STORES))

# drop levels of UPC_SHORT non-existing in COMPLETE_UPCs
sales<-sales%>%
  filter(UPC_SHORT %in% COMPLETE_UPCs$UPC_SHORT&STORE_NUM %in% POPULAR_STORES$STORE_NUM) 
sales$UPC_SHORT<-droplevels(sales$UPC_SHORT)

# Check that now we have only combinations of UPCs and stores with 156 weeks of data table(sales$UPC_SHORT,sales$STORE_NUM)

# delete unnecessary columns 
library(dplyr)
sales<-select(sales,-VISITS,-HHS,-SPEND,-UPC)

# reorder columns
sales<-cbind(select(sales,UPC_SHORT),select(sales,-UPC_SHORT))

# create new variables
sales$DISCOUNT=(sales$PRICE-sales$BASE_PRICE)/sales$BASE_PRICE*100 #if it's 0, then no TPR

# change date from factor to date 
library(lubridate)
sales$WEEK_END_DATE<-dmy(sales$WEEK_END_DATE)

# sort
sales<-sales%>%arrange(UPC_SHORT, STORE_NUM,WEEK_END_DATE)

# delete base price (a redundant variable given that we have price and discount)
sales<-sales%>%select(-BASE_PRICE)

#generate lags
sales$UPC_SHORT_STORE_NUM<-paste(sales$UPC_SHORT,sales$STORE_NUM) 
predictors<-c("PRICE","FEATURE","DISPLAY","TPR_ONLY","DISCOUNT")
library(DataCombine)
for (i in predictors) { for (j in 1:6) {
  sales<-slide(sales, Var=i,
               TimeVar="WEEK_END_DATE", GroupVar="UPC_SHORT_STORE_NUM",
               NewVar=paste(i,j,sep="."), slideBy = -j,
               keepInvalid = TRUE)
  
}
}


# rows to columns 
library(reshape2)
sales<-data.frame(sales) 
sales_wide<-reshape(sales,
                    idvar = c("STORE_NUM","WEEK_END_DATE"),
                    timevar = "UPC_SHORT", direction = "wide")

sales_wide<-data.frame(sales_wide)

# make STORE_NUM a factor variable to use it as a predictor 
sales_wide$STORE_NUM<-factor(sales_wide$STORE_NUM)

# create month
library(lubridate)
sales_wide$month<-lubridate::month(sales_wide$WEEK_END_DATE)

# create dummy variables for STORE_NUM, weekday and month library(fastDummies)
sales_wide<- fastDummies::dummy_cols(sales_wide, select_columns = c("STORE_NUM","month"))


# create holiday dummies 
library(timeDate) 
library(dplyr) 
library(lubridate)
interval<-interval(sales_wide$WEEK_END_DATE-6,sales_wide$WEEK_END_DATE) 
sales_wide$NewYearsDay=ifelse(as.Date(holiday(year = lubridate::year(sales_wide$WEEK_END_DATE), Holiday = "USNewYearsDay")) %within% interval,1,0)
sales_wide$MemorialDay=ifelse(as.Date(holiday(year = lubridate::year(sales_wide$WEEK_END_DATE), Holiday = "USMemorialDay")) %within% interval,1,0)
sales_wide$IndependenceDay=ifelse(as.Date(holiday(year = lubridate::year(sales_wide$WEEK_END_DATE), Holiday = "USIndependenceDay")) %within% interval,1,0)
sales_wide$LaborDay=ifelse(as.Date(holiday(year = lubridate::year(sales_wide$WEEK_END_DATE), Holiday = "USLaborDay")) %within% interval,1,0)
sales_wide$ThanksgivingDay=ifelse(as.Date(holiday(year = lubridate::year(sales_wide$WEEK_END_DATE), Holiday = "USThanksgivingDay")) %within% interval,1,0)
sales_wide$ChristmasDay=ifelse(as.Date(holiday(year = lubridate::year(sales_wide$WEEK_END_DATE), Holiday = "USChristmasDay")) %within% interval,1,0)
sales_wide$GoodFriday=ifelse(as.Date(holiday(year = lubridate::year(sales_wide$WEEK_END_DATE), Holiday = "USGoodFriday")) %within% interval,1,0) 
sales_wide$Halloween=ifelse(as.Date(paste(lubridate::year(sales_wide$WEEK_END_DATE), 10, 31, sep='-')) %within% interval,1,0)

# create week number
sales_wide$week_num<-rep(1:156,nrow(sales_wide)/156)

## TRAINING, VALIDATION and LEADERBOARD FRAMES
# version 2 (final)
train_frame2<-sales_wide[sales_wide$week_num<131,] # training frame 2
valid_frame2<-sales_wide[sales_wide$week_num>=131&sales_wide$week_num<=143,] # validation frame 2 
leader_frame2<-sales_wide[sales_wide$week_num>143,] # leaderboard frame 2

# omit rows with NAs
sales_wide<-na.omit(sales_wide)

## MODELING
# change temp directory (technical lines of code allowing h2o to work smoothly on our computer) 
tempdir <- function() "C:/temp"
unlockBinding("tempdir", baseenv())
assignInNamespace("tempdir", tempdir, ns="base", envir=baseenv()) 
assign("tempdir", tempdir, baseenv())
lockBinding("tempdir", baseenv())

# convert data frames to h2o object 
library(h2o)
h2o.init(ip = 'localhost', port = 54321, nthreads= -1,max_mem_size = '5g') 
sales_wide.h2o <- as.h2o(sales_wide)
train_frame2.h2o<-as.h2o(train_frame2) 
valid_frame2.h2o<-as.h2o(valid_frame2) 
leader_frame2.h2o<-as.h2o(leader_frame2)

# specify the features and the target column
# dependent variable 
y <- "UNITS.AFC3"
# predictors
x <- colnames(select(sales_wide,
                     -starts_with("UNITS"),
                     -starts_with("UPC_SHORT_STORE_NUM"),
                     -WEEK_END_DATE,
                     -STORE_NUM,
                     -month,
                     -week_num))

# Random forest model
# hyperparameter grid 
hyper_grid.h2o <- list(
  ntrees	= c(300, 500, 700), mtries	= seq(50, 400, by = 10)
)

# random grid search criteria 
search_criteria <- list(
  strategy = "RandomDiscrete", stopping_metric = "mae", stopping_tolerance = 0.001,
  stopping_rounds = 15, # if after 15 models the improvement is not achieved, then the search stops 
  max_runtime_secs=7200, # max runtime of 7200 seconds
  seed = 1
)

# build grid search 
grid <- h2o.grid(
  algorithm = "randomForest", grid_id = "rf_grid",
  x = x, y = y,
  training_frame = train_frame2.h2o, validation_frame=valid_frame2.h2o, 
  hyper_params = hyper_grid.h2o, search_criteria = search_criteria, seed=1
)

# collect the results and sort by our model performance metric of choice 
grid_perf.rf <- h2o.getGrid(
  grid_id = "rf_grid", sort_by = "mae", decreasing = FALSE
)
print(grid_perf.rf)


# GBM
# create hyperparameter grid 
hyper_grid.gbm <- list(
  max_depth = c(1,2,3,4,5), min_rows = c(1,5,10),
  learn_rate = c(0.01, 0.05, 0.1),
  learn_rate_annealing = c(.99, 1), sample_rate = c(.5, .75, 1),
  col_sample_rate = c(.8, .9, 1)
)

# perform grid search 
grid.gbm <- h2o.grid(
  algorithm = "gbm", grid_id = "gbm_grid1", x = x,
  y = y,
  training_frame = train_frame2.h2o, validation_frame=valid_frame2.h2o, hyper_params = hyper_grid.gbm, ntrees = 5000,
  search_criteria = search_criteria, seed=1
)

# collect the results and sort by our model performance metric of choice 
grid_perf.gbm <- h2o.getGrid(
  grid_id = "gbm_grid1", sort_by = "mae", decreasing = FALSE
)
grid_perf.gbm


# Regularized GLM
# grid over 'alpha' and 'lambda'
# select the values for `alpha` to grid over 
hyper_grid.glm <- list(
  alpha = seq(0, 1, by = 0.05), lambda = seq(0, 10, by = 0.1)
)

# build grid search with previously selected hyperparameters 
grid_perf.glm <- h2o.grid(
  x = x, y = y,
  training_frame = train_frame2.h2o, validation_frame=valid_frame2.h2o, algorithm = "glm",
  grid_id = "glm_grid1", hyper_params = hyper_grid.glm, search_criteria = search_criteria,
  seed=1
)

# Sort the grid models by mae
grid_perf.glm <- h2o.getGrid("glm_grid1", sort_by = "mae", decreasing = FALSE) 
grid_perf.glm

# Grab 4 top RF models, top GBM and top GLM
best_rf1 <- h2o.getModel(grid_perf.rf @model_ids[[1]]) 
best_rf2 <- h2o.getModel(grid_perf.rf @model_ids[[2]]) 
best_rf3 <- h2o.getModel(grid_perf.rf @model_ids[[3]]) 
best_rf4 <- h2o.getModel(grid_perf.rf @model_ids[[4]])
best_gbm1 <- h2o.getModel(grid_perf.gbm @model_ids[[1]]) 
best_glm1 <- h2o.getModel(grid_perf.glm @model_ids[[1]])

# PERFORMANCE OF ENSEMBLE VS. THE BASE LEARNER
# Eval ensemble performance on a test set
perf_gbm_best <- h2o.performance(best_gbm1, newdata = leader_frame2.h2o) 
perf_rf_best <- h2o.performance(best_rf1, newdata = leader_frame2.h2o) 
perf_glm_best <- h2o.performance(best_glm1, newdata = leader_frame2.h2o) 
perf_gbm_best 
perf_rf_best 
perf_glm_best

# Use best_rf parameters and estimate model.rf based on the whole dataset
#model.rf<-h2o.randomForest(x=x,
#                           y=y,
#                           training_frame=as.h2o(sales_wide),
#                           mtries=350,
#                           ntrees=700,      
#                           nfolds=0,
#                           seed=1)
