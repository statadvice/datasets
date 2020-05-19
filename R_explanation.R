setwd("D:/DATA/Dunnhumby/")
## START H2O SERVER
tempdir <- function() "C:/temp"
unlockBinding("tempdir", baseenv())
assignInNamespace("tempdir", tempdir, ns="base", envir=baseenv()) 
assign("tempdir", tempdir, baseenv())
lockBinding("tempdir", baseenv())
library(h2o)
h2o.init(ip = 'localhost', port = 54321, nthreads= -1,max_mem_size = '5g') 

# BEST MODEL
# Use best_gbm parameters and estimate model.gbm based on the whole dataset
model.gbm<-h2o.gbm(x=x,
                   y=y,
                   training_frame=as.h2o(sales_wide),
                   col_sample_rate =0.8,
                   learn_rate =0.05,
                   learn_rate_annealing =0.99,
                   max_depth =5,
                   min_rows =1.0,
                   sample_rate=0.5,                           
                   nfolds=0,
                   seed=1)
library(dplyr)
# 1. create a data frame with just the features
features <- sales_wide %>% select(-starts_with("UNITS"),
                                  -starts_with("UPC_SHORT_STORE_NUM"),
                                  -WEEK_END_DATE,
                                  -STORE_NUM,
                                  -month)

# 2. Create a vector with the actual responses
response <- as.numeric(as.vector(sales_wide$UNITS.AFC3))

# 3. Create custom predict function that returns the predicted values as a
#	vector (probability of purchasing in our example) 
custom_predict <- function(model, newdata)  {
  newdata_h2o <- as.h2o(newdata)
  res <- as.data.frame(h2o.predict(model, newdata_h2o))
  return(as.numeric(res$predict))
}

# predicted values
sales_wide$model.gbm.pred<-custom_predict(model.gbm, features)

# generate absolute errors
sales_wide$model.gbm.abserror<-abs(sales_wide$model.gbm.pred-sales_wide$UNITS.AFC3)

# observations with the highest and the lowest predicted sales 
high<-which.max(sales_wide$model.gbm.pred)
low<-which.min(sales_wide$model.gbm.pred)

# create data frames for each of these two observations 
high_sales_ob <- features[high, ] 
low_sales_ob <- features[low, ]

# VARIABLE IMPORTANCE
varimp<-h2o.varimp(model.gbm) 
write.csv(varimp,"varimp.csv")

# SHAP
library(iml)
predictor.gbm <- Predictor$new( model = model.gbm,
                               data = features, 
                               y = response,
                               predict.fun = pred, 
                               class = "regression"
)
library(iml)
set.seed(1)
shapley.gbm.high <- Shapley$new(predictor.gbm, 
                               x.interest = high_sales_ob)
shapley.gbm.high 
plot(shapley.gbm.high)

# Shapley value decomposition set.seed(1)
shapley.gbm.low <- Shapley$new(predictor.gbm, 
                              x.interest = low_sales_ob) 
shapley.gbm.low
plot(shapley.gbm.low)

write.csv(shapley.gbm.high$results,"shapley.gbm.high.csv") 
write.csv(shapley.gbm.low$results,"shapley.gbm.low.csv")

averages<-as.data.frame(lapply (features, mean)) 
write.csv(averages,"averages.csv")




# library(DALEX)
# exp_gbm <- explain(model.gbm, 
#                    data = features,
#                    y=response, 
#                    predict_function = custom_predict,
#                    label="h2o gbm")
# 
# library("iBreakDown")
# shap_high_sales <- iBreakDown::shap(exp_gbm, high_sales_ob, B = 25)
# plot(shap_high_sales) 
# 
# shap_low_sales <- iBreakDown::shap(exp_gbm, low_sales_ob, B = 25)
# plot(shap_low_sales) 
