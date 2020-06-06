# Set working directory 
setwd("D:/DATA/CompleteJourney/")

# Load various data frames from the package
library(completejourney)
promotions<-get_promotions()
transactions<-get_transactions()
products<-products
coupons<-coupons
campaign_descriptions<-campaign_descriptions
campaigns<-campaigns
demographics<-demographics

# save some of the data frames to csv files for inspection in Excel
# write.csv(products,"product.csv")
# write.csv(transactions,"transactions.csv")

# create a data frame of transactions containing product information and promotions as well
library(dplyr)
trans_full<-left_join(transactions,products)
trans_full<-left_join(trans_full,promotions,by=c("product_id","store_id","week"))

# In order to identify for which product categories/types coupons 
# were used actively we merge 2 tables (coupons and products)
coupons_products<-left_join(coupons,products)
coupon_categories<-coupons_products%>%group_by(product_category)%>%summarise(n=n())
coupon_prodtypes<-coupons_products%>%group_by(product_type)%>%summarise(n=n())

# assess variety of display locations for each product type 
# (to check that they varied sufficiently for the chosen product type)
display<-trans_full%>%group_by(product_type)%>%
  summarise(
    n_0=sum(display_location=="0",na.rm=TRUE),
    n_1=sum(display_location=="1",na.rm=TRUE),
    n_2=sum(display_location=="2",na.rm=TRUE),
    n_3=sum(display_location=="3",na.rm=TRUE),
    n_4=sum(display_location=="4",na.rm=TRUE),
    n_5=sum(display_location=="5",na.rm=TRUE),
    n_6=sum(display_location=="6",na.rm=TRUE),
    n_7=sum(display_location=="7",na.rm=TRUE),
    n_9=sum(display_location=="9",na.rm=TRUE),
    n_A=sum(display_location=="A",na.rm=TRUE))

# assess variety of mailer locations for each product type
mailer<-trans_full%>%group_by(product_type)%>%
  summarise(
    n_0=sum(mailer_location=="0",na.rm=TRUE),
    n_a=sum(mailer_location=="A",na.rm=TRUE),
    n_c=sum(mailer_location=="C",na.rm=TRUE),
    n_d=sum(mailer_location=="D",na.rm=TRUE),
    n_f=sum(mailer_location=="F",na.rm=TRUE),
    n_h=sum(mailer_location=="H",na.rm=TRUE),
    n_j=sum(mailer_location=="J",na.rm=TRUE),
    n_l=sum(mailer_location=="L",na.rm=TRUE),
    n_p=sum(mailer_location=="P",na.rm=TRUE),
    n_x=sum(mailer_location=="X",na.rm=TRUE),
    n_Z=sum(mailer_location=="Z",na.rm=TRUE))

# prepare shredded cheese data
trans_cheese<-trans_full[(trans_full$product_type=="SHREDDED CHEESE")&!is.na(trans_full$product_type)&(!trans_full$quantity==0),]
# inspect frequencies of key variables
table(trans_cheese$display_location)
table(trans_cheese$mailer_location)
table(trans_cheese$mailer_location,trans_cheese$manufacturer_id)
table(trans_cheese$display_location,trans_cheese$manufacturer_id)
sum(trans_cheese$coupon_disc>0,na.rm=TRUE)
table(trans_cheese$manufacturer_id)
table(trans_cheese$product_id)
table(trans_cheese$package_size)

# create a data frame of basket size for cheese purchases 
# (only those where 1 SKU was chosen are valid for discrete choice modeling)
cheese_basket_size<-trans_cheese%>%group_by(basket_id)%>%summarise(n=n())
table(cheese_basket_size$n)

# only one sku chosen 
trans_cheese<-left_join(trans_cheese,cheese_basket_size)
trans_cheese_1_sku<-trans_cheese%>%filter(n==1,!manufacturer_id=="683") # manufacturer ID is just in case we need to delete some small manufacturer
table(trans_cheese_1_sku$display_location)
table(trans_cheese_1_sku$mailer_location)
sum(trans_cheese_1_sku$coupon_disc>0,na.rm=TRUE)
table(trans_cheese_1_sku$manufacturer_id)
table(trans_cheese_1_sku$product_id)

# store basket and product IDs corresponding to purchases where a single SKU was purchased
baskets<-unique(trans_cheese_1_sku$basket_id)
products<-unique(trans_cheese_1_sku$product_id)

# create choice sets (all basket-product IDs combinations)
choice<-expand.grid(basket_id=baskets,product_id=products)
choice<-left_join(choice,
                  select(trans_cheese_1_sku,
                         basket_id,
                         household_id,
                         store_id,
                         week,
                         transaction_timestamp),
                  by="basket_id")
choice<-choice%>%mutate(basket_product_id=paste(basket_id,product_id,sep = "_"))
choice<-choice[!duplicated(choice["basket_product_id"]),]

# vector of actualy purchased basket_id-product_id combinations
basket_product_ids<-unique(paste(trans_cheese_1_sku$basket_id,trans_cheese_1_sku$product_id,sep = "_"))

# indicate choice=1 if basket-product combination is in the corresponding vector of actually purchased ones
choice<-choice%>%mutate(choice=ifelse(basket_product_id %in% basket_product_ids,1,0))

# information on price/discounts
price_discounts<-trans_cheese_1_sku%>%mutate(
  price=sales_value/quantity,
  retail_disc2=retail_disc/quantity, 
  coupon_disc2=coupon_disc/quantity,
  coupon_match_disc2=coupon_match_disc/quantity)%>%
  select(product_id, 
         store_id, 
         week,
         price,
         retail_disc2,
         coupon_disc2,
         coupon_match_disc2)

choice<-left_join(choice, price_discounts,
                  by=c("product_id", "store_id", "week"))

# some additional checks to  make sure 
basket_id_count<-distinct(choice)%>%na.omit()%>%group_by(basket_id)%>%summarise(n=n())%>%filter(n>1)
choice<-distinct(choice)%>%na.omit()%>%filter(basket_id %in% basket_id_count$basket_id)

# merge choice with promotion data
choice<-left_join(choice,promotions,by=c("product_id","store_id","week"))

# expand so that we have information which coupon was available on each day for each household
coupons_all<-inner_join(campaigns,campaign_descriptions)
coupons<-coupons%>%filter(product_id %in% products)
coupons_all<-inner_join(coupons_all,coupons)
coupons_all<-left_join(coupons_all,coupon_redemptions)
coupons_all$end_date<-ifelse(!is.na(coupons_all$redemption_date)&(coupons_all$redemption_date<coupons_all$end_date),
                             coupons_all$redemption_date,
                             coupons_all$end_date)
coupons_all$end_date<-as.Date(coupons_all$end_date)
coupons_all$days<-as.numeric(difftime(coupons_all$end_date ,
                           coupons_all$start_date,
                           units = c("days")))
expanded <-coupons_all[rep(row.names(coupons_all), coupons_all$days), ]
coupons_all<-data.frame(expanded,date=expanded$start_date+(sequence(coupons_all$days)-1))


# add coupon availability to choice data by checking date, household id and product id
library(lubridate)
choice$date=as_date(choice$transaction_timestamp)
choice<-left_join(choice,coupons_all,by=c("date","household_id","product_id"))

# statistics on shredded cheese purchases
cheese_frequency<-trans_full%>%
  group_by(household_id)%>%
  summarise(n_purchases=sum(product_type=="SHREDDED CHEESE"&!is.na(product_type)))%>%
  filter(n_purchases>0)

# Simplified loyalty measure: brand of previous purchase: ..., ... or no information 
# (to avoid confusion between lack of loyalty and lack of information). 
# This is less of a problem in longer panels typically available for retailers.
# loyalty measure
# loyalty<-trans_cheese%>%
#   group_by(household_id)%>%
#   arrange(transaction_timestamp)%>%
#   mutate(manufacturer_id_previous=lag(manufacturer_id),
#          retail_disc2_previous=lag(retail_disc2),
#          coupon_disc2_previous=lag(coupon_disc2))

# check the number of households
choice<-left_join(choice, completejourney::products,by="product_id")
length(unique(choice$household_id))


# filter out those who chose  more than 1 option in each basket (set)
choice<-choice%>%group_by(basket_id)%>%mutate(n_chosen=sum(choice))%>%filter(n_chosen==1)

# create choice set number and number of alternative within each choice set
basket_seq<-choice%>%group_by(household_id,basket_id)%>%summarise(n_options=n(),date=first(date))%>%ungroup()%>%arrange(household_id,date)%>%mutate(set=row_number())
choice<-left_join(choice,basket_seq,by=c("basket_id", "household_id", "date"))
choice<-as.data.frame(choice%>%group_by(basket_id)%>%mutate(alt=row_number()))

# arrange observations in a convinient order
choice<-choice%>%arrange(household_id,set,alt)

# add demographic data about households to choice data
choice<-left_join(choice,completejourney::demographics)

# add shelf price
choice$shelf_price<-choice$price+choice$retail_disc2+choice$coupon_match_disc2

# Final variable transfarmations
choice$display_location[is.na(choice$display_location)] = 0
choice$mailer_location[is.na(choice$mailer_location)] = 0
choice$campaign_type<-factor(choice$campaign_type,levels=c("None","Type A","Type B"),labels=c("None","TypeA","TypeB"))
choice$campaign_type[is.na(choice$campaign_type)]="None"
choice$manufacturer_id<-as.factor(choice$manufacturer_id)
choice$household_id<-as.factor(choice$household_id)
choice$package_size[choice$package_size=="B   8 OZ"]<-"8 OZ"
choice$basket_id<-as.factor(choice$basket_id)
choice$choice<-as.numeric(choice$choice)
choice$package_size_oz[choice$package_size=="12 OZ"]<-12
choice$package_size_oz[choice$package_size=="16 OZ"]<-16
choice$package_size_oz[choice$package_size=="2 LB"]<-32
choice$package_size_oz[choice$package_size=="24 OZ"]<-24
choice$package_size_oz[choice$package_size=="5 OZ"]<-5
choice$package_size_oz[choice$package_size=="6 OZ"]<-6
choice$package_size_oz[choice$package_size=="7 OZ"]<-7
choice$package_size_oz[choice$package_size=="8 OZ"]<-8
choice$price_per_oz<-choice$price/choice$package_size_oz
choice$shelf_price_per_oz<-choice$shelf_price/choice$package_size_oz
choice$discount_perc<-choice$retail_disc2/choice$shelf_price*100
library(gdata)
choice<-drop.levels(choice)

# convert some demographic factors to numeric
library(dplyr)
choice$income_num<-recode(choice$income,
                          "Under 15K"=12.5,
                          "15-24K"=19.5,
                          "25-34K"=29.5,
                          "35-49K"=42,
                          "50-74K"=62,
                          "75-99K"=87,
                          "100-124K"=112,
                          "125-149K"=137,
                          "150-174K"=162,
                          "175-199K"=187,
                          "200-249K"=224.5,
                          "250K+"=275)
choice$household_size_num<-recode(choice$household_size,
                                  "1"=1,
                                  "2"=2,
                                  "3"=3,
                                  "4"=4,
                                  "5+"=5.5)
choice$kids_count_num<-recode(choice$kids_count,
                              "0"=0,
                              "1"=1,
                              "2"=2,
                              "3+"=3.5)

# convert factor variables to dummy variables
library(fastDummies)
choice <- dummy_cols(choice, 
                select_columns = c("display_location",
                            "mailer_location",
                            "campaign_type",
                            "manufacturer_id",
                            "home_ownership",
                            "marital_status",
                            "household_comp"))

# subsets suitable for panel data analysis (several baskets sets per household)
n_baskets<-choice%>%
  group_by(household_id,basket_id)%>%
  summarise(x=mean(basket_id))%>%
  ungroup()%>%
  group_by(household_id)%>%
  summarise(n_baskets=n())%>%
  group_by(household_id)%>%
  summarise(n_baskets=mean(n_baskets))

choice<-left_join(choice,n_baskets)

choice2<-choice%>%filter(n_baskets>1)
choice3<-choice%>%filter(n_baskets>2)
choice4<-choice%>%filter(n_baskets>3)
choice6<-choice%>%filter(n_baskets>5)

# save choice and choice4 in Stata format
library(foreign)
write.dta(choice,"choice.dta")
write.dta(choice2,"choice2.dta")
write.dta(choice3,"choice3.dta")
write.dta(choice4,"choice4.dta")

# import beta estimates and join them with demographic data
library(foreign)
betas<-read.dta("betas2.dta")
betas$household_id<-as.character(betas$household_id)
betas<-left_join(betas,demographics)
# convert some demographic factors to numeric
library(dplyr)
betas$income_num<-recode(betas$income,
                          "Under 15K"=12.5,
                          "15-24K"=19.5,
                          "25-34K"=29.5,
                          "35-49K"=42,
                          "50-74K"=62,
                          "75-99K"=87,
                          "100-124K"=112,
                          "125-149K"=137,
                          "150-174K"=162,
                          "175-199K"=187,
                          "200-249K"=224.5,
                          "250K+"=275)
betas$household_size_num<-recode(betas$household_size,
                                  "1"=1,
                                  "2"=2,
                                  "3"=3,
                                  "4"=4,
                                  "5+"=5.5)
betas$kids_count_num<-recode(betas$kids_count,
                              "0"=0,
                              "1"=1,
                              "2"=2,
                              "3+"=3.5)
write.dta(betas,"betas.dta")
