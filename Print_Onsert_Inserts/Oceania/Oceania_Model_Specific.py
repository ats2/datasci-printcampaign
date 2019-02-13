# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime,timedelta
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

def load_crm_file(path,filter_codes):
  crm_data=spark.read.option('header','True').option('delimiter','|').csv(data_paths.get('CRM_Path'))\
    .filter(F.col("STATE").isin(filter_codes.get('states'))==False).withColumn('nrsp',F.lit(1)).drop_duplicates(subset=['IND_URN'])
  return crm_data

# COMMAND ----------

def apply_Oceania_filters(df_ind_mag_info,filter_codes):
#   Active_Subscriber=df_ind_mag_info.filter((F.col('country_code').isin(filter_codes.get('country_code')))&\
#                                            (F.col('type_of_address').isin(filter_codes.get('country_code')))&\
#                                            (((F.col('ib_household_income')>filter_codes.get('income_code'))&((F.col('age')>40)|\
#                                            (F.col('age')=='0')))|((F.col('age')>59)&(F.col('HomeValue').\
#                                             isin(filter_codes.get('home_values')))))).\
#                                        drop_duplicates(subset=['ind_urn']).drop('mag_magazine_code','mag_paid_status','mag_record_status').\
#                                        withColumn('Active_Subscriber',F.lit('Y'))
  Active_Subscriber=df_ind_mag_info.filter((F.col('country_code')=='U')&(F.col('type_of_address')=='C')&
                            (((F.col('ib_household_income')>5)&((F.col('age')>40)|(F.col('age')=='0')))|
                             ((F.col('age')>59)&(F.col('HomeValue').isin ('K','L','M','N','O','P','Q','R','S'))))).\
                             drop_duplicates(subset=['ind_urn']).drop('mag_magazine_code','mag_paid_status','mag_record_status').\
                             withColumn('Active_Subscriber',F.lit('Y'))
  return Active_Subscriber

# COMMAND ----------

# MAGIC %run /Users/Yogeswaran_Sankar@condenast.com/Print_Onsert_Inserts/Onsert_Insert_Data_Prep

# COMMAND ----------

#Prepare Model Training Data  
data_paths={'indiv':'s3://cn-consumerintelligence/People/asilcox/acxiom/orc/individual/',
            'infobase':'s3://cn-consumerintelligence/People/asilcox/acxiom/orc/infobase/',
            'Mag_level':'s3://cn-consumerintelligence/People/asilcox/acxiom/orc/mag_level/',
            'Mag_Ord_level':'s3://cn-consumerintelligence/People/asilcox/acxiom/orc/mag_order_level/',
            'CRM_Path':'s3://cn-data-reporting/spire/oceania_mp/mp_20190117_193916.txt'}
dates={'date':'2019-01-19',
       'ib_date':'2019-01-19',
       'end_date':'2019-01-19'}
magazine_codes={'CT':['2','3','4','5','6']}
filter_codes={'income_code':5,
              'home_values':['K','L','M','N','O','P','Q','R','S'],
              'country_code':['U'],
              'type_of_address':['C'],
             'states':['AA','AE','AP','PR'],
             'mag_record_status':[0],
             'add_deliv':[' ','A'],
             'order_paid_status':['2','3','4','5']}

#Load CRM file
crm_data=load_crm_file(data_paths,filter_codes)
df_ind_mag_info=load_acxiom_data(dates,data_paths,magazine_codes,filter_codes)
Active_Subscriber=apply_Oceania_filters(df_ind_mag_info,filter_codes)
final_users=load_negative_samples(crm_data,Active_Subscriber)
model_training_data=create_model_training_data(final_users,dates,data_paths)

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
model_training_data.count()

# COMMAND ----------

model_training_data.cache()

# COMMAND ----------

model_training_data.coalesce(1).write.format('csv').save('s3://cn-consumerintelligence/Yoges/Oceania/Oceania_training_data',header=True,sep='\t',mode='overwrite')

# COMMAND ----------

train_data=spark.read.format('csv').load('s3://cn-consumerintelligence/Yoges/Oceania/Oceania_training_data', header=True, sep='\t', inferSchema='true')

# COMMAND ----------

dbutils.fs.ls('s3://cn-consumerintelligence/Yoges/Oceania/')

# COMMAND ----------

mean_var='''AGE
IB_HOUSEHOLD_INCOME
IB_Household_Size_100
IB_INCOME_ROLLUP
IB_Length_of_Residence_100
IB_PREM_DWELLING_SIZE
IB_NET_WORTH_ESTIMATOR
IB_PP_HOUSEHOLD_SIZE
IB_PP_NUMBER_OF_ADULTS
IB_PP_NUMBER_OF_CHILDREN
IB_PREM_HOUSEHOLD_SIZE
IB_PREM_NUMBER_OF_CHILDREN
homevalue
IB_Adults_Num_HH_100
IB_Age_1st_Individual_100
IB_Age_Input_Individual'''
mean_var=mean_var.split(sep='\n')
mean_lower=[i.lower() for i in mean_var]
model_training_data1=model_training_data.select(mean_lower)

# COMMAND ----------

display(model_training_data1)

# COMMAND ----------

