# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime,timedelta
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Users/Yogeswaran_Sankar@condenast.com/Print_Onsert_Inserts/Onsert_Insert_Data_Prep

# COMMAND ----------

def load_crm_file(path):
  crm_data=spark.read.option('header','True').option('delimiter','|').csv(path)\
    .filter(F.col("STATE").isin(['AA','AE','AP','PR'])==False).withColumn('nrsp',F.lit(1)).drop_duplicates(subset=['IND_URN'])
  return crm_data

# COMMAND ----------

def apply_Oceania_filters(df_ind_mag_info):
  Active_Subscriber=df_ind_mag_info.filter((F.col('country_code')=='U')&(F.col('type_of_address')=='C')&
                            (((F.col('ib_household_income')>5)&((F.col('age')>40)|(F.col('age')=='0')))|
                             ((F.col('age')>59)&(F.col('HomeValue').isin ('K','L','M','N','O','P','Q','R','S'))))).\
                             drop_duplicates(subset=['ind_urn']).drop('mag_magazine_code','mag_paid_status','mag_record_status').\
                             withColumn('Active_Subscriber',F.lit('Y'))
  return Active_Subscriber


# COMMAND ----------

#Prepare Model Training Data  
date='2019-01-19'
infobase_date='2019-01-19'
indiv_path='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/individual/'
ib_path='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/infobase/'
mag_level='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/mag_level/'
mag_ord_level='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/mag_order_level/'
crm_file_path='s3://cn-consumerintelligence/Yoges/mp_20170810_174990.txt'
mag_list=['AD','AL','BA','BR','CT','DE','GL','GQ','NY','SL','VF','VO','WD','WW','LK','TV','GD','GW']
#Load CRM file
crm_data=load_crm_file(crm_file_path)
df_ind_mag_info=load_acxiom_data(date,infobase_date,'CT',indiv_path,ib_path,mag_level)
Active_Subscriber=apply_Oceania_filters(df_ind_mag_info)
final_users=load_negative_samples(crm_data,Active_Subscriber)
model_training_data=create_model_training_data(final_users)

# COMMAND ----------



# COMMAND ----------

