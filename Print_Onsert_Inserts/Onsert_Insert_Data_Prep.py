# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime,timedelta
from pyspark.sql.window import Window

# COMMAND ----------

def load_acxiom_data(date,ib_date,mag_codes,indiv_path,ib_path,mag_level):
  
  # Load Individual Data
  df_ind=spark.read.orc(indiv_path+'dt='+date) \
    .select(['ind_urn','age','type_of_address','country_code','address_deliverable']) \
    .filter((F.col('address_deliverable')!='')&(F.col('address_deliverable')!='A'))  

  # Load Infobase Data
  df_infobase=spark.read.orc(ib_path+'dt='+ib_date) \
    .select(['ind_urn','ib_household_income','ib_home_market_value']) \
    .withColumn('HomeValue',F.substring(F.col('ib_home_market_value'), 1, 1))

  # Load Mag Level Data
### asilcox - would prefer to make this more generic - i.e. at a min. pass through the mag code or set of mag codes
  df_mag_level=spark.read.orc(mag_level+'dt='+date) \
    .select(['ind_urn','mag_magazine_code','mag_paid_status','mag_record_status']) \
    .filter((F.col('mag_magazine_code').isin(mag_codes)) & (F.col('mag_paid_status').isin('2','3','4','5','6')) & (F.col('mag_record_status')=='0'))

  # Get badpay cross all titles -- NEW!!! --
  df_badpay=spark.read.orc(mag_level+'dt='+date) \
    .select(['ind_urn','mag_badpay_times','mag_recent_badpay_date']) \
    .groupBy('ind_urn').agg(F.sum('mag_badpay_times').alias('mag_badpay_times'),F.max('mag_recent_badpay_date').alias('mag_recent_badpay_date'))
  
  # Merge tables
  df_ind_mag_info=df_mag_level.join(df_ind,'ind_urn') \
    .join(df_badpay,'ind_urn').filter(~(F.col('mag_badpay_times')>2) & (~(F.col('mag_recent_badpay_date')>=(datetime.strptime(date,'%Y-%m-%d')-timedelta(days=365)).strftime("%Y-%m-%d")))) \
    .join(df_infobase,'ind_urn','left_outer')

  return df_ind_mag_info

# COMMAND ----------

### asilcox - the splits will depend on the sample size - not always 20% - this should be a random selection train/test

#Collect matched CRM users and negative samples from active subscribers
def load_negative_samples(matched_crm,Active_Subscriber):
  final_users=matched_crm.join(Active_Subscriber,Active_Subscriber.ind_urn==matched_crm.IND_URN,'right_outer').\
                    drop(matched_crm.IND_URN).withColumn('splits',(F.col('ind_urn')%10)).filter((F.col('splits')=='2') |\
                    (F.col('splits')=='7')|(F.col('nrsp')==1)).withColumn('nrsp',F.when(F.col('nrsp')=='1',1).otherwise(0)).\
                    select('ind_urn','nrsp')
  return final_users

# COMMAND ----------

def create_individual_order_data(final_users,indiv_variables,ord_variables,Ib_vars,date,info_date,cat_cols,indiv_path,ib_path,mag_ord_level):
  ind_df=spark.read.orc(indiv_path+'dt='+date).select(indiv_variables).\
                      filter((F.col('address_deliverable')!='')&(F.col('address_deliverable')!='A'))
  info_df=spark.read.orc(ib_path+'dt='+info_date).select(Ib_vars).\
          withColumn('Married',F.when(F.col('ib_marital_status').isin('A','M'),F.lit(1)).otherwise(0))
  for i in cat_cols: 
    info_df=info_df.withColumn(i,F.when(F.substring(F.col(i),1,1)=='Y',F.lit(1)).otherwise(0))
  
  df_mag_ord=spark.read.orc(mag_ord_level+'dt='+date).select(ord_variables).\
                            filter(F.col('order_record_status')!=5)
  individual_data=ind_df.join(info_df,'ind_urn')
  Indiv_data=final_users.join(individual_data,['ind_urn'],'left_outer')
  Order_data=final_users.join(df_mag_ord,['ind_urn'],'left_outer').orderBy('ind_urn','order_magazine_code','order_date').drop('nrsp')
  return Order_data,Indiv_data

# COMMAND ----------

#create individual variable
def create_individual_vars(Indiv_data):
  Ind_Data=Indiv_data.withColumn('PSNrespond',F.when(F.substring(F.col('RESPONDED_TO_PSN'),1,1)=='Y',F.lit(1)).otherwise(0)).\
                    withColumn('email',F.when(F.substring(F.col('email_presence_flag'),1,1)=='Y',F.lit(1)).otherwise(0)).\
                    withColumn('HighSchool',F.when(F.substring(F.col('ib_education_1st_Ind_100'),1,1)==1,F.lit(1)).otherwise(0)).\
                    withColumn('College',F.when(F.substring(F.col('ib_education_1st_Ind_100'),1,1)==2,F.lit(1)).otherwise(0)).\
                    withColumn('GradSchool',F.when(F.substring(F.col('ib_education_1st_Ind_100'),1,1)==3,F.lit(1)).otherwise(0)).\
                    withColumn('hispanic',F.when(F.col('IB_ETHNIC_CODE')==20,F.lit(1)).otherwise(0)).\
                    withColumn('black',F.when(F.substring('IB_ETHNIC_CODE',1,1).isin('8','9','A','D','E','F','I','S','U','W'),F.lit(1)).\
                    when(F.col('IB_ETHNIC_CODE').isin('UC','8D','8E','8G','8H','9F','9J','9K','9L','9M','9N','9P','9Q','9S','9U','9V','9X'),\
                    F.lit(0)).otherwise(0)).withColumn('homevalue',F.when(F.col('ib_home_market_value').isin('A','B','C','D'),1).\
                    when(F.col('ib_home_market_value').isin('E','F','G','H'),2).\
                    when(F.col('ib_home_market_value').isin('I','J','K','L'),3).when(F.col('ib_home_market_value').isin('M','N'),4).\
                    when(F.col('ib_home_market_value').isin('O','P'),5).when(F.col('ib_home_market_value').isin('Q'),6).\
                    when(F.col('ib_home_market_value').isin('R'),7).when(F.col('ib_home_market_value').isin('S'),8).otherwise(None))
  return Ind_Data

# COMMAND ----------

def create_order_variables(Order_data,mag_list,End_date):
  #End_date=Order_data.agg({'order_date':'max'}).collect()[0][0]
  End_date=datetime.strptime(End_date, '%Y-%m-%d')
  for cd in mag_list:
    Order_data=Order_data.withColumn('Active'+cd,F.when((F.col('order_magazine_code')==cd)&(F.col('order_expire_date')>=(End_date).\
                              strftime("%Y-%m-%d"))&(F.col('order_paid_status').isin('2','3','4','5')),1).\
                              when((F.col('order_magazine_code')==cd)&(F.col('order_expire_date')>=(End_date).\
                              strftime("%Y-%m-%d"))&(F.col('order_paid_status').isin('6'))&\
                              (F.col('order_start_date')>=(End_date-timedelta(days=91.5)).strftime("%Y-%m-%d")),2).\
                              when((F.col('order_magazine_code')==cd)&(F.col('order_expire_date')<(End_date).\
                              strftime("%Y-%m-%d")),3).otherwise(0))
  windowspec=Window.partitionBy(Order_data.ind_urn).orderBy(Order_data.ind_urn)
  Order_data = Order_data.withColumn("rn", F.row_number().over(windowspec)).withColumn("max",\
                         F.max('rn').over(windowspec)).filter(F.col('max')==F.col('rn')).drop('max','rn')
  #Ever Active subscribers variables based on active indicator code
  Order_data=Order_data.withColumn('EVERSUBSR',sum([F.when(F.col('ACTIVEAL')!=0,1).otherwise(0),F.when(F.col('ACTIVEBR')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVESL')!=0,1).otherwise(0),F.when(F.col('ACTIVEVO')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVELK')!=0,1).otherwise(0)])).\
                withColumn('EVERACTIVE',sum([F.when(F.col('ACTIVEAD')!=0,1).otherwise(0),F.when(F.col('ACTIVEAL')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVEBA')!=0,1).otherwise(0),F.when(F.col('ACTIVEBR')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVECT')!=0,1).otherwise(0),F.when(F.col('ACTIVEDE')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVEGD')!=0,1).otherwise(0),F.when(F.col('ACTIVEGL')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVEGQ')!=0,1).otherwise(0),F.when(F.col('ACTIVEGW')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVELK')!=0,1).otherwise(0),F.when(F.col('ACTIVENY')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVESL')!=0,1).otherwise(0),F.when(F.col('ACTIVETV')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVEVF')!=0,1).otherwise(0),F.when(F.col('ACTIVEVO')!=0,1).otherwise(0),\
                                           F.when(F.col('ACTIVEWD')!=0,1).otherwise(0),F.when(F.col('ACTIVEWW')!=0,1).otherwise(0)])).\
               withColumn('CURRACTIVE',sum([F.when(F.col('ACTIVEAD')==1,1).otherwise(0),F.when(F.col('ACTIVEAL')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVEBA')==1,1).otherwise(0),F.when(F.col('ACTIVEBR')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVECT')==1,1).otherwise(0),F.when(F.col('ACTIVEDE')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVEGD')==1,1).otherwise(0),F.when(F.col('ACTIVEGL')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVEGQ')==1,1).otherwise(0),F.when(F.col('ACTIVEGW')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVELK')==1,1).otherwise(0),F.when(F.col('ACTIVENY')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVESL')==1,1).otherwise(0),F.when(F.col('ACTIVETV')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVEVF')==1,1).otherwise(0),F.when(F.col('ACTIVEVO')==1,1).otherwise(0),\
                                            F.when(F.col('ACTIVEWD')==1,1).otherwise(0),F.when(F.col('ACTIVEWW')==1,1).otherwise(0)])).\
              withColumn('GLAMOUR_GRP',sum([F.when(F.col('ACTIVEAL')!=0,1).otherwise(0),F.when(F.col('ACTIVEGL')!=0,1).otherwise(0)])).\
              withColumn('LITERARY',sum([F.when(F.col('ACTIVEVF')!=0,1).otherwise(0),F.when(F.col('ACTIVENY')!=0,1).otherwise(0)])).\
              withColumn('HIGH_FASHION',sum([F.when(F.col('ACTIVEVO')!=0,1).otherwise(0),F.when(F.col('ACTIVEWW')!=0,1).otherwise(0)])).\
              withColumn('MENS_GRP',sum([F.when(F.col('ACTIVEGQ')!=0,1).otherwise(0),F.when(F.col('ACTIVEDE')!=0,1).otherwise(0)])).\
              withColumn('GOLFER',sum([F.when(F.col('ACTIVEGD')!=0,1).otherwise(0),F.when(F.col('ACTIVEGW')!=0,1).otherwise(0)]))
  
  

  return Order_data

# COMMAND ----------

#Merging order and individual data
def create_universal_data_vars(Ind_data,Ord_data,uni_drop):
  universal_data=Ind_data.join(Ord_data,'ind_urn','left_outer')
  universal_data=universal_data.withColumn('ActiveLKGL',F.when(((F.col('ACTIVEGL').isin(1,2))&(F.col('ACTIVELK').isin(1,2))),2).\
                                   when(((F.col('ACTIVEGL').isin(1,2))|(F.col('ACTIVELK').isin(1,2))),1).otherwise(0)).\
                        withColumn('Male',F.when(F.col('Gender')=='M',1).otherwise(0)).\
                        withColumn('HealthyFash',sum([F.col('ib_fashion'),F.col('ib_reading_mags'),F.col('ib_health_medical_gen'),\
                                              F.col('ib_dieting_weight_loss'),F.col('ib_natural_foods'),F.col('ib_exercise_health')])).\
                        withColumn('pacific',F.when(F.col('state').isin('AK','HI','WA','OR','CA'),1).otherwise(0)).\
                        withColumn('mountain',F.when(F.col('state').isin('MT','ID','WY','CO','UT','NV','AZ','NM'),1).otherwise(0)).\
                        withColumn('MidWest',F.when(F.col('state').isin('ND','SD','NE','KS','MN','IA','MO','WI','MI','IL','IN','OH'),1).\
                                   otherwise(0)).\
                        withColumn('WSCentral',F.when(F.col('state').isin('OK','AR','LA','TX'),1).otherwise(0)).\
                        withColumn('ESCentral',F.when(F.col('state').isin('KY','TN','MS','AL'),1).otherwise(0)).\
                        withColumn('MidAtlantic',F.when(F.col('state').isin('NY','PA','NJ'),1).otherwise(0)).\
                        withColumn('SouthAtlantic',F.when(F.col('state').isin('WV','MD','DC','DE','VA','NC','SC','GA','FL'),1).\
                                   otherwise(0)).\
                        withColumn('MidAtlantic',F.when(F.col('state').isin('ME','VT','NH','MA','CT','RI'),1).otherwise(0)).\
                        withColumn('ac_nielsen_county_size_code',F.when(F.col('state').isin('A'),1).otherwise(0)).\
                        withColumn('ac_nielsen_county_size_code',F.when(F.col('state').isin('B'),1).otherwise(0)).\
                        withColumn('ac_nielsen_county_size_code',F.when(F.col('state').isin('C'),1).otherwise(0)).\
                        withColumn('ac_nielsen_county_size_code',F.when(F.col('state').isin('D'),1).otherwise(0)).\
                        drop(*uni_drop)
  return universal_data

# COMMAND ----------

#Create total paid order variables for DTP and Non DTPorder orders to all brand 
def create_paid_dtp_non_dtp_ordvars(ind_date,mag_list,End_date,ord_variables,dic,mag_ord_level):
  #Create empty variables values with 0
  End_date=datetime.strptime(End_date, '%Y-%m-%d')
  mag_ord_data=spark.read.orc(mag_ord_level+'dt='+ind_date).select(ord_variables).\
                            filter(F.col('order_record_status')!=5).orderBy('ind_urn','order_magazine_code','order_date')
  for d in mag_list:
     mag_ord_data=mag_ord_data.withColumn('totord_'+d,F.lit(0)).withColumn('pdtotord_'+d,F.lit(0)).withColumn('pdtotord_3yrs_'+d,F.lit(0)).\
                       withColumn('proppd_'+d,F.lit(0)).withColumn('pdord_12_'+d,F.lit(0)).withColumn('pddtp_'+d,F.lit(0)).\
                       withColumn('pddtp_dm_'+d,F.lit(0)).withColumn('pddtp_ren_'+d,F.lit(0)).withColumn('pddtp_auto_'+d,F.lit(0)).\
                       withColumn('pddtp_ins_'+d,F.lit(0)).withColumn('pddtp_int_'+d,F.lit(0)).withColumn('pddtp_12_'+d,F.lit(0)).\
                       withColumn('pddtp_3yrs_'+d,F.lit(0)).withColumn('pddtp_dm_12_'+d,F.lit(0)).withColumn('pddtp_ren_12_'+d,F.lit(0)).\
                       withColumn('pddtp_auto_12_'+d,F.lit(0)).withColumn('pddtp_auto_3yrs_'+d,F.lit(0)).\
                       withColumn('pddtp_ins_12_'+d,F.lit(0)).withColumn('pddtp_int_12_'+d,F.lit(0)).\
                       orderBy('ind_urn','order_magazine_code','order_date')
  for j in mag_list:
    mag_ord_data=mag_ord_data.withColumn('totord_'+j,F.when(F.col('order_magazine_code')==j,F.col('totord_'+j)+1).otherwise(0)).\
    withColumn('pdtotord_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))),\
                                F.col('pdtotord_'+j)+1).otherwise(0)).\
    withColumn('pdtotord_3yrs_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                          (F.col('order_date')>=(End_date-timedelta(days=36*30.44)).strftime("%Y-%m-%d"))),\
                                           F.col('pdtotord_3yrs_'+j)+1).otherwise(0)).\
    withColumn('pdord_12_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                  (F.col('order_date')>=(End_date-timedelta(days=12*30.44)).strftime("%Y-%m-%d"))),\
                                   F.col('pdord_12_'+j)+1).otherwise(0)).\
    withColumn('proppd_'+j,F.when(F.col('order_magazine_code')==j,F.col('pdtotord_'+j)/F.col('totord_'+j))).\
    withColumn('pddtp_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                  ((F.substring(F.col('order_document_key'),1,1)).isin('R','''#''','A','D','C','X','G','U','W','J','K'))),\
                                  F.col('pddtp_'+j)+1).otherwise(0)).\
    withColumn('pddtp_12_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                  ((F.substring(F.col('order_document_key'),1,1)).isin('R','''#''','A','D','C','X','G','U','W','J','K'))&\
                                   (F.col('order_date')>=(End_date-timedelta(days=12*30.44)).strftime("%Y-%m-%d"))),\
                                    F.col('pddtp_12_'+j)+1).otherwise(0)).\
    withColumn('pddtp_3yrs_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                     ((F.substring(F.col('order_document_key'),1,1)).isin('R','''#''','A','D','C','X','G','U','W','J','K'))&\
                                      (F.col('order_date')>=(End_date-timedelta(days=36*30.44)).strftime("%Y-%m-%d"))),\
                                       F.col('pddtp_3yrs_'+j)+1).otherwise(0)).\
    withColumn('pddtp_ren_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                      (F.substring(F.col('order_document_key'),1,1).isin('R','A','''#'''))),\
                                       F.col('pddtp_ren_'+j)+1).otherwise(0)).\
    withColumn('pddtp_ins_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                     (F.substring(F.col('order_document_key'),1,1).isin('J','K'))),\
                                      F.col('pddtp_ins_'+j)+1).otherwise(0)).\
    withColumn('pddtp_dm_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                     (F.substring(F.col('order_document_key'),1,1).isin('D'))),\
                                      F.col('pddtp_dm_'+j)+1).otherwise(0)).\
    withColumn('pddtp_auto_'+j,F.when(((F.col('order_magazine_code')==j)&(F.col('order_paid_status').isin('2','3','4','5'))&\
                                       (F.substring(F.col('order_document_key'),1,1).isin('''#'''))),\
                                        F.col('pddtp_auto_'+j)+1).otherwise(0)).\
    withColumn('pddtp_auto_12_'+j,F.when(((F.col('order_magazine_code')==j)&(F.substring(F.col('order_document_key'),1,1).isin('''#'''))&\
                                          (F.col('order_date')>=(End_date-timedelta(days=12*30.44)).strftime("%Y-%m-%d"))),\
                                           F.col('pddtp_auto_12_'+j)+1).otherwise(0)).\
    withColumn('pddtp_auto_3yrs_'+j,F.when(((F.col('order_magazine_code')==j)&(F.substring(F.col('order_document_key'),1,1).isin('''#'''))&\
                                            (F.col('order_date')>=(End_date-timedelta(days=36*30.44)).strftime("%Y-%m-%d"))),\
                                             F.col('pddtp_auto_3yrs_'+j)+1).otherwise(0)).\
    withColumn('pddtp_ren_12_'+j,F.when(((F.col('order_magazine_code')==j)&(F.substring(F.col('order_document_key'),1,1).\
                                          isin('R','A','''#'''))&(F.col('order_date')>=(End_date-timedelta(days=12*30.44)).\
                                          strftime("%Y-%m-%d"))),F.col('pddtp_auto_12_'+j)+1).otherwise(0)).\
    withColumn('pddtp_ins_12_'+j,F.when(((F.col('order_magazine_code')==j)&(F.substring(F.col('order_document_key'),1,1).isin('J','K'))&\
                                         (F.col('order_date')>=(End_date-timedelta(days=12*30.44)).strftime("%Y-%m-%d"))),\
                                          F.col('pddtp_ins_12_'+j)+1).otherwise(0)).\
    withColumn('pddtp_dm_12_'+j,F.when(((F.col('order_magazine_code')==j)&(F.substring(F.col('order_document_key'),1,1).isin('D'))&\
                                        (F.col('order_date')>=(End_date-timedelta(days=12*30.44)).strftime("%Y-%m-%d"))),\
                                         F.col('pddtp_dm_12_'+j)+1).otherwise(0)).\
    withColumn('pddtp_int_12_'+j,F.when(((F.col('order_magazine_code')==j)&(F.substring(F.col('order_document_key'),1,1).\
                                          isin('I','7','8','9'))&(F.col('order_paid_status').isin('2','3','4','5'))&(F.col('order_date')>=\
                                         (End_date-timedelta(days=12*30.44)))),F.col('pddtp_int_12_'+j)+1).otherwise(0)).\
    withColumn('pddtp_int_'+j,F.when(((F.col('order_magazine_code')==j)&(F.substring(F.col('order_document_key'),1,1).\
                                       isin('I','7','8','9'))&(F.col('order_paid_status').\
                                       isin('2','3','4','5'))),F.col('pddtp_int_'+j)+1).otherwise(0)).\
                orderBy('ind_urn','order_magazine_code','order_date')
    
  windowspec=Window.partitionBy(mag_ord_data.ind_urn).orderBy(mag_ord_data.ind_urn)
  mag_ord_data = mag_ord_data.withColumn("rn", F.row_number().over(windowspec)).withColumn("max",\
                         F.max('rn').over(windowspec)).filter(F.col('max')==F.col('rn')).drop('max','rn') 
  #Variables for non DTP order
  for k in mag_list:
    mag_ord_data=mag_ord_data.withColumn('pdnondtp_'+k,(F.col('pdtotord_'+k)-F.col('pddtp_'+k))).\
                           withColumn('pdnondtp_12_'+k,(F.col('pdord_12_'+k)-F.col('pddtp_12_'+k))).\
                           withColumn('pdnondtp_3yrs_'+k,(F.col('pdtotord_3yrs_'+k)-F.col('pddtp_3yrs_'+k)))
  
  #Creating total order variables for DTP, non DTP orders 
  for key,value in dic.items():
    mag_ord_data=mag_ord_data.withColumn(key,(F.col(value[0])+F.col(value[1])+F.col(value[2])+F.col(value[3])+F.col(value[4])+\
                                            F.col(value[5])+F.col(value[6])+F.col(value[7])+F.col(value[8])+F.col(value[9])+\
                                            F.col(value[10])+F.col(value[11])+F.col(value[12])+F.col(value[13])+F.col(value[14])+\
                                            F.col(value[15])+F.col(value[16])+F.col(value[17])))
    
  return mag_ord_data

# COMMAND ----------

def Merge_census_data(universal_data,ord_summary):
  censes_df=spark.read.csv('s3://cn-consumerintelligence/Yoges/SAS_Census_Data/Census2018_BlockGroup_Data_updated.csv',header='true',inferSchema = 'true').\
                      drop('_c0')
  univ2=universal_data.join(ord_summary,'ind_urn','left_outer')
  drop_vars=['order_magazine_code','order_paid_status','order_date','order_accum_amount','order_expire_date','order_record_status',\
            'order_start_date','order_document_key']
  univ2=univ2.drop(*drop_vars).withColumnRenamed('state','state_code')
  final_data=univ2.join(censes_df,[univ2.state_code==censes_df.State,univ2.fips_county_code==censes_df.County,univ2.tract==censes_df.Tract,\
                    univ2.block==censes_df.Blkgrp],'left_outer').drop('state','fips_county_code','tract','block')
  return final_data

# COMMAND ----------

#declare required Variables from acxioms data source
def declare_required_variables():
  ord_variables=['ind_urn','order_magazine_code','order_paid_status','order_start_date','order_expire_date','order_record_status',
                'order_document_key','order_accum_amount','order_date']
  indiv_variables='''AC_NIELSEN_COUNTY_SIZE_CODE
    AGE
    BLOCK
    COUNTRY_CODE
    CUSTOMER_TYPE
    EMAIL_PRESENCE_FLAG
    FIPS_COUNTY_CODE
    GENDER
    HH_urn
    IND_urn
    RESPONDED_TO_PSN
    STATE
    TRACT
    postal_code
    address_deliverable
    email_presence_flag'''
  indiv_variables=indiv_variables.split('\n')
  indiv_variables=[x.lower() for x in indiv_variables]
  indiv_variables=[x.strip('   ') for x in indiv_variables]
  # InfoBase Variable which needs to be loaded from S3
  Ib_var='''ind_urn
    ib_adult_age_hh_18_24_female
    ib_adult_age_hh_18_24_male
    ib_adult_age_hh_18_24_unknown
    ib_adult_age_hh_25_34_female
    ib_adult_age_hh_25_34_male
    ib_adult_age_hh_25_34_unknown
    ib_adult_age_hh_35_44_female
    ib_adult_age_hh_35_44_male
    ib_adult_age_hh_35_44_unknown
    ib_adult_age_hh_45_54_female
    ib_adult_age_hh_45_54_male
    ib_adult_age_hh_45_54_unknown
    ib_adult_age_hh_55_64_female
    ib_adult_age_hh_55_64_male
    ib_adult_age_hh_55_64_unk
    ib_adult_age_hh_65_74_female
    ib_adult_age_hh_65_74_male
    ib_adult_age_hh_65_74_unk
    ib_adult_age_hh_75_over_female
    ib_adult_age_hh_75_over_male
    ib_adult_age_hh_75_over_unk
    ib_child_age_hh_0_2_female
    ib_child_age_hh_0_2_male
    ib_child_age_hh_0_2_unk
    ib_child_age_hh_11_15_female
    ib_child_age_hh_11_15_male
    ib_child_age_hh_11_15_unk
    ib_child_age_hh_16_17_female
    ib_child_age_hh_16_17_male
    ib_child_age_hh_16_17_unk
    ib_child_age_hh_3_5_female
    ib_child_age_hh_3_5_male
    ib_child_age_hh_3_5_unk
    ib_child_age_hh_6_10_female
    ib_child_age_hh_6_10_male
    ib_child_age_hh_6_10_unk
    ib_comm_involve_don_cultural
    ib_comm_involve_animal_welf
    ib_comm_involve_children
    ib_comm_involve_wildlife_env
    ib_comm_involve_health
    ib_comm_involve_internat_aid
    ib_comm_involve_political
    ib_comm_involve_political_cons
    ib_comm_involve_religious
    ib_comm_involve_veterans
    ib_comm_involve_other
    ib_beauty_cosmetic_aids
    ib_crafts_hobbies
    ib_electronics_gadgets
    ib_fine_jewelry
    ib_home_furnishings
    ib_mag_bus_industry
    ib_mag_children_teens
    ib_mag_fitness
    ib_mag_food_cooking
    ib_mag_health
    ib_mag_home_gardening
    ib_mag_mens_interest
    ib_mag_music_entertainment
    ib_mag_news_political
    ib_mag_science_tech
    ib_mag_sports_rec
    ib_mag_travel_leisure
    ib_mag_womens_interest
    ib_magazines
    ib_men_apparel
    ib_travel_purchase
    ib_womens_apparel
    ib_bank_credcard
    ib_gas_credcard
    ib_travel_and_ent_credcard
    ib_credcard_buyer
    ib_premium_gold_credcard
    ib_upscale_dept_store_credcard
    ib_beauty_cosmetics
    ib_boat_owner
    ib_broader_living
    ib_career
    ib_chiphead
    ib_christian_families
    ib_collectibles_sports_mem
    ib_collector_avid
    ib_common_living
    ib_diy_living
    ib_home_improve_diy
    ib_nascar
    ib_reading_fin_newsltr_subs
    ib_tv_reception_hdtv_sat_dish
    ib_wireless_product_buyer
    ib_state_lotteries
    ib_casino_gambling
    ib_sweepstakes_contest
    ib_sports
    ib_outdoors
    ib_travel
    ib_reading
    ib_food_cooking
    ib_exercise_health
    ib_stereo_video
    ib_electronics_computers
    ib_home_improvements
    ib_investing_finance
    ib_antiques_collectibles
    ib_adults_num_hh_100
    ib_age_1st_individual_100
    ib_age_input_individual
    ib_buy_auto
    ib_car_purchase
    ib_children_num_hh_100
    ib_children_presence_hh_100
    ib_education_1st_ind_100
    ib_ethnic_code
    ib_gender_head_of_hh
    ib_gender_input_ind
    ib_home_equity_avail
    ib_home_market_value
    ib_home_owner_renter_100
    ib_home_year_built
    ib_household_income
    ib_household_size_100
    ib_income_rollup
    ib_investments_foreign
    ib_investors_highly_likely
    ib_investors_likely
    ib_length_of_residence_100
    ib_mail_order_buyer
    ib_mail_responder
    ib_marital_status
    ib_net_worth_estimator
    ib_occupation_customer
    ib_occupation_head_of_hh
    ib_orders_num_low_scale_cat
    ib_orders_num_lw_mid_scl_cat
    ib_orders_num_mid_scale_cat
    ib_orders_num_mid_up_cat
    ib_orders_num_unk_scl_cat
    ib_orders_num_up_cat
    ib_own_rent_home
    ib_pp_household_size
    ib_pp_number_of_adults
    ib_pp_number_of_children
    ib_prem_dwelling_size
    ib_prem_household_size
    ib_prem_number_of_children
    ib_presence_of_children
    ib_race_code_input_ind
    ib_religion_code
    ib_spoken_language_code
    ib_travel_business_domestic
    ib_travel_freq_flyer
    ib_voter_party
    ib_fashion
    ib_smoking_tobacco
    ib_current_affairs_politics
    ib_community_charities
    ib_science_space
    ib_career_improvement
    ib_arts
    ib_reading_top_sellers
    ib_reading_scifi
    ib_reading_audio_books
    ib_cooking_gourmet
    ib_travel_us
    ib_travel_rv
    ib_travel_cruise
    ib_walking
    ib_crafts
    ib_aviation
    ib_sew_knit_needlework
    ib_board_games_puzzles
    ib_cd_player_owner
    ib_avid_music_listener
    ib_vcr_ld_dvd_player
    ib_health_medical_gen
    ib_self_improvement
    ib_dog_owner
    ib_house_plants
    ib_childrens_interests
    ib_auto_motorcycle_race
    ib_baseball
    ib_hockey
    ib_tennis_spectator
    ib_collectible_stamps
    ib_collectible_art
    ib_investing_personal
    ib_investing_stock_bond
    ib_camping_hiking
    ib_boating_sailing
    ib_biking_mountain_bike
    ib_tennis_participant
    ib_snow_ski_participant
    ib_equestrian
    ib_home_improvement
    ib_history_military
    ib_celebrities
    ib_theater_performing_arts
    ib_religious_inspire
    ib_wines
    ib_reading_general
    ib_reading_relig_insp
    ib_reading_mags
    ib_cooking_general
    ib_cooking_low_fat
    ib_natural_foods
    ib_travel_foreign
    ib_travel_family
    ib_running_jogging
    ib_aerobic_cardiovascular
    ib_photography
    ib_auto_work_mechanic
    ib_woodworking
    ib_home_stereo_owner
    ib_record_tape_cd_collect
    ib_vcr_ld_dvd_movie
    ib_satellite_dish_owner
    ib_dieting_weight_loss
    ib_cat_owner
    ib_other_pets
    ib_parenting
    ib_grandchildren
    ib_football
    ib_basketball
    ib_soccer
    ib_collectible_gen
    ib_collectible_coins
    ib_collectible_antique
    ib_investing_real_estate
    ib_fishing
    ib_hunting_shooting
    ib_environmental_issues
    ib_golf_participant
    ib_motorcycling
    ib_home_furnish_decorate
    ib_gardening'''
  Ib_vars=Ib_var.split('\n')
  Ib_vars=[x.lower() for x in Ib_vars]
  Ib_vars=[x.strip('   ') for x in Ib_vars]
  cat_cols='''ib_adult_age_hh_18_24_female
    ib_adult_age_hh_18_24_male
    ib_adult_age_hh_18_24_unknown
    ib_adult_age_hh_25_34_female
    ib_adult_age_hh_25_34_male
    ib_adult_age_hh_25_34_unknown
    ib_adult_age_hh_35_44_female
    ib_adult_age_hh_35_44_male
    ib_adult_age_hh_35_44_unknown
    ib_adult_age_hh_45_54_female
    ib_adult_age_hh_45_54_male
    ib_adult_age_hh_45_54_unknown
    ib_adult_age_hh_55_64_female
    ib_adult_age_hh_55_64_male
    ib_adult_age_hh_55_64_unk
    ib_adult_age_hh_65_74_female
    ib_adult_age_hh_65_74_male
    ib_adult_age_hh_65_74_unk
    ib_adult_age_hh_75_over_female
    ib_adult_age_hh_75_over_male
    ib_adult_age_hh_75_over_unk
    ib_child_age_hh_0_2_female
    ib_child_age_hh_0_2_male
    ib_child_age_hh_0_2_unk
    ib_child_age_hh_11_15_female
    ib_child_age_hh_11_15_male
    ib_child_age_hh_11_15_unk
    ib_child_age_hh_16_17_female
    ib_child_age_hh_16_17_male
    ib_child_age_hh_16_17_unk
    ib_child_age_hh_3_5_female
    ib_child_age_hh_3_5_male
    ib_child_age_hh_3_5_unk
    ib_child_age_hh_6_10_female
    ib_child_age_hh_6_10_male
    ib_child_age_hh_6_10_unk
    ib_comm_involve_don_cultural
    ib_comm_involve_animal_welf
    ib_comm_involve_children
    ib_comm_involve_wildlife_env
    ib_comm_involve_health
    ib_comm_involve_internat_aid
    ib_comm_involve_political
    ib_comm_involve_political_cons
    ib_comm_involve_religious
    ib_comm_involve_veterans
    ib_comm_involve_other
    ib_beauty_cosmetic_aids
    ib_crafts_hobbies
    ib_electronics_gadgets
    ib_fine_jewelry
    ib_home_furnishings
    ib_mag_bus_industry
    ib_mag_children_teens
    ib_mag_fitness
    ib_mag_food_cooking
    ib_mag_health
    ib_mag_home_gardening
    ib_mag_mens_interest
    ib_mag_music_entertainment
    ib_mag_news_political
    ib_mag_science_tech
    ib_mag_sports_rec
    ib_mag_travel_leisure
    ib_mag_womens_interest
    ib_magazines
    ib_men_apparel
    ib_travel_purchase
    ib_womens_apparel
    ib_bank_credcard
    ib_gas_credcard
    ib_travel_and_ent_credcard
    ib_credcard_buyer
    ib_premium_gold_credcard
    ib_upscale_dept_store_credcard
    ib_beauty_cosmetics
    ib_boat_owner
    ib_broader_living
    ib_career
    ib_chiphead
    ib_christian_families
    ib_collectibles_sports_mem
    ib_collector_avid
    ib_common_living
    ib_diy_living
    ib_home_improve_diy
    ib_nascar
    ib_reading_fin_newsltr_subs
    ib_tv_reception_hdtv_sat_dish
    ib_wireless_product_buyer
    ib_state_lotteries
    ib_casino_gambling
    ib_sweepstakes_contest
    ib_sports
    ib_outdoors
    ib_travel
    ib_reading
    ib_food_cooking
    ib_exercise_health
    ib_stereo_video
    ib_electronics_computers
    ib_home_improvements
    ib_investing_finance
    ib_smoking_tobacco
    ib_current_affairs_politics
    ib_community_charities
    ib_science_space
    ib_career_improvement
    ib_arts
    ib_reading_top_sellers
    ib_reading_scifi
    ib_reading_audio_books
    ib_cooking_gourmet
    ib_travel_us
    ib_travel_rv
    ib_travel_cruise
    ib_walking
    ib_crafts
    ib_aviation
    ib_sew_knit_needlework
    ib_board_games_puzzles
    ib_cd_player_owner
    ib_avid_music_listener
    ib_vcr_ld_dvd_player
    ib_health_medical_gen
    ib_self_improvement
    ib_dog_owner
    ib_house_plants
    ib_childrens_interests
    ib_auto_motorcycle_race
    ib_baseball
    ib_hockey
    ib_tennis_spectator
    ib_collectible_stamps
    ib_collectible_art
    ib_investing_personal
    ib_investing_stock_bond
    ib_camping_hiking
    ib_boating_sailing
    ib_biking_mountain_bike
    ib_tennis_participant
    ib_snow_ski_participant
    ib_equestrian
    ib_home_improvement
    ib_history_military
    ib_celebrities
    ib_theater_performing_arts
    ib_religious_inspire
    ib_wines
    ib_reading_general
    ib_reading_relig_insp
    ib_reading_mags
    ib_cooking_general
    ib_cooking_low_fat
    ib_natural_foods
    ib_travel_foreign
    ib_travel_family
    ib_running_jogging
    ib_aerobic_cardiovascular
    ib_photography
    ib_auto_work_mechanic
    ib_woodworking
    ib_home_stereo_owner
    ib_record_tape_cd_collect
    ib_vcr_ld_dvd_movie
    ib_satellite_dish_owner
    ib_dieting_weight_loss
    ib_cat_owner
    ib_other_pets
    ib_parenting
    ib_grandchildren
    ib_football
    ib_basketball
    ib_soccer
    ib_collectible_gen
    ib_collectible_coins
    ib_collectible_antique
    ib_investing_real_estate
    ib_fishing
    ib_hunting_shooting
    ib_antiques_collectibles
    ib_environmental_issues
    ib_golf_participant
    ib_motorcycling
    ib_home_furnish_decorate
    ib_gardening
    ib_presence_of_children
    ib_investments_foreign
    ib_investors_highly_likely
    ib_investors_likely
    ib_children_presence_hh_100
    ib_fashion'''  
  cat_cols=cat_cols.split('\n')
  cat_cols=[x.strip('   ') for x in cat_cols]
  uni_drop='''order_magazine_code
    order_paid_status
    order_expire_date
    order_record_status
    order_document_key
    order_start_date
    ACTIVEAD
    ACTIVEAL
    ACTIVEBA
    ACTIVEBR
    ACTIVECT
    ACTIVEDE
    ACTIVEGL
    ACTIVEGQ
    ACTIVENY
    ACTIVESL
    ACTIVEVF
    ACTIVEVO
    ACTIVEWD
    ACTIVEWW
    ACTIVELK
    ACTIVETV
    ACTIVEGD
    ACTIVEGW'''
  uni_drop=uni_drop.split('\n')
  uni_drop=[x.strip('   ') for x in uni_drop]
  return ord_variables,indiv_variables,Ib_vars,cat_cols,uni_drop

# COMMAND ----------

#Creating total order variables for DTP, non DTP orders 
def total_order_variables():
  newvar='''totord_
  pdtotord_
  pdord_12_
  pddtp_
  pdnondtp_
  pddtp_12_
  pdnondtp_12_
  pddtp_dm_
  pddtp_ins_
  pddtp_int_
  pddtp_ren_
  pddtp_auto_
  pddtp_dm_12_
  pddtp_ins_12_
  pddtp_int_12_
  pddtp_ren_12_
  pddtp_auto_12_
  pddtp_auto_3yrs_
  pdnondtp_3yrs_'''
  newvar=newvar.split(sep='\n')
  newvar=[x.strip('   ') for x in newvar]
  magcd=['AD','AL','BA','BR','CT','DE','GL','GQ','NY','SL','VF','VO','WD','WW','LK','TV','GD','GW']
  dic={}
  for l in newvar:
    l1=[]
    for m in magcd:
        l1.append(l+m)
    dic[l]=l1
  oldkey='''totord_
  pdtotord_
  pdord_12_
  pddtp_
  pdnondtp_
  pddtp_12_
  pdnondtp_12_
  pddtp_dm_
  pddtp_ins_
  pddtp_int_
  pddtp_ren_
  pddtp_auto_
  pddtp_dm_12_
  pddtp_ins_12_
  pddtp_int_12_
  pddtp_ren_12_
  pddtp_auto_12_
  pddtp_auto_3yrs_
  pdnondtp_3yrs_'''
  oldkey=oldkey.split(sep='\n')
  oldkey=[x.strip('   ') for x in oldkey]
  newkey='''totord
  totalpdord
  totorpdord_12
  totalpddtpord
  totalpdnondtpord
  totalpddtpord_12
  totalpdnondtpord_12
  totalpddmord
  totalpdinsord
  totalpdintord
  totalpdrenord
  totalpdautoord
  totalpddmord_12
  totalpdinsord_12
  totalpdintord_12
  totalpdrenord_12
  totalpdautoord_12
  totalpdautoord_3yrs
  totalpdnondtpord_3yrs'''
  newkey=newkey.split(sep='\n')
  newkey=[x.strip('   ') for x in newkey]
  for d in range(0,18):
    dic[newkey[d]]=dic[oldkey[d]]
    del dic[oldkey[d]]
  return dic

# COMMAND ----------

#Common data preperation function 
def create_model_training_data(final_users):
  date='2019-01-19'
  EndDate='2019-01-18'
  infobase_date='2019-01-19'
  indiv_path='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/individual/'
  ib_path='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/infobase/'
  mag_level='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/mag_level/'
  mag_ord_level='s3://cn-consumerintelligence/People/asilcox/acxiom/orc/mag_order_level/'
  mag_list=['AD','AL','BA','BR','CT','DE','GL','GQ','NY','SL','VF','VO','WD','WW','LK','TV','GD','GW']
  #Variables declaration 
  ord_variables,indiv_variables,Ib_vars,cat_cols,uni_drop=declare_required_variables()
  total_ord_vars_dic=total_order_variables()
  #Extract order and individual data for final matched users -infobaseActives.sas,AttachIndivOrder.sas
  Order_data,Indiv_data =create_individual_order_data(final_users,indiv_variables,ord_variables,Ib_vars,date,\
                                                           infobase_date,cat_cols,indiv_path,ib_path,mag_ord_level)
  #Creating new individual variables- infobaseActives.sas
  Indiv_data=create_individual_vars(Indiv_data)
  #Creating new order variables - CreateOrderVar.sas
  Order_data=create_order_variables(Order_data,mag_list,EndDate)
  #create universal data by combining individual and order data - InfobaseActives.sas
  univ1=create_universal_data_vars(Indiv_data,Order_data,uni_drop)
  #create total paid dtp and non dtp order variables - CreateOrderVar.sas 
  ord_summary=create_paid_dtp_non_dtp_ordvars(date,mag_list,EndDate,ord_variables,total_ord_vars_dic,mag_ord_level)
  training_data=Merge_census_data(univ1,ord_summary)
  
  return training_data