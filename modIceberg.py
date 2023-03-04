import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd


spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]")\
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.spark_catalog.type","hive") \
    .getOrCreate()
     
#spark.sql("UPDATE spark_catalog.default.telco_iceberg SET tenure = 15 WHERE customerid = '1452-KIOVK'")


#CREATE EXTERNAL TABLE ext_t1 STORED AS ICEBERG TBLPROPERTIES ('iceberg.table_identifier'='default.telco_iceberg');

from pyspark.sql.functions import rand,abs,when
df=spark.read.format("iceberg").load("spark_catalog.default.telco_iceberg_string2")
#df = df.withColumn("tenure", df["tenure"].cast(DoubleType()))
#df = df.withColumn("monthlycharges", df["monthlycharges"].cast(DoubleType()))
#df = df.withColumn("totalcharges", df["totalcharges"].cast(DoubleType()))

df=df.select('customerid','monthlycharges','churn').where(df.churn != 'Yes')
df=df.drop_duplicates()
df.monthlycharges=df.monthlycharges+df.monthlycharges*(2*rand()-1)
#df['monthlycharges']=str(df['monthlycharges'])
df = df.withColumn('churn', when(rand() > 0.1, df['churn']).otherwise('Yes'))
                    
spark.sql("drop table if exists modtable")
df.write.mode("overwrite").saveAsTable("modtable")
#for i in range(df.count()):
#  spark.sql("UPDATE spark_catalog.default.telco_iceberg SET monthlycharges ="+ str(df.collect()[i]['monthlycharges'])+ " WHERE customerid = '"+ df.collect()[i]['customerid']+"'")

  
spark.sql("MERGE INTO spark_catalog.default.telco_iceberg_string2 t USING modtable s ON t.customerid = s.customerid WHEN MATCHED THEN UPDATE SET monthlycharges = s.monthlycharges")
spark.sql("MERGE INTO spark_catalog.default.telco_iceberg_string2 t USING modtable s ON t.customerid = s.customerid WHEN MATCHED THEN UPDATE SET churn = s.churn")

#exec(open("1b_create_iceberg_impala.py").read())

spark.read.format("iceberg").load("spark_catalog.default.telco_iceberg_string2.history").show()
