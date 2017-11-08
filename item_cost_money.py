# -*- coding:utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
import sys
import random

reload(sys)
sys.setdefaultencoding('utf8')

input_paths = sys.argv[1]
save_path = sys.argv[2]

spark = SparkSession.builder.appName("test").enableHiveSupport().getOrCreate()

# load table as a DataFrame
ord = spark.table(input_paths)

# group item by item id, we get item id and the total gmv of the item in each day
sale_ords = ord.filter("dt >= '2017-10-31' and sale_ord_dt=='2017-10-31'").select("item_sku_id", "after_prefr_amount").\
    groupBy("item_sku_id").agg({'after_prefr_amount': 'sum'}).rdd

# store the data into hdfs
sale_ords.saveAsTextFile(save_path)
