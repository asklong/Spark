#https://www.iteblog.com/archives/1566.html
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
#save_path = sys.argv[2]

spark = SparkSession.builder.appName("test").enableHiveSupport().getOrCreate()

# load table as a DataFrame
ord = spark.table(input_paths)

sale_ords = ord.filter("dt >= '2017-10-31' and sale_ord_dt=='2017-10-31'").select("sale_ord_id").distinct().count()

print sale_ords
