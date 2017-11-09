# -*- coding:utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
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
#sale_ords = ord.filter("dt >= '2017-10-31' and sale_ord_dt=='2017-10-31'").select("item_sku_id", "after_prefr_amount").groupBy("item_sku_id").agg(sum('after_prefr_amount')).withColumnRenamed("item_id", "cost").rdd.map(lambda row: row.item_id)
sale_ords = ord.filter("dt >= '2017-10-31' and sale_ord_dt=='2017-10-31'").select("item_sku_id", "after_prefr_amount").groupBy("item_sku_id").agg({"after_prefr_amount": "sum"}).rdd.map(lambda row: ','.join([str(i) for i in row]))


# for row data to rdd:
# Row(item_sku_id=u'11640440767', sum(after_prefr_amount)=0.0)
# store the data into hdfs
sale_ords.saveAsTextFile(save_path)

# get the fields we need
#rdd1 = input_rdd.map(choose_column).reduceByKey(lambda x, y: x + y).map(gen_random).filter(lambda x: x != ' ')

# sort the items from the same use by time
#rdd1 = rdd1.flatMap(sort_values).filter(lambda x: x[0] != ' ')

#sort the list for each user by timestamp
#sorted_rdd2 = processed_rdd1.map(sort_line)

# 变换key为c_id3,且筛选出属性表中的属性
#processed_rdd2 = processed_rdd1.map(map_to_cid3).filter(lambda x: x[1][0][1] != '')

# 按key聚合数据，聚合后形式为:
# cid_3,[(item1),(item2),.........]
# processed_rdd3 = processed_rdd2.reduceByKey(lambda x, y: x + y)

# 筛选出query sample，每个cid选择N个
# processed_rdd_query = processed_rdd3.flatMap(select_query)

# 随机抽样，配对Pair, 分别找到positive 和 negative sample
# processed_pair = processed_rdd_query.leftOuterJoin(processed_rdd3).flatMap(sample_pair)
