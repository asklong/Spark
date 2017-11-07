# -*- coding:utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import sys
import json
import random

reload(sys)
sys.setdefaultencoding('utf8')

input_paths = sys.argv[1]
save_path = sys.argv[2]
cross_cid_paths = sys.argv[1]




def parse_line(str):
    strs = str.split()
    skuid = strs[0]
    expose_ids = []
    for item in strs[1:]:
        expose_ids.append(item.split(":")[0])
    return ','.join([skuid, ' '.join(expose_ids)])

# sc = SparkContext("master","test")
sc = SparkSession.builder.appName("gen_cross_cid_pair").enableHiveSupport().getOrCreate()

# spark从Input_path里Load原始表
input_rdd = sc.sparkContext.textFile(input_paths)

# get the fields we need
rdd1 = input_rdd.map(parse_line)

# store result
rdd1.repartition(100).saveAsTextFile(save_path)

