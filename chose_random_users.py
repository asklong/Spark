# -*- coding:utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
import sys
import random

reload(sys)
sys.setdefaultencoding('utf8')

input_paths = sys.argv[1]
save_path = sys.argv[2]


def choose_column(line):
    """
    for each line:
    use account, item id, click time, type
    this is the example data: (ascall,12435656604,1509426256632,expose)
    on this step, we cansider all the items as expose items data, so we ignore the file type
    """
    if line is None:
        return
    line = line.strip().split('\t')
    if len(line) != 4 or line[0] is None or line[1] is None or line[2] is None:
        return
    #we will return click time, type, item id
    type = '0'
    if line[3] == "click":
        type = '1'
    # key value pair
    return ( line[0], [ (line[2], type, line[1]) ] )

def cause_expose(line):
    values = line[1]
    if len(values) == 1:
        return [' ']

    sessions = []
    time_period = 3600000
    start = 0
    end = 0
    while start < len(values):
        end = start + 1
        while end < len(values) and long(values[end][0].encode('utf8')) - long(values[start][0].encode('utf8')) <= time_period:
            end = end + 1

        sessions.append(values[start:end])
        start = end

    #for each session, generate cause id, expose ids, click id(could not show)
    res = []
    for sess in sessions:
        #for each session
        #each tuple is:  click time, type, item id
        if len(sess) >= 2:
            have_click = False
            for item in sess:
                if item[1] == 1:
                    have_click = True

            if have_click == False:
                #cause id, exposed ids, no click ids
                exposed_ids = [s[2] for s in sess[1:]]
                tmp = [sess[0][2], ' '.join(exposed_ids), '|']
                res.append(','.join(tmp))
            else:
                #get cause id
                front_ids = []
                click_id = ' '
                for item in sess:
                    if item[1] == 0:
                        front_ids.append(item[2])
                    else:
                        if len(front_ids) >= 2:
                            tmp = [front_ids[0], ' '.join(front_ids[1:]), item[2]]
                            res.append(','.join(tmp))

    if len(res) == 0:
        res = [' ']

    return res


def gen_random(line):
    r = random.random()
    if r > 0.1:
        return ' '

    user = line[0]
    # tuple: click time, type, item id
    l = line[1]
    l.sort(key=lambda x: x[0])
    tmp = []
    for t in l:
        tmp.append('|'.join([ t[0], t[1], t[2] ]))

    return ','.join([ user, ' '.join(tmp) ])

sc = SparkSession.builder.appName("gen_cause_items_pair").enableHiveSupport().getOrCreate()

# spark从Input_path里Load原始表
input_rdd = sc.sparkContext.textFile(input_paths)

# get the fields we need
rdd1 = input_rdd.map(choose_column).reduceByKey(lambda x, y: x + y).map(gen_random).filter(lambda x: x != ' ')

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

# 储存结果
rdd1.saveAsTextFile(save_path)
