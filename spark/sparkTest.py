# -*- coding: UTF-8 -*-
import findspark
findspark.init()
import pandas as pd
from os import walk
from pyspark.sql import SparkSession

spark=SparkSession \
        .builder \
        .appName('my_first_app_name') \
        .getOrCreate()

'''
data = [(123, "Katie", 19, "brown"),
        (234, "Michael", 22, "green"),
        (345, "Simone", 23, "blue")]
df = spark.createDataFrame(data, schema=['id', 'name', 'age', 'eyccolor'])
df.show()

'''

## 读取txt文件
io1 = "D:/data/pyspark/sk/1111.txt"
data1 = pd.read_table(io1,sep=',',
                     usecols=[0,1,2,3,4],  ### 选取指定的列数
                     names = ['data_date','msg_id','targetNumber','status','message']
                     )
sdf=spark.createDataFrame(data1)
# 利用DataFrame创建一个临时视图
sdf.registerTempTable("callback_table")


# 执行sql
spark.sql("select msg_id from callback_table").show()

