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
dataframe_list = []
# walk会返版回3个参数，分别是路径，目录list，文件list，你可以权按需修改下
for f, _, i in walk("D:\\data\\pyspark\\sk\\ww_data"):
    for j in i:
        io = 'D:/data/pyspark/sk/ww_data/'+ j
        print(io)
        dataframe_list.append(pd.read_excel(io,sheet_name = 0,encoding="gbk",
                     usecols=[0,1,2,3,4,5,6,7],  ### 选取指定的列数
                     names = ['qy_name','moban_id','msg_id','zhujiao_phone','beijiao_phone','jieshou_phone','request_send_time','send_status']
                     ))

df = pd.concat(dataframe_list)
sdf=spark.createDataFrame(df)

# 利用DataFrame创建一个临时视图
sdf.registerTempTable("ww_table")

## 读取txt文件
io1 = "D:/data/pyspark/sk/Vibeac_callback.txt"
data1 = pd.read_table(io1,sep=',',
                     usecols=[0,1,2,3,4],  ### 选取指定的列数
                     names = ['data_date','msg_id','targetNumber','status','message']
                     )
callback_data=spark.createDataFrame(data1)
# 利用DataFrame创建一个临时视图
callback_data.registerTempTable("callback_table")

# 执行sql
result_data = spark.sql("select t.msg_id,t.request_send_time,t1.data_date,t.send_status,t1.status from ww_table t "
          "inner join callback_table t1 on t.msg_id = t1.msg_id")

