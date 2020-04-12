'''
    pandas 性能测试

'''

#-*- coding : utf-8 -*-
# coding: unicode_escape

import pandas as pd
import datetime
from os import walk
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)

'''
    1、维度查看：
        df.shape
    2、数据表基本信息（维度、列名称、数据格式、所占空间等）：
        df.info()
    3、查看列名称：
        -df.columns

'''
### 读取excel文件中的内容

'''
dataframe_list = []
# walk会返版回3个参数，分别是路径，目录list，文件list，你可以权按需修改下
for f, _, i in walk("D:\\data\\pyspark\\sk\\ww_data"):
    for j in i:
        io = 'D:/data/pyspark/sk/ww_data/'+ j
        print(io)
        dataframe_list.append(pd.DataFrame(pd.read_excel(io,sheet_name = 0,encoding="gbk",
                     usecols=[0,1,2,3,4,5,6,7],  ### 选取指定的列数
                     names = ['qy_name','moban_id','msg_id','zhujiao_phone','beijiao_phone','jieshou_phone','request_send_time','send_status']
                     )))
#io = r'D:/data/pyspark/sk/ww_data/1911.xlsx'

### 读取文本数据
io1 = "D:/data/pyspark/sk/Vibeac_callback.txt"
data1 = pd.read_table(io1,sep=',',
                     usecols=[0,1,2,3,4],  ### 选取指定的列数
                     names = ['data_date','msg_id','targetNumber','status','message']
                     )

###data = pd.read_excel(io,sheet_name = 0,encoding="gbk",)

##  创建一个DataFrame
df = pd.concat(dataframe_list)
#print(df.info())
df1 = pd.DataFrame(data1)
#print(df.shape)

## 两表关联
df3 = df.merge(df1, left_on='msg_id',right_on='msg_id')
##df3 = df.join(df1, how='inner',on='msg_id',lsuffix='_left', rsuffix='_right')  数据异常
#print(df3.info())
cols = ['msg_id','request_send_time','data_date','send_status','status']   ####  选取多个列
resultData = df3[cols]
resultData.to_csv('D:/data/pyspark/sk/resultData.txt')

'''




io1 = "D:/data/pyspark/sk/resultData1.txt"
data1 = pd.read_table(io1,sep=',',
                     usecols=[0,1,2,3,4],  ### 选取指定的列数
                     names = ['msg_id','request_send_time','data_date','send_status','status']
                     )

def compareStatus(a, b):
	if a == b:
		return 1
	else:
		return 0

'''
(d1 - d2).days  两个时间差，返回天数
(b-a).seconds   时间差的计算，单位为秒
'''
##  返回两个时间值的差值
def handleDate(a):
	print(a)
	print(type(a))
	print('-------------------------')
	#start = datetime.datetime.strptime(a, '%Y-%m-%d %H:%M:%S')
	#end = datetime.datetime.strptime(b, '%Y-%m-%d %H:%M:%S')
	#return (start-end).days

##  判断值是否大于，5
def daysFlag(a):
	pass
	#if a >= 5:
	#	return 1
	#else:
	#	return 0

data1['status_flag'] = data1.apply(lambda x: compareStatus(x.send_status, x.status), axis = 1)
data1['date_chazhi'] = data1.apply(lambda x: handleDate(x), axis = 1)
data1['date_flag'] = data1.apply(lambda x: daysFlag(x.date_chazhi), axis = 1)

cols = ['msg_id','request_send_time','data_date','date_flag','send_status','status','status_flag']   ####  选取多个列
resultData = data1[cols]
resultData.to_csv('D:/data/pyspark/sk/resultData1.txt')
