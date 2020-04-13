#-*- coding : utf-8 -*-
# coding: unicode_escape

import pandas as pd
import datetime
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)


io1 = "D:/data/pyspark/sk/dataTest.txt"
data1 = pd.read_table(io1,sep=',',
                     usecols=[1,2,3,4,5],  ### 选取指定的列数
                     names = ['msg_id','request_send_time','data_date','send_status','status']
                     )



'''
(d1 - d2).days  两个时间差，返回天数
(b-a).seconds   时间差的计算，单位为秒
'''
##  返回两个时间值的差值
def handleDate(a,b):
    print(a)
    print(b.split('.')[0])
    start = datetime.datetime.strptime(a, '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.strptime(b.split('.')[0], '%Y-%m-%d %H:%M:%S')
    return (start-end).days



data1['date_chazhi'] = data1.apply(lambda x: handleDate(x[1],x[2]), axis = 1)