#-*- coding : utf-8 -*-
# coding: unicode_escape

import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)


'''
    sheet_name，要读取的工作表名称
        可以是整型数字、列表名或SheetN，也可以是上述三种组成的列表。
        整型数字：目标sheet所在的位置，以0为起始，比如sheet_name = 1代表第2个工作表
    data = pd.read_excel(io, sheet_name = '英超射手榜', 
                     names = ['rank','player','club','goal','common_goal','penalty'])  为列定义名字
    usecols，需要读取哪些列
        可以使用整型，从0开始，如[0,2,3]；
        可以使用Excel传统的列名“A”,“B”等字母，如“A：C, E” ="A, B, C, E"，注意两边都包括。
        data = pd.read_excel(io, sheet_name = '西甲射手榜', usecols = [0, 1, 3])
    skiprows，跳过特定行，skipfooter ， 跳过末尾n行
        skiprows= n， 跳过前n行； skiprows = [a, b, c]，跳过第a+1,b+1,c+1行（索引从0开始）
        data = pd.read_excel(io, sheet_name = '英超射手榜', skiprows = 3)
    nrows ，需要读取的行数
        data = pd.read_excel(io, sheet_name = '英超射手榜', nrows = 10)
    行索引：index_col，列索引：header    
        data = pd.read_excel(io, sheet_name = '英超射手榜', index_col = '排名')
        data = pd.read_excel(io, sheet_name = '英超积分榜', header = [0,1]) 
'''

### 读取excel文件中的内容
io = r'D:/data/pyspark/sk/ww_data/1911.xlsx'
#data = pd.read_csv("D:/data/pyspark/sk/303.csv",encoding="gbk")
data = pd.read_excel(io,sheet_name = 0,encoding="gbk")
print(data.head())