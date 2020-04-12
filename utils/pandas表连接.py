'''
    merge表关联查询
        函数介绍
            DataFrame.merge(left, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False,
            sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
        参数介绍
            how	默认为inner，可设为inner/outer/left/right
            on	根据某个字段进行连接，必须存在于两个DateFrame中（若未同时存在，则需要分别使用left_on和right_on来设置）
            left_on	左连接，以DataFrame1中用作连接键的列
            right_on	右连接，以DataFrame2中用作连接键的列
            left_index	将DataFrame1行索引用作连接键
            right_index	将DataFrame2行索引用作连接键
            sort	根据连接键对合并后的数据进行排列，默认为True
            suffixes	对两个数据集中出现的重复列，默认在新数据集中加上后缀_x,_y进行区别，可以通过 suffixes('_df1','df2')指定

    join数据合并
        函数介绍：DataFrame.join(other, on=None, how=’left’, lsuffix=”, rsuffix=”, sort=False)
        参数说明：
            other:(DataFrame，或者带有名字的Series，或者DataFrame的list)如果传递的是Series，那么其name属性应当是一个集合，并且该集合将会作为结果DataFrame的列名
            on:(列名称，或者列名称的list/tuple，或者类似形状的数组)连接的列，默认使用索引连接
            how:{‘left’, ‘right’, ‘outer’, ‘inner’}, default:‘left’连接的方式，默认为左连接
            lsuffix:左DataFrame中重复列的后缀
            rsuffix:右DataFrame中重复列的后缀
            sort:[boolean, default ，False]按照字典顺序对结果在连接键上排序。如果为False，连接键的顺序取决于连接类型（关键字）

    concat()函数:数据拼接：concat函数是在pandas底下的方法，可以将数据根据不同的轴作简单的融合
            pd.concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False,keys=None, levels=None, names=None, verify_integrity=False)
        参数说明：
            objs: series，dataframe或者是panel构成的序列lsit
            axis： 需要合并链接的轴，0是行(默认)，1是列
            join：连接的方式 inner，或者outer

'''
#-*- coding : utf-8 -*-
# coding: unicode_escape

import pandas as pd
import numpy as np


#样集1
df1=pd.DataFrame(np.arange(12).reshape(3,4),columns=['a','b','c','d'])
#print(df1)
#样集2
df2=pd.DataFrame({'b':[1,5],'d':[3,7],'a':[0,4]})
#print(df2)
#两张表df1和df2的列名有重叠，且重叠列的内容完全相同，直接用pd.merge(df1, df2)
df3 = pd.merge(df1,df2)
#print(df3)

####################################   join测试  ##############################################
first=pd.DataFrame({'item_id':['a','b','c','b','d'],'item_price':[1,2,3,2,4]})
other=pd.DataFrame({'item_id':['a','b','f'],'item_atr':['k1','k2','k3']})
#print(first)
#print(other)
#print(first.join(other, lsuffix='_left', rsuffix='_right'))

df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                    'B': ['B0', 'B1', 'B2', 'B3'],
                    'C': ['C0', 'C1', 'C2', 'C3'],
                    'D': ['D0', 'D1', 'D2', 'D3']},
                   index = [0, 1, 2, 3])
df2 = pd.DataFrame({'A': ['A4', 'A5', 'A6', 'A7'],
                    'B': ['B4', 'B5', 'B6', 'B7'],
                    'C': ['C4', 'C5', 'C6', 'C7'],
                    'D': ['D4', 'D5', 'D6', 'D7']},
                   index = [4, 5, 6, 7])

df3 = pd.DataFrame({'A': ['A8', 'A9', 'A10', 'A11'],
                    'B': ['B8', 'B9', 'B10', 'B11'],
                    'C': ['C8', 'C9', 'C10', 'C11'],
                    'D': ['D8', 'D9', 'D10', 'D11']},
                   index = [8, 9, 10, 11])
df4 = pd.DataFrame({'B': ['A8', 'A9', 'A10', 'A11'],
                    'F': ['B8', 'B9', 'B10', 'B11'],
                    'D': ['D8', 'D9', 'D10', 'D11']},
                   index = [8, 9, 10, 11])
frames = [df1, df2, df3]
result = pd.concat(frames)

#print(result)
## 当axis = 1的时候，concat就是行对齐，然后将不同列名称的两张表合并
result = pd.concat([df1, df4], axis=1)
#print(result)

## 加上join参数的属性，如果为’inner’得到的是两表的交集，如果是outer，得到的是两表的并集
result = pd.concat([df1, df4], axis=1, join='inner')


### 选取指定的列
result[['A', 'B', 'C']].head(5)

##  根据列值进行选取
result[result['A'] == 'A1'].head(5)
result1 = result['A'] == 'A1'
#result[(result['time'] == 'Dinner') & (result['tip'] > 5.00)]
#result[(result['size'] >= 5) | (result['total_bill'] > 45)]

'''
    group by 统计
        1、计数使用size（）方法而不是count（）方法，因为count（）是对所有列生效

'''

result.groupby('A').size()


'''
filter()
    selected_numbers = filter(lambda x: x % 3 == 0, range(1, 11))
apply函数用法(axis=0 表示按列，axis=1 表示按行)
    设置指定列的类型
        df[['col2','col3']] = df[['col2','col3']].apply(pd.to_numeric)
    将所有可以设置的都设置，无法设置的跳过
        df.apply(pd.to_numeric, errors='ignore')

'''
'''

df1 = pd.DataFrame({'name': ['张三', '李四', '王五'],
                    'Nationality': ['汉族', '回族', '汉族'],
                    'Score': [400, 450, 460]}
                   )
                   
'''

df = pd.read_csv('D:/data/pyspark/sk/222.csv',encoding="gbk")
df['ExtraScore'] = df['Nationality'].apply(lambda x : 5 if x != '汉族' else 0)
df['TotalScore'] = df['Score'] + df1['ExtraScore']

import numpy as np

matrix = [
    [1,2,3],
    [4,5,6],
    [7,8,9]
]

df = pd.DataFrame(matrix, columns=list('xyz'), index=list('abc'))
## 元素所有值都求平方
#df.apply(np.square)

##如果只想 apply() 作用于指定的行和列，可以用行或者列的 name 属性进行限定。比如下面的示例将 x 列进行平方运算：
#df.apply(lambda x : np.square(x) if x.name=='x' else x)
## 对xy列求平方和
#df.apply(lambda x : np.square(x) if x.name in ['x', 'y'] else x)