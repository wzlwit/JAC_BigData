# -- * pyspark
# myfile = sc.textFile("/data/input/miniProject/trans_log.csv")

# -- rdd1 = myfile.map(lambda x: x.split(',')[1]=='TT')

# rdd1 = myfile.filter(lambda x:\
#  r = x.split(',')\
#  r[1] == 'TT'\
#  )

# rddfiltered = myfile.filter(lambda x: x.split(',')[1] == 'TT' or x.split(',')[1] == 'PP' or x.split(',')[1]=='LL')

# def trans_map(str):
#     r = str.split(',')
#     if r.[1] =='TT':
#         return ()

# pyspark
myfile = sc.textFile("/data/input/miniProject/trans_log.csv")
rdd = myfile.map(lambda x: x.split(','))
rddTT = rdd.filter(lambda x: x[1] == 'TT')
rddPP = rdd.filter(lambda x: x[1] == 'PP')
rddLL = rdd.filter(lambda x: x[1] == 'LL')

# schemaTT = StructType(StructField('Trans_Seq',IntegerType(),true)).add(StructField('Trans_code',StringType())).add(StructField('scan_seq',IntegerType())).add(StructField('Product_code',StringType())).add(StructField('amount',IntegerType())).add(StructField('discount',IntegerType())).add(StructField('add_remove_flag',IntegerType())).add(StructField('store_num'))

from pyspark.sql import Row
dfTT = rddTT.map(lambda x:Row(trans_seq=int(x[0]), trans_code=x[1], scan_seq=int(x[2]), product_code=x[3], amount=float(x[4]), discount=float(x[5]), add_remove_flag=int(x[6]), store_num=x[7], pos_emp_num=int(x[8]), lane=int(x[9]), timestamp=x[10])).toDF()

dfPP = rddPP.map(lambda x: Row(trans_seq=int(x[0]), trans_code=x[1],promotion_code=x[2], store_num=x[3], pos_emp_num=int(x[4]), lane=int(x[5]), timestamp=x[6])).toDF()

dfLL = rddLL.map(lambda x: Row(trans_seq=int(x[0]), trans_code=x[1],loyalty_card_no=x[2], store_num=x[3], pos_emp_num=int(x[4]), lane=int(x[5]), timestamp=x[6])).toDF()
