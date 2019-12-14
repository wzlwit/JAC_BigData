from pyspark.sql.functions import udf

def generateTxId(_c1, _c2):
  c1 = _c1.replace("-","").zfill(5)
  c2 = str(_c2).zfill(2)
  return c1+c2
  
generateTxId_udf = udf(generateTxId)

df.select(generateTxId_udf(df1.val1,df1.val2).alias("TxId")).show()