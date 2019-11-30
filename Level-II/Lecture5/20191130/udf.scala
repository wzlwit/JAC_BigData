def getTransId(_c1:String, _c2:Int): String = {
  val c1_temp = _c1.replaceAll("-","").toInt
  val c1 = "%05d".format(c1_temp)
  val c2 = "%02d".format(_c2)
  (c1.toString + c2.toString)
}

val transId = getTransId(_,_)

val tid = sqlContext.udf.register("getTransId", transId)

df.select(tid(df("c1"), df("c2")))
