import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive._

object myMain extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Mini Project"))
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  import sqlContext.implicits._

  sqlContext.sql("show databases").show
  sqlContext.sql("select * from t1").show
  println("Hello, world")
  var input = sc.textFile("/data/input/miniProject/trans_log.csv")
  var fields = input.map(x=>x.split(","))
  var ttRdd = fields.filter(x=>x(1)=="TT")
  var llRdd = fields.filter(x=>x(1)=="LL")
  var ppRdd = fields.filter(x=>x(1)=="PP")

  case class tt(Trans_Seq:Int, Trans_code:String, scan_seq:Int, Product_code:String, amount:Double, discount:Double, add_remove_flag:Int, store_num:String, POS_emp_num:Int, lane:Int, timestamp:String)

  var ttDf = ttRdd.map(x=>tt(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toDouble, x(5).toDouble, x(6).toInt, x(7), x(8).toInt, x(9).toInt, x(10))).toDF

  case class pp(trans_seq:Int, trans_code:String,promotion_code:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

  var ppDf = ppRdd.map(x=>pp(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDF

  case class ll(trans_seq:Int, trans_code:String,loyalty_card_no:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

  var llDf = llRdd.map(x=>ll(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDF

  def getTransID(_c1:String, _c2:Int, _c3:Int, _c4:Int): Long = {
    val c1 = _c1.split(" ")(0).replaceAll("-","").toInt
    val c2 = "%05d".format(_c2)
    val c3 = "%02d".format(_c3)
    val c4 = "%04d".format(_c4)
    (c1 + c2 + c3 + c4).toLong
  }
  var transId = getTransID(_,_,_,_)
  var getTxID = sqlContext.udf.register("getTxID", transId)

  ttDf.registerTempTable("ttTemp")
  llDf.registerTempTable("llTemp")
  ppDf.registerTempTable("ppTemp")

  sqlContext.sql("select *, row_number() over (partition by Trans_Seq,Product_code order by scan_seq) as RowNum from ttTemp where add_remove_flag = 1").registerTempTable("ttTempPlus")

  sqlContext.sql("select *, row_number() over (partition by Trans_Seq,Product_code order by scan_seq) as RowNum from ttTemp where add_remove_flag = -1").registerTempTable("ttTempMinus")

  var ttDf_clean = sqlContext.sql("select p.* from ttTempPlus p left join ttTempMinus m on m.trans_seq = p.trans_seq and m.product_code = p.product_code and m.RowNum=p.RowNum where m.add_remove_flag is null")

  ttDf_clean.registerTempTable("ttTemp")

  var tt_tb = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, store_id, product_id, POS_emp_num as Emp_Id,discount as Discount_Amt, amount as Qty, substr(timestamp,0,10) as tx_date from ttTemp t join staging.store s on t.store_num = s.store_num JOIN staging.product p on t.product_code = p.product_code")


  var pp_tb = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, Promo_Code_Id from ppTemp t join staging.store s on t.store_num = s.store_num JOIN staging.promotion p on t.promotion_code = p.promo_code")


  var ll_tb = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, Loyalty_member_num from llTemp t join staging.store s on t.store_num = s.store_num JOIN staging.loyalty l on t.loyalty_card_no = l.card_no")


  var j1= tt_tb.join(pp_tb, tt_tb("Tx_id")=== pp_tb("Tx_id"),"left_outer")
  var j2 = j1.join(ll_tb,tt_tb("Tx_id")===ll_tb("Tx_id"),"left_outer")
  var finalDf = j2.select(tt_tb("Tx_id"),j1("store_id"),j1("Product_id"),ll_tb("Loyalty_Member_Num"),j1("promo_code_id"),j1("Emp_Id"),j1("Discount_Amt"),j1("Qty"),j1("tx_date"))
  finalDf.registerTempTable("finalTemp")

  sqlContext.sql("select * from finalTemp").show()

  sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  sqlContext.sql("insert into target_table partition(tx_date) select * from finalTemp")
  sqlContext.sql("Select * from staging.target_table").show()
}
