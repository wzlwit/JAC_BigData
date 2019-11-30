CREATE DATABASE staging LOCATION '/data/input/staging';

use staging;

create table store(store_id int, store_num string, city string, address string, open_date string, close_date string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table employee(employee_id int, employee_num string,store_num string, employee_name string, joining_date string, designation string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table promotion(promo_code_id int, promo_code string, description string, promo_start_date string, promo_end_date string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table loyalty(loyalty_member_num int, cust_num int, card_no string, joining_date string, points int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

alter table employee change column employee_num employee_num int;

create table product(product_id int, product_code string, add_dt string, remove_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table trans_codes(trans_code_id int, trans_code string, description string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table trans_strings(trans_code_id int, trans_code string,col3 string, col4 string, col5 string, col6 string, col7 string, col8 string, col9 string, col10 string, col11 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


-- * create partition table
create table target_table(Tx_Id int, Store_Id int, Product_Id int,Loyalty_Member_Num int, Promo_Code_Id int, Emp_Id int, Discount_Amt double,	Qty int) partitioned by(Tx_Date string) row format delimited fields terminated by ','

-- put file into hdfs
hdfs dfs -put -f  data/miniProject/trans_log.csv /data/input/miniProject

hdfs dfs -put data/miniProject/store.csv /data/input/stagin/store
hdfs dfs -put data/miniProject/employee.csv /data/input/stagin/employee
hdfs dfs -put data/miniProject/loyalty.csv /data/input/stagin/loyalty
hdfs dfs -put data/miniProject/product.csv /data/input/stagin/product
hdfs dfs -put data/miniProject/promotions.csv /data/input/stagin/promotion



"%05d".format(12)

-- * Spark
val input = sc.textFile("/data/input/miniProject/trans_log.csv")
val fields = input.map(x=>x.split(","))
val ttRdd = fields.filter(x=>x(1)=="TT")
val llRdd = fields.filter(x=>x(1)=="LL")
val ppRdd = fields.filter(x=>x(1)=="PP")


case class tt(Trans_Seq:Int, Trans_code:String, scan_seq:Int, Product_code:String, amount:Double, discount:Double, add_remove_flag:Int, store_num:String, POS_emp_num:Int, lane:Int, timestamp:String)

var ttDf = ttRdd.map(x=>tt(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toDouble, x(5).toDouble, x(6).toInt, x(7), x(8).toInt, x(9).toInt, x(10))).toDF

case class pp(trans_seq:Int, trans_code:String,promotion_code:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

var ppDf = ppRdd.map(x=>pp(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDF

case class ll(trans_seq:Int, trans_code:String,loyalty_card_no:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

var llDf = llRdd.map(x=>ll(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDF


llDf.collect.foreach(x=>println(x(3)+("%04d".format(x(4)))))

-- Transaction ID = 'daydate-yyyymmdd' + 'store_id' + 'lane' + 'Trans_Seq'

def getTransID(_c1:String, _c2:Int, _c3:Int, _c4:Int): Long = {
    val c1 = _c1.split(" ")(0).replaceAll("-","").toInt
    val c2 = "%05d".format(_c2)
    val c3 = "%02d".format(_c3)
    val c4 = "%04d".format(_c4)
    (c1 + c2 + c3 + c4).toLong
}
def getTransID(_c1:String, _c2:Int, _c3:Int, _c4:Int): Long = {
    val c1 = _c1.subString(10).replaceAll("-","").toInt
    val c2 = "%05d".format(_c2)
    val c3 = "%02d".format(_c3)
    val c4 = "%04d".format(_c4)
    (c1 + c2 + c3 + c4).toLong
}


var transId = getTransID(_,_,_,_)

var getTxID = sqlContext.udf.register("getTxID", transId)

-- df.select(tid(df("c1"), df("c2")))

-- sqlContext.sql("select * from staging.store")

val stDf = sqlContext.sql("select * from staging.store")

-- tsDF: join fo ttDf and stDf
var tsDf = ttDf.join(stDf, "store_num")
ttDf.join(stDf, ttDf("store_num") === stDf("store_num"))


-- register temporary table
ttDf.registerTempTable("ttTemp")
llDf.registerTempTable("llTemp")
ppDf.registerTempTable("ppTemp")

-- tt
    -- by group, using 'Discount' column
var tt = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, store_id, product_id, POS_emp_num as Emp_Id,discount as Discount_Amt, sum(amount*add_remove_flag) as Qty from ttTemp t join staging.store s on t.store_num = s.store_num JOIN staging.product p on t.product_code = p.product_code group by getTxID(timestamp,store_id,lane,trans_seq), store_id, product_id, POS_emp_num,discount having sum(amount*add_remove_flag)>0")

    -- no group


var pp = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, Promo_Code_Id from ppTemp t join staging.store s on t.store_num = s.store_num JOIN staging.promotion p on t.promotion_code = p.promo_code")


var ll = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, Loyalty_member_num from llTemp t join staging.store s on t.store_num = s.store_num JOIN staging.loyalty l on t.loyalty_card_no = l.card_no")

var finalDf= tt.join(pp, tt("Tx_id")=== pp("Tx_id"),"left_outer").join(ll,tt("Tx_id")===ll("Tx_id"),"left_outer")

-- or
var j1= tt.join(pp, tt("Tx_id")=== pp("Tx_id"),"left_outer")
var j2 = j1.join(ll,tt("Tx_id")===ll("Tx_id"),"left_outer")
var finalDf = j2.select(tt("Tx_id"),col("store_id"),col("Product_id"),ll("Loyalty_Member_Num"),col("promo_code_id"),col("Emp_Id"),col("Discount_Amt"),col("Qty"))
finalDf.show()
-- better to use temporary table to join

tsDf.select(getTxID(col("timestamp"),col("store_id"),col("lane"),col("trans_seq"))).show()