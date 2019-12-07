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
var input = sc.textFile("/data/input/miniProject/trans_log.csv")
var fields = input.map(x=>x.split(","))
var ttRdd = fields.filter(x=>x(1)=="TT")
var llRdd = fields.filter(x=>x(1)=="LL")
var ppRdd = fields.filter(x=>x(1)=="PP")


case class tt(Trans_Seq:Int, Trans_code:String, scan_seq:Int, Product_code:String, amount:Double, discount:Double, add_remove_flag:Int, store_num:String, POS_emp_num:Int, lane:Int, timestamp:String)

var ttDf = ttRdd.map(x=>tt(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toDouble, x(5).toDouble, x(6).toInt, x(7), x(8).toInt, x(9).toInt, x(10))).toDf

case class pp(trans_seq:Int, trans_code:String,promotion_code:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

var ppDf = ppRdd.map(x=>pp(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDf

case class ll(trans_seq:Int, trans_code:String,loyalty_card_no:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

var llDf = llRdd.map(x=>ll(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDf


-- llDf.collect.foreach(x=>println(x(3)+("%04d".format(x(4)))))

-- Transaction ID = 'daydate-yyyymmdd' + 'store_id' + 'lane' + 'Trans_Seq'

def getTransID(_c1:String, _c2:Int, _c3:Int, _c4:Int): Long = {
    val c1 = _c1.split(" ")(0).replaceAll("-","").toInt
    val c2 = "%05d".format(_c2)
    val c3 = "%02d".format(_c3)
    val c4 = "%04d".format(_c4)
    (c1 + c2 + c3 + c4).toLong
}
var transId = getTransID(_,_,_,_)
var getTxID = sqlContext.udf.register("getTxID", transId)

-- !!! there is some problem in the following UDF
def getTransID(_c1:String, _c2:Int, _c3:Int, _c4:Int): Long = {
    val c1 = _c1.substring(10).replaceAll("-","").toInt
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

-- tsDf.select(getTxID(col("timestamp"),col("store_id"),col("lane"),col("trans_seq"))).show()

-- register temporary table
ttDf.registerTempTable("ttTemp")
llDf.registerTempTable("llTemp")
ppDf.registerTempTable("ppTemp")


-- better way  to use temporary table to delete first/last record accordingly, by join
-- * \ to escape
sqlContext.sql("select *, row_number() over (partition by Trans_Seq,Product_code order by scan_seq) as RowNum from ttTemp where add_remove_flag = 1").registerTempTable("ttTempPlus")

sqlContext.sql("select *, row_number() over (partition by Trans_Seq,Product_code order by scan_seq) as RowNum from ttTemp where add_remove_flag = -1").registerTempTable("ttTempMinus")

var ttDf_clean = sqlContext.sql("select p.* from ttTempPlus p left join ttTempMinus m on m.trans_seq = p.trans_seq and m.product_code = p.product_code and m.RowNum=p.RowNum where m.add_remove_flag is null")

ttDf_clean.registerTempTable("ttTemp")

-- tt
    -- by group, using 'Discount' column
-- var tt = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, store_id, product_id, POS_emp_num as Emp_Id,discount as Discount_Amt, sum(amount*add_remove_flag) as Qty from ttTemp t join staging.store s on t.store_num = s.store_num JOIN staging.product p on t.product_code = p.product_code group by getTxID(timestamp,store_id,lane,trans_seq), store_id, product_id, POS_emp_num,discount having sum(amount*add_remove_flag)>0")

var tt_tb = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, store_id, product_id, POS_emp_num as Emp_Id,discount as Discount_Amt, amount as Qty, substr(timestamp,0,10) as tx_date from ttTemp t join staging.store s on t.store_num = s.store_num JOIN staging.product p on t.product_code = p.product_code")


var pp_tb = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, Promo_Code_Id from ppTemp t join staging.store s on t.store_num = s.store_num JOIN staging.promotion p on t.promotion_code = p.promo_code")


var ll_tb = sqlContext.sql("select getTxID(timestamp,store_id,lane,trans_seq) as Tx_Id, Loyalty_member_num from llTemp t join staging.store s on t.store_num = s.store_num JOIN staging.loyalty l on t.loyalty_card_no = l.card_no")


var j1= tt_tb.join(pp_tb, tt_tb("Tx_id")=== pp_tb("Tx_id"),"left_outer")
var j2 = j1.join(ll_tb,tt_tb("Tx_id")===ll_tb("Tx_id"),"left_outer")
var finalDf = j2.select(tt_tb("Tx_id"),j1("store_id"),j1("Product_id"),ll_tb("Loyalty_Member_Num"),j1("promo_code_id"),j1("Emp_Id"),j1("Discount_Amt"),j1("Qty"),j1("tx_date"))
finalDf.registerTempTable("finalTemp")
sqlContext.sql("use staging")


finalDf.show()
sqlContext.sql("select * from finalTemp").show()

sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
sqlContext.sql("insert into target_table partition(tx_date) select * from finalTemp")
sqlContext.sql("Select * from staging.target_table").show()


/* OPTIONS */
var finalDf= tt_tb.join(pp_tb, tt_tb("Tx_id")=== pp_tb("Tx_id"),"left_outer").join(ll_tb,tt_tb("Tx_id")===ll_tb("Tx_id"),"left_outer")










