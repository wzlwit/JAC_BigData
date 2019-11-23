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



-- * Spark
val input = sc.textFile("/data/input/miniProject/trans_log.csv")
val fields = input.map(x=>x.split(","))
val ttRdd = fields.filter(x=>x(1)=="TT")
val llRdd = fields.filter(x=>x(1)=="LL")
val ppRdd = fields.filter(x=>x(1)=="PP")


"%05d".format(12)


case class tt(Trans_Seq:Int, Trans_code:String, scan_seq:Int, Product_code:String, amount:Double, discount:Double, add_remove_flag:Int, store_num:String, POS_emp_num:Int, lane:Int, timestamp:String)

val ttDf = ttRdd.map(x=>tt(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toDouble, x(5).toDouble, x(6).toInt, x(7), x(8).toInt, x(9).toInt, x(10))).toDF

case class pp(trans_seq:Int, trans_code:String,promotion_code:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

val ppDf = ppRdd.map(x=>pp(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDF

case class ll(trans_seq:Int, trans_code:String,loyalty_card_no:String, store_num:String, pos_emp_num:Int, lane:Int, timestamp:String)

val llDf = llRdd.map(x=>ll(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6))).toDF


llDf.collect.foreach(x=>println(x(3)+("%04d".format(x(4)))))