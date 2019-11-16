CREATE DATABASE staging LOCATION '/data/input/stagin';

use staging;

create table store(store_id int, store_num string, city string, address string, open_date string, close_date string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table employee(employee_id int, employee_num string,store_num string, employee_name string, joining_date string, designation string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table promotion(promo_code_id int, promo_code string, description string, promo_start_date string, promo_end_date string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table loyalty(loyalty_member_num int, cust_num int, card_no string, joining_date string, points int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

alter table employee change column employee_num employee_num int;

create table product(product_id int, product_code string, add_dt string, remove_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table trans_codes(trans_code_id int, trans_code string, description string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table trans_strings(trans_code_id int, trans_code string,col3 string, col4 string, col5 string, col6 string, col7 string, col8 string, col9 string, col10 string, col11 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

