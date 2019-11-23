create table transaction(tx_id int, product_id string, amt double, qty double) partitioned by (trans_dt string) row format delimited fields terminated by ','

create table transaction_non(tx_id int, product_id string, trans_dt string, amt double, qty double) row format delimited fields terminated by ','
insert overwrite transaction_non select * from transaction_non where trans_dt <> 'trans_date';
sed -i 1d Retail_Transactions.csv
-- insert overwrite transaction_non select * from transaction_non where trans_dt <> 'trans_date'


insert overwrite transaction partition (trans_dt) select * from transaction where trans_dt='12-Jun-12' limit 1; --!!! only touch the record that are selected

-- directly deleting folder will cause problem, because partition information is stored, which has error 'cannot find directory'


insert into transaction partition(trans_dt) select tx_id, product_id, amt, qty, trans_dt from transaction_non;