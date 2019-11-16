set hive.cli.print.header=true;
set hive.cli.print.current.db=true;

add jar /home/cloudera/Desktop/jar/agegroup.jar;
create temporary function agegroup as 'udf.AgeGroup';

create function agegroup2 as 'udf.AgeGroup' using jar 'hdfs://quickstart.cloudera:8020/udf/agegroup.jar';

create function isaccepted as 'udf.IsAcceptedNew' using jar 'hdfs://quickstart.cloudera:8020/udf/agegroup.jar';

create table result(student_id int, bands array<double>) row format delimited fields terminated by '|' collection items terminated by ',';

insert into result select 10 , array(cast(4.5 as double),cast(6.7 as double));

create table result2 (student_id int, band double);
insert into result2 values(1,4.5);
insert into result2 values(1,6.5);
insert into result2 values(1,7.5);
insert into result2 values(2,6.5);
insert into result2 values(2,7.5);

create function haspassed as 'udaf.HasPassed' using jar 'hdfs://quickstart.cloudera:8020/udf/agegroup.jar';

select haspassed(band, 6.0, 5.0) from result2 group by student_id;


create temporary function flat as 'udtf.FlatTrans';