sqlContext.sql("use stagedb")

#Loading Movie Table

sqlContext.sql("insert overwrite table movies select distinct * from movies")

cnt_df = sqlContext.sql("select count(*) as cnt from movies where movieid is null")

output = cnt_df.collect()
cnt=output[0][0]

if cnt > 0:
    print "Null violation"
    exit(1)
else:
    print "Null check passed"

upi_df = sqlContext.sql("select movieid, count(*) from movies group by movieid having count(*)>1 limit 1")

cnt=upi_df.count()

if cnt > 0:
    print "UPI violation within stage table"
    exit(1)
else:
    print "UPI check passed within stage table"

upi_target_df = sqlContext.sql("select count(*) from movies m1 join targetdb.movies m2 on m1.movieid = m2.movieid")

output = upi_target_df.collect()
cnt=output[0][0]

if cnt > 0:
    print "UPI violation with the parent"
    exit(1)
else:
    print "UPI check passed with the parent"


# We can use LOAD DATA command as well.
sqlContext.sql("insert into targetdb.movies select * from movies")

#Loading Ratings Table

sqlContext.sql("insert overwrite table ratings select distinct * from ratings")

cnt_df = sqlContext.sql("select count(*) from ratings r left join targetdb.movies m on r.movieid = m.movieid where m.movieid is null")

output = cnt_df.collect()
cnt=output[0][0]

if cnt > 0:
    print "movieid violation"
    exit(1)
else:
    print "movieid check passed"

# We can use LOAD DATA command as well.
sqlContext.sql("insert into targetdb.ratings select * from ratings")

#Loading Tags Table

sqlContext.sql("insert overwrite table tags select distinct * from tags")

cnt_df = sqlContext.sql("select count(*) from tags r left join targetdb.movies m on r.movieid = m.movieid where m.movieid is null")

output = cnt_df.collect()
cnt=output[0][0]

if cnt > 0:
    print "movieid violation"
    exit(1)
else:
    print "movieid check passed"

# We can use LOAD DATA command as well.
sqlContext.sql("insert into targetdb.tags select * from tags")












