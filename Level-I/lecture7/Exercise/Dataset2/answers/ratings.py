
sqlContext.sql('use stagedb')

ctn = sqlContext.sql("select count(*) from ratings r left join targetdb.movies m on r.movieid = m.movieid where m.movieid is null" )

if ctn >0 :
    print("Movie id key violation")
    exit(1)
else:
    print("Movie id check passed")

# We can use LOAD DATA command as well.
sqlContext.sql("insert into targetdb.ratings select * from ratings")
