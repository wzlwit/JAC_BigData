
impala-shell -d stagedb -q "select count(*) from tags r left join targetdb.movies m on r.movieid = m.movieid where m.movieid is null" -o output.out -B --quiet

cnt=`cat output.out`

if [ $cnt -gt 0 ]
then
    echo "Movie id key violation"
    exit 1
else
    echo "Movie id check passed"
    rm -f output.out 
fi


# We can use LOAD DATA command as well.
impala-shell -d stagedb -q "insert into targetdb.tags select * from tags" -o output.out -B --quiet --output_delimiter ","

