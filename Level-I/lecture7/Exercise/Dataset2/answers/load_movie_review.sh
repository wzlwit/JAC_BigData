
step=`tail -1 logfile.log | cut -d',' -f 2`

if [ $step -le 10 ]
then 
    impala-shell -f dedupe.hql
fi

step=10
echo "load_movie_review,$step" >> logfile.log
	
if [ $step -le 20 ]
then
    sh movies.sh	
	if [ $? -ne 0 ]
	then
		echo "Movie load failed"
		exit 1
	fi
fi

step=20
echo "load_movie_review,$step" >> logfile.log

if [ $step -le 30 ]
then
	sh ratings.sh
	if [ $? -ne 0 ]
	then
		echo "Ratings load failed"
		exit 1
	fi
fi

step=30
echo "load_movie_review,$step" >> logfile.log

if [ $step -le 30 ]
then
	sh tags.sh
	if [ $? -ne 0 ]
	then
		echo "tags load failed"
		exit 1
	fi
fi

step=0
echo "load_movie_review,$step" >> logfile.log