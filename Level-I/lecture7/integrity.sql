use stagedb
set var:@rn = 0
set var:@mID = 0
SELECT 
    @rn = CASE
        WHEN @mID = movieid 
            THEN @rn + 1
        ELSE 1
    END AS num,
    @mID=movieid as movieid,
    title,genre
FROM
    movies
ORDER BY movieid;