year=1988

while [ $year -lt 2009 ]
do
    echo $year
    hadoop fs -put ~/data/ontime/On_Time_On_Time_Performance_${year}.csv /project/input
    year=`expr $year + 1`
done

