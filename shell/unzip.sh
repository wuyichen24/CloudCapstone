#!/bin/sh

n=1
filename=empty
filebase=/root/disk/aviation/airline_ontime/2008/

while [ $n -lt 11 ]
do
    filefullpath=$filebase'On_Time_On_Time_Performance_2008_'$n'.zip' 
    echo $filefullpath
    unzip -o $filefullpath -d ~/data/ontime
    n=`expr $n + 1`
done

rm -f readme.html
