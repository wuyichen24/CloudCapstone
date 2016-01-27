#!/bin/sh

n=1
filename=empty
year=2007

filebase='/root/disk/aviation/airline_ontime/'$year'/'

while [ $n -lt 13 ]
do
    filefullpath=$filebase'On_Time_On_Time_Performance_'$year'_'$n'.zip' 
    echo $filefullpath
    unzip -o $filefullpath -d ~/data/ontime
    n=`expr $n + 1`
done

rm -f readme.html
