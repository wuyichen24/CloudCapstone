#!/bin/sh

hadoop fs -mkdir -p /project/input
hadoop fs -mkdir -p /project/tmp
hadoop fs -put ~/data/ontime/On_Time_On_Time_Performance_2008.csv /project/input
