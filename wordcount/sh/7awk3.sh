SRC=example1/input
DST=example1/awkout3
MAP=map2.awk
RED=reduce2.awk
echo $(date '+%Y-%m-%d %H:%M:%S') awk3-wordcount start >> log.log
hadoop fs -rmr $DST
hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-0.20.2-cdh3u2.jar -mapper "awk -f $MAP" -combiner "awk -f $RED" -reducer "awk -f $RED" -input $SRC -output $DST -file $MAP -file $RED
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') awk3-wordcount end $res >> log.log
