SRC=example1/input
DST=example1/cout
MAP=map.out
RED=reduce.out
echo $(date '+%Y-%m-%d %H:%M:%S') c-wordcount start >> log.log
hadoop fs -rmr $DST
hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-0.20.2-cdh3u2.jar -mapper $MAP -combiner $RED -reducer $RED -input $SRC -output $DST -file $MAP -file $RED
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') c-wordcount end $res >> log.log
