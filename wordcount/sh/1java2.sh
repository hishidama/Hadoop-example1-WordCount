SRC=example1/input
DST=example1/javaout2
echo $(date '+%Y-%m-%d %H:%M:%S') java2-wordcount start >> log.log
hadoop fs -rmr $DST
hadoop jar hadoop1.jar example1.mr.WordCount2 $SRC $DST
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') java2-wordcount end $res >> log.log
