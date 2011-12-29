SRC=example1/input
DST=example1/javaout
echo $(date '+%Y-%m-%d %H:%M:%S') java-wordcount start >> log.log
hadoop fs -rmr $DST
hadoop jar hadoop1.jar example1.mr.WordCount $SRC $DST
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') java-wordcount end $res >> log.log
