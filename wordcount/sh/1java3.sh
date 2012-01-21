SRC=example1/input
DST=example1/javaout3
echo $(date '+%Y-%m-%d %H:%M:%S') java3-wordcount start >> log.log
hadoop fs -rmr $DST
hadoop jar hadoop1.jar example1.mr.WordCount3 $SRC $DST 1024
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') java3-wordcount end $res >> log.log
