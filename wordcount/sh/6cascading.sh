SRC=example1/input
DST=example1/casout
export HADOOP_CLASSPATH+=:"$CASCADING_HOME/*":"$CASCADING_HOME/lib/*"
echo $(date '+%Y-%m-%d %H:%M:%S') cascading-wordcount start >> log.log
hadoop jar cascading1.jar example1.cascading.WordCount $SRC $DST
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') cascading-wordcount end $res >> log.log
