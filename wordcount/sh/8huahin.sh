BASEDIR=$(cd $(dirname $0);pwd)
SRC=example1/input
DST=example1/huaout
export HADOOP_CLASSPATH+=:$BASEDIR/huahin1.jar
echo $(date '+%Y-%m-%d %H:%M:%S') huahin-wordcount start >> log.log
hadoop fs -rmr $DST
hadoop jar cascading1.jar example1.huahin.WordCountJob $SRC $DST
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') huahin-wordcount end $res >> log.log
