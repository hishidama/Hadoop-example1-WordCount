echo $(date '+%Y-%m-%d %H:%M:%S') afw-wordcount start >> log.log
hadoop fs -rmr example1/afwout
afwWordcountBatch/bin/experimental.sh
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') afw-wordcount end $res >> log.log
