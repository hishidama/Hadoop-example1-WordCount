echo $(date '+%Y-%m-%d %H:%M:%S') afw2-wordcount start >> log.log
hadoop fs -rmr example1/afwout
afwWordcountBatch2/bin/experimental.sh
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') afw2-wordcount end $res >> log.log
