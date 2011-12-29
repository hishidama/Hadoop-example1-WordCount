echo $(date '+%Y-%m-%d %H:%M:%S') hive-wordcount start >> log.log
hive -f wordcount.hive.txt
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') hive-wordcount end $res >> log.log
