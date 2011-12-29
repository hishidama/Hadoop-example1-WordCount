SRC=example1/input
DST=example1/pigout
echo $(date '+%Y-%m-%d %H:%M:%S') pig-wordcount start >> log.log
pig -param SRC=$SRC -param DST=$DST wordcount.pig.txt
res=$?
echo $(date '+%Y-%m-%d %H:%M:%S') pig-wordcount end $res >> log.log
