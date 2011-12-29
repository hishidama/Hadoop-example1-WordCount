# 連想配列に入れてカウントしてみるバージョン
BEGIN {
	OFS = "\t";
}
{
	count[$1] += $2;
}
END {
	for (key in count) {
		output(key, count[key]);
	}
}

function output(key, sum) {
	print key, sum;
}
