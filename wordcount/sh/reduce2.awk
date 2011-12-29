# $BO"A[G[Ns$KF~$l$F%+%&%s%H$7$F$_$k%P!<%8%g%s(B
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
