BEGIN {
	OFS = "\t";
	old = ""; sum = 0;
}
{
	if ($1 != old) {
		output(old, sum);
		old = $1; sum = 0;
	}
	sum += $2;
}
END {
	output(old, sum);
}

function output(key, sum) {
	if (sum != 0) {
		print key, sum;
	}
}
