{ for(i = 1; i <= NF; i++) count[$i]++ }
END {
	for (key in count) {
		print key, count[key]
	}
}
