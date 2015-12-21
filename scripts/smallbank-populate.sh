if [ "$#" -ne 2 ]; then
	echo "usage: $0 scale_factor dir_name";
else
	./smallbank_benchmark -a0 -sf$1 -p$2
fi
