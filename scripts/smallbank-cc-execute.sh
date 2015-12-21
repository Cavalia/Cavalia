if [ "$#" -ne 4 ]; then
	echo "usage: $0 scale_factor skewness num_core dir_name";
else
	NUM_TXN=$(($3 * 100000));
	echo "./smallbank_benchmark -a2 -sf$1 -sf$2 -c$3 -t$NUM_TXN -p$4 -o0"
	./smallbank_benchmark -a2 -sf$1 -sf$2 -c$3 -t$NUM_TXN -p$4 -o0
fi
