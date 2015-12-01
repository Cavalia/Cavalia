if [ "$#" -ne 4 ]; then
	echo "usage: $0 num_warehouse scale_factor num_core dir_name";
else
	NUM_TXN=$(($3 * 100000));
	echo "./tpcc_benchmark -a2 -sf$1 -sf$2 -c$3 -t$NUM_TXN -p$4 -o0"
	./tpcc_benchmark -a2 -sf$1 -sf$2 -c$3 -t$NUM_TXN -p$4 -o0
fi
