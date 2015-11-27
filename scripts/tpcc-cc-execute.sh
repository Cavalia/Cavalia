if [ "$#" -ne 3 ]; then
	echo "usage: $0 num_warehouse scale_factor num_core";
else
	NUM_TXN=$(($3 * 100000));
	echo "./tpcc_benchmark -a2 -sf$1 -sf$2 -c$3 -t$NUM_TXN -o0"
	./tpcc_benchmark -a2 -sf$1 -sf$2 -c$3 -t$NUM_TXN -o0
fi
