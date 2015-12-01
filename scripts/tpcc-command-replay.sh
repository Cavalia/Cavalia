if [ "$#" -ne 3 ]; then
	echo "usage: $0 num_warehouse scale_factor num_core";
else
	echo "./tpcc_benchmark -a1 -sf$1 -sf$2 -c$3 -r0"
	./tpcc_benchmark -a1 -sf$1 -sf$2 -c$3 -r0
fi
