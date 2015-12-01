if [ "$#" -ne 4 ]; then
	echo "usage: $0 num_warehouse scale_factor num_core dir_name";
else
	echo "./tpcc_benchmark -a1 -sf$1 -sf$2 -c$3 -p$4 -r0"
	./tpcc_benchmark -a1 -sf$1 -sf$2 -c$3 -p$4 -r0
fi
