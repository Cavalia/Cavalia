if [ "$#" -ne 2 ]; then
	echo "usage: $0 num_warehouse scale_factor";
else
	./tpcc_benchmark -a0 -sf$1 -sf$2
fi
