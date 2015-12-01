if [ "$#" -ne 3 ]; then
	echo "usage: $0 num_warehouse scale_factor dir_name";
else
	./tpcc_benchmark -a0 -sf$1 -sf$2 -p$3
fi
