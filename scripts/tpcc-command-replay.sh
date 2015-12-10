if [ "$#" -ne 2 ]; then
	echo "usage: $0 num_core dir_name";
else
	echo "./tpcc_benchmark -a1 -c$1 -p$2 -r0"
	./tpcc_benchmark -a1 -c$1 -p$2 -r0
fi
