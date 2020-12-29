#!/bin/bash

FILE_TO_OUTPUT="remote_ratio.txt"
CORE_TO_BIND="7"

declare -a NODES_TO_SHARD=(
	"3"
	"6"
	"9"
	"12"
	"24"
	"48"
	"128"
)


rm -rf ${FILE_TO_OUTPUT}
## now loop through the above array

for TOTAL_NODES in "${NODES_TO_SHARD[@]}" ; do
   if [ ! -f "./venmo_dataset_metis_format.txt.part.${TOTAL_NODES}" ]; then
       gpmetis venmo_dataset_metis_format.txt ${TOTAL_NODES}
   fi
   echo "Run with ${TOTAL_NODES} nodes." >> ${FILE_TO_OUTPUT}
   taskset -c ${CORE_TO_BIND} python3 ./simulate_txs_on_metis_clustered_venmo_graph.py  ${TOTAL_NODES} >> ${FILE_TO_OUTPUT}
   echo " "  >> ${FILE_TO_OUTPUT}
   echo " "  >> ${FILE_TO_OUTPUT}
done
