# build graph from cleaned (only contains actor id and receiver id of each tx)
#  & normalized venmo dataset

import csv
import networkx as nx
import os
os.environ["METIS_DLL"] = "/usr/local/lib/libmetis.so"
import metis

dg = nx.MultiDiGraph()
fp = open("venmo_txes_sampled.csv", "r") # , encoding='utf-8')
csv_file = csv.reader(fp)
for row in csv_file:
    dg.add_edge(row[0], row[1])
fp.close()

(edgecuts, parts) = metis.part_graph(dg, 6)  # num of shards

fp = open("clustered_venmo.txt", "w")
print(parts, file=fp)
fp.close()
