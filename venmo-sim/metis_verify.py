# Generate some graph to verify the quality of Metis' partitioning
# A graph with 300 vertices, vertices 0~99, 100~199, 200~299 form
#  a complete graph respectively
# and add some random disturbing edges

import random
import os
os.environ["METIS_DLL"] = "/usr/local/lib/libmetis.so"
import metis
import networkx as nx
dg = nx.MultiDiGraph()

def gen_complete_graph(low, high):  # [low, high)
    for l in range(low, high):
        for r in range(low, high):
            if l != r:
                dg.add_edge(l, r)

gen_complete_graph(0, 100)
gen_complete_graph(100, 200)
gen_complete_graph(200, 300)

for _ in range(100):
    l = random.randint(0, 299)
    r = random.randint(0, 299)
    dg.add_edge(l, r)

(edgecuts, parts) = metis.part_graph(dg, 3)  # num of shards

fp = open("clustered_verify_metis.txt", "w")
print(parts, file=fp)
fp.close()
