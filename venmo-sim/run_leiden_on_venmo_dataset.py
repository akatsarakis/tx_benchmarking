import json
import leidenalg
import igraph as ig

# build igraph from venmo dataset
fp = open("venmo_dataset_sample_repaired.json", "r", encoding='utf-8')
venmo_edges = json.load(fp)
G = ig.Graph(directed=True)  # directed, unweighted
# the data does not specify the amount of each transaction, since Venmo does not make this data public.
G.add_vertices(2)  # 7178381)  # For this given dataset, total number of vertices is already known
uid_dict = dict()

for venmo_edge in venmo_edges:
    aid = venmo_edge['payment']['actor']['id']
    if aid not in uid_dict:
        uid_dict[aid] = len(uid_dict)
    anode = uid_dict[aid]
    
    tid = venmo_edge['payment']['target']['user']['id']
    if tid not in uid_dict:
        uid_dict[tid] = len(uid_dict)
    tnode = uid_dict[tid]
    
    G.add_edge(anode, tnode)

print(G)  # debug
fp.close()

# run Leiden algorithm
part = leidenalg.find_partition(G, leidenalg.ModularityVertexPartition)
print(part)  # debug

# save result into file
fp = open("clustered_venmo_dataset.txt", "w")
print(part, file=fp)
fp.close()
