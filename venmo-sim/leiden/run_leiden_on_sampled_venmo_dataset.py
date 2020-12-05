import csv
import leidenalg
import igraph as ig

# build igraph from venmo dataset
fp = open("cleaned_normalized_txes_sampled.csv", "r")  # , encoding='utf-8')
venmo_edges = csv.reader(fp)
G = ig.Graph(directed=True)  # TODO , weighted=True)  # directed, weighted (edge weight = multiple of edge)
# the data does not specify the amount of each transaction, since Venmo does not make this data public.
G.add_vertices(7178381)  # For this given dataset, total number of vertices is already known
uid_dict = dict()

for venmo_edge in venmo_edges:
    aid = venmo_edge[0]
    if aid not in uid_dict:
        uid_dict[aid] = len(uid_dict)
    anode = uid_dict[aid]
    
    tid = venmo_edge[1]
    if tid not in uid_dict:
        uid_dict[tid] = len(uid_dict)
    tnode = uid_dict[tid]
    
    try:
        G.add_edge(anode, tnode)
    except:
        print(aid, tid, anode, tnode)
        break

# print(G)  # debug
fp.close()

# run Leiden algorithm
part = leidenalg.find_partition(G, leidenalg.ModularityVertexPartition)
# print(part)  # debug

# save result into file
fp = open("cleaned_normalized_clustered_venmo_dataset.txt", "w")
print(part, file=fp)
fp.close()
