# Convert the Venmo dataset into Metis' canonical adjacent list format (as defined in official Metis manual; for Metis' stand-alone program)

import csv
VMAX = 7178389

fp = open("../venmo_dataset_normalized_shorted.csv", "r")
csv_file = csv.reader(fp)
vertex_tot = VMAX  # Because we know vertex number in advance, here we hardcode it
edge_tot = 0

adj_list = [dict() for i in range(VMAX+1)]  # in Metis, no of vertex begins from 1
def add_edge(u, v):
    global adj_list
    global edge_tot
    if v in adj_list[u]:
        adj_list[u][v] += 1
    else:
        adj_list[u][v] = 1
        edge_tot += 1  # incoming edge is uncounted

for row in csv_file:
    u = int(row[0]) + 1
    v = int(row[1]) + 1
    add_edge(u, v)
    add_edge(v, u)  # because graph partitioning demands undirected graphs
edge_tot //= 2  # each edge counted twice

fp.close()
# print(adj_list)
# print(vertex_tot, edge_tot)


fp = open("venmo_dataset_metis_format.txt", "w")
fmt = '001'  # This Metis param denotes that edges are weighted in this graph, while vertices aren't weighted and don't have sizes.
print(vertex_tot, edge_tot, fmt, file=fp)
for u in range(1, vertex_tot+1):
    for v in adj_list[u].keys():
        print(v, adj_list[u][v], end=' ', file=fp)
        # adjacent vertices and corresponding edge weight
    print(file=fp)  # newline
fp.close()
