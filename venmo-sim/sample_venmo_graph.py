# build graph from cleaned (only contains actor id and receiver id of each tx)
#  & normalized venmo dataset

import csv
import random
import math

fp = open("venmo_txes_date.csv", "r", encoding='utf-8')
csv_file = csv.reader(fp)
all_edges = []
all_vertex_set = set()
uid_dict = dict()
for row in csv_file:
    try:
        all_edges.append([int(row[0]), int(row[1]), row[2]])
        all_vertex_set.add(int(row[0]))
        all_vertex_set.add(int(row[1]))
    except:  # some rows have 'null' in them, for simplicity we ignore
        pass
fp.close()

sample_size = math.floor(0.1 * len(all_vertex_set))  # retain 1% num of edges of original graph
print(sample_size)
sample_vertex_set = set(random.sample(all_vertex_set, sample_size))
sample_real_vertex_set = set()
sample_edges = []
for row in all_edges:
    if row[0] in sample_vertex_set and row[1] in sample_vertex_set:
        sample_edges.append(row)
        sample_real_vertex_set.add(row[0])
        sample_real_vertex_set.add(row[1])
print(len(sample_edges))
print(len(sample_real_vertex_set))

fp = open("venmo_txes_sampled.csv", "w")
for row in sample_edges:  # meanwhile normalize vertex no
    if row[0] not in uid_dict:
        uid_dict[row[0]] = len(uid_dict)
    anode = uid_dict[row[0]]
    
    if row[1] not in uid_dict:
        uid_dict[row[1]] = len(uid_dict)
    tnode = uid_dict[row[1]]
    
    print('%d, %d, %s' % (anode, tnode, row[2]), file=fp)

fp.close()
