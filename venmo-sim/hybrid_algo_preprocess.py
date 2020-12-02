# We use a hybrid algorithm to extract locality patterns of Venmo:
# - Run Leiden on upper 3M txes + all concerned lower txes of Venmo dataset
#   (concerned: at least one end appeared in upper part)
# - Run Metis or other fast algorithms on the rest
# Insight: The upper part of the dataset is denser than the lower part

import csv
fp = open("venmo_dataset_normalized_shorted.csv", "r")
csv_file = csv.reader(fp)
all_tx = []
for row in csv_file:
    all_tx.append([int(row[0]), int(row[1]), row[2]])
fp.close()

part1_edges = []
part1_vertex_set = set()
part2_edges = []

# first 3M txes
for row in all_tx[:3000000]:
    part1_edges.append(row)
    part1_vertex_set.add(row[0])
    part1_vertex_set.add(row[1])

# concerned lower txes
for row in all_tx[3000000:]:
    u = row[0]
    v = row[1]
    if u in part1_vertex_set or v in part1_vertex_set:
        part1_edges.append(row)
    else:
        part2_edges.append(row)

# write results
fp1 = open("venmo_hybrid_algo_leiden_part.csv", "w")
for row in part1_edges:
    print(row[0], row[1], row[2], sep=', ', file=fp1)
fp1.close()

fp2 = open("venmo_hybrid_algo_metis_part.csv", "w")
for row in part2_edges:
    print(row[0], row[1], row[2], sep=', ', file=fp2)
fp2.close()
