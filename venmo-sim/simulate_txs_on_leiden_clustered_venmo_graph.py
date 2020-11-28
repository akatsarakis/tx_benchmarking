import argparse
import csv
import re


# read arg (the num of shards, also the num of distributed nodes) from command line
def parse_args():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument('node_tot', type=int,
        help='total number of distributed-nodes/shards in the system')
    ARGS = parser.parse_args()

parse_args()
node_tot = ARGS.node_tot


# read clustering result of the graph, and bind them to distributed nodes
#  (ensure the total vertex to each node is as equal as possible)

## first, read in total number of vertices and clusters
##  from the first line of the input file
fp = open("clustered_venmo_dataset_2000000.txt", "r") # , encoding='utf-8')
firstline_str = fp.readline()
firstline_pattern = re.compile(r"Clustering with (\d+) elements and (\d+) clusters")
firstline_match = firstline_pattern.match(firstline_str)
if not firstline_match:
    raise Exception('Failed to identify total vertex number and cluster number in input file')
vertex_tot = eval(firstline_match.group(1))
cluster_tot = eval(firstline_match.group(2))
print('%d vertices, %d clusters, %d shards' % (vertex_tot, cluster_tot, node_tot))

## binding initialization
curline_str = fp.readline()
cur_cluster = -1
expected_node_size = vertex_tot / node_tot
vertex_cluster_no = [-1] * vertex_tot
vertex_node_no = [-1] * vertex_tot
cluster_size = [0] * cluster_tot
cluster_vertices = [[] for i in range(cluster_tot)]  # '[[]] * cluster_tot' is wrong, because it is shallow copy
cluster_node_no = [-1] * cluster_tot

## read in each line and count
while curline_str:
    if curline_str[0] == '[':  # beginning of a new cluster
        cur_cluster += 1
        curline_str = curline_str[curline_str.find(']')+1:]
        # remove the preceding '[ cluster_no]' of the string

    strs = curline_str[:-1].split(',')  # slice off newline character, and split by comma
    for cur_str in strs:
        if not cur_str:
            continue  # ignore empty substrings (in the case the last char is a comma)

        # bind vertex to its cluster written in the input file
        cur_vertex = eval(cur_str)
        vertex_cluster_no[cur_vertex] = cur_cluster
        cluster_vertices[cur_cluster].append(cur_vertex)
        cluster_size[cur_cluster] += 1

    curline_str = fp.readline()  # next line

## bind clusters to distributed nodes according to counting result
cur_node = 0
cur_node_size = 0
for cur_cluster in range(cluster_tot):
    if cur_node_size + cluster_size[cur_cluster] / 2 > expected_node_size and cur_node_size != 0:
        # should bind to the next node, unless this cluster is the first cluster on the current node
        cur_node += 1
        cur_node_size = 0
    
    cur_node_size += cluster_size[cur_cluster]
    cluster_node_no[cur_cluster] = cur_node
    for cur_vertex in cluster_vertices[cur_cluster]:
        vertex_node_no[cur_vertex] = cur_node

fp.close()
print('-- Binding result:')
node_size = [0] * node_tot
for cur_vertex in range(vertex_tot):
    node_size[vertex_node_no[cur_vertex]] += 1
for cur_node in range(node_tot):
    print('Shard %d size:' % cur_node, node_size[cur_node])


# read txes and sort by time
def takeThird(elem):
    return elem[2]
    # This function is used to sort txes; here we
    #  assume dates can be sorted in string sort manner

fp = open("venmo_dataset_normalized_shorted.csv", "r") # , encoding='utf-8')
csv_file = csv.reader(fp)
all_tx = []
for row in csv_file:
    all_tx.append([int(row[0]), int(row[1]), row[2]])
all_tx = all_tx[:2000000]  # we only used the first 2000000 lines of txes
all_tx.sort(key=takeThird)
fp.close()

# simulate txs, calculate ratio of remote tx
tx_remote_cnt = 0
tx_cnt = 0
for tx in all_tx:
    tx_cnt += 1
    if vertex_node_no[tx[0]] != vertex_node_no[tx[1]]:
        tx_remote_cnt += 1
        vertex_node_no[tx[0]] = vertex_node_no[tx[1]]

print('-- Result:')
print('tx total:', tx_cnt)
print('remote:', tx_remote_cnt)
print('remote ratio:', tx_remote_cnt / tx_cnt)
