# read the result of Metis' stand-alone program (gpmetis) and do simulation

import argparse
import csv


# read arg (num of shards) from command line
def parse_args():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument('node_tot', type=int,
        help='total number of nodes/shards in the system')
    ARGS = parser.parse_args()

parse_args()
node_tot = ARGS.node_tot


# read txes and sort by time
def takeThird(elem):
    return elem[2]
# assume dates can be sorted in string sort manner

fp = open("../venmo_dataset_normalized_shorted.csv", "r")
csv_file = csv.reader(fp)
all_tx = []
for row in csv_file:
    all_tx.append([int(row[0]), int(row[1]), row[2]])
all_tx.sort(key=takeThird)
fp.close()


# read clustering result of the graph
#fp = open("clustered_venmo_gpmetis.txt", "r")
fp = open("venmo_dataset_metis_format.txt.part." + str(node_tot), "r")

# vertex_node_no = eval(fp.read())  # pymetis
vertex_node_no = [eval(line) for line in fp.readlines()]  # gpmetis
vertex_tot = len(vertex_node_no)


# simulate txs, calculate ratio of remote tx
tx_remote_cnt = 0
tx_cnt = 0
for tx in all_tx:
    tx_cnt += 1
    if vertex_node_no[tx[0]] != vertex_node_no[tx[1]]:
        tx_remote_cnt += 1
        vertex_node_no[tx[0]] = vertex_node_no[tx[1]]
print(tx_remote_cnt / tx_cnt)

