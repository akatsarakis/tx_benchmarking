import random
import math
import argparse
import scipy.stats
# from scipy.integrate import quad

class trip_len_generator(scipy.stats.rv_continuous):
    def _pdf(self, x):
        return (x + 14.6) ** -0.78 * math.exp(-x/60) / 2.88
        # 2.88: scaling factor. Must ensure the pdf integrates to 1
        #  on the given domain [1, 300]
trip_len_gen = trip_len_generator(name="trip_len", a=1, b=300)
# print(quad(trip_len._pdf, 1, 300))  # to verify the pdf integrates to 1

def parse_args():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument('node_tot', type=int,
        help='total number of nodes/shards in the system')
    ARGS = parser.parse_args()

parse_args()

# Area of Boston: 60km x 40km; Population: 5.5M
ue_tot = 5499025
# If we use 5500000, then len(ue_pos) < ue_tot and sometimes trip_ue (declared below)
#  will run out of bound. so we use 5499025, a perfect square number
x_len = 60
y_len = 40
sqrt_ue_tot = math.sqrt(ue_tot)
trip_tot = 10000 # ue_tot * 4
# In theory, this needs to be very big to get rid of systematic error; given that
#  each user makes ~4 trips per day on average, we assign 4*ue_tot
# However, scipy.stats.rv_continuous is quite slow with custom pdf (probability
#  density function), confining trip_tot to lower
ue_pos = [(x_len / sqrt_ue_tot * cur_x, y_len / sqrt_ue_tot * cur_y)
            for cur_x in range(math.floor(sqrt_ue_tot + 0.5))
            for cur_y in range(math.floor(sqrt_ue_tot + 0.5))]
            # Assume initial positions are uniformly random
node_tot = ARGS.node_tot
shard_len = math.sqrt(x_len * y_len / node_tot)
# assume each shard is a perfect square, and area of all shards add up to the whole area

handover_cnt = 0
handover_remote_cnt = 0
for cur_trip in range(trip_tot):
    trip_ue = random.randint(0, ue_tot-1)
    trip_len = trip_len_gen.rvs(size=1)[0]
    trip_dir = random.random() * 2 * math.pi  # assume trip directions are uniformly random distributed

    for_range = range(math.ceil(trip_len))
    for cur_step in for_range:
        if trip_len < 1:
            trip_vector = (trip_len * math.cos(trip_dir), trip_len * math.sin(trip_dir))
            trip_len = 0
        else:
            trip_vector = (1 * math.cos(trip_dir), 1 * math.sin(trip_dir))
            trip_len -= 1
        # divide a trip into handovers of 1km each (except the last one)

        x0 = ue_pos[trip_ue][0]
        y0 = ue_pos[trip_ue][1]
        x1 = x0 + trip_vector[0]
        y1 = y0 + trip_vector[1]
        ue_pos[trip_ue] = (x1, y1)

        handover_cnt += 1
        if x1 // shard_len != x0 // shard_len or y1 // shard_len != y0 // shard_len:  # crossed shard boundary
            # print(x0, y0, x1, y1)  # debug
            handover_remote_cnt += 1

print(handover_remote_cnt / handover_cnt)
