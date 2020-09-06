# In the Boston experiment, We have 3 nodes, and data are 3-way replicated
# Each node stores all data (6 MME instances + all phones)
# But has update permissions for only ~1/3 of all data (MMEs + phones)

import random
import argparse
import math
from queue import Queue as queue


def parse_args():  # node_tot, ue_tot, enb_tot, p_handovers, p_remote, p_ue_moving, tx_tot
    # args to be added: mme_tot, enb_per_mme, ue_per_mme
    # For now we assume there exists only 1 MME which has no effect
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument('node_tot', type=int,
        help='total number of emulated nodes')
    parser.add_argument('ue_tot', type=int,
        help='total number of UEs')
    parser.add_argument('enb_tot', type=int,
        help='total number of eNodeBs')
    parser.add_argument('p_handovers', type=float,
        help='probability that a generated tx is start/finish handover')
    parser.add_argument('p_remote', type=float,
        help='Locality argument; probability that a "start handover" happen between a user and a remote base station (base station that\'s not collocated on the same emulated node)')
    parser.add_argument('p_ue_moving', type=float,
        help='ratio of movable UEs in all UEs')  # movable UE: see below
    parser.add_argument('tx_tot', type=int,
        help='total number of transactions in generated trace')
    ARGS = parser.parse_args()


def main():
    ## read in user-specified arguments and calculate derived arguments
    parse_args()
    node_tot     = ARGS.node_tot
    enb_tot      = ARGS.enb_tot  # assume divisble by node_tot
    ue_tot       = ARGS.ue_tot   # assume divisble by node_tot
    p_handovers  = ARGS.p_handovers
    p_remote     = ARGS.p_remote
    p_ue_moving  = ARGS.p_ue_moving
    tx_tot       = ARGS.tx_tot

    enb_per_node = enb_tot // node_tot
    # number of eNodeBs that each node captures locality
    #  (in this experiment, has update permission)
    ue_per_node  = ue_tot  // node_tot
    # number of UEs that each node captures locality

    # pre-set home eNodeB of UEs are divided equally
    #  (if the number is indivisible, then some eNBs have 1 more UE than the others)
    # assume 3 eNodeB per MME, 8 UE per MME
    # then each MME is like:
    #  {B0, u0, u1, u2, B1, u3, u4, u5, B2, u6, u7}
    #  with u0, u1, u2 pre-set to be home to B0,  u3, u4, u5 to B1,  etc.
    ue_home_node_table   = [ue_id // ue_per_node for ue_id in range(0, ue_tot)]
    ue_preset_enb_table  = [ue_home_node_table[ue_id] * enb_per_node + ue_id % enb_per_node
                          for ue_id in range(0, ue_tot)]  # {B0, u0, u3, u6, B1, u1, u4, u7, B2, u2, u5}
    ue_preset_enb_table.sort()  # {B0, u0, u1, u2, B1, u3, u4, u5, B2, u6, u7}
    ue_home_enb_table = ue_preset_enb_table.copy()
    # enb_home_mme  = enb_id // enb_per_mme,   for given enb_id (constant)
    # enb_home_node = enb_id // enb_per_node,  for given enb_id (constant)

    # local_enb_table[cur_enb] are enbs that are on the same node with cur_enb
    local_enb_table = [[x for x in range(cur_node * enb_per_node,
                        (1+cur_node) * enb_per_node)] for cur_node in range(node_tot)]
    # in the same node are the eNBs [node_id * enb_per_node, (1+node_id) * enb_per_node)

    # We'll split the data (UEs) of each node into: always local UEs; moving UEs
    # - Moving UEs are uniformly activated at the beginning and deactivated at the end
    # - Remote handover will happen only on moving UEs in numerical order; i.e.,
    #   - Assume that the following are local UEs: of node1: U40-50; of node2: U90-100. whenever Node1 has a remote transaction it does gets: U91, then U92 etc.. until it get 100; similarly in the meantime Node2 would have gotten U40 ... U50. for subsequent remote transactions Node1 gets again back U40, then U41, until it reaches U50
    #     As long as the sequence is big enough and node 1 and node 2 execution does not drift very further apart this will not have any problems
    #   - For 3 or more nodes,
    #     when we do remote transactions with the moving UEs we have two phases (which might repeat again and again):
    #     1st phase: remote transaction with an initially non-local moving UE (getting other node local UEs)
    #     2nd phase: remote transaction with an initially local moving UE (getting our UEs back)
    #     Imagine the three nodes in a circle now: ... [n1], [n2], [n3] .. circles back to n1
    #     Every node does phase1 fetching object from its next (on the right) node,
    #     then it does phase 2 getting objects back from its previous (on the left) node
    # - tx type on these two kinds of UES:
    #   - always local UEs:
    #     activate/deactivate tx
    #     local handover tx (handover to another eNB in the same node)
    #   - movable UEs:
    #     remote handover tx (handover to another node)
    #     uniform activate tx in the beginning and deactivate tx in the end
    ue_moving_per_node = math.floor(ue_per_node * p_ue_moving + 1e-6)
    # number of moving UEs per node
    ue_remote_handover_queue = [queue() for node_id in range(0, node_tot)]  # We need to process objects circularly, so we use a queue to simulate a circular queue
    for node_id in range(0, node_tot-1):
        for ue_id in range((node_id+2) * ue_per_node - ue_moving_per_node, (node_id+2) * ue_per_node):
            ue_remote_handover_queue[node_id].put_nowait(ue_id)
        for ue_id in range((node_id+1) * ue_per_node - ue_moving_per_node, (node_id+1) * ue_per_node):
            ue_remote_handover_queue[node_id].put_nowait(ue_id)
    # for the last node, its next node wraps back to node 0
    for ue_id in range(1 * ue_per_node - ue_moving_per_node, 1 * ue_per_node):
        ue_remote_handover_queue[node_tot-1].put_nowait(ue_id)
    for ue_id in range(node_tot * ue_per_node - ue_moving_per_node, node_tot * ue_per_node):
        ue_remote_handover_queue[node_tot-1].put_nowait(ue_id)

    ue_active_set = set()  # set of active always-local UEs
    ue_inactive_set = set()
    for node_id in range(0, node_tot):
        for ue_id in range(node_id * ue_per_node, (node_id+1) * ue_per_node - ue_moving_per_node):
            ue_inactive_set.add(ue_id)
    # A deactivated UE has no home eNodeB. Once it gets activated it gets a home eNodeB.
    # except for the initial activation of a UE, where it is associated to a pre-set home eNodeB

    p_handovers_in_local_ue = p_handovers * (1 - p_remote) / (1 - p_handovers * p_remote) / (tx_tot - 2 * node_tot * ue_moving_per_node) * tx_tot
    # of a local UE, ratio of (local) handovers in all its txs
    # 2 * node_tot * ue_moving_per_node: number of txs taken by activation and deactivation of moving UEs

    ## generate trace
    fp = [open("tx_node%d.csv" % i, "w") for i in range(node_tot)]
    fp_params = open("tx_params.csv", "w")  # file pointer that parameters print to
    remote_handovers_lim = math.floor(tx_tot * p_handovers * p_remote + 1e-6)
    # remaining number of remote handovers
    ue_remote_cnt = 0

    print(ue_tot, enb_tot, p_handovers, p_remote, p_ue_moving, tx_tot,
            sep=', ', file=fp_params)
    for cur_node in range(node_tot):
        print(ue_tot, enb_tot, p_handovers, p_remote, p_ue_moving,
            sep=', ', file=fp[cur_node])
        print(cur_node * ue_per_node, (cur_node+1) * ue_per_node - 1,
            cur_node * enb_per_node, (cur_node+1) * enb_per_node - 1,
            sep=', ', file=fp[cur_node])
        print(file=fp[cur_node])
        # a blank line between first 2 lines of parameters and following txs to ensure readability
    # First 2 lines of each trace:
    # - <UEs-overall-total>, <ENodeB-overall-total> .. (+ every variable parameter)
    # - <UEs-min>, <UEs-max>, <ENodeB-min>, <ENodeB-max> (in current node)

    # Phase 1: Preset
    # - activate all moving UEs
    for node_id in range(0, node_tot):
        for ue_id in range((node_id+1) * ue_per_node - ue_moving_per_node, (node_id+1) * ue_per_node):
            enb_id = ue_home_enb_table[ue_id]
            # first time activating this UE: home_enb is preset
            fp[node_id].write('a, %d, %d\n' % (ue_id, enb_id))

    # Phase 2: Normal generate
    for tx_no in range(node_tot * ue_moving_per_node, tx_tot):
        if tx_tot - tx_no == \
        len(ue_active_set) + node_tot * ue_moving_per_node + ue_remote_cnt:
            break
        # When remaining number of txs equals number of tx needed to
        #  disable all active UEs and restore all moving UEs,
        #  goto Phase 2 (Clean-up)

        ue_type = 1 if remote_handovers_lim > ue_remote_cnt + 1 \
                    and random.random() < p_handovers * p_remote \
                    else 0

        if ue_type == 1:  # moving UE (remote handover)
            node_id = random.randint(0, node_tot - 1)
            # select node's next moving UE, and maintain its circular queue of moving UEs
            ue_id = ue_remote_handover_queue[node_id].get_nowait()
            ue_remote_handover_queue[node_id].put_nowait(ue_id)

            if (node_id+1) * ue_per_node - ue_moving_per_node <= ue_id < (node_id+1) * ue_per_node:
            # Take back its own UE from prev node (phase 2)
                dst_enb_id = ue_preset_enb_table[ue_id]
                ue_remote_cnt -= 1
                remote_handovers_lim -= 1
                # print('b', node_id, ue_id)  # debug

            else:  # Fetch next node's local UE (phase 1)
                dst_enb_id = (ue_preset_enb_table[ue_id] - enb_per_node + enb_tot) % enb_tot
                ue_remote_cnt += 1
                remote_handovers_lim -= 1
                # print('a', node_id, ue_id)

            ue_home_enb_table[ue_id] = dst_enb_id
            ue_home_node_table[ue_id] = node_id
            fp[node_id].write('h, %d, %d\n' % (ue_id, dst_enb_id))
            # h: (start-)handover
            # For now we don't consider finish handover

        else:  # always local UE
            tx_type = 1 if bool(ue_active_set) == True \
                        and random.random() < p_handovers_in_local_ue \
                        else 0

            if tx_type == 1: # local handover
                ue_id = random.sample(ue_active_set, 1)[0]
                ue_home_enb = ue_home_enb_table[ue_id]
                ue_home_node = ue_home_enb // enb_per_node

                dst_enb_id = random.choice(local_enb_table[ue_home_node])
                while dst_enb_id == ue_home_enb:
                    dst_enb_id = random.choice(local_enb_table[ue_home_node])
                    # dst_enb cannot be home_enb itself

                ue_home_enb_table[ue_id] = dst_enb_id
                fp[ue_home_node].write('h, %d, %d\n' % (ue_id, dst_enb_id))
            
            else:  # activate/deactivate UE
                act_type = 1 if bool(ue_inactive_set) == False \
                    or (bool(ue_active_set) == True and random.random() < tx_no / tx_tot) \
                    else 0
                # To make sure we have more activates than deactivates in the beginning
                #  and less in the end

                if act_type == 1:  # deactivate UE
                    ue_id = random.sample(ue_active_set, 1)[0]
                    ue_active_set.remove(ue_id)
                    # ue_id = ue_active_set.pop() is much faster, but it pops elements
                    #  in numerical order on my computer which is absurd
                    # Whether Python sets are ordered is implementation-specific
                    #  according to Python docs, and obviously we need it
                    #  unordered here
                    ue_inactive_set.add(ue_id)
                    ue_home_enb_table[ue_id] = -1

                    ue_home_node = ue_home_node_table[ue_id]
                    fp[ue_home_node].write('d, %d\n' % ue_id)
                
                else:  # activate UE
                    ue_id = random.sample(ue_inactive_set, 1)[0]  # ue_id = ue_inactive_set.pop()
                    ue_inactive_set.remove(ue_id)
                    ue_active_set.add(ue_id)

                    ue_home_node = ue_home_node_table[ue_id]
                    enb_id = ue_home_enb_table[ue_id] if ue_home_enb_table[ue_id] != -1 \
                                else random.choice(local_enb_table[ue_home_node])
                    # ue is inactive but home_enb is set: first time activating this UE

                    ue_home_enb_table[ue_id] = enb_id
                    fp[ue_home_node].write('a, %d, %d\n' % (ue_id, enb_id))

    # Because in the last part (1~5%) of phase 2, all UEs tend to be deactivated,
    #  this part of the trace is composed of 'a; d; a; d', and local handovers
    #  can't be generated. Therefore, the number of local handovers will be
    #  slightly fewer (expected deviation < 5%) than expectation.
    # However, we care more that the number of handovers shouldn't exceed the
    #  given ratio, and care less in the opposite direction. So this decrease
    #  can be accepted.
    # print(remote_handovers_lim)  # debug
    # print(ue_remote_cnt)

    # Phase 3: Clean-up
    # - handover moving UEs to their initial pre-set home node for next repetitive execution
    # - deactivate still active UEs (including moving UEs)

    # cnt2 = 0
    for node_id in range(0, node_tot):
        for ue_id in range((node_id+1) * ue_per_node - ue_moving_per_node, \
                            (node_id+1) * ue_per_node):
            if ue_home_node_table[ue_id] == node_id:
                continue
            # print(ue_id, end=' ')  # debug
            dst_enb_id = ue_preset_enb_table[ue_id]
            ue_home_enb_table[ue_id] = dst_enb_id
            ue_home_node_table[ue_id] = node_id
            fp[node_id].write('h, %d, %d\n' % (ue_id, dst_enb_id))
            # cnt2 += 1
    # print()
    # print(cnt2)  # debug
    
    for node_id in range(0, node_tot):
        for ue_id in range((node_id+1) * ue_per_node - ue_moving_per_node, \
                            (node_id+1) * ue_per_node):
            fp[node_id].write('d, %d\n' % ue_id)

    while bool(ue_active_set) == True:
        ue_id = random.sample(ue_active_set, 1)[0]  # ue_id = ue_active_set.pop()
        ue_active_set.remove(ue_id)
        ue_home_node = ue_home_node_table[ue_id]
        fp[ue_home_node].write('d, %d\n' % ue_id)

    fp_params.close()
    for each_fp in fp:
        each_fp.close()


if __name__ == '__main__':
    main()

