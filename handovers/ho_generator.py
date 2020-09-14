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
    parser.add_argument('thread_tot', type=int,
        help='total number of threads')
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
    thread_tot   = ARGS.thread_tot # assume divisible by node_tot
    enb_tot      = ARGS.enb_tot    # assume divisible by thread_tot
    ue_tot       = ARGS.ue_tot     # assume divisible by thread_tot
    p_handovers  = ARGS.p_handovers
    p_remote     = ARGS.p_remote
    p_ue_moving  = ARGS.p_ue_moving
    tx_tot       = ARGS.tx_tot

    enb_per_thread  = enb_tot  // thread_tot  # must > 1, otherwise will dead loop when generating a local handover
    ue_per_thread   = ue_tot   // thread_tot
    # number of eNodeBs/UES that each thread operates on
    thread_per_node = thread_tot // node_tot

    # pre-set home eNodeB of UEs are divided equally
    #  (if the number is indivisible, then some eNBs have 1 more UE than the others)
    # assume 3 eNodeB per MME, 8 UE per MME
    # then each MME is like:
    #  {B0, u0, u1, u2, B1, u3, u4, u5, B2, u6, u7}
    #  with u0, u1, u2 pre-set to be home to B0,  u3, u4, u5 to B1,  etc.
    ue_home_thread_table   = [ue_id // ue_per_thread for ue_id in range(0, ue_tot)]
    ue_preset_enb_table  = [ue_home_thread_table[ue_id] * enb_per_thread + ue_id % enb_per_thread
                          for ue_id in range(0, ue_tot)]  # {B0, u0, u3, u6, B1, u1, u4, u7, B2, u2, u5}
    ue_preset_enb_table.sort()  # {B0, u0, u1, u2, B1, u3, u4, u5, B2, u6, u7}
    ue_home_enb_table = ue_preset_enb_table.copy()
    # enb_home_mme  = enb_id // enb_per_mme,   for given enb_id (constant)
    # enb_home_thread = enb_id // enb_per_thread,  for given enb_id (constant)

    # local_enb_table[cur_enb] are enbs that are on the same thread with cur_enb
    local_enb_table = [[x for x in range(cur_thread * enb_per_thread,
                        (1+cur_thread) * enb_per_thread)] for cur_thread in range(thread_tot)]
    # in the same thread are the eNBs [thread_id * enb_per_thread, (1+thread_id) * enb_per_thread)

    # We'll split the UEs of each thread (thus also of each node) into: always local UEs; moving UEs
    # - Moving UEs are uniformly activated at the beginning and deactivated at the end
    # - Remote handover will happen only on moving UEs in numerical order between threads with the same number in different nodes; i.e.,
    #   - Assume there are 2 nodes and the following are moving UEs: thread2 of node1 (t2n1): U40-50; thread2 of node2 (t2n2): U90-100. whenever t2n1 has a remote transaction it does gets: U91, then U92 etc.. until it get 100; similarly in the meantime t2n2 would have gotten U40 ... U50. for subsequent remote transactions t2n1 gets again back U40, then U41, until it reaches U50
    #     As long as the sequence is big enough and t2n1 and t2n2 execution does not drift very further apart this will not have any problems
    #   - For 3 or more nodes,
    #     when we do remote transactions with the moving UEs we have two phases (which might repeat again and again):
    #     1st phase: remote transaction with an initially non-local moving UE (getting other node local UEs)
    #     2nd phase: remote transaction with an initially local moving UE (getting our UEs back)
    #     Imagine the three nodes in a circle now: ... [n1], [n2], [n3] .. circles back to n1
    #     Every thread does phase1 fetching object from the corresponding thread (thread with same number) of next (on the right) node,
    #     then it does phase 2 getting objects back from the corresponding thread of previous (on the left) node
    # - tx type on these two kinds of UES:
    #   - always local UEs:
    #     activate/deactivate tx
    #     local handover tx (handover to another eNB in the same thread)
    #   - movable UEs:
    #     remote handover tx (handover to corresponding thread of another node)
    #     uniform activate tx in the beginning and deactivate tx in the end
    ue_moving_per_thread = math.floor(ue_per_thread * p_ue_moving + 1e-6)
    # number of moving UEs per thread
    ue_remote_handover_queue = [queue() for thread_id in range(0, thread_tot)]  # We need to process objects circularly, so we use a queue to simulate a circular queue
    for thread_id in range(0, thread_tot-thread_per_node):
        for ue_id in range((thread_id+thread_per_node+1) * ue_per_thread - ue_moving_per_thread, (thread_id+thread_per_node+1) * ue_per_thread):
            ue_remote_handover_queue[thread_id].put_nowait(ue_id)
        for ue_id in range((thread_id+1) * ue_per_thread - ue_moving_per_thread, (thread_id+1) * ue_per_thread):
            ue_remote_handover_queue[thread_id].put_nowait(ue_id)
    # for the last node, its next node wraps back to node 0
    for k in range(0, thread_per_node):
        thread_id = k + thread_tot - thread_per_node
        for ue_id in range((k+1) * ue_per_thread - ue_moving_per_thread, (k+1) * ue_per_thread):
            ue_remote_handover_queue[thread_id].put_nowait(ue_id)
        for ue_id in range((thread_id+1) * ue_per_thread - ue_moving_per_thread, (thread_id+1) * ue_per_thread):
            ue_remote_handover_queue[thread_id].put_nowait(ue_id)

    ue_active_set = set()  # set of active always-local UEs
    ue_inactive_set = set()
    for thread_id in range(0, thread_tot):
        for ue_id in range(thread_id * ue_per_thread, (thread_id+1) * ue_per_thread - ue_moving_per_thread):
            ue_inactive_set.add(ue_id)
    # A deactivated UE has no home eNodeB. Once it gets activated it gets a home eNodeB.
    # except for the initial activation of a UE, where it is associated to a pre-set home eNodeB

    p_handovers_in_local_ue = p_handovers * (1 - p_remote) / (1 - p_handovers * p_remote) / (tx_tot - 2 * thread_tot * ue_moving_per_thread) * tx_tot
    # ratio of (local) handovers in all local txs
    # 2 * thread_tot * ue_moving_per_thread: number of txs taken by activation and deactivation of moving UEs

    ## generate trace
    fp = [open("tx_thread%02d.csv" % i, "w") for i in range(thread_tot)]
    fp_params = open("tx_params.csv", "w")  # file pointer that parameters print to
    remote_handovers_lim = math.floor(tx_tot * p_handovers * p_remote + 1e-6)
    # remaining number of remote handovers
    ue_remote_cnt = 0

    print(ue_tot, enb_tot, p_handovers, p_remote, p_ue_moving, tx_tot,
            sep=', ', file=fp_params)
    for cur_thread in range(thread_tot):
        print(ue_tot, enb_tot, p_handovers, p_remote, p_ue_moving,
            sep=', ', file=fp[cur_thread])
        print(cur_thread * ue_per_thread, (cur_thread+1) * ue_per_thread - 1,
            cur_thread * enb_per_thread, (cur_thread+1) * enb_per_thread - 1,
            sep=', ', file=fp[cur_thread])
        print(file=fp[cur_thread])
        # a blank line between first 2 lines of parameters and following txs to ensure readability
    # First 2 lines of each trace:
    # - <UEs-overall-total>, <ENodeB-overall-total> .. (+ every variable parameter)
    # - <UEs-min>, <UEs-max>, <ENodeB-min>, <ENodeB-max> (in current thread)

    # Phase 1: Preset
    # - activate all moving UEs
    for thread_id in range(0, thread_tot):
        for ue_id in range((thread_id+1) * ue_per_thread - ue_moving_per_thread, (thread_id+1) * ue_per_thread):
            enb_id = ue_home_enb_table[ue_id]
            # first time activating this UE: home_enb is preset
            # fp[thread_id].write('a, %d, %d\n' % (ue_id, enb_id))
            fp[thread_id].write('0, %d, %d\n' % (ue_id, enb_id))

    # Phase 2: Normal generate
    for tx_no in range(thread_tot * ue_moving_per_thread, tx_tot):
        if tx_tot - tx_no == \
        len(ue_active_set) + thread_tot * ue_moving_per_thread + ue_remote_cnt:
            break
        # When remaining number of txs equals number of tx needed to
        #  disable all active UEs and restore all moving UEs,
        #  goto Phase 2 (Clean-up)

        ue_type = 1 if remote_handovers_lim > ue_remote_cnt + 1 \
                    and random.random() < p_handovers * p_remote \
                    else 0

        if ue_type == 1:  # moving UE (remote handover)
            thread_id = random.randint(0, thread_tot - 1)
            # select thread's next moving UE, and maintain its circular queue of moving UEs
            ue_id = ue_remote_handover_queue[thread_id].get_nowait()
            ue_remote_handover_queue[thread_id].put_nowait(ue_id)

            if (thread_id+1) * ue_per_thread - ue_moving_per_thread <= ue_id < (thread_id+1) * ue_per_thread:
            # Take back its own UE from prev node (phase 2)
                dst_enb_id = ue_preset_enb_table[ue_id]
                ue_remote_cnt -= 1
                remote_handovers_lim -= 1
                # print('b', thread_id, node_id, ue_id)  # debug

            else:  # Fetch next node's local UE (phase 1)
                dst_enb_id = (ue_preset_enb_table[ue_id] - enb_per_thread * thread_per_node + enb_tot) % enb_tot
                ue_remote_cnt += 1
                remote_handovers_lim -= 1
                # print('a', thread_id, node_id, ue_id)

            ue_home_enb_table[ue_id] = dst_enb_id
            ue_home_thread_table[ue_id] = thread_id
            # fp[thread_id].write('h, %d, %d\n' % (ue_id, dst_enb_id))
            fp[thread_id].write('2, %d, %d\n' % (ue_id, dst_enb_id))
            # h: (start-)handover
            # For now we don't consider finish handover

        else:  # always local UE
            tx_type = 1 if bool(ue_active_set) == True \
                        and random.random() < p_handovers_in_local_ue \
                        else 0

            if tx_type == 1: # local handover
                ue_id = random.sample(ue_active_set, 1)[0]
                ue_home_enb = ue_home_enb_table[ue_id]
                ue_home_thread = ue_home_enb // enb_per_thread

                dst_enb_id = random.choice(local_enb_table[ue_home_thread])
                while dst_enb_id == ue_home_enb:
                    dst_enb_id = random.choice(local_enb_table[ue_home_thread])
                    # dst_enb cannot be home_enb itself

                ue_home_enb_table[ue_id] = dst_enb_id
                # fp[ue_home_thread].write('h, %d, %d\n' % (ue_id, dst_enb_id))
                fp[ue_home_thread].write('2, %d, %d\n' % (ue_id, dst_enb_id))

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

                    ue_home_thread = ue_home_thread_table[ue_id]
                    # fp[ue_home_thread].write('d, %d\n' % ue_id)
                    fp[ue_home_thread].write('1, %d\n' % ue_id)

                else:  # activate UE
                    ue_id = random.sample(ue_inactive_set, 1)[0]  # ue_id = ue_inactive_set.pop()
                    ue_inactive_set.remove(ue_id)
                    ue_active_set.add(ue_id)

                    ue_home_thread = ue_home_thread_table[ue_id]
                    enb_id = ue_home_enb_table[ue_id] if ue_home_enb_table[ue_id] != -1 \
                                else random.choice(local_enb_table[ue_home_thread])
                    # ue is inactive but home_enb is set: first time activating this UE

                    ue_home_enb_table[ue_id] = enb_id
                    # fp[ue_home_thread].write('a, %d, %d\n' % (ue_id, enb_id))
                    fp[ue_home_thread].write('0, %d, %d\n' % (ue_id, enb_id))

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
    # - handover moving UEs to their initial pre-set home thread/node for next repetitive execution
    # - deactivate still active UEs (including moving UEs)

    for thread_id in range(0, thread_tot):
        for ue_id in range((thread_id+1) * ue_per_thread - ue_moving_per_thread, \
                            (thread_id+1) * ue_per_thread):
            if ue_home_thread_table[ue_id] == thread_id:
                continue
            dst_enb_id = ue_preset_enb_table[ue_id]
            ue_home_enb_table[ue_id] = dst_enb_id
            ue_home_thread_table[ue_id] = thread_id
            # fp[thread_id].write('h, %d, %d\n' % (ue_id, dst_enb_id))
            fp[thread_id].write('2, %d, %d\n' % (ue_id, dst_enb_id))

    for thread_id in range(0, thread_tot):
        for ue_id in range((thread_id+1) * ue_per_thread - ue_moving_per_thread, \
                            (thread_id+1) * ue_per_thread):
            # fp[thread_id].write('d, %d\n' % ue_id)
            fp[thread_id].write('1, %d\n' % ue_id)

    while bool(ue_active_set) == True:
        ue_id = random.sample(ue_active_set, 1)[0]  # ue_id = ue_active_set.pop()
        ue_active_set.remove(ue_id)
        ue_home_thread = ue_home_thread_table[ue_id]
        # fp[ue_home_thread].write('d, %d\n' % ue_id)
        fp[ue_home_thread].write('1, %d\n' % ue_id)

    fp_params.close()
    for each_fp in fp:
        each_fp.close()


if __name__ == '__main__':
    main()

