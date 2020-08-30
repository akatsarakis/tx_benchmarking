import random

## arguments

p_tx = 0.025  # ratio of txs that are start/finish handovers
p_loc = 0.1  # Locality argument
#  i.e., ratio of "start handover"s that happen between a user and a base station
#  which are not collocated on the same (emulated) node

# mme_tot = 6
# enb_per_mme = 10
node_tot = 3
enb_per_node = 20  # number of enbs that each node captures locality (in this experiment, has update permission)
enb_tot = enb_per_node * node_tot
ue_per_node = 5400  # number of UEs that each node captures locality
ue_tot = ue_per_node * node_tot
# ue_per_mme = 2700
# ue_tot = ue_per_mme * mme_tot
# For now we assume there exists only 1 MME which has no effect

ue_home_node_table = [ue_id // ue_per_node for ue_id in range(0, ue_tot)]
ue_home_enb_table = [ue_home_node_table[ue_id] * enb_per_node + ue_id % enb_per_node
                    for ue_id in range(0, ue_tot)]
# A deactivated UE has no home eNodeB. Once it gets activated it gets a home eNodeB.
# except for the initial activation of a UE
ue_active_set = set()
ue_inactive_set = set(range(ue_tot))
# enb_home_mme  = enb_id // enb_per_mme  (constant)
# enb_home_node = enb_id // enb_per_node (constant)

local_enb_table = [[x for x in range(cur_node * enb_per_node,
                    (1+cur_node) * enb_per_node)] for cur_node in range(node_tot)]
remote_enb_table = [[x for x in range(0, enb_tot) if x not in
                    local_enb_table[cur_node]] for cur_node in range(node_tot)]

# In the Boston experiment, We have 3 nodes, and data are 3-way replicated
# Each node stores all data (6 MME instances + all phones)
# But has update permissions for only ~2/6 of all data (MMEs + phones)

# UEs are divided among eNodeBs equally (even if the number is indivisible)
# assume 3 eNodeB per MME, 5 UE per MME
# then each MME is like:
#  {B0, u0, u3, B1, u1, u4, B2, u2}
#  with u0, u3 home to B0,  u1, u4 home to B1,  etc.


if __name__ == '__main__':
    fp = [open("tx_node%d.txt" % i, "w") for i in range(node_tot)]
    tx_tot = 100000
    
    for tx_no in range(tx_tot):
        if tx_tot - tx_no == len(ue_active_set):
            break
            # need to deactivate all remaining UEs to ensure all UEs get deactivated by the end of the application program

        tx_type = 1 if bool(ue_active_set) == True and random.random() < p_tx else 0

        if tx_type == 1:  # start/finish handover
            ue_id = random.sample(ue_active_set, 1)[0]
            ue_home_enb = ue_home_enb_table[ue_id]
            # ue_home_mme = ue_home_enb // enb_per_mme
            ue_home_node = ue_home_enb // enb_per_node

            is_loc = 1 if random.random() > p_loc else 0
            # in the same node are the eNBs [node_id * enb_per_node, (1+node_id) * enb_per_node)
            if is_loc:
                dst_enb_id = random.choice(local_enb_table[ue_home_node])
                while dst_enb_id == ue_home_enb:
                    dst_enb_id = random.choice(local_enb_table[ue_home_node])
                    # dst_enb cannot be home_enb itself
            else:
                dst_enb_id = random.choice(remote_enb_table[ue_home_node])

            ue_home_enb_table[ue_id] = dst_enb_id
            dst_enb_home_node = dst_enb_id // enb_per_node
            ue_home_node_table[ue_id] = dst_enb_home_node  # will also handover UE's locality (in this experiment, upd permission) to the enb's home node
            fp[dst_enb_home_node].write('2 %d %d\n' % (ue_id, dst_enb_id))  # 2: start handover
            # for now we don't consider finish handover

        else:  # activate/deactivate UE
            act_type = 1 if bool(ue_inactive_set) == False \
                        or (bool(ue_active_set) == True and random.random() < tx_no / tx_tot) \
                        else 0
            # to make sure we have more activates than deactivates in the beginning
            #  and less in the end

            # ue_id = random.randint(0, ue_tot-1)

            if act_type == 1:  # deactivate UE
            # if ue_id in ue_active_set:  
                ue_id = random.sample(ue_active_set, 1)[0]
                ue_active_set.remove(ue_id)
                # ue_id = ue_active_set.pop() is much faster, but it pops elements in numerical order on my computer which is absurd
                # Whether Python sets are ordered is implementation-specific according to Python docs, and obviously we need it unordered here
                ue_inactive_set.add(ue_id)
                ue_home_enb_table[ue_id] = -1

                ue_home_node = ue_home_node_table[ue_id]
                fp[ue_home_node].write('1 %d\n' % ue_id)
            
            else:  # activate UE
                ue_id = random.sample(ue_inactive_set, 1)[0]  # ue_id = ue_inactive_set.pop()
                ue_inactive_set.remove(ue_id)
                ue_active_set.add(ue_id)

                ue_home_node = ue_home_node_table[ue_id]
                enb_id = ue_home_enb_table[ue_id] if ue_home_enb_table[ue_id] != -1 \
                            else random.choice(local_enb_table[ue_home_node])
                # ue in inactive_set but home_enb != -1: first time activating this UE
                # in the same node are the eNBs [node_id * enb_per_node, (1+node_id) * enb_per_node)

                ue_home_enb_table[ue_id] = enb_id
                fp[ue_home_node].write('0 %d %d\n' % (ue_id, enb_id))
    
    while bool(ue_active_set) == True:
        ue_id = random.sample(ue_active_set, 1)[0]  # ue_id = ue_active_set.pop()
        ue_active_set.remove(ue_id)
        ue_home_node = ue_home_node_table[ue_id]
        fp[ue_home_node].write('1 %d\n' % ue_id)


    for each_fp in fp:
        each_fp.close()
    
