import random

W = 1  # number of warehouses

def NURand(A, x, y):
    '''
    non-uniform random over range [x, y]
    A is a constant chosen according to the size of the range [x .. y]
      for C_LAST, the range is [0 .. 999] and  A = 255
      for C_ID, the range is [1 .. 3000] and  A = 1023
      for OL_I_ID, the range is [1 .. 100000] and A = 8191
    '''
    C = random.randint(0, A)
    return (((random.randint(0, A) | random.randint(x, y)) + C) % (y - x + 1)) + x


c_last_name_sections = ("BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING")
def gen_rand_lastname(x):
    '''
    Used for generating customer last name (C_LAST).
    Concatenation of three variable length syllables selected from c_last_name_sections.
    '''
    if x < 0: x = NURand(255, 0, 999)
    # x = 371  # debug
    x1 = x % 10
    x2 = x // 10 % 10
    x3 = x // 100
    lastname = ''.join((c_last_name_sections[x3], c_last_name_sections[x2], c_last_name_sections[x1]))
    return lastname


def gen_new_order(use_next_warehouse, file):
    '''
    Only generate data, doesn't insert into database.
    The argument use_next_warehouse specifies whether to use
     a randomly selected warehouse or simply the next warehouse in the case of rbk = 1.
    The New-Order business transaction consists of entering a complete order
     through a single database transaction.
    '''
    # output format:
    #  transaction type, w_id, d_id, c_id, ol_cnt
    #   - (order line 1) ol_i_id, ol_supply_w_id, ol_quantity
    #   ...
    
    w_id = random.randint(1, W)
    # For any given terminal, the home warehouse number (W_ID) is constant
    #  over the whole measurement interval

    d_id = random.randint(1, 10)
    # The district number (D_ID) is randomly selected within [1 .. 10] from 
    #  the home warehouse (D_W_ID = W_ID).

    c_id = NURand(1023, 1, 3000)
    # The non-uniform random customer number (C_ID) is selected
    #  using the NURand(1023,1,3000) function from 
    #  the selected district number (C_D_ID = D_ID) and
    #  the home warehouse number (C_W_ID = W_ID).

    ol_cnt = random.randint(5, 15)
    # The number of items in the order (ol_cnt) is randomly selected
    #  within  [5 .. 15] (an average of 10). This field is not part of the input.

    file.write('%d %d %d %d %d\n' % (1, w_id, d_id, c_id, ol_cnt))
    
    rbk = random.randint(1, 100)
    # A fixed 1% of the New-Order transactions are chosen at random
    #  to simulate user data entry errors and exercise the performance of 
    #  **rolling back** update transactions.

    remote_warehouses = list(i for i in range(1, W+1) if i != w_id)

    for i in range(0, ol_cnt):  # For each of the ol_cnt items on the order
        ol_i_id = 0 if rbk == 1 and i == ol_cnt - 1 else NURand(8191, 1, 100000)
        # A non-uniform random item number (OL_I_ID) is selected using the
        #  NURand(8191, 1, 100000) function. If this is the last item on the order
        #  and rbk = 1, then the item number is set to an unused value.
        # Comment: An unused value for an item number is a value not found in the
        #  database such that its use will produce a "not-found" condition within 
        #  the application program. This condition should result in rolling back 
        #  the current database transaction.

        x = 100 if W == 1 else random.randint(1, 100)
        if x > 1:
            ol_supply_w_id = w_id
        else:
            ol_supply_w_id = w_id % W + 1 if use_next_warehouse else random.sample(remote_warehouses, 1)[0]
            # file.write('hhh')  # debug
        # A supplying warehouse number (OL_SUPPLY_W_ID) is selected as
        #  the home warehouse 99% of the time and as a remote warehouse 1%
        #  of the time. (If the system is configured for a single warehouse, then
        #  all items are supplied from that single home warehouse.)

        ol_quantity = random.randint(1, 10)
        # A quantity (OL_QUANTITY) is randomly selected within [1 .. 10].

        file.write(' %d %d %d\n' % (ol_i_id, ol_supply_w_id, ol_quantity))

    # The order entry date (O_ENTRY_D) is generated within the SUT by
    #  using the current system date and time. (Not generated here)


def gen_payment(use_next_warehouse, file):
    '''
    The Payment business transaction updates the customer's balance and reflects
     the payment on the district and warehouse sales statistics.
    In addition, this transaction includes non-primary key access to the CUSTOMER table.
    '''
    # output format:
    #  transaction type, w_id, d_id, c_w_id, c_d_id, h_amount, mode of selection, c_id / c_last_name
    
    w_id = random.randint(1, W)
    d_id = random.randint(1, 10)

    remote_warehouses = list(i for i in range(1, W+1) if i != w_id)
    x = 1 if W == 1 else random.randint(1, 100)
    if x <= 85:
        c_d_id = d_id
        c_w_id = w_id
    else:
        c_d_id = random.randint(1, 10)
        c_w_id = w_id % W + 1 if use_next_warehouse else random.sample(remote_warehouses, 1)[0]
        # file.write('haha ')  # debug
    # The customer resident warehouse is the home warehouse 85% of the time (x <= 85)
    #  and is a randomly selected remote warehouse 15% of the time (x > 85).
    # But if the system is configured for a single warehouse, then all customers are selected from
    #  that single home warehouse.

    h_amount = random.randint(100, 500000) / 100.0
    
    # The payment date (H_DATE) in generated within the SUT by using the current system date and time. (Not here)
    
    file.write('%d %d %d %d %d %.2f ' % (2, w_id, d_id, c_w_id, c_d_id, h_amount))

    y = random.randint(1, 100)
    c_last = ""
    c_id = -1
    if y <= 60:
        c_last = gen_rand_lastname(-1)
        file.write('%d %s\n' % (1, c_last))
    else:
        c_id = NURand(1023, 1, 3000)
        file.write('%d %d\n' % (2, c_id))
    # The customer is randomly selected 60% of the time by last name (y <= 60)
    #  and 40% of the time by number (y > 60).


def gen_order_status(file):
    '''
    The Order-Status business transaction queries the status of a customer's last order.
    In addition, this table includes non-primary key access to the CUSTOMER table.
    '''
    # output format:
    #  transaction type, w_id, d_id, mode of selection, c_id / c_last_name
    
    w_id = random.randint(1, W)
    d_id = random.randint(1, 10)

    file.write('%d %d %d ' % (3, w_id, d_id))
    
    y = random.randint(1, 100)
    c_last = ""
    c_id = -1
    if y <= 60:
        c_last = gen_rand_lastname(-1)
        file.write('%d %s\n' % (1, c_last))
    else:
        c_id = NURand(1023, 1, 3000)
        file.write('%d %d\n' % (2, c_id))


def gen_delivery(file):
    '''
    The Delivery business transaction consists of processing a batch of 10 new (not yet delivered) orders.
    The Delivery transaction is intended to be executed in deferred mode through a queuing mechanism,
     rather than interactively, with terminal response indicating transaction completion. The result
     of the deferred execution is recorded into a result file.
    '''
    # output format:
    #  transaction type, w_id, o_carrier_id
    
    w_id = random.randint(1, W)
    o_carrier_id = random.randint(1, 10)
    # The delivery date (OL_DELIVERY_D) is generated within the SUT by using the current system date and time. (not here)

    file.write('%d %d %d\n' % (4, w_id, o_carrier_id))


def gen_stock_level(file):
    '''
    The Stock-Level business transaction determines the number of recently sold items that
     have a stock level below a specified threshold.
    '''
    # output format:
    #  transaction type, w_id, d_id, threshold

    w_id = random.randint(1, W)
    d_id = random.randint(1, 10)
    # Each terminal must use a unique value of (W_ID, D_ID) that is constant over the whole measurement,
    #  i.e., D_IDs cannot be re-used within a warehouse.

    threshold = random.randint(10, 20)
    # The threshold of minimum quantity in stock (threshold) is selected at random within [10 .. 20].
    
    file.write('%d %d %d %d\n' % (5, w_id, d_id, threshold))


if __name__ == '__main__':
    file = open('trans_trace.txt', 'w')

    # total number of transactions, and numbers of each type of transaction
    # Portion of each type is fixed. See https://hstore.cs.brown.edu/papers/hstore-endofera.pdf
    tot = 1000
    t1 = int(tot * 0.44)
    t2 = int(tot * 0.44)
    t3 = int(tot * 0.04)
    t4 = int(tot * 0.04)
    t5 = int(tot * 0.04)
    
    for i in range(t1): gen_new_order(0, file)
    for i in range(t2): gen_payment(0, file)
    for i in range(t3): gen_order_status(file)
    for i in range(t4): gen_delivery(file)
    for i in range(t5): gen_stock_level(file)
    
    file.close()
