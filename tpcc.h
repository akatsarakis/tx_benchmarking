#include <stdint.h>
#include <stdlib.h>
#include <time.h>  // struct tm 
#include <string.h>

//////////////////////
// Schema Definition
//////////////////////

typedef struct warehouse_t
{
    int w_id;  // primary key; this field need to hold up to 2*W unique IDs
    char w_name[11];
    char w_street_1[21];
    char w_street_2[21];
    char w_city[21];  // varchar
    char w_state[3];
    char w_zip[10];
    float w_tax; // numeric(4, 4) signed; sales tax
    float w_ytd; // numeric(12, 2) signed; Year to date balance
} __attribute__((packed)) warehouse_t;

// ???
// Can simulate varchar with char[]? (I know in C++ we can use std::vector<char> but not in C)
//  also, do we need padding on char array length? i.e., char[17] for varchar(16)

// ???
// Why do we use __attribute__((packed)), bit field or Neil field? Is it because we have
//  stringent limit on memory use? (__attribute__((packed)) may in fact slow down the program...)
// What is a Neil field? No information about Neil field on Google

typedef struct district_t
{  // 10 districts populated per warehouse
    int8_t d_id; // 20 unique IDs
    int d_w_id;  // 2*W unique IDs
    char d_name[11];
    char d_street_1[21];
    char d_street_2[21];
    char d_city[21];
    char d_state[3];
    char d_zip[10];
    float d_tax;  // numeric(4, 4) signed
    float d_ytd;  // numeric(12, 2) signed
    int d_next_o_id;  // Next available Order number
    // primary key (D_W_ID, D_ID),
    // foreign key (D_W_ID) references WAREHOUSE(W_ID)
} __attribute__((packed)) district_t;

typedef struct customer_t
{  // 3000 customers populated per district
    int c_id;  // 96,000 unique IDs
    int8_t c_d_id;  // 20 unique IDs
    int c_w_id;  // 2*W unique IDs
    char c_first[17];  // varchar(16)
    char c_middle[3];
    char c_last[17];
    char c_street_1[21];
    char c_street_2[21];
    char c_city[21];
    char c_state[3];
    char c_zip[10];
    char c_phone[17];
    struct tm* c_since;
    char c_credit[3];  // "GC" == good, "BC" == bad
    float c_credit_lim;  // numeric(12, 2) signed
    float c_discount;  // numeric(4, 4) signed
    float c_balance;  // numeric(12, 2) signed
    float c_ytd_payment;  // numeric(12, 2) signed
    uint16_t c_payment_cnt;  // numeric(4) unsigned
    uint16_t c_delivery_cnt;  // numeric(4) unsigned
    char c_data[501];  // miscellaneous information
    // primary key (C_W_ID, C_D_ID, C_ID),
    // foreign key (C_W_ID, C_D_ID) references DISTRICT(D_W_ID, D_ID)
} __attribute__((packed)) customer_t;

typedef struct history_t
{
    int h_c_id; // 96,000 unique IDs
    int8_t h_c_d_id;  // 20 unique IDs
    int h_c_w_id;  // 2*W unique IDs
    int8_t h_d_id;  // 20 unique IDs
    int h_w_id;  // 2*W unique IDs
    struct tm* h_date;
    float h_amount;  // numeric(6, 2)
    char h_data[25];  // Miscellaneous information
    // foreign key (H_C_W_ID, H_C_D_ID, H_C_ID) references CUSTOMER(C_W_ID, C_D_ID, C_ID),
    // foreign key (H_W_ID, H_D_ID) references DISTRICT(D_W_ID, D_ID)
} __attribute__((packed)) history_t;
// The TPC-C application does not have to be capable of utilizing the
//  increased range of C_ID values beyond 6,000.

typedef struct order_t
{
    int o_id; // 10,000,000 unique IDs
    int8_t o_d_id;  // 20 unique IDs
    int o_w_id;  // 2*W unique IDs
    int o_c_id;  // 96,000 unique IDs
    struct tm* o_entry_d;
    int o_carrier_id; // 10 unique IDs or -1 (denotes null)
    uint8_t o_ol_cnt; // numeric(2); Count of Order-Lines
    uint8_t o_all_local; // numeric(1)
    // primary key (O_W_ID, O_D_ID, O_ID),
    // foreign key (O_W_ID, O_D_ID, O_C_ID) references CUSTOMER(C_W_ID, C_D_ID, C_ID)
} __attribute__((packed)) order_t;

typedef struct neworder_t
{
    int no_o_id;  // 10,000,000 unique IDs
    int8_t no_d_id;  // 20 unique IDs
    int no_w_id;  // 2*W unique IDs
    // primary key (NO_W_ID, NO_D_ID, NO_O_ID),
    // foreign key (NO_W_ID, NO_D_ID, NO_O_ID) references ORDERtable(O_W_ID, O_D_ID, O_ID)
} __attribute__((packed)) neworder_t;

typedef struct item_t
{  // 100,000 items are populated (constant)
    int i_id;  // primary key; 200,000 unique IDs
    int i_im_id;  // 200,000 unique IDs; Image ID associated to Item
    char i_name[25];  // varchar(24)
    float i_price;
    char i_data[51];  // Brand information
} __attribute__((packed)) item_t;

typedef struct stock_t
{  // 100,000 populated per warehouse
    int s_i_id;  // 200,000 unique IDs
    int s_w_id;  // 2*W unique IDs
    int s_quantity;  // numeric(4) signed
    char s_dist[10][25]; // S_DIST_01 char(24), S_DIST_02 char(24), ..., S_DIST_10 char(24)
    uint32_t s_ytd;  // numeric(8)
    uint16_t s_order_cnt;  // numeric(4)
    uint16_t s_remote_cnt;  // numeric(4)
    char s_data[51];  // Make information
    // primary key (S_W_ID, S_I_ID),
    // foreign key (S_W_ID) references WAREHOUSE(W_ID),
    // foreign key (S_I_ID) references ITEM(I_ID)
} __attribute__((packed)) stock_t;

typedef struct orderline_t
{
    int ol_o_id;  // 10,000,000 unique IDs
    int8_t ol_d_id;  // 20 unique IDs
    int ol_w_id;  // 2*W unique IDs
    int8_t ol_number;  // 15 unique IDs
    int ol_i_id;  // 200,000 unique IDs
    int ol_supply_w_id;  // 2*W unique IDs
    struct tm* ol_delivery_d;  // datetime or null
    uint8_t ol_quantity;  // numeric(2)
    float ol_amount;  // numeric(6, 2) signed
    char ol_dist_info[25];  // char(24)
    // primary key (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
    // foreign key (OL_W_ID, OL_D_ID, OL_O_ID) references ORDERtable(O_W_ID, O_D_ID, O_ID),
    // foreign key (OL_SUPPLY_W_ID, OL_I_ID) references STOCK(S_W_ID, S_I_ID)
} __attribute__((packed)) orderline_t;

/////////////////////////////////
// Data Generation & Population
/////////////////////////////////

void initialize_and_permute_random(int* permutation, int n);
int gen_rand_astr(char* str, int lenl, int lenh);
void gen_rand_nstr(char* str, int len);
void gen_rand_lastname(char* lastname, int x);
void gen_rand_datafield(char* data);
void gen_rand_zip(char* zipcode);

void init_db_population(const int n_warehouse);

static inline int Random(int l, int r)  // uniform, inclusive
{
    // ??? How to implement this function? (See the comments below)
    if (r - l > RAND_MAX) return rand() / (double)RAND_MAX * (r-l) + l;
    // Due to the discrete nature of floating-point numbers, this may mean that
    //  some values will just never show up in your output stream.
    // In C++11 we can use the std::default_random_engine library to solve this problem,
    //  but not in C
    else return rand() % (r-l+1) + l;
    // So I use another way (as above) when r - l <= RAND_MAX, but this is also problematic 
    //  because the result is not uniformly distributed when RAND_MAX % (r-l+1) != 0.
    // Maybe we shall choose "the lesser of two evils"
}
static inline struct tm cur_local_time()
{
    time_t cur_time = time(NULL);
    return *localtime(&cur_time);
}
static inline void swap(int* a, int* b) { int t = *a; *a = *b; *b = t; }
static inline int nurand(int A, int x, int y)
// non-uniform random over range [x, y]
// A is a constant chosen according to the size of the range [x .. y]
//   for C_LAST, the range is [0 .. 999] and  A = 255
//   for C_ID, the range is [1 .. 3000] and  A = 1023
//   for OL_I_ID, the range is [1 .. 100000] and A = 8191
{
    // const int a[3] = {255, 1023, 8191}, A = a[aa];

    int C = Random(0, A);
    // C is a run-time constant randomly chosen within [0 .. A] that
    //  can be varied without altering performance. The same C value,
    //  per field (C_LAST, C_ID, OL_I_ID), must be used by all emulated terminals.
    // ??? How to calculate C? Recalculate once per call to NURand(), or just one initial call at the beginning?

    return (((Random(0, A) | Random(x, y)) + C) % (y - x + 1)) + x;
}

////////////////////////
// Database Operations
////////////////////////

extern inline void Insert(tx_trans_t* trans, char* key, void* val);
extern inline void Select(tx_trans_t* trans, char* key, void** val);
extern inline void Delete(tx_trans_t* trans, char* key);

extern const int prikey_len_warehouse;
extern const int prikey_len_district;
extern const int prikey_len_customer;
extern const int prikey_len_item;
extern const int prikey_len_order;
extern const int prikey_len_orderline;
extern const int prikey_len_stock;
extern const int prikey_len_neworder;
extern const int prikey_len_history;
extern const int prikey_len_c2;
extern const int prikey_len_o2;
extern const int prikey_len_no2;

extern inline void Get_prikey_warehouse(char* pri_key, int w_id);
extern inline void Get_prikey_district (char* pri_key, int d_w_id, int d_id);
extern inline void Get_prikey_customer (char* pri_key, int c_w_id, int c_d_id, int c_id);
extern inline void Get_prikey_item     (char* pri_key, int i_id);
extern inline void Get_prikey_order    (char* pri_key, int o_w_id, int o_d_id, int o_id);
extern inline void Get_prikey_orderline(char* pri_key, int ol_w_id, int ol_d_id, int ol_o_id, int ol_number);
extern inline void Get_prikey_stock    (char* pri_key, int s_w_id, int s_i_id);
extern inline void Get_prikey_neworder (char* pri_key, int no_w_id, int no_d_id, int no_o_id);
extern inline void Get_prikey_history  (char* pri_key, int h_w_id, int h_d_id, int h_c_id);
extern inline void Get_prikey_c2       (char* pri_key, int c_w_id, int c_d_id, char* c_last);
extern inline void Get_prikey_o2       (char* pri_key, int o_w_id, int o_d_id, int o_c_id);
extern inline void Get_prikey_no2      (char* pri_key, int no_w_id, int no_d_id);

void Select_warehouse           (tx_trans_t* trans, int w_id, warehouse_t** w);
void Select_district            (tx_trans_t* trans, int d_w_id, int d_id, district_t** d);
void Select_customer            (tx_trans_t* trans, int c_w_id, int c_d_id, int c_id, customer_t** c);
void Select_item                (tx_trans_t* trans, int i_id, item_t** i);
void Select_order               (tx_trans_t* trans, int o_w_id, int o_d_id, int o_id, order_t** o);
void Select_orderline           (tx_trans_t* trans, int ol_w_id, int ol_d_id, int ol_o_id, int ol_number, orderline_t** ol);
void Select_stock               (tx_trans_t* trans, int s_w_id, int s_i_id, stock_t** s);
void Select_neworder            (tx_trans_t* trans, int no_w_id, int no_d_id, int no_o_id, neworder_t** no);
void Select_history             (tx_trans_t* trans, int h_w_id, int h_d_id, int h_c_id, history_t** h);
void Select_customer_byname     (tx_trans_t* trans, int c_w_id, int c_d_id, char* c_last, customer_t** c);
void Select_latest_order        (tx_trans_t* trans, int o_w_id, int o_d_id, int o_c_id, order_t** o);
void Select_undelivered_neworder(tx_trans_t* trans, int no_w_id, int no_d_id, neworder_t** no);

void Insert_warehouse(tx_trans_t* trans, warehouse_t* w);
void Insert_district (tx_trans_t* trans, district_t* d);
void Insert_customer (tx_trans_t* trans, customer_t* c);
void Insert_item     (tx_trans_t* trans, item_t* i);
void Insert_order    (tx_trans_t* trans, order_t* o);
void Insert_orderline(tx_trans_t* trans, orderline_t* ol);
void Insert_stock    (tx_trans_t* trans, stock_t* s);
void Insert_neworder (tx_trans_t* trans, neworder_t* no);
void Insert_history  (tx_trans_t* trans, history_t* h);

void Insert_warehouse_trans(tx_ctx_t* ctx, warehouse_t* w);
void Insert_district_trans (tx_ctx_t* ctx, district_t* d);
void Insert_customer_trans (tx_ctx_t* ctx, customer_t* c);
void Insert_item_trans     (tx_ctx_t* ctx, item_t* i);
void Insert_order_trans    (tx_ctx_t* ctx, order_t* o);
void Insert_orderline_trans(tx_ctx_t* ctx, orderline_t* ol);
void Insert_stock_trans    (tx_ctx_t* ctx, stock_t* s);
void Insert_neworder_trans (tx_ctx_t* ctx, neworder_t* no);
void Insert_history_trans  (tx_ctx_t* ctx, history_t* h);

void Delete_neworder(tx_trans_t* trans, neworder_t* no);
