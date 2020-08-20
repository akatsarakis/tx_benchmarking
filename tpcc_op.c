#include "tx_shim.h"
#include "tpcc.h"
#include <stdio.h>
#define new(T) malloc(sizeof(T))

const int prikey_len_warehouse = 11;  // With 1 bit padding
const int prikey_len_district  = 14;
const int prikey_len_customer  = 19;
const int prikey_len_item      = 8;
const int prikey_len_order     = 19;
const int prikey_len_orderline = 23;
const int prikey_len_stock     = 19;
const int prikey_len_neworder  = 20;
const int prikey_len_history   = 19;
const int prikey_len_c2        = 31;
const int prikey_len_o2        = 20;
const int prikey_len_no2       = 16;

inline void Insert(tx_trans_t* trans, char* key, void* val)
{
    tx_trans_kv_set(trans, key, strlen(key), val, sizeof(val));
}
inline void Select(tx_trans_t* trans, char* key, void** val)
{
    tx_trans_kv_get(trans, key, strlen(key), val);
}
inline void Delete(tx_trans_t* trans, char* key)
{
    tx_trans_kv_del(trans, key, sizeof(key));
}

inline void Get_prikey_warehouse(char* pri_key, int w_id)
{
    sprintf(pri_key, "w%d", w_id);
}
inline void Get_prikey_district(char* pri_key, int d_w_id, int d_id)
{
    sprintf(pri_key, "d%d,%d", d_w_id, d_id);
}
inline void Get_prikey_customer(char* pri_key, int c_w_id, int c_d_id, int c_id)
{
    sprintf(pri_key, "c%d,%d,%d", c_w_id, c_d_id, c_id);
}
inline void Get_prikey_item(char* pri_key, int i_id)
{
    sprintf(pri_key, "i%d", i_id);
}
inline void Get_prikey_order(char* pri_key, int o_w_id, int o_d_id, int o_id)
{
    sprintf(pri_key, "o%d,%d,%d", o_w_id, o_d_id, o_id);
}
inline void Get_prikey_orderline(char* pri_key, int ol_w_id, int ol_d_id, int ol_o_id, int ol_number)
{
    sprintf(pri_key, "ol%d,%d,%d,%d", ol_w_id, ol_d_id, ol_o_id, ol_number);
}
inline void Get_prikey_stock(char* pri_key, int s_w_id, int s_i_id)
{
    sprintf(pri_key, "s%d,%d", s_w_id, s_i_id);
}
inline void Get_prikey_neworder(char* pri_key, int no_w_id, int no_d_id, int no_o_id)
{
    sprintf(pri_key, "no%d,%d,%d", no_w_id, no_d_id, no_o_id);
}
inline void Get_prikey_history(char* pri_key, int h_w_id, int h_d_id, int h_c_id)
{
    sprintf(pri_key, "h%d,%d,%d", h_w_id, h_d_id, h_c_id);
}
inline void Get_prikey_c2(char* pri_key, int c_w_id, int c_d_id, char* c_last)
{
    sprintf(pri_key, "c2%d,%d,%s", c_w_id, c_d_id, c_last);
}
inline void Get_prikey_o2(char* pri_key, int o_w_id, int o_d_id, int o_c_id)
{
    sprintf(pri_key, "o2%d,%d,%d", o_w_id, o_d_id, o_c_id);
}
inline void Get_prikey_no2(char* pri_key, int no_w_id, int no_d_id)
{
    sprintf(pri_key, "no2%d,%d", no_w_id, no_d_id);
}

void Select_warehouse(tx_trans_t* trans, int w_id, warehouse_t** w)
{
    char w_pri_key[prikey_len_warehouse];
    Get_prikey_warehouse(w_pri_key, w_id);
    Select(trans, w_pri_key, (void**)w);
}
void Select_district(tx_trans_t* trans, int d_w_id, int d_id, district_t** d)
{
    char d_pri_key[prikey_len_district];
    Get_prikey_district(d_pri_key, d_w_id, d_id);
    Select(trans, d_pri_key, (void**)d);
}
void Select_customer(tx_trans_t* trans, int c_w_id, int c_d_id, int c_id, customer_t** c)
{
    char c_pri_key[prikey_len_customer];
    Get_prikey_customer(c_pri_key, c_w_id, c_d_id, c_id);
    Select(trans, c_pri_key, (void**)c);
}
void Select_item(tx_trans_t* trans, int i_id, item_t** i)
{
    char i_pri_key[prikey_len_item];
    Get_prikey_item(i_pri_key, i_id);
    Select(trans, i_pri_key, (void**)i);
}
void Select_order(tx_trans_t* trans, int o_w_id, int o_d_id, int o_id, order_t** o)
{
    char o_pri_key[prikey_len_order];
    Get_prikey_order(o_pri_key, o_w_id, o_d_id, o_id);
    Select(trans, o_pri_key, (void**)o);
}
void Select_orderline(tx_trans_t* trans, int ol_w_id, int ol_d_id, int ol_o_id, int ol_number, orderline_t** ol)
{
    char ol_pri_key[prikey_len_orderline];
    Get_prikey_orderline(ol_pri_key, ol_w_id, ol_d_id, ol_o_id, ol_number);
    Select(trans, ol_pri_key, (void**)ol);
}
void Select_stock(tx_trans_t* trans, int s_w_id, int s_i_id, stock_t** s)
{
    char s_pri_key[prikey_len_stock];
    Get_prikey_stock(s_pri_key, s_w_id, s_i_id);
    Select(trans, s_pri_key, (void**)s);
}
void Select_neworder(tx_trans_t* trans, int no_w_id, int no_d_id, int no_o_id, neworder_t** no)
{
    char no_pri_key[prikey_len_neworder];
    Get_prikey_neworder(no_pri_key, no_w_id, no_d_id, no_o_id);
    Select(trans, no_pri_key, (void**)no);
}
void Select_history(tx_trans_t* trans, int h_w_id, int h_d_id, int h_c_id, history_t** h)
{
    char h_pri_key[prikey_len_history];
    Get_prikey_history(h_pri_key, h_w_id, h_d_id, h_c_id);
    Select(trans, h_pri_key, (void**)h);
}
void Select_customer_byname(tx_trans_t* trans, int c_w_id, int c_d_id, char* c_last, customer_t** c)
{
    int* c_id;
    char c2_key[prikey_len_c2];
    Get_prikey_c2(c2_key, c_w_id, c_d_id, c_last);
    Select(trans, c2_key, &c_id);
    Select_customer(trans, c_w_id, c_d_id, *c_id, c);
}
void Select_latest_order(tx_trans_t* trans, int o_w_id, int o_d_id, int o_c_id, order_t** o)
{
    int* largest_o_id;
    char o2_key[prikey_len_o2];
    Get_prikey_o2(o2_key, o_w_id, o_d_id, o_c_id);
    Select(trans, o2_key, &largest_o_id);
    Select_order(trans, o_w_id, o_d_id, *largest_o_id, o);
}
void Select_undelivered_neworder(tx_trans_t* trans, int no_w_id, int no_d_id, neworder_t** no)
{
    int* min_no_o_id;
    char no2_key[prikey_len_no2];
    Get_prikey_no2(no2_key, no_w_id, no_d_id);
    Select(trans, no2_key, &min_no_o_id);
    Select_neworder(trans, no_w_id, no_d_id, *min_no_o_id, no);
}

// Also used as update (temporarily)
void Insert_warehouse(tx_trans_t* trans, warehouse_t* w)
{
    char w_pri_key[prikey_len_warehouse];
    Get_prikey_warehouse(w_pri_key, w->w_id);
    Insert(trans, w_pri_key, w);
}
void Insert_district(tx_trans_t* trans, district_t* d)
{
    char d_pri_key[prikey_len_district];
    Get_prikey_district(d_pri_key, d->d_w_id, d->d_id);
    Insert(trans, d_pri_key, d);
}
void Insert_customer(tx_trans_t* trans, customer_t* c)
{
    char c_pri_key[prikey_len_customer];
    Get_prikey_customer(c_pri_key, c->c_w_id, c->c_d_id, c->c_id);
    Insert(trans, c_pri_key, c);

    // maintain aux table (TODO: use a data structure to maintain "mid-position" customer)
    char c2_key[prikey_len_c2];
    Get_prikey_c2(c2_key, c->c_w_id, c->c_d_id, c->c_last);
    Insert(trans, c2_key, c);
}
void Insert_item(tx_trans_t* trans, item_t* i)
{
    char i_pri_key[prikey_len_item];
    Get_prikey_item(i_pri_key, i->i_id);
    Insert(trans, i_pri_key, i);
}
void Insert_order(tx_trans_t* trans, order_t* o)
{
    char o_pri_key[prikey_len_order];
    Get_prikey_order(o_pri_key, o->o_w_id, o->o_d_id, o->o_id);
    Insert(trans, o_pri_key, o);

    // aux table
    char o2_key[prikey_len_o2];
    Get_prikey_o2(o2_key, o->o_w_id, o->o_d_id, o->o_c_id);
    int* o2_o_id = new(int);  *o2_o_id = o->o_id;
    Insert(trans, o2_key, o2_o_id);  // must be the new largest o_id
}
void Insert_orderline(tx_trans_t* trans, orderline_t* ol)
{
    char ol_pri_key[prikey_len_orderline];
    Get_prikey_orderline(ol_pri_key, ol->ol_w_id, ol->ol_d_id, ol->ol_o_id, ol->ol_number);
    Insert(trans, ol_pri_key, ol);
}
void Insert_stock(tx_trans_t* trans, stock_t* s)
{
    char s_pri_key[prikey_len_stock];
    Get_prikey_stock(s_pri_key, s->s_w_id, s->s_i_id);
    Insert(trans, s_pri_key, s);
}
void Insert_neworder(tx_trans_t* trans, neworder_t* no)
{
    char no_pri_key[prikey_len_neworder];
    Get_prikey_neworder(no_pri_key, no->no_w_id, no->no_d_id, no->no_o_id);
    Insert(trans, no_pri_key, no);

    // aux table
    char no2_key[prikey_len_no2];
    Get_prikey_no2(no2_key, no->no_w_id, no->no_d_id);

    neworder_t* no;
    Select(trans, no2_key, &no);
    if (no != NULL) return;
    // Orders and neworders must come with increasing o_id,
    //  so this neworder must not be the min neworder in this (w_id, d_id)
    
    int* no2_o_id = new(int); *no2_o_id = no->no_o_id;
    Insert(trans, no2_key, no2_o_id);
}
void Insert_history(tx_trans_t* trans, history_t* h)
{
    char h_pri_key[prikey_len_history];  // In fact, history_t doesn't have a primary key (??? Then how to insert it?)
    Get_prikey_history(h_pri_key, h->h_w_id, h->h_d_id, h->h_c_id);
    Insert(trans, h_pri_key, h);
}

void Insert_warehouse_trans(tx_ctx_t* ctx, warehouse_t* w)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_warehouse(trans, w);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_district_trans(tx_ctx_t* ctx, district_t* d)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_district(trans, d);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_customer_trans(tx_ctx_t* ctx, customer_t* c)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_customer(trans, c);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_item_trans(tx_ctx_t* ctx, item_t* i)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_item(trans, i);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_order_trans(tx_ctx_t* ctx, order_t* o)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_order(trans, o);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_orderline_trans(tx_ctx_t* ctx, orderline_t* ol)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_orderline(trans, ol);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_stock_trans(tx_ctx_t* ctx, stock_t* s)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_stock(trans, s);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_neworder_trans(tx_ctx_t* ctx, neworder_t* no)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_neworder(trans, no);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}
void Insert_history_trans(tx_ctx_t* ctx, history_t* h)
{
    tx_trans_t* trans = tx_trans_create(ctx);  tx_trans_init(ctx, trans);
    Insert_history(trans, h);
    tx_trans_commit(trans);  tx_trans_destroy(trans);
}

void Delete_neworder(tx_trans_t* trans, neworder_t* no)
{
    char no_pri_key[prikey_len_neworder];
    Get_prikey_neworder(no_pri_key, no->no_w_id, no->no_d_id, no->no_o_id);
    Delete(trans, no_pri_key);
}
