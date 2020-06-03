//
// Created by akatsarakis on 03/06/2020.
//

// Implementation steps: 1) single thread, 2) multi-threaded, 3) across-node distributed
// Benchmarks/applications: 1) TATP, TPC-* (e.g., TPC-C), smallbank etc.

// use deterministic keys in a key-value interface to create necessary structures for benchmarks
// for performance keep keys short and avoid indirections (i.e., do not get[key1]-->get[key2]-->...-->get[actual_value])
//

#ifndef TX_SHIM_H
#define TX_SHIM_H

#include <stdint.h>

typedef enum
{
    committed,
    failed
    // ...
} tx_trans_result;
const char* tx_trans_result_str[] = { [committed] = "committed", [failed] = "failed"};

typedef enum
{
    successful,
    successfully_buffered,  // equivalent to successful but for trans_* operations which may fail only during commit
    non_existent,           // get or delete of a non-existent key
    err_exceeds_internal_allocated_space,
    err_exceeds_provided_allocated_space,
    err_other
    //...
} tx_op_result;

const char* tx_ox_result_str[] = {
        [successful] = "successful", [successfully_buffered] = "successfully_buffered",
        [non_existent] = "non_existent", [err_other] = "err_other",
        [err_exceeds_internal_allocated_space] = "err_exceeds_internal_allocated_space",
        [err_exceeds_provided_allocated_space] = "err_exceeds_provided_allocated_space"
};

typedef enum
{
    read_only,
    update
} tx_trans_type;

const char* tx_trans_type_str[] = { [read_only] = "read_only", [update] = "update"};


typedef struct
{
    void * ctx;
} tx_ctx_t;


typedef struct
{
    tx_ctx_t *tx_ctx;
    tx_trans_type type;
    void * ctx;
} tx_trans_t;


// object header: type, allocated and current len, lock
typedef struct
{
//    uint8_t lock;
    uint16_t alloc_len;
    uint16_t  curr_len;
} tx_obj_header_t;




///////////////////////////////
/// Functions
///////////////////////////////
tx_ctx_t tx_ctx_init(/*....*/);
void  tx_ctx_destroy(tx_ctx_t *tx_ctx);

tx_trans_t* tx_trans_create(tx_ctx_t *tx_ctx); // update read-only but not known a priory
tx_trans_result tx_trans_commit(tx_trans_t* trans);
void tx_trans_abort_n_clear(tx_trans_t* trans);
void tx_trans_destroy(tx_trans_t* trans);
//void tx_trans_replay(tx_trans_t* trans);

// check if object is already part of our tx
// otherwise copy it from memory or kvs to our temporary trans buff

// Memory object interface
void* tx_alloc_ptr (tx_trans_t* trans, uint32_t len);
tx_op_result tx_free_ptr   (tx_trans_t* trans, void* obj_ptr);
tx_op_result tx_trans_read (tx_trans_t* trans, void* obj_ptr);
tx_op_result tx_trans_write(tx_trans_t* trans, void* obj_ptr, uint32_t obj_len, void* val_ptr);

// KV object interface /
tx_op_result  tx_trans_get(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void* buf_ptr, uint32_t buf_len);
tx_op_result  tx_trans_set(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len);
tx_op_result  tx_trans_del(tx_trans_t* trans, void* key_ptr, uint32_t key_len);

// single-key operations (i.e., non-transactional interface)
tx_op_result tx_single_get(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* buf_ptr, uint32_t buf_len);
tx_op_result tx_single_set(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len);
tx_op_result tx_single_del(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len);




// read-only tx known a priory
static inline tx_trans_t* tx_rd_only_trans_create(tx_ctx_t *tx_ctx){
    tx_trans_t* ret_trans = tx_trans_create(tx_ctx);
    ret_trans->type = read_only;
    return ret_trans;
}

//////// TODO some unit tests

//// TODO Test w/ traces?? port logic from ccKVS/Hermes' if useful to emulate any of the benchmarks



//////// Implementation
tx_op_result tx_single_get(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* buf_ptr, uint32_t buf_len);
tx_op_result tx_single_set(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len);
tx_op_result tx_single_del(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len);

#endif //UNTITLED_TX_SHIM_H

