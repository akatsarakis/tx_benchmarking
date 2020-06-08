//
// Created by akatsarakis on 03/06/2020.
//

/// Implementation steps: 1) single thread, 2) multi-threaded, 3) across-node distributed
/// Benchmarks/applications: 1) TATP, TPC-* (e.g., TPC-C), smallbank etc.

/// use deterministic keys in a key-value interface to create necessary structures for benchmarks
/// for performance keep keys short and avoid indirections (i.e., do not get[key1]-->get[key2]-->...-->__get[actual_value])
//

//// TODO Benchmarks
/// 1. TATP
/// 2. TPC-C
///     -- Check if TPC-C can be implemented without B-Trees (see RIFL paper)
///     -- Check FaRM's port any if any other implementation exists on github
/// 3. Test w/ traces?? port logic from ccKVS/Hermes' if useful to emulate any of the benchmarks

//// TODO Backends
/// 0. Port to multi-threaded transactional (e.e., simply with per-key versions and locks)
/// 1. Port to distributed setting (rVNF/Zeus)
/// 2. Port to traditional available (fault-tolerant via replication) Atomic commit protocols
///     -- FASST / FaRM
///     -- SLOG
///     -- DynaStar
/// 3. Port to traditional non-replicated Atomic commit protocols (optional!!)


/// TODO Stats
/// 0. Stats (runtime and logged) for throughput and latency of all and individual operations

#ifndef TX_SHIM_H
#define TX_SHIM_H

#include <stdint.h>

/////////////////////////
/// Enums
////////////////////////
typedef enum
{
    committed,
    failed
    // ...
} tx_trans_result;


typedef enum
{
    successful,
    successfully_buffered,  // equivalent to successful but for trans_* operations which may fail only during commit
    non_existent,           // __get or delete of a non-existent key
    err_exceeds_internal_allocated_space,
    err_exceeds_provided_allocated_space,
    err_other
    //...
} tx_op_result;


typedef enum
{
    TX_FREE = 0,
    TX_DYN_READ_ONLY,
    TX_READ_ONLY,
    TX_UPDATE
} tx_trans_state;



// READ     --> must be upgraded for UPDATE | DELETE
// UPDATE   --> DELETE
// ALLOCATE --> has full access | can become a NOOP if followed by a DELETE
// DELETE   --> assumption: it can be upgraded to nothing in the same TX
typedef enum
{
    ALLOCATE,
    READ,      // check if version and tx_id that allocated object are same on commit
    UPDATE,    // READ + update and inc version (i.e., if obj exists on commit it does not necessarily fail)
    TO_DELETE,
    DELETED    // NOOP // ALLOC --> DELETE in same tx
} tx_op_type_t;


/////////////////////////
/// Enum to str literals
////////////////////////
const char* tx_trans_type_str  [] = { [TX_READ_ONLY] = "TX_READ_ONLY", [TX_UPDATE] = "TX_UPDATE"};
const char* tx_trans_result_str[] = { [committed] = "committed", [failed] = "failed"};
const char* tx_op_type_str     [] = { [ALLOCATE] = "ALLOCATE", [READ] = "READ",
                                      [UPDATE] = "UPDATE", [TO_DELETE] = "TO_DELETE",
                                      [DELETED] = "DELETED"};
const char* tx_op_result_str   [] = { [successful] = "successful", [successfully_buffered] = "successfully_buffered",
                                      [non_existent] = "non_existent", [err_other] = "err_other",
                                      [err_exceeds_internal_allocated_space] = "err_exceeds_internal_allocated_space",
                                      [err_exceeds_provided_allocated_space] = "err_exceeds_provided_allocated_space" };


///////////////////////////////
/// TX / Trans related Structs
///////////////////////////////

#define MAX_KEY_LEN 64 // in bytes
#define MAX_VAL_LEN 4096
#define MAX_OBJ_IN_TX 64
#define MAX_CONCUR_TX 16

// object/kv header: state, allocated and current len, lock
typedef struct
{
    uint8_t   lock;     // TODO add proper locking (e.g., seqlocks)
    uint16_t  curr_len; // w/o the object header
    uint16_t alloc_len; // w/o the object header
    uint32_t version;
    uint32_t unique_alloc_id; // e.g., unique transaction id that allocates the object
} __attribute__((packed)) tx_header_t;


// Both object and kv items start with a header followed by their val
typedef struct
{
    tx_header_t hdr; // This is __set internally by the backend
    uint8_t val[];
} __attribute__((packed)) tx_internal_obj_val_t;
#define INT_OBJ_LEN(obj_len) ((obj_len) + sizeof(tx_internal_obj_val_t))


// Same as tx_internal_obj_val but with MAX_VAL_LEN for VAL
// used to statically allocate tx temporary buffs for transactions
typedef struct
{
    tx_header_t hdr;
    uint8_t val[MAX_VAL_LEN];
} __attribute__((packed)) tx_max_internal_obj_val_t;



// ID struct that uniquelly identifies (+ some meta) a bufed object/kv item opened by a tx
typedef struct
{
    uint8_t   is_mem;
    uint8_t   existed_prior_tx; // if obj exists on commit it fails (for kv | obj cannot be allocated by others!)
    tx_op_type_t type;
    union {
        void *obj_ptr;
        struct {
            uint16_t key_len;
            uint8_t key[MAX_KEY_LEN];
        } kv;
    };
} __attribute__((packed)) tx_bufed_obj_id;


struct _tx_ctx_t;

// transaction state
typedef struct
{
    struct _tx_ctx_t*    parent;
    tx_trans_state        state;
    uint32_t              tx_id; // unique transaction id
    uint16_t                      curr_num_objs_in_tx; // <= MAX_OBJ_IN_TX
    tx_bufed_obj_id           obj_ids[MAX_OBJ_IN_TX];
    tx_max_internal_obj_val_t obj_vals[MAX_OBJ_IN_TX];
} tx_trans_t;

// Main context struct
typedef struct _tx_ctx_t {
    tx_trans_t trans_arr[MAX_CONCUR_TX]; // Transaction structure
    uint32_t tx_ids;
    // TODO
    // KVS metadata
    // Stats
} tx_ctx_t;


///////////////////////////////
/// Functions
///////////////////////////////
void tx_ctx_init(tx_ctx_t* tx_ctx /*....*/);
void tx_ctx_destroy(tx_ctx_t *tx_ctx);

void tx_trans_init(tx_ctx_t *tx_ctx, tx_trans_t* trans);
tx_trans_t* tx_trans_create(tx_ctx_t *tx_ctx); // update read-only but not known a priory
tx_trans_t* tx_rd_only_trans_create(tx_ctx_t *tx_ctx); // read-only known a priory
tx_trans_result tx_trans_commit(tx_trans_t* trans);
void tx_trans_abort_n_clear(tx_trans_t* trans);
void tx_trans_destroy(tx_trans_t* trans);

void __tx_trans_state_update(tx_trans_t* trans, uint8_t type);



// trans_* read / write / get / put --> copies the current header + value to tx's buffer if item does not exists
//                                 there already and updates or returns a ptr to the copied value
// single_* read / write / get / put --> Updates or copies the current value directly to memory / KVS / specified return buf

///////////////////////
/// Transactional API
///////////////////////

// Since TX reads and gets return values immutable by others (i.e.,  copied to tmp tx space)
// they can actually provide a direct pointer to the obj value

/// Memory object interface
void* tx_trans_obj_alloc(tx_trans_t* trans, uint32_t len);
void  tx_trans_obj_free (tx_trans_t* trans, void* obj_ptr);
void* tx_trans_obj_read (tx_trans_t* trans, void* obj_ptr);
void  tx_trans_obj_write(tx_trans_t* trans, void* obj_ptr, uint8_t* val_ptr, uint32_t upd_len, uint8_t is_blind);

//tx_op_result tx_trans_obj_free (tx_trans_t* trans, void* obj_ptr);
//tx_op_result tx_trans_obj_read (tx_trans_t* trans, void* obj_ptr, void* ret_buf, uint32_t bytes_to_read);
//tx_op_result tx_trans_obj_write(tx_trans_t* trans, void* obj_ptr, void* val_ptr, uint32_t bytes_to_write);


/// KV object interface
int  tx_trans_kv_del(tx_trans_t* trans, void* key_ptr, uint32_t key_len);
int  tx_trans_kv_get(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void** value_ptr);
void tx_trans_kv_set(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len);

//tx_op_result tx_trans_kv_get(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void* buf_ptr, uint32_t buf_len);
//tx_op_result tx_trans_kv_set(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len);
//tx_op_result tx_trans_kv_del(tx_trans_t* trans, void* key_ptr, uint32_t key_len);




///////////////////////
/// Single-key operations (i.e., non-transactional interface --  Mostly an optimization for single-key ops)
///////////////////////
// Memory object interface
void*        tx_single_obj_alloc(tx_ctx_t *tx_ctx, uint32_t max_obj_len, uint32_t unique_alloc_id);
tx_op_result tx_single_obj_free (tx_ctx_t *tx_ctx, void* obj_ptr);
tx_op_result tx_single_obj_read (tx_ctx_t *tx_ctx, void* obj_ptr, void* ret_buf, uint32_t bytes_to_read);
tx_op_result tx_single_obj_write(tx_ctx_t *tx_ctx, void* obj_ptr, void* val_ptr, uint32_t bytes_to_write);

// Memory object interface
tx_op_result tx_single_kv_get(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* buf_ptr, uint32_t buf_len);
tx_op_result tx_single_kv_set(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len);
tx_op_result tx_single_kv_del(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len);





/// Supposedly represent the get/set/del of a third-party KVS
int __del(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len);
int __set(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len);
int __get(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len, void* buf_ptr, uint32_t buf_len); // returns length of object or < 0 if object does not exists






// Lock-free read based on version % 2 == 0 and didn't change while memcpying
/// WARNING: nested LOCK_FREE_READS are not supported !
#define LOCK_FREE_READ_BEGIN() \
    uint32_t __prev_ver, __after_ver; \
    do{ \
    __prev_ver = int_obj_ptr->hdr.version++;

#define LOCK_FREE_READ_END() \
    __after_ver = int_obj_ptr->hdr.version++; \
    }while (__prev_ver != __after_ver || __prev_ver % 2);

// translates seqlock_version to actual object version for transaction commit
#define GET_OBJ_VERSION(int_obj_ptr) ((int_obj_ptr)->hdr.version / 2)





static inline void* __internal_obj_ptr_2_obj_ptr(tx_internal_obj_val_t * obj_ptr){
    return obj_ptr->val;
}

static inline tx_internal_obj_val_t* __obj_ptr_2_internal_obj_ptr(void* obj_ptr){
    return (tx_internal_obj_val_t *) (((uint8_t *) obj_ptr) - sizeof(tx_header_t));
}



#endif //UNTITLED_TX_SHIM_H

