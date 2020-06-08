//
// Created by akatsarakis on 08/06/2020.
//

#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include <stdio.h>
#include "tx_shim.h"


// Check if KV/object is already part of our tx
// otherwise copy it from memory or kvs to our temporary trans buff expose the copy to the app

//////////////////////////////////////////////////////////////////////////
/// Transactional API
//////////////////////////////////////////////////////////////////////////

// returns -1 if not in tx or obj idx of obj_id array if found
static int __tx_trans_obj_in_tx(tx_trans_t* trans, void* obj_ptr) {
    for(int i = 0; i < trans->curr_num_objs_in_tx; ++i){
        if(!trans->obj_ids[i].is_mem) { continue; }
        if(trans->obj_ids[i].obj_ptr == obj_ptr) {
            assert(trans->obj_ids[i].type != DELETED && trans->obj_ids[i].type != TO_DELETE);
            return i;
        }
    }
    return -1;
}

static int __tx_trans_add_obj(tx_trans_t* trans, void* obj_ptr, uint8_t type, uint32_t upd_len, uint8_t is_blind_upd) {
    assert(__tx_trans_obj_in_tx(trans, obj_ptr) == -1);
    assert(trans->curr_num_objs_in_tx < MAX_OBJ_IN_TX);
    assert(type != UPDATE || upd_len > 0 || is_blind_upd);
    assert(type == READ || type == UPDATE || type == TO_DELETE);


    tx_bufed_obj_id* tx_id_position = &trans->obj_ids[trans->curr_num_objs_in_tx];

    tx_id_position->is_mem = 1;
    tx_id_position->type = type;
    tx_id_position->obj_ptr = obj_ptr;
    tx_id_position->existed_prior_tx = 1;

    // Copy the value
    tx_max_internal_obj_val_t* tx_val_position = &trans->obj_vals[trans->curr_num_objs_in_tx];
    tx_internal_obj_val_t* int_obj_ptr = __obj_ptr_2_internal_obj_ptr(obj_ptr);
    uint32_t curr_len = int_obj_ptr->hdr.curr_len;

    if(type == TO_DELETE || (type == UPDATE && (is_blind_upd || upd_len >= curr_len))){
        // for pre-tx alloc values or updates that are either blind (change curr length) or try to write equal or higher
        // number of bytes that already exist we copy only the header as an optimization
        memcpy(tx_val_position, int_obj_ptr, sizeof(tx_internal_obj_val_t));
    }else{
        // copy of header + current length of object
        memcpy(tx_val_position, int_obj_ptr, INT_OBJ_LEN(int_obj_ptr->hdr.curr_len));
    }

    return trans->curr_num_objs_in_tx++;
}

//////////////////////////////////////////////////////////////////////////

void* tx_trans_obj_alloc(tx_trans_t* trans, uint32_t obj_len)
{

    void* ret_ptr = tx_single_obj_alloc(trans->parent, obj_len, trans->tx_id);

    tx_bufed_obj_id* tx_id_position = &trans->obj_ids[trans->curr_num_objs_in_tx];
    tx_id_position->is_mem = 1;
    tx_id_position->type = ALLOCATE;
    tx_id_position->obj_ptr = ret_ptr;
    tx_id_position->existed_prior_tx = 0;

    tx_max_internal_obj_val_t* tx_val_position = &trans->obj_vals[trans->curr_num_objs_in_tx];
    tx_val_position->hdr = ((tx_internal_obj_val_t*) ret_ptr)->hdr;

    __tx_trans_state_update(trans, ALLOCATE);

    trans->curr_num_objs_in_tx++;
    return ret_ptr;
}

// If an object is found opened by tx and in DELETE or NOOP state then error
void tx_trans_obj_free(tx_trans_t* trans, void* obj_ptr)
{
    assert(obj_ptr != NULL);

    int obj_id_idx = __tx_trans_obj_in_tx(trans, obj_ptr);

    if(obj_id_idx >= 0){ // object was in tx

        if(trans->obj_ids[obj_id_idx].existed_prior_tx) {
            trans->obj_ids[obj_id_idx].type = TO_DELETE;
        }else{
            trans->obj_ids[obj_id_idx].type = DELETED;
            tx_single_obj_free(trans->parent, obj_ptr);
        }

    }else{ // object was NOT in tx
        __tx_trans_add_obj(trans, obj_ptr, TO_DELETE, 0, 0);
    }

    __tx_trans_state_update(trans, TO_DELETE); // Note that passing either TO_DELETE or DELETED is same

}

void* tx_trans_obj_read(tx_trans_t* trans, void* obj_ptr)
{
    assert(obj_ptr != NULL);

    int obj_id_idx = __tx_trans_obj_in_tx(trans, obj_ptr);

    if(obj_id_idx < 0) { // if object was not in tx
        obj_id_idx = __tx_trans_add_obj(trans, obj_ptr, READ, 0, 0);
    }

    return __internal_obj_ptr_2_obj_ptr((tx_internal_obj_val_t *) &trans->obj_vals[obj_id_idx]);
}

// TODO may add support for starting an update with padding i.e., avoid updating X first bytes
void tx_trans_obj_write(tx_trans_t* trans, void* obj_ptr, uint8_t* val_ptr, uint32_t upd_len, uint8_t is_blind)
{
    assert(obj_ptr != NULL && val_ptr != NULL);
    assert(upd_len <= MAX_VAL_LEN);

    int obj_id_idx = __tx_trans_obj_in_tx(trans, obj_ptr);

    if(obj_id_idx >= 0) { // object was in tx
        assert(trans->obj_ids[obj_id_idx].type != DELETED &&
               trans->obj_ids[obj_id_idx].type != TO_DELETE);
        trans->obj_ids[obj_id_idx].type = UPDATE;

    }else { // object was NOT in tx
        obj_id_idx = __tx_trans_add_obj(trans, obj_ptr, UPDATE, upd_len, is_blind);
    }

    assert(upd_len <= trans->obj_vals[obj_id_idx].hdr.alloc_len);
    memcpy(trans->obj_vals[obj_id_idx].val, val_ptr, upd_len);
    if(is_blind || upd_len > trans->obj_vals[obj_id_idx].hdr.curr_len){
        trans->obj_vals[obj_id_idx].hdr.curr_len = upd_len;
    }

    __tx_trans_state_update(trans, UPDATE);
}






//////////////////////////////////////////////////////////////////////////
/// KV API
//////////////////////////////////////////////////////////////////////////


// returns -1 if not in tx or obj idx of obj_id array if found
static int __tx_trans_kv_in_tx(tx_trans_t* trans, uint8_t* key_ptr, uint16_t key_len)
{
    for(int i = 0; i < trans->curr_num_objs_in_tx; ++i){
        if(trans->obj_ids[i].is_mem) { continue; }
        if(trans->obj_ids[i].kv.key_len != key_len) { continue; }
        if(memcmp(trans->obj_ids[i].kv.key, key_ptr, key_len) == 0) { return i; }
    }
    return -1;
}

static int __tx_trans_add_kv(tx_trans_t* trans, uint8_t* key_ptr, uint16_t key_len, uint8_t type)
{
    assert(__tx_trans_kv_in_tx(trans, key_ptr, key_len) == -1);
    assert(trans->curr_num_objs_in_tx < MAX_OBJ_IN_TX);
    assert(type == READ || type == UPDATE || type == TO_DELETE);

    tx_bufed_obj_id* tx_id_position = &trans->obj_ids[trans->curr_num_objs_in_tx];

    tx_id_position->is_mem = 0;
    tx_id_position->type = type;
    tx_id_position->obj_ptr = NULL;
    tx_id_position->existed_prior_tx = 1; // Being optimistic
    tx_id_position->kv.key_len = key_len;
    memcpy(&tx_id_position->kv.key, key_ptr, key_len);

    // Copy the value to tx buf
    tx_max_internal_obj_val_t* tx_val_position = &trans->obj_vals[trans->curr_num_objs_in_tx];
    int length = __get(trans->parent, key_ptr, key_len, tx_val_position, INT_OBJ_LEN(MAX_KEY_LEN));


    if(length < 0){  // Key not found
        tx_id_position->existed_prior_tx = 0;
        if(type == TO_DELETE){
            tx_id_position->type = DELETED;
        }

        // we need to fill the val header as well
        tx_val_position->hdr.lock = 0;
        tx_val_position->hdr.version = 0;
        tx_val_position->hdr.curr_len = 0;
        tx_val_position->hdr.alloc_len = 0;
        tx_val_position->hdr.unique_alloc_id = trans->tx_id;

    }

    return trans->curr_num_objs_in_tx++;
}

//////////////////////////////////////////////////////////////////////////

// returns value length (-1 if not found) and value_ptr (NULL if not found)
int tx_trans_kv_get(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void** value_ptr)
{
    assert(key_ptr != NULL);

    int obj_id_idx = __tx_trans_kv_in_tx(trans, key_ptr, key_len);

    if(obj_id_idx < 0) { // if object was not in tx
        obj_id_idx = __tx_trans_add_kv(trans, key_ptr, key_len, READ);
    }

    tx_bufed_obj_id* tx_id_position = &trans->obj_ids[obj_id_idx];
    if(!tx_id_position->existed_prior_tx &&
       (tx_id_position->type == READ || tx_id_position->type == TO_DELETE))
    {
        *value_ptr = NULL;
        return -1;
    }

    *value_ptr = __internal_obj_ptr_2_obj_ptr((tx_internal_obj_val_t *) &trans->obj_vals[obj_id_idx]);
    return trans->obj_vals[obj_id_idx].hdr.curr_len;
}

void tx_trans_kv_set(tx_trans_t* trans, void* key_ptr, uint32_t key_len, void* val_ptr, uint32_t val_len)
{
    assert(key_ptr != NULL && val_ptr != NULL);
    assert(val_len <= MAX_VAL_LEN);

    int obj_id_idx = __tx_trans_kv_in_tx(trans, key_ptr, key_len);

    if(obj_id_idx >= 0) { // object was in tx
        assert(trans->obj_ids[obj_id_idx].type != DELETED &&
               trans->obj_ids[obj_id_idx].type != TO_DELETE);
        trans->obj_ids[obj_id_idx].type = UPDATE;

    }else { // object was NOT in tx
        obj_id_idx = __tx_trans_add_kv(trans, key_ptr, key_len, UPDATE);
    }

    // Always treated as overwriting the whole value
    memcpy(trans->obj_vals[obj_id_idx].val, val_ptr, val_len);
    trans->obj_vals[obj_id_idx].hdr.curr_len = val_len;
    trans->obj_vals[obj_id_idx].hdr.alloc_len = val_len;

    __tx_trans_state_update(trans, UPDATE);
}

// If an object is found opened by tx and in DELETE or NOOP state then error
int tx_trans_kv_del(tx_trans_t* trans, void* key_ptr, uint32_t key_len)
{
    assert(key_ptr != NULL);

    int obj_id_idx = __tx_trans_kv_in_tx(trans, key_ptr, key_len);

    if(obj_id_idx >= 0){ // object was in tx

        if(trans->obj_ids[obj_id_idx].existed_prior_tx) {
            trans->obj_ids[obj_id_idx].type = TO_DELETE;
        }else{
            trans->obj_ids[obj_id_idx].type = DELETED;
        }

    }else{ // object was NOT in tx
        __tx_trans_add_kv(trans, key_ptr, key_len, TO_DELETE);
    }

    __tx_trans_state_update(trans, TO_DELETE); // Note passing either TO_DELETE or DELETED is same
}




