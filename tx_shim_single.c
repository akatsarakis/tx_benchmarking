//
// Created by akatsarakis on 08/06/2020.
//

#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include "tx_shim.h"



///////////////////////////////////////////////////////
//////// Single object Implementation
///////////////////////////////////////////////////////

void* tx_single_obj_alloc(tx_ctx_t *tx_ctx, uint32_t max_obj_len, uint32_t unique_alloc_id)
{
    tx_internal_obj_val_t* obj_ptr = malloc(INT_OBJ_LEN(max_obj_len));
    obj_ptr->hdr.lock = 0;
    obj_ptr->hdr.version = 0;
    obj_ptr->hdr.curr_len = 0;
    obj_ptr->hdr.alloc_len = max_obj_len;
    obj_ptr->hdr.unique_alloc_id = unique_alloc_id;
    return __internal_obj_ptr_2_obj_ptr(obj_ptr);
}


tx_op_result tx_single_obj_free(tx_ctx_t *tx_ctx, void* obj_ptr)
{
    tx_internal_obj_val_t* int_obj_ptr = __obj_ptr_2_internal_obj_ptr(obj_ptr);
    free(int_obj_ptr);
}


tx_op_result tx_single_obj_read(tx_ctx_t *tx_ctx, void* obj_ptr, void* ret_buf, uint32_t bytes_to_read)
{
    tx_internal_obj_val_t* int_obj_ptr = __obj_ptr_2_internal_obj_ptr(obj_ptr);
    // Lock
    LOCK_FREE_READ_BEGIN();
    assert(bytes_to_read <= int_obj_ptr->hdr.curr_len);
    memcpy(ret_buf, int_obj_ptr->val, bytes_to_read);
    LOCK_FREE_READ_END();
    // Unlock
}


tx_op_result tx_single_obj_write(tx_ctx_t *tx_ctx, void* obj_ptr, void* val_ptr, uint32_t bytes_to_write)
{
    tx_internal_obj_val_t* int_obj_ptr = __obj_ptr_2_internal_obj_ptr(obj_ptr);
    // Lock
    int_obj_ptr->hdr.version++;

    assert(bytes_to_write <= int_obj_ptr->hdr.alloc_len);
    memcpy(int_obj_ptr->val, val_ptr, bytes_to_write);
    int_obj_ptr->hdr.curr_len = bytes_to_write;

    int_obj_ptr->hdr.version++;
    // Unlock
}



///////////////////////////////////////////////////////
//////// Single KV Implementation
///////////////////////////////////////////////////////

tx_op_result tx_single_get(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len,
                           tx_internal_obj_val_t *ret_obj, uint32_t buf_len)
{
    // TODO locking might be required if the same object can be accessed by the TX API
    /// I.E. May need to retry if ret_obj->hdr.version % 2 == 1; (seqlocks)
    __get(tx_ctx, key_ptr, key_len, ret_obj, INT_OBJ_LEN(ret_obj->hdr.alloc_len));
}

tx_op_result tx_single_set(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len,
                           tx_internal_obj_val_t *val_obj, uint32_t val_len)
{
    // TODO locking might be required if the same object can be accessed by the TX API
    uint8_t __tmp_buff[INT_OBJ_LEN(MAX_VAL_LEN)];
    tx_internal_obj_val_t *max_val_obj = (tx_internal_obj_val_t *) __tmp_buff;
    max_val_obj->hdr.alloc_len = MAX_VAL_LEN;
    int length = __get(tx_ctx, key_ptr, key_len, max_val_obj, INT_OBJ_LEN(max_val_obj->hdr.alloc_len));
    if(length < 0){ // object does not exists
        // TODO see trans of KV implementation of creating a header
    }
    val_obj->hdr.alloc_len = max_val_obj->hdr.alloc_len;
    val_obj->hdr.alloc_len = max_val_obj->hdr.alloc_len;
    __del(tx_ctx, key_ptr, key_len);
}

tx_op_result tx_single_del(tx_ctx_t *tx_ctx, void* key_ptr, uint32_t key_len)
{
    __del(tx_ctx, key_ptr, key_len);
}
