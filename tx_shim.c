//
// Created by akatsarakis on 08/06/2020.
//

#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include <stdio.h>
#include "tx_shim.h"

void tx_trans_init(tx_ctx_t *tx_ctx, tx_trans_t* trans)
{
    trans->tx_id = 0; //TODO
    trans->state = TX_FREE;
    trans->parent = tx_ctx;
    trans->curr_num_objs_in_tx = 0;
}

void tx_ctx_init(tx_ctx_t* tx_ctx /*....*/)
{
    tx_ctx->tx_ids = 0;
    for(int i = 0; i < MAX_CONCUR_TX; ++i){
        tx_trans_init(tx_ctx, &tx_ctx->trans_arr[i]);
    }
}

tx_trans_t* tx_trans_create(tx_ctx_t *tx_ctx)
{
    int tx_idx = -1;
    for(int i = 0; i < MAX_CONCUR_TX; ++i){
        if(tx_ctx->trans_arr[i].state == TX_FREE) {
            tx_idx = i;
            break;
        }
    }

    if(tx_idx == -1){
        return NULL; // no free tx buffs
    }

    // TODO needs a lock!
    // LOCK
    tx_ctx->trans_arr[tx_idx].tx_id = tx_ctx->tx_ids++;
    // UNLOCK

    tx_ctx->trans_arr[tx_idx].state = TX_DYN_READ_ONLY;

    return &tx_ctx->trans_arr[tx_idx];
}

// read-only tx known a priory
// -- in contrast if a tx reaches commit in TX_DYN_READ_ONLY it is a read-only not known a priory
tx_trans_t* tx_rd_only_trans_create(tx_ctx_t *tx_ctx)
{
    tx_trans_t* ret_trans = tx_trans_create(tx_ctx);
    if(ret_trans == NULL) { assert(0); } // no free tx buf // TODO handle properly e.g., via a spin trans_create
    ret_trans->state = TX_READ_ONLY;
    return ret_trans;
}


void tx_trans_abort_n_clear(tx_trans_t* trans)
{
    for(int i = 0; i < trans->curr_num_objs_in_tx; ++i){
        if(trans->obj_ids[i].is_mem &&
           !trans->obj_ids[i].existed_prior_tx)
        {
            free(trans->obj_ids[i].obj_ptr);
        }
    }

    trans->tx_id = 0;
    trans->state = TX_FREE;
    trans->curr_num_objs_in_tx = 0;
}

void tx_trans_destroy(tx_trans_t* trans)
{
    tx_trans_abort_n_clear(trans);
}

void tx_ctx_destroy(tx_ctx_t *tx_ctx)
{
    // no alloced objs atm so just clear things
    tx_ctx->tx_ids = 0;
    for(int i = 0; i < MAX_CONCUR_TX; ++i){
        tx_trans_abort_n_clear(&tx_ctx->trans_arr[i]);
    }
}

void __tx_trans_state_update(tx_trans_t* trans, uint8_t type){
    assert(trans->state != TX_FREE);
    if(type != READ){
        if(trans->state == TX_READ_ONLY){
            printf("ERROR: An allocated tx as known a priory "
                   "TX tries to apply a NON-READ operation!\n");
            assert(trans->state != TX_READ_ONLY);
        }
        trans->state = TX_UPDATE;
    }
}

tx_trans_result tx_trans_commit(tx_trans_t* trans){
    //TODO
    /// ~~~~ TX commit ~~~~~~
    /// 1. Lock all ALLOCATE / UPDATE / TO_DELETE objects and check if versions are same --> <otherwise abort TX by releasing locks>
    /// 2. Check with lock-free reads READS / DELETES that versions are same (or non-existant) --> <otherwise abort TX by releasing locks>
    /// 3. apply UPDATES / ALLOCATES / TO_DELETE --> <TX is committed | unlock any locked objects>

}
