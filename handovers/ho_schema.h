#pragma once

#include "../tx_shim.h"

// fields containing *data* in their name are essentially unused by the benchmark
// (we only keep them to incur the overhead of object-size during replication)
// the same holds for fields named otherwise but also have an //unused comment

// Info about tables
// MME_SESSION_MAP_MS1APID_TABLE --> get UE based on MME UE S1AP ID (default)
// MME_SESSION_TABLE             --> get UE based on SGW (secondary)
// MME_ENB_MAP_SCTP_TABLE        --> get ENODB
// MME_ENB_UE_CONTEXT_TABLE      --> active UEs on ENODEB

// ~~~ eNodeB (base station) context

#define MME_ENB_MAP_SCTP_TABLE			"mme_enb_m_sctp"

typedef struct _mme_enodeb_list_slot_t {
    uint8_t data0[4];

    struct {
        uint32_t sctp_idx;
        uint8_t data1[20];
    } sctp;

    uint8_t data1[4];
    uint32_t enb_id;

    struct _mme_enodeb_list_t_slot *prev, *next; //unused
} mme_enodeb_t;



// ******* Context for UEs (phones) on an eNodeB

#define MME_ENB_UE_CONTEXT_TABLE		"mme_enb_ue_ctx"

typedef struct {
    uint32_t enodeb_idx; // matches sctp_idx
    uint32_t enodeb_s1ap_id; // matches enb.id
} mme_enodeb_ue_context_id_t;


typedef struct _mme_enodeb_ue_context_list_t_slot {
    uint8_t data1[8];
    tx_addr tx_mme_session; // (addr) key of an mme_session
} mme_enodeb_ue_context_t;



// ******* UE (phone) context (session) on MME

#define MME_SESSION_TABLE               "mme_table_t"
#define MME_SESSION_MAP_MS1APID_TABLE   "mme_map_ms1apid_t"


typedef struct _mme_s1ap_context_t {
    uint32_t sctp_idx; // for simplicity we use the same as enb_id

    struct {
        uint8_t data1[8];
        uint32_t id;
    } mme;
    struct {
        uint32_t id;
        uint8_t data1[10];
    } enb;
} mme_s1ap_context_t;


typedef struct _mme_session_t {
    uint32_t mme_id;

    struct {
        mme_s1ap_context_t primary;
        mme_s1ap_context_t secondary;
    } s1ap;

    int active;

    // We don't need to specify this data but
    // this is approximately what will get replicated.
    uint8_t data1[200];


} mme_session_t;



/////////////////////////
/// only for population/destruction
//////////////////////////////
void mme_create_enodeb(tx_ctx_t *tx_ctx, uint32_t enb_id);
void mme_delete_enodeb(tx_ctx_t *tx_ctx, uint32_t enb_id);
void mme_create_session(tx_ctx_t *tx_ctx, uint32_t ue_idx);
void mme_delete_session(tx_ctx_t *tx_ctx, uint32_t ue_idx);



//////////////////////////////
/// main txs of the benchmark
//////////////////////////////


//////////////////////////////
/// session start/sleep (most frequent) should be local
/// --- TX involve structs of one UE with its associated enodb

// UE becoming active
// ue_id - UE's unique ID from MME's point of view.
// enb_id - index of the eNodeB that it is getting associated do.
int mme_session_activate(tx_ctx_t *tx_ctx, uint32_t ue_id, uint32_t enb_id);

// get phone [MME_SESSION_MAP_MS1APID_TABLE]
// ins phone to eNodeB [MME_ENB_UE_CONTEXT_TABLE]
// set phone (activate --> set primary) [MME_SESSION_MAP_MS1APID_TABLE]



// UE going to sleep
// ue_id - UE's unique ID from MME's point of view.
int mme_session_deactivate(tx_ctx_t *tx_ctx, uint32_t ue_id);


// get phone [MME_SESSION_MAP_MS1APID_TABLE]
// del phone from eNodeB [MME_ENB_UE_CONTEXT_TABLE]
// get phone, del phone from eNodeB, set (deactivate) phone [MME_SESSION_MAP_MS1APID_TABLE]



//////////////////////////////
/// handover (less frequent)

/// --- TX involve structs of one UE and NEW enodb
// ue_id - UE's unique ID from MME's point of view.
// dst_enb_idx - index of the destination eNodeB
int mme_handover_start(tx_ctx_t *tx_ctx, uint32_t ue_id, uint32_t dst_enb_id);

// get phone + dst eNodeB,
// insert new session to dst eNodeB
// set phone (dst eNodeB info in secondary ctx)



/// --- TX involve structs of one UE and OLD enodb
// mme_index - UE's index from SGW point of view
//    (we keep a separate table but keep all values the same)
int mme_handover_finish(tx_ctx_t *tx_ctx, uint32_t sgw_ue_id);

// Get ue_id from secondary sgw_ue_id then get phone
// Delete UE context from old eNodeB [MME_ENB_UE_CONTEXT_TABLE]
// [Skip the next step for now --> assuming a single MME here]
//      If primary != secondary MME ID
//          Delete old (primary) MME ID + Insert new (secondary) MME ID
// Set phone (upd primary & secondary info --> primary = secondary & secondary = 0)
