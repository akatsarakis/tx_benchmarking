#include "tx_shim.h"
#include "tpcc.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>  // struct tm 
#include <string.h>
#define new(T) malloc(sizeof(T))

////////////////////////
// Database Population
////////////////////////

inline int nurand(int A, int x, int y)
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
void initialize_and_permute_random(int* permutation, int n)
// Fisher-Yates shuffles for generating random permutations
// Used for generating order-customer id (O_C_ID)
{
	for (int i = 0; i < n; i++) permutation[i] = i+1;
    for (int i = 0; i <= n-2; i++)
	{
        int j = i + Random(0, n-i-1);
			// A random integer such that i ≤ j < n 
        swap(permutation + i, permutation + j);
			// Swap the randomly picked element with permutation[i] 
    }
}

const int alphanum_len = 10 + 26 + 26;
const char* alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const char* numbers = "0123456789";
const char* c_last_name_sections[] = {
	"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
};
int gen_rand_astr(char* str, int lenl, int lenh)
// generate a string of random alphanumeric characters with
//  random length between [lenl, lenh], and return its length
{
	int len = Random(lenl, lenh);
	str[len] = 0;
	for (int i = 0; i < len; i++) str[i] = alphanum[Random(0, alphanum_len-1)];
	return len;
}
void gen_rand_nstr(char* str, int len)
// numeric
{
	str[len] = 0;
	for (int i = 0; i < len; i++) str[i] = numbers[Random(0, 9)];
}
void gen_rand_lastname(char* lastname, int x)
// Used for generating customer last name (C_LAST).
// Concatenation of three variable length syllables selected from c_last_name_sections.
{
	if (x < 0) x = nurand(255, 0, 999);
	int x1 = x % 10, x2 = x / 10 % 10, x3 = x / 100;
	lastname[0] = 0;
	strcat(lastname, c_last_name_sections[x3]);
	strcat(lastname, c_last_name_sections[x2]);
	strcat(lastname, c_last_name_sections[x1]);
}
void gen_rand_datafield(char* data)
// Used for generating I_DATA and S_DATA.
// Random a-string [26 .. 50]. For 10% of the rows, selected at random,
//  the string "ORIGINAL" must be held by 8 consecutive characters starting at
//  a random position within it. (Allow 5% variation)
// In our code we don't enforce a 5%~15% limit on the number of rows containing "ORIGINAL",
//  we just expect the rand() in <stdlib.h> doesn't produce too much deviation. (??? Do we need to enforce it?)
{
	int x = Random(1, 10);
	int data_len = gen_rand_astr(data, 26, 50);
	if (x == 1)
	{
		int pos = Random(0, data_len-9);
		memcpy(data + pos, "ORIGINAL", 8*sizeof(char));
	}
}
void gen_rand_zip(char* zipcode)
// Used for W_ZIP, D_ZIP and C_ZIP.
// Concatenation of:
//  1. A random n-string of 4 numbers, and
//  2. The constant string '11111'.
{
	gen_rand_nstr(zipcode, 4);
	strcat(zipcode, "11111");
}
inline void Insert(tx_ctx_t* ctx, char* key, void* val)
{
	tx_trans_t* trans = tx_trans_create(ctx);
	tx_trans_init(ctx, trans);
	tx_trans_kv_set(trans, key, strlen(key), val, sizeof(val));
	tx_trans_commit(trans);
}

void init_db_population(const int n_warehouse)
{
	struct tm populated_time = cur_local_time(); 
	tx_ctx_t* ctx = new(tx_ctx_t);
	tx_ctx_init(ctx);
	// FILE* debug_txt = fopen("db_population.txt", "w");  // debug
	
	// ITEM table
	for (int i = 0; i < 100000; i++)
	{
		item_t* c = new(item_t);
		c -> i_id = i+1;
		c -> i_im_id = Random(1, 10000);
		gen_rand_astr(c -> i_name, 14, 24);
		c -> i_price = Random(100, 10000) / 100.0;
		gen_rand_datafield(c -> i_data);
		// fprintf(debug_txt, "%d %d %f %s %s\n", c -> i_id, c -> i_im_id, c -> i_price, c -> i_name, c -> i_data);

		char c_pri_key[8];
		sprintf(c_pri_key, "i%d", c -> i_id);
		Insert(ctx, c_pri_key, c);
	}
	/*
		Store or deletes tuples in each table as follows:
		(1) Create a globally unique key for each row of a table by concatenating
		 a unique prefix of a table with the primary-key of each table.
		 For example, here we could use 'a', 's', 'c' as prefixes for our
		 Account, Savings, Checking tables respectively. Assuming that below are
		 my records of in such a database that we would like to insert those records
		 can be globally identified by unique keys based on our prefixes
		 Account: ["Antonis", 1723] --> "aAntonis"
		 Savings:   [1723, 200]         --> "s"1723
		 Checking [1723, 323.2]      --> "c"1723
		(2) Then in our KVS-based implementation we can simply insert my records
		 as follows:
		 transaction_start()
		 set("aAntonis", ["Antonis", 1723])
		 set("s"1723, [1723, 200])
		 set("c"1723, [1723, 323.2])
		 transaction_end()
	*/
	
	for (int i = 0; i < n_warehouse; i++)
	{
		warehouse_t* w = new(warehouse_t);
		w -> w_id = i+1;
		gen_rand_astr(w -> w_name, 6, 10);
		gen_rand_astr(w -> w_street_1, 10, 20);
		gen_rand_astr(w -> w_street_2, 10, 20);
		gen_rand_astr(w -> w_city, 10, 20);
		gen_rand_astr(w -> w_state, 2, 2);
		// ??? In the specification it says, w_state should be "random a-string of 2 letters",
		//  while the definition of a-string includes digits and English letters. Contradictory?
		gen_rand_zip(w -> w_zip);
		w -> w_tax = Random(0, 2000) / 10000.0;
		w -> w_ytd = 300000.0;
		// fprintf(debug_txt, "%d %f %f %s %s %s\n", w -> w_id, w -> w_tax, w -> w_ytd, w -> w_name, w -> w_street_1, w -> w_street_2);
		// fprintf(debug_txt, "%s %s %s\n", w -> w_state, w -> w_zip, w -> w_city);

		char w_pri_key[11];
		sprintf(w_pri_key, "w%d", w->w_id);
		Insert(ctx, w_pri_key, w);
		
		// For each row in the WAREHOUSE table: 
		for (int j = 0; j < 100000; j++)
		{
			stock_t* s = new(stock_t);
			s -> s_i_id = j+1;
			s -> s_w_id = w -> w_id;
			s -> s_quantity = Random(10, 100);
			for (int k = 0; k < 10; k++) gen_rand_astr(s -> s_dist[k], 24, 24);
			s -> s_ytd = 0;
			s -> s_order_cnt = 0;
			s -> s_remote_cnt = 0;
			gen_rand_datafield(s -> s_data);
			// fprintf(debug_txt, "%d %d %d\n", s -> s_w_id, s -> s_i_id, s -> s_quantity);
			// fprintf(debug_txt, "%d %d %d\n", s -> s_ytd, s -> s_order_cnt, s -> s_remote_cnt);
			// for (int k = 0; k < 10; k++) fprintf(debug_txt, "%s\n", s -> s_dist[k]);
			// fprintf(debug_txt, "%s\n", s -> s_data);

			char s_pri_key[19];
			sprintf(s_pri_key, "s%d,%d", s->s_w_id, s->s_i_id);
			Insert(ctx, s_pri_key, s);
		}
		
		for (int j = 0; j < 10; j++)
		{
			district_t* d = new(district_t);
			d -> d_id = j+1;
			d -> d_w_id = w -> w_id;
			gen_rand_astr(d -> d_name, 6, 10);
			gen_rand_astr(d -> d_street_1, 10, 20);
			gen_rand_astr(d -> d_street_2, 10, 20);
			gen_rand_astr(d -> d_city, 10, 20);
			gen_rand_astr(d -> d_state, 2, 2);
			gen_rand_zip(d -> d_zip);
			d -> d_tax = Random(0, 2000) / 10000.0;
			d -> d_ytd = 30000.00;
			d -> d_next_o_id = 3001;
			// fprintf(debug_txt, "%d %d %f %f %d\n", d -> d_id, d -> d_w_id, d -> d_tax, d -> d_ytd, d -> d_next_o_id);
			// Test of d_name, d_city, etc are similar to w_name, w_city, so omitted.

			char d_pri_key[14];
			sprintf(d_pri_key, "d%d,%d", d->d_w_id, d->d_id);
			Insert(ctx, d_pri_key, d);

			// For each row in the DISTRICT table:
			for (int k = 0; k < 3000; k++)
			{
				customer_t* c = new(customer_t);
				c -> c_id = k+1;
				c -> c_d_id = d -> d_id;
				c -> c_w_id = d -> d_w_id;
				gen_rand_lastname(c -> c_last, k < 1000 ? k : -1);
					// Iterating through the range of [0 .. 999] for the first 1,000 customers,
					//  and generating a non-uniform random number using the function
					//  NURand(255,0,999) for each of the remaining 2,000 customers. The
					//  run-time constant C used for the database population
					//  must be randomly chosen independently from the test run(s).
				strcpy(c -> c_middle, "OE");
				gen_rand_astr(c -> c_first, 8, 16);
				gen_rand_astr(c -> c_street_1, 10, 20);
				gen_rand_astr(c -> c_street_2, 10, 20);
				gen_rand_astr(c -> c_city, 10, 20);
				gen_rand_astr(c -> c_state, 2, 2);
				gen_rand_zip(c -> c_zip);
				gen_rand_nstr(c -> c_phone, 16);
				c -> c_since = new(struct tm);
				*(c -> c_since) = populated_time;
					// C_SINCE date/time given by the operating system when
					//  the CUSTOMER table was populated.
				strcpy(c -> c_credit, Random(1, 10) > 1 ? "GC" : "BC");
					// C_CREDIT = "GC". For 10% of the rows, selected at random, C_CREDIT = "BC"
				c -> c_credit_lim = 50000.00;
				c -> c_discount = Random(0, 5000) / 10000.0;
				c -> c_balance = -10.00;
				c -> c_ytd_payment = 10.00;
				c -> c_payment_cnt = 1;
				c -> c_delivery_cnt = 0;
				gen_rand_astr(c -> c_data, 300, 500);
				// Test of c_street_1, c_city, c_state, etc are similar to above, so omitted.
				// fprintf(debug_txt, "%d %d %d %s\n", c->c_id, c->c_d_id, c->c_w_id, c->c_middle);
				// fprintf(debug_txt, "%s %s %f\n", c->c_phone, c->c_credit, c->c_credit_lim);
				// fprintf(debug_txt, "%s", asctime(c->c_since));
				// fprintf(debug_txt, "%f %f %f %d %d\n", c->c_discount, c->c_balance, c->c_ytd_payment, c->c_payment_cnt, c->c_delivery_cnt);
				// fprintf(debug_txt, "%d %s\n", c->c_id, c->c_last);

				char c_pri_key[19];
				sprintf(c_pri_key, "c%d,%d,%d", c->c_w_id, c->c_d_id, c->c_id);
				Insert(ctx, c_pri_key, c);
				
				// For each row in the CUSTOMER table, 1 row in the HISTORY table with:
				history_t* h = new(history_t);
				h -> h_c_id = c -> c_id;
				h -> h_c_d_id = h -> h_d_id = d -> d_id;
				h -> h_c_w_id = h -> h_w_id = w -> w_id;
				h -> h_date = new(struct tm); *(h -> h_date) = cur_local_time();
				h -> h_amount = 10.00;
				gen_rand_astr(h -> h_data, 12, 24);
				// fprintf(debug_txt, "%d %d %d %d %d %f %s\n", h->h_c_id, h->h_c_d_id, h->h_d_id, h->h_c_w_id, h->h_w_id, h->h_amount, h->h_data);
				// fprintf(debug_txt, "%s", asctime(h->h_date));

				char h_pri_key[19];  // In fact, history_t doesn't have a primary key (??? Then how to insert it?)
				sprintf(h_pri_key, "h%d,%d,%d", h->h_w_id, h->h_d_id, h->h_c_id);
				Insert(ctx, h_pri_key, h);
			}
			
			// ORDER table
			int perm[3000];
			initialize_and_permute_random(perm, 3000);
			for (int k = 0; k < 3000; k++)
			{
				order_t* o = new(order_t);
				o -> o_id = k+1;
				o -> o_c_id = perm[k];
					// O_C_ID selected sequentially from a random permutation of [1 .. 3,000]
				o -> o_d_id = d -> d_id;
				o -> o_w_id = w -> w_id;
				o -> o_entry_d = new(struct tm); *(o -> o_entry_d) = cur_local_time();
				o -> o_carrier_id = o -> o_id < 2101 ? Random(1, 10) : -1;
				o -> o_ol_cnt = Random(5, 15);
				o -> o_all_local = 1;
				// fprintf(debug_txt, "%d %d %d %d %d %d %d\n", o->o_id, o->o_d_id, o->o_w_id, o->o_carrier_id, o->o_ol_cnt, o->o_all_local, o->o_c_id);
				// fprintf(debug_txt, "%s", asctime(o->o_entry_d));

				char o_pri_key[19];
				sprintf(o_pri_key, "o%d,%d,%d", o->o_w_id, o->o_d_id, o->o_id);
				Insert(ctx, o_pri_key, o);
				
				for (int p = 0; p < o -> o_ol_cnt; p++)
				{
					orderline_t* ol = new(orderline_t);
					ol -> ol_o_id = o -> o_id;
					ol -> ol_d_id = d -> d_id;
					ol -> ol_w_id = w -> w_id;
					ol -> ol_number = p+1;
					ol -> ol_i_id = Random(1, 100000);
					ol -> ol_supply_w_id = w -> w_id;
					if (ol -> ol_o_id < 2101) { ol -> ol_delivery_d = new(struct tm); *(ol -> ol_delivery_d) = *(o -> o_entry_d); }
					else ol -> ol_delivery_d = NULL;
					ol -> ol_quantity = 5;
					ol -> ol_amount = ol -> ol_o_id < 2101 ? 0.00 : Random(1, 999999) / 100.0;
					gen_rand_astr(ol -> ol_dist_info, 24, 24);
					// fprintf(debug_txt, "%d %d %d %d %d %d %d\n", ol->ol_o_id, ol->ol_d_id, ol->ol_w_id, ol->ol_supply_w_id, ol->ol_number, ol->ol_i_id, ol->ol_quantity);
					// fprintf(debug_txt, "%d %f\n", ol->ol_o_id, ol->ol_amount);
					// fprintf(debug_txt, "%s\n", ol->ol_dist_info);
					// fprintf(debug_txt, "%d %s", ol->ol_o_id, ol->ol_delivery_d == NULL ? "NULL\n" : asctime(ol->ol_delivery_d));

					char ol_pri_key[23];
					sprintf(ol_pri_key, "ol%d,%d,%d,%d", ol->ol_w_id, ol->ol_d_id, ol->ol_o_id, ol->ol_number);
					Insert(ctx, ol_pri_key, ol);
				}
			}
			
			for (int k = 0; k < 900; k++)
			{
				neworder_t* no = new(neworder_t);
				no -> no_o_id = k + 2101;
					// 900 rows in the NEW-ORDER table corresponding to the last 900 rows in the ORDER table
					//  for that district with NO_O_ID = O_ID (i.e., with NO_O_ID between 2,101 and 3,000)
				no -> no_d_id = d -> d_id;
				no -> no_w_id = w -> w_id;
				// fprintf(debug_txt, "%d %d %d\n", no->no_o_id, no->no_d_id, no->no_w_id);

				char no_pri_key[20];
				sprintf(no_pri_key, "no%d,%d,%d", no->no_w_id, no->no_d_id, no->no_o_id);
				Insert(ctx, no_pri_key, no);
			}
		}
	}

	// fclose(debug_txt);
	tx_ctx_destroy(ctx);
}

int main(void)  // for debug
{
	srand(time(NULL));
	init_db_population(1);
	return 0;
}
