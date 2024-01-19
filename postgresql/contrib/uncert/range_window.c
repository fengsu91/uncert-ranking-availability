/*
 * contrib/uncert/range_window.c
 *
 * usage:  range_window (query)
 */
#include "postgres.h"

#include <ctype.h>

#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "miscadmin.h"

#include "access/printtup.h"

PG_MODULE_MAGIC;

static void prepTuplestoreResult(FunctionCallInfo fcinfo);
static void printtuple(HeapTuple tup, TupleDesc tupdesc);
void static printNumeric(Datum in, char* pre);

/*
 * Declaration of dynamic array and linked heap for internal usage
 *
 *
 *
 *
 */

typedef struct DArray {
    int indk; //index of the key to want to sort on
    int cur;
    int len;
    TupleDesc tupdesc;
    //some statistics
    int swapct;
    int maxlen;
    void** data; // list of pointers to HNode
    //void *data[];
} DArray;

DArray *newA(int initS, int ind, TupleDesc tupdesc);
void initA(DArray *a, int initS, int ind, TupleDesc tupdesc);
void insertA(DArray *a, void* e);
void freeA(DArray *a);

typedef struct HNode {
    HeapTuple item; //holds the tuple
    DArray *bA; //back reference heap
    int bP; //back reference position
} HNode;

// Tup *newT(int a, int b);
HNode *newN(HeapTuple item, DArray *bA);

int p(int i);
int l_c(int i);
int r_c(int i);
void swap(void **x, void **y, bool br);
Datum getKeyVal(int ind, HNode *nd, TupleDesc tupdesc);
Datum getKeyValT(int ind, HeapTuple tup, TupleDesc tupdesc);
bool compareVal(int ind, DArray *a, int lind, int rind, bool gt);

void insert(DArray *a, HNode *in);
void insertTup(HeapTuple item, DArray *a, DArray *bA);
HNode *get_max(DArray *a);
HNode *extract_max(DArray *a);
void printHNode(HNode *a, TupleDesc tupdesc);
void print_heap(DArray *a);
void print_heap_stats(DArray *a);
void max_heapify(DArray *a, int i);

void initA(DArray *a, int initS, int ind, TupleDesc tupdesc) {
    a->data = malloc(initS * (int)sizeof(void*));
    a->cur = 0;
    a->len = initS;
    a->indk = ind;
    a->tupdesc = tupdesc;
    //stat data
    a->swapct = 0;
    a->maxlen = 0;
    // elog(NOTICE,"Heap with initial size %d sorting on index %d.\n",(int)a->len, ind);
}

DArray *newA(int initS, int ind, TupleDesc tupdesc) {
    DArray *a = malloc(sizeof(DArray));
    initA(a, initS, ind, tupdesc);
    return a;
}

void insertA(DArray *a, void* e) {
    if (a->cur == a->len) {
        a->len *= 2;
        a->data = realloc(a->data, a->len * (int)sizeof(void*));
    }
    (a->data)[a->cur++] = e;
    //stat update
    if(a->cur > a->maxlen){
    	a->maxlen = a->cur;
    }
}

void insertTup(HeapTuple item, DArray *a, DArray *bA) {
	// elog(NOTICE, "inserting tuple: ");
	// printtuple(item, a->tupdesc);
	HNode *nd = newN(item, bA);
	// elog(NOTICE, "node is: ");
	// printHNode(nd, a->tupdesc);
	insert(a, (void *)nd);
	// elog(NOTICE, "inserting done.");
}

void freeA(DArray *a) {
    free(a->data);
    a->data = NULL;
    a->cur = a->len = 0;
}

// Parent Node index
int p(int i) {
    return (i - 1) / 2;
}

// Leftchild index
int l_c(int i) {
    return 2*i + 1;
}

// Right child index
int r_c(int i) {
    return 2*i + 2;
}

HNode *newN(HeapTuple item, DArray *bA) {
    HNode *nd = malloc(sizeof(HNode));
    nd->item = item;
    nd->bA = bA;
    nd->bP = -1;
    return nd;
}

void swap(void **x, void **y, bool br) {
    void *temp = *x;
    *x = *y;
    *y = temp;
    if (br) {
        int tmp = ((HNode *)*x)->bP;
        ((HNode *)*x)->bP = ((HNode *)*y)->bP;
        ((HNode *)*y)->bP = tmp;
    }
}

//given sorting index and HNode return the key value.
Datum getKeyVal(int ind, HNode *nd, TupleDesc tupdesc){

    Datum val;
    // char * cval;
	bool 	isnull = false;

	// cval = SPI_getvalue(nd->item, tupdesc, ind);
	val = SPI_getbinval(nd->item, tupdesc, ind, &isnull);

    return val;
}

//given sorting index and tuple return the key value.
Datum getKeyValT(int ind, HeapTuple tup, TupleDesc tupdesc){

    Datum val;
    // char * cval;
	bool 	isnull = false;

	// cval = SPI_getvalue(tup, tupdesc, ind);
	val = SPI_getbinval(tup, tupdesc, ind, &isnull);

    return val;
}

void insert(DArray *a, HNode *in) {
    // insert to last position
    insertA(a, in);
    int ind = a->indk;
    bool br = (in->bA == a);
    if(br){
        in->bP = a->cur-1;
    }
    // move up
    int i = (int)a->cur - 1;
    // bool isles = DatumGetBool(DirectFunctionCall2(numeric_lt,
    // 	getKeyVal(ind, (HNode *)(a->data[p(i)]), a->tupdesc),
    // 	getKeyVal(ind, (HNode *)(a->data[i]), a->tupdesc)));
    // elog(NOTICE, "insert: compare les than key values: %i", (int)isles);
    // while (i != 0 && getKeyVal(ind, (HNode *)(a->data[p(i)]), a->tupdesc) < getKeyVal(ind, (HNode *)(a->data[i]), a->tupdesc)) {
    while (i != 0 && DatumGetBool(DirectFunctionCall2(numeric_gt,
    	getKeyVal(ind, (HNode *)(a->data[p(i)]), a->tupdesc),
    	getKeyVal(ind, (HNode *)(a->data[i]), a->tupdesc)))) {
        swap(&a->data[p(i)], &a->data[i], br);
    	a->swapct++;
        i = p(i);
    }
    // elog(NOTICE, "After insertion, heap is: ");
    // print_heap(a);
    // printHNode((HNode *)(a->data[0]), a->tupdesc);
}

void max_heapify(DArray *a, int i){
   	//elog(NOTICE, "Heapify %i for heap --", i);

    int n = a->cur-1;
    int left = l_c(i);
    int right = r_c(i);
    int largest = i;

    //elog(NOTICE, "left: %i right: %i.", left, right);

    int ind = a->indk;
    bool br = (((HNode *)(a->data[0]))->bA==a);

    bool isgt = compareVal(ind, a, left, largest, true);
    // DatumGetBool(DirectFunctionCall2(numeric_gt,
    	// getKeyVal(ind, (HNode *)(a->data[left]), a->tupdesc),
    	// getKeyVal(ind, (HNode *)(a->data[largest]), a->tupdesc)));

    // if (left <= n && getKeyVal(ind, (HNode *)(a->data[left]), a->tupdesc) > getKeyVal(ind, (HNode *)(a->data[largest]), a->tupdesc)) {
    if (left <= n && isgt){
        largest = left;
    }

    isgt = compareVal(ind, a, right, largest, true);
    // DatumGetBool(DirectFunctionCall2(numeric_gt,
    // 	getKeyVal(ind, (HNode *)(a->data[right]), a->tupdesc),
    // 	getKeyVal(ind, (HNode *)(a->data[largest]), a->tupdesc)));

    // if (right <= n && getKeyVal(ind, (HNode *)(a->data[right]), a->tupdesc) > getKeyVal(ind, (HNode *)(a->data[largest]), a->tupdesc)) {
    if (right <= n && isgt){
        largest = right;
    }

    if (largest != i) {
        swap(&a->data[i], &a->data[largest], br);
        a->swapct++;
        // elog(NOTICE, "swap %i and %i.", i, largest);
        max_heapify(a, largest);
    }
}

bool compareVal(int ind, DArray *a, int lind, int rind, bool gt){
	if(lind > a->cur-1 || rind > a->cur-1){
		return false;
	}
	if(gt){
		return DatumGetBool(DirectFunctionCall2(numeric_lt,
    		getKeyVal(ind, (HNode *)(a->data[lind]), a->tupdesc),
    		getKeyVal(ind, (HNode *)(a->data[rind]), a->tupdesc)));
	}
	return DatumGetBool(DirectFunctionCall2(numeric_gt,
    		getKeyVal(ind, (HNode *)(a->data[lind]), a->tupdesc),
    		getKeyVal(ind, (HNode *)(a->data[rind]), a->tupdesc)));
}

void printHNode(HNode *nd, TupleDesc tupdesc){
    printtuple(nd->item,tupdesc);
}

// peak root
HNode *get_max(DArray *a) {
    return (HNode *)a->data[0];
}

//Extract a node will also remove from back reference heap.
//Extract directly from back reference heap will clear back reference info from the node.
HNode *extract_max(DArray *a) {

    if(a->cur - 1 == -1) {
        return NULL;
    }

    HNode *max_item = (HNode *)a->data[0];

    // replace root
    a->data[0] = a->data[a->cur - 1];
    a->data[a->cur - 1] = NULL;
    a->cur = a->cur - 1;

    if(max_item->bA && max_item->bA != a && max_item->bP >= 0){
        DArray *bA = max_item->bA;
        int bP = max_item->bP;

        bA->data[bP] = bA->data[bA->cur - 1];
        bA->data[bA->cur - 1] = NULL;
        bA->cur = bA->cur - 1;
        if(bA->cur > 1){
			max_heapify(bA, bP);
        }
        // elog(NOTICE, "=========== auto remove ==========");
        // print_heap(bA);
    }

    //No need to back reference for extracted nodes
//    if(max_item->bA==a){
        max_item->bA = NULL;
        max_item->bP = -1;
//    }
    // heapify
    if(a->cur > 1){
		max_heapify(a, 0);
    }
    // print_heap(a);
    return max_item;
}

// prints the heap
void print_heap(DArray *a) {
    int i;
    elog(NOTICE, " ");
    elog(NOTICE, "*****************************************");
    elog(NOTICE, "*****************************************");
    elog(NOTICE, "Printing heap:");
    elog(NOTICE, "Sorting index = %d, size: ", a->indk);
    elog(NOTICE, "[%d/%d]", a->cur, a->len);
    for (i = 0; i < a->cur; i++) {
    	elog(NOTICE, "%i-th tuple is: ", i);
    	HNode *nd = (HNode *)(a->data[i]);
        printHNode(nd, a->tupdesc);
    }
    elog(NOTICE, "*****************************************");
    elog(NOTICE, "*****************************************");
    elog(NOTICE, " ");
}

void static printNumeric(Datum in, char* pre){
	elog(NOTICE, "%s %s", pre, DatumGetCString(DirectFunctionCall1(numeric_out, in)));
}

void print_heap_stats(DArray *a) {
    elog(NOTICE, " ");
    elog(NOTICE, "*****************************************");
    elog(NOTICE, "# of swaps: %d.", a->swapct);
    elog(NOTICE, "max length: %d.", a->maxlen);
    elog(NOTICE, "*****************************************");
    elog(NOTICE, " ");
}

/*
 * End linked heap
 *
 *
 *
 *
 */

typedef struct aggStats
{
	Datum curval;
	int ub_rank_ind;
	int lb_rank_ind;
	int agg_ind;
	Datum window;
	Datum cetval;
	Datum posval;
	DArray *ub_sort;
	DArray *window_sort;
	DArray *window_agg;
	DArray *window_cert;
} aggStats;

typedef struct sortStats
{
	int32 lb_rank;
	int32 ub_rank;
	int ub_order_ind;
	int lb_order_ind;
	int lb_rank_ind;
	int ub_rank_ind;
	int32 k;
	DArray *ub_sort;
	DArray *bg_sort;
} sortStats;

static void initStats(aggStats *state, int ub_rank_ind, int lb_rank_ind, int lb_agg_ind, int32 window, TupleDesc tupdesc);
static void initSortStats(sortStats *state, int ub_order_ind, int lb_order_ind, int ub_rank_ind, int lb_rank_ind, int32 k, TupleDesc tupdesc);
static void processTuple(aggStats* state, HeapTuple tup, Tuplestorestate *tupstore, TupleDesc tupdesc);
HeapTuple updateTupleBounds(HeapTuple tup, TupleDesc tupdesc, int ub_ind, int lb_ind, Datum ub_val, Datum lb_val);
HeapTuple updateTupleAttr(HeapTuple tup, TupleDesc tupdesc, int ind, Datum val);
Datum getCertain(aggStats* state, TupleDesc tupdesc, HeapTuple pop);


PG_FUNCTION_INFO_V1(range_window);
PG_FUNCTION_INFO_V1(range_topk);

Datum
range_topk(PG_FUNCTION_ARGS)
{
	// elog(NOTICE, "range_topk: start.");

	int 	retspi;
	int 	row;
	char*	relname;
	char*	ordername;
	int32 	k;
	// Datum		val;
	Tuplestorestate *tupstore;
	TupleDesc tupdesc;

	if (PG_NARGS() != 3) /* usage: (relname, sortby, k) */
		/* internal error */
		elog(ERROR, "range_topK: incorrect number of arguments (%d) expecting (3).", PG_NARGS());

	relname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ordername = text_to_cstring(PG_GETARG_TEXT_PP(1));
	k = PG_GETARG_INT32(2);

	prepTuplestoreResult(fcinfo);

	// elog(NOTICE, "range_topk: preped result.");

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	if ((retspi = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "range_window: SPI_connect returned %d", retspi);

	char		sql[8192];
	snprintf(sql, sizeof(sql),
		"SELECT *, 0::numeric AS lb_rank, 0::numeric AS ub_rank FROM %s ORDER BY lb_%s",
		relname, ordername
	);

	// elog(NOTICE, "SQL: %s", sql);

	retspi = SPI_execute(sql, true, 0);

	if (retspi < 0)
		/* internal error */
		elog(ERROR, "range_window: SPI_exec returned %d", retspi);

	//tuple store
	MemoryContext oldcontext;

	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "status", TEXTOID, -1, 0);

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->setResult = tupstore;
	tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	/* get indexes */

	char lb_ordername[1024];
	char ub_ordername[1024];
	snprintf(lb_ordername, sizeof(lb_ordername), "lb_%s", ordername);
	snprintf(ub_ordername, sizeof(lb_ordername), "ub_%s", ordername);
	int lb_order_ind = SPI_fnumber(tupdesc, lb_ordername); //order by index
	int ub_order_ind = SPI_fnumber(tupdesc, ub_ordername); //order by index

	int ub_rank_ind = SPI_fnumber(tupdesc, "ub_rank"); //rank uppder bound
	int lb_rank_ind = SPI_fnumber(tupdesc, "lb_rank"); //rank lower bound

	int lb_row_ind = SPI_fnumber(tupdesc, "cet_r"); //row lower bound

	sortStats* state = (sortStats *)palloc0fast(sizeof(aggStats));
	initSortStats(state, ub_order_ind, lb_order_ind, ub_rank_ind, lb_rank_ind, k, tupdesc);

	HeapTuple 		pop;
	HeapTuple 		ret;

	for(row = 0; row < SPI_processed; row++){
		HeapTuple tuple;
		// elog(NOTICE, "range_window: row %d", row);
		tuple = SPI_copytuple((SPI_tuptable->vals)[row]);

		DArray *ub_sort = state->ub_sort;
		//DArray *bg_sort = state->bg_sort;

		while(state->ub_sort->cur>0 && DatumGetBool(DirectFunctionCall2(numeric_lt,
    		getKeyValT(state->ub_order_ind, get_max(ub_sort)->item, tupdesc),
    		getKeyValT(state->lb_order_ind, tuple, tupdesc)))){

			pop = extract_max(ub_sort)->item;
			state->lb_rank++;

			if(state->ub_rank>k){
				ret = updateTupleBounds(pop, tupdesc, ub_rank_ind, lb_row_ind, DirectFunctionCall1(int4_numeric,k), (int32)0);
			} else {
				ret = updateTupleAttr(pop, tupdesc, ub_rank_ind, DirectFunctionCall1(int4_numeric,state->ub_rank));
			}
			tuplestore_puttuple(tupstore, ret);
		}
		if(k>0 && state->lb_rank > k){
			break;
		}
		tuple = updateTupleAttr(tuple, tupdesc, lb_rank_ind, DirectFunctionCall1(int4_numeric,state->lb_rank));
		insertTup(tuple, ub_sort, NULL);
		state->ub_rank++;
	}
	//output all remaining tuples
	// if(k>0){
	// 	state->ub_rank = (int32)-1;
	// }
	while(get_max(state->ub_sort)){
		pop = extract_max(state->ub_sort)->item;
		if(state->ub_rank>k){
			ret = updateTupleBounds(pop, tupdesc, ub_rank_ind, lb_row_ind, DirectFunctionCall1(int4_numeric,k), (int32)0);
		} else {
			ret = updateTupleAttr(pop, tupdesc, ub_rank_ind, DirectFunctionCall1(int4_numeric,state->ub_rank));
		}
		tuplestore_puttuple(tupstore, ret);
	}

	//print stats
	elog(NOTICE, "UB");
	print_heap_stats(state->ub_sort);
	// elog(NOTICE, "BG");
	// print_heap_stats(state->bg_sort);

	tuplestore_donestoring(tupstore);

	SPI_finish();

	PG_RETURN_NULL();
}

Datum
range_window(PG_FUNCTION_ARGS)
{
	// elog(NOTICE, "range_window: start.");

	int 	ret;
	int 	row;
	char*	funcname;
	char*	aggname;
	char*	ordername;
	char*	relname;
	int32 	window;
	// Datum		val;
	Tuplestorestate *tupstore;
	TupleDesc tupdesc;
	// int attnum;
	// bool 	isnull = false;

	// int 	mprec = 3;

	if (PG_NARGS() != 5) /* usage: (funtion, aggAttr, orderAttr, relname) */
		/* internal error */
		elog(ERROR, "range_window: incorrect number of arguments (%d) expecting (5).", PG_NARGS());

	funcname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	aggname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	ordername = text_to_cstring(PG_GETARG_TEXT_PP(2));
	relname = text_to_cstring(PG_GETARG_TEXT_PP(3));
	window = PG_GETARG_INT32(4);

	/* Sanity checks
     *
	 *
	 */
	// attnum = SPI_fnumber(tupdesc, args[0]);
	// if (attnum <= 0)
	// 	ereport(ERROR,
	// 			(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
	// 			 errmsg("\"%s\" has no attribute \"%s\"", relname, args[0])));

	prepTuplestoreResult(fcinfo);

	// elog(NOTICE, "range_window: preped result.");

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "range_window: SPI_connect returned %d", ret);

	// elog(NOTICE, "range_window: SPI connected.");
	char		sql[8192];
	snprintf(sql, sizeof(sql),
		"WITH endpoints AS ("
		"SELECT id, %s, ub_%s, lb_%s, %s, ub_%s, lb_%s, lb_%s as pt, 0 AS isend , cet_r , bst_r , pos_r FROM %s "
		"UNION ALL "
		"SELECT id, %s, ub_%s, lb_%s, %s, ub_%s, lb_%s, ub_%s as pt, 1 AS isend , cet_r , bst_r , pos_r FROM %s"
		"), "
		"bounds AS (SELECT id, %s, ub_%s, lb_%s, %s, ub_%s, lb_%s, cet_r , bst_r , pos_r,"
		"( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY %s ASC ) ELSE 0 END) AS rank ,"
		"( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,"
		"( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank "
		"FROM endpoints)"
		"SELECT id, min(%s) AS %s, min(ub_%s) AS ub_%s, min(lb_%s) AS lb_%s, min(%s) AS %s, min(ub_%s) AS ub_%s, min(lb_%s) AS lb_%s, "
		"SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum "
		"FROM bounds GROUP BY id Order by lb_rank",
		aggname, aggname, aggname, ordername, ordername, ordername, ordername,
		relname,
		aggname, aggname, aggname, ordername, ordername, ordername, ordername,
		relname,
		aggname, aggname, aggname, ordername, ordername, ordername,
		ordername,
		aggname, aggname, aggname, aggname, aggname, aggname, ordername, ordername, ordername, ordername, ordername, ordername
		// aggname, aggname
	);

	// elog(NOTICE, "range_window: SPI query -- %s", sql);

	ret = SPI_execute(sql, true, 0);

	if (ret < 0)
		/* internal error */
		elog(ERROR, "range_window: SPI_exec returned %d", ret);

	// elog(NOTICE, "range_window: number of tuples -- %d", (int)SPI_processed);

	//tuple store
	MemoryContext oldcontext;

	/*
	 * Need 2*3+3=9 number of attributes in total:
	 * CET_R, BST_R, POS_R
	 * ub_[attrname], lb_[attrname]
	 */

	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "status", TEXTOID, -1, 0);

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->setResult = tupstore;
	tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	/* get indexes */

	// int aggnamenum = SPI_fnumber(tupdesc, aggname); //aggregation attribute
	// int ordernamenum = SPI_fnumber(tupdesc, ordername); //order by attribute

	char lb_aggname[1024];
	snprintf(lb_aggname, sizeof(lb_aggname), "lb_%s", aggname);
	int lb_agg_ind = SPI_fnumber(tupdesc, lb_aggname); //aggregation attribute upperbound

	int ub_rank_ind = SPI_fnumber(tupdesc, "ub_rank"); //rank uppder bound
	int lb_rank_ind = SPI_fnumber(tupdesc, "lb_rank"); //rank lower bound

	// elog(NOTICE, "ub_rank index is %d, lb_rank index is %d, aggregation lb index is %d.", ub_rank_ind, lb_rank_ind, lb_agg_ind);

	// Form_pg_attribute attr;
	// char 		*value;
	// Oid			typoutput;
	// bool		typisvarlena;

	aggStats* state = (aggStats *)palloc0fast(sizeof(aggStats));
	initStats(state, ub_rank_ind, lb_rank_ind, lb_agg_ind, window, tupdesc);

	for(row = 0; row < SPI_processed; row++){
		HeapTuple tuple;
		//HeapTuple tuplec;
		// elog(NOTICE, "range_window: row %d", row);
		tuple = SPI_copytuple((SPI_tuptable->vals)[row]);
		//tuplec = SPI_copytuple((SPI_tuptable->vals)[row]);

		processTuple(state, tuple, tupstore, tupdesc);

		// val = heap_getattr(tuple, aggnamenum, tupdesc, &isnull);
		// text* attval = DatumGetTextPP(val);

		// int col=1;
		// elog(NOTICE, "\t col:%d\n", col);
		// val = heap_getattr(tuple, col, tupdesc, &isnull);
		// attr = TupleDescAttr(tupdesc, col);
		// getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
		// value = OidOutputFunctionCall(typoutput, val);
		// elog(NOTICE, "\t ---%s(%u)---",value, attr->atttypid);

		// attr = TupleDescAttr(tupdesc, aggnamenum);
		// getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
		// value = OidOutputFunctionCall(typoutput, val);
		// elog(NOTICE, "-----------%s(%u)-------------\n",value, attr->atttypid);

		// elog(NOTICE, "range_window: value for %s is %s.", aggname, text_to_cstring(attval));
		// val = heap_getattr(tuple, ordernamenum, tupdesc, &isnull);
		// attval = DatumGetTextPP(val);
		// elog(NOTICE, "range_window: value for %s is %s.", ordername, text_to_cstring(attval));
		// printtuple(tuple, tupdesc);
	}
	//output all remaining tuples
	HeapTuple 		pop;
	HeapTuple 		rettup;
	int ub_sum_ind = SPI_fnumber(tupdesc, "ub_sum");
	int lb_sum_ind = SPI_fnumber(tupdesc, "lb_sum");
	while(get_max(state->ub_sort)){
		pop = extract_max(state->ub_sort)->item;
		rettup = updateTupleBounds(pop, tupdesc, ub_sum_ind, lb_sum_ind, state->posval, getCertain(state, tupdesc, pop));
		tuplestore_puttuple(tupstore, rettup);
	}

	//print stats
	elog(NOTICE, "UB");
	print_heap_stats(state->ub_sort);
	elog(NOTICE, "window");
	print_heap_stats(state->window_sort);
	elog(NOTICE, "cert");
	print_heap_stats(state->window_cert);

	tuplestore_donestoring(tupstore);

	SPI_finish();

	PG_RETURN_NULL();
}

static void
prepTuplestoreResult(FunctionCallInfo fcinfo)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* check to see if query supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* let the executor know we're sending back a tuplestore */
	rsinfo->returnMode = SFRM_Materialize;

	/* caller must fill these to return a non-empty result */
	rsinfo->setResult = NULL;
	rsinfo->setDesc = NULL;
}

static void
printtuple(HeapTuple tup, TupleDesc tupdesc){
	Oid			typoutput;
	bool		typisvarlena;
	char	   *value;
	int 		col;
	Datum		val;
	bool 	isnull = false;
	Form_pg_attribute attr;
	char output[1024];
	// elog(NOTICE, "# of cols: (%i)", tupdesc->natts);
	snprintf(output, sizeof(output), "%s", "");
	for(col=0; col<tupdesc->natts; ++col){
		// elog(NOTICE, "col: (%i)", col);
		val = heap_getattr(tup, col+1, tupdesc, &isnull);
		attr = TupleDescAttr(tupdesc, col);
		getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
		value = OidOutputFunctionCall(typoutput, val);
		// elog(NOTICE, "\t -%s-",value);
		snprintf(output+strlen(output), sizeof(output)-strlen(output), " %s |", value);
		// elog(NOTICE, "\t ---%s(%u)---",value, attr->atttypid);
	}
	elog(NOTICE, "%s", output);
}

static void
initStats(aggStats *state, int ub_rank_ind, int lb_rank_ind, int agg_ind, int32 window, TupleDesc tupdesc){
	int size = 16;
	state->ub_rank_ind = ub_rank_ind;
	state->lb_rank_ind = lb_rank_ind;
	state->agg_ind = agg_ind;
	state->window = DirectFunctionCall1(int4_numeric, Int32GetDatum(window));
	state->curval = DirectFunctionCall1(int4_numeric, Int64GetDatum((int64)0));
	state->cetval = DirectFunctionCall1(int4_numeric, Int64GetDatum((int64)0));
	state->posval = DirectFunctionCall1(int4_numeric, Int64GetDatum((int64)0));
	state->ub_sort = newA(size, ub_rank_ind, tupdesc);
	state->window_sort = newA(size, ub_rank_ind, tupdesc);
	state->window_agg = newA(size, agg_ind, tupdesc);
	state->window_cert = newA(size, lb_rank_ind, tupdesc);
}

static void
initSortStats(sortStats *state, int ub_order_ind, int lb_order_ind, int ub_rank_ind, int lb_rank_ind, int32 k, TupleDesc tupdesc){
	int size = 16;
	state->ub_order_ind = ub_order_ind;
	state->lb_order_ind = lb_order_ind;
	state->ub_rank_ind = ub_rank_ind;
	state->lb_rank_ind = lb_rank_ind;
	state->k = k;
	state->lb_rank = (int32)1;
	state->ub_rank = (int32)0;
	state->ub_sort = newA(size, ub_order_ind, tupdesc);
	state->bg_sort = newA(size, ub_order_ind, tupdesc);
}

/* main algorithm */
static void
processTuple(aggStats *state, HeapTuple tup, Tuplestorestate *tupstore, TupleDesc tupdesc){
	/* Assume assending order and window are N precedings.
	 * Need 4 heaps
	 * ub_ord sorted by ub_ordername_ranking -- tuples waiting for all
	 * w_ord sorted by lb_ordername_ranking -- window sorted by order attr
	 * w_aggr sorted by lb_aggrname -- window sorted by aggregation attr
	 * w_cert sorted by lb_ordername_ranking -- tuples must be in the result (we want to greedily pop from this.)
	 */
	// int32 window = state->window;
	// Datum dwindow = DirectFunctionCall1(int4_numeric, Int32GetDatum(window));

	HeapTuple 		top;
	HeapTuple 		pop;
	HeapTuple 		extract;
	HeapTuple 		ret;

	DArray *ub_sort = state->ub_sort;
	DArray *window_sort = state->window_sort;
	DArray *window_agg = state->window_agg;
	DArray *window_cert = state->window_cert;

	int ub_sum_ind = SPI_fnumber(tupdesc, "ub_sum");
	int lb_sum_ind = SPI_fnumber(tupdesc, "lb_sum");

	// elog(NOTICE, "processing tuple: =======================================================");
	// printtuple(tup, tupdesc);

	insertTup(tup, ub_sort, NULL);
	// print_heap(ub_sort);

	/*
		main algorithm.
		for each tuple.
	*/
	// Datum bound;

	top = get_max(ub_sort)->item;
	bool isles = DatumGetBool(DirectFunctionCall2(numeric_lt,
    	getKeyValT(state->ub_rank_ind, top, tupdesc),
    	getKeyValT(state->lb_rank_ind, tup, tupdesc)));
	// elog(NOTICE, "=========== comparing tuple: =========");
	// printtuple(top, tupdesc);
	// elog(NOTICE, "== less than ==");
	// printtuple(top, tupdesc);
	// elog(NOTICE, "========== [%i][%i] ========(%i)", state->ub_rank_ind, state->lb_rank_ind, isles);
	while(isles){
		// find all tuples we have a result already.
		pop = extract_max(ub_sort)->item;

		// elog(NOTICE, "pop tuple: ");
		// printtuple(pop, tupdesc);

		top = get_max(ub_sort)->item;
		isles = DatumGetBool(DirectFunctionCall2(numeric_lt,
    		getKeyValT(state->ub_rank_ind, top, tupdesc),
    		getKeyValT(state->lb_rank_ind, tup, tupdesc)));
		//pop unqulified certain members
		// bound = DirectFunctionCall2(numeric_add, getKeyVal(state->lb_rank_ind, get_max(window_cert), tupdesc), dwindow);
		getCertain(state, tupdesc, pop);
		// while(window_cert->cur>0 && DatumGetBool(DirectFunctionCall2(numeric_lt,
  //   		DirectFunctionCall2(numeric_add, getKeyVal(state->ub_rank_ind, get_max(window_cert), tupdesc), dwindow),
  //   		getKeyValT(state->lb_rank_ind, pop, tupdesc)))){
		// 	extract = extract_max(window_cert)->item;
		// 	state->cetval = DirectFunctionCall2(numeric_sub, state->cetval, getKeyValT(state->agg_ind, extract, tupdesc));
		// 	return state->cetval;
		// }
		// elog(NOTICE, "== certain is ==");
		// print_heap(window_cert);
		// elog(NOTICE, "== for ==");
		// printtuple(pop, tupdesc);

		//compute aggregation result for pop
			//todo
		ret = updateTupleBounds(pop, tupdesc, ub_sum_ind, lb_sum_ind, state->posval, state->cetval);
		tuplestore_puttuple(tupstore, ret);
		//if certain push to certain heap
		// bound = DirectFunctionCall2(numeric_add, getKeyValT(state->lb_rank_ind, pop, tupdesc), dwindow);

		if(ub_sort->cur>0 && DatumGetBool(DirectFunctionCall2(numeric_ge,
    		DirectFunctionCall2(numeric_add, getKeyValT(state->lb_rank_ind, pop, tupdesc), state->window),
    		getKeyVal(state->ub_rank_ind, get_max(ub_sort), tupdesc)))){
			insertTup(pop, window_cert, NULL);
			//update certain state value
			state->cetval = DirectFunctionCall2(numeric_add, getKeyValT(state->agg_ind, pop, tupdesc), state->cetval);
		}

		// elog(NOTICE, "== certain is ==");
		// print_heap(window_cert);
	}
	HNode *nd = newN(tup, window_agg);
	insert(window_sort, (void *)nd);
	insert(window_agg, (void *)nd);
	// insertTup(tup, window_sort, NULL);
	// insertTup(tup, window_agg, window_sort);
	state->posval = DirectFunctionCall2(numeric_add, getKeyValT(state->agg_ind, tup, tupdesc), state->posval);

	// elog(NOTICE, "== possible is ==");
	// print_heap(window_sort);

	//pop unqulified tuples in window
	// Datum ubbound = DirectFunctionCall2(numeric_add, getKeyVal(state->ub_rank_ind, get_max(window_sort), tupdesc), state->window);
	// Datum lbbound = getKeyValT(state->lb_rank_ind, tup, tupdesc);
	// elog(NOTICE, "ubbound = %s", DatumGetCString(DirectFunctionCall1(numeric_out, ubbound)));
	// elog(NOTICE, "lbbound = %s", DatumGetCString(DirectFunctionCall1(numeric_out, lbbound)));

	while(window_sort->cur>0 && DatumGetBool(DirectFunctionCall2(numeric_lt,
		DirectFunctionCall2(numeric_add, getKeyVal(state->ub_rank_ind, get_max(window_sort), tupdesc), state->window),
    	getKeyValT(state->lb_rank_ind, tup, tupdesc)))){
		extract = extract_max(window_sort)->item;
		state->posval = DirectFunctionCall2(numeric_sub, state->posval, getKeyValT(state->agg_ind, extract, tupdesc));
	}
	// elog(NOTICE, "== possible is after pop ==");
	// print_heap(window_sort);

	// elog(NOTICE, "state: cetval = %s", DatumGetCString(DirectFunctionCall1(numeric_out, state->cetval)));
	// elog(NOTICE, "state: posval = %s", DatumGetCString(DirectFunctionCall1(numeric_out, state->posval)));
	/*
		end main algorithm

	*/

	return;
}

Datum getCertain(aggStats* state, TupleDesc tupdesc, HeapTuple pop){
	HeapTuple 		extract;
	DArray *window_cert = state->window_cert;
	while(window_cert->cur>0 && DatumGetBool(DirectFunctionCall2(numeric_lt,
    	DirectFunctionCall2(numeric_add, getKeyVal(state->ub_rank_ind, get_max(window_cert), tupdesc), state->window),
    	getKeyValT(state->lb_rank_ind, pop, tupdesc)))){
		extract = extract_max(window_cert)->item;
		state->cetval = DirectFunctionCall2(numeric_sub, state->cetval, getKeyValT(state->agg_ind, extract, tupdesc));
	}
	return state->cetval;
}

HeapTuple
updateTupleBounds(HeapTuple tup, TupleDesc tupdesc, int ub_ind, int lb_ind, Datum ub_val, Datum lb_val){
	int		   	*chattrs;		/* attnums of attributes to change */
	int			chnattrs = 2;	/* # of above */
	Datum	  	*newvals;		/* vals of above */
	bool	   	*newnulls;
	HeapTuple  	ret;

	chattrs = (int *) palloc(2 * sizeof(int));
	newvals = (Datum *) palloc(2 * sizeof(Datum));
	newnulls = (bool *) palloc(2 * sizeof(bool));

	chattrs[0] = ub_ind;
	chattrs[1] = lb_ind;
	newnulls[0] = false;
	newnulls[1] = false;
	newvals[0] = ub_val;
	newvals[1] = lb_val;
	ret = heap_modify_tuple_by_cols(tup, tupdesc, chnattrs, chattrs, newvals, newnulls);
	// elog(NOTICE, "*** fill result to tuple ***");
	// printtuple(tup, tupdesc);
	// elog(NOTICE, "****************************");
	return ret;
}

HeapTuple
updateTupleAttr(HeapTuple tup, TupleDesc tupdesc, int ind, Datum val){
	int		   	*chattrs;		/* attnums of attributes to change */
	int			chnattrs = 1;	/* # of above */
	Datum	  	*newvals;		/* vals of above */
	bool	   	*newnulls;
	HeapTuple  	ret;

	chattrs = (int *) palloc(1 * sizeof(int));
	newvals = (Datum *) palloc(1 * sizeof(Datum));
	newnulls = (bool *) palloc(1 * sizeof(bool));

	chattrs[0] = ind;
	newnulls[0] = false;
	newvals[0] = val;
	ret = heap_modify_tuple_by_cols(tup, tupdesc, chnattrs, chattrs, newvals, newnulls);
	return ret;
}

// static void
// finalizeStats(aggStats state, HeapTuple *tup){
// 	return;
// }
