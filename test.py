import os
import math
import subprocess
#pip3 install pg8000
#import pg8000
import psycopg2
from config import config
import tablegen as mcg
import ctable_solver as csolv
import time
import argparse
from multiprocessing import Process, Manager


import numpy as np
import random
import sys
import copy
import statistics

dir = None
#conn = None
#cur = None
conn2 = None
cur2 = None

mcdbrep = 20

pgport = "5432"

#select * from range_window('sum','a','b','r_range', 1) AS t1(id numeric, a numeric, ub_a numeric, lb_a numeric, b numeric , ub_b numeric, lb_b numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);

#psqlbin = '/Applications/Postgres.app/Contents/Versions/10/bin/psql -p5432 "uadb" -c '
gpromcom = [str("gprom"), "-host", "none", "-db", "/Users/sufeng/git/UADB_Reproducibility/dbs/incomp.db", "-port", "none", "-user", "none", "-passwd", "none", "-loglevel", "0", "-backend", "sqlite", "-Pexecutor", "sql", "-query"]
gprompg = [str("gprom"), "-host", "127.0.0.1", "-db", "postgres", "-port", "%s"%(pgport), "-user", "postgres", "-loglevel", "0", "-backend", "postgres", "-Pexecutor", "sql", "-query"]

def stdoutquery(query, fname, setion='postgresql'):
    conn = None
    cur = None
    query = "COPY ({0}) TO STDOUT".format(query)
    print(query)
    try:
        params = config.config(section=setion)
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)
    except Exception as error:
        print(error)
    cur = conn.cursor()
    #    print(query)
    try:
        with open(fname, 'a') as f:
            cur.copy_expert(query, f)
            conn.commit()
    except Exception as e:
        print("query error: %s"%e)
    #        pass
    cur.close()
    conn.close()
    
def pushQuery(query, setion='postgresql'):
    conn = None
    cur = None
    try:
        params = config.config(section=setion)
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)
    except Exception as error:
        print(error)
    cur = conn.cursor()
#    print(query)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("query error: %s"%e)
#        pass
    cur.close()
    conn.close()
        
def runQuery(query, setion='postgresql'):
    conn = None
    cur = None
    try:
        # read connection parameters
        params = config.config(section=setion)
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
    except Exception as error:
        print(error)
    cur = conn.cursor()
#    print(query)
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("query error: %s"%e)
        pass
#   get outpu if any
    try:
        ret = cur.fetchall()
#        disconnect()
        return ret
    except Exception as e:
        print(e)
        pass
    except TypeError as e:
        print(e)
        pass
    cur.close()
    conn.close()
    
def timeQueryMaterilize(query, setion='postgresql'):
    query = 'EXPLAIN ANALYSE create table dummy as %s'%query
    ret = runQuery(query, setion)
    runQuery("drop table IF EXISTS dummy;")
    rt = ret[-1][0].split()[-2]
    print("time Query: %s."%rt)
    return rt
    
def materializequery(query, tablename='dummy', setion='postgresql'):
    runQuery("drop table IF EXISTS %s;"%(tablename))
    query = 'create table %s as %s'%(tablename,query)
    tm = timeQuery(query, setion)
    print(tm)
    return

def timeQuery(query, setion='postgresql'):
    ret = runQuery('EXPLAIN ANALYSE %s'%query, setion)
    rt = ret[-1][0].split()[-2]
    print("time Query: %s."%rt)
    return rt
    
def timeQueryTimeout(query, ret_dict, setion='postgresql'):
    ret = runQuery('EXPLAIN ANALYSE %s'%query, setion)
    rt = ret[-1][0].split()[-2]
    print("time Query: %s."%rt)
    ret_dict[0] = rt
    
def timeoutQuery(query, tout):
    manager = Manager()
    ret_dict = manager.dict()
    p1 = Process(target=timeQueryTimeout, args=(query,ret_dict,), name='process_timeout')
    p1.start()
    p1.join(timeout=tout)
    p1.terminate()
    if p1.exitcode == 0:
        return ret_dict[0]
    return "-1"

def timeQueryMult(query, setion='postgresql', rep = 5):
    ret = []
    for i in range(rep):
        ret.append(timeQuery(query))
    ret = ret[int(rep/2):]
    retint = [float(i) for i in ret]
    return (', '.join(ret)), str(statistics.mean(retint))
    
def timeQueryMultInput(query, setion='postgresql', targ="radb", rep = 20, x = 0.02, s = 1):
    ret = []
    rsec = "_"+str(int(x*100))+"_"+str(int(s*100)) +"_"+ targ
    print(rsec)
    query = query.replace("_"+targ, rsec)
    for i in range(rep):
        ret.append(timeQuerySel(query))
    ret = ret[int(rep/2):]
    retint = [float(i) for i in ret]
    return (', '.join(ret)), str(statistics.mean(retint))
    
def timeQueryFile(filename='pdQuery/Q3.sql', setion='postgresql', targ="radb", rep = 20, x = 0.02, s = 1):
    with open(filename) as fp:
        query = fp.read()
        return timeQueryMultInput(query, setion, targ, rep, x, s)
        
def timeQueryFileDef(filename='pdQuery/Q3.sql', setion='postgresql', rep = 20):
    with open(filename) as fp:
        query = fp.read()
        return timeQueryMult(query, setion, rep)
        
    
def sizeQuery(query, setion='postgresql'):
    ret = runQuery('select count(*) from (%s) xyz;'%query.split(";")[0], setion)
    return ret[-1][0]
    
def writetofile(fn, content):
    with open(fn,"w+") as f:
        f.write(content)
        f.close()
    
def exittest():
#    print("Closing postgres servers")
#    os.system('sudo -u postgres /usr/lib/postgresql/10/bin/pg_ctl -D /home/perm/su/pgdata -m fast stop')
#    os.system('sudo -u postgres /maybms/install/bin/pg_ctl -D /maybms/data -m fast stop')
#    time.sleep(10)
    print("Test done")
    quit()
    
def importmicrotable(colnum, rolnum, rangeval, uncert, minval, maxval, tn="range_r", aoff=0):
    tname = tn
    attrs = mcg.tablegen(colnum, rolnum, rangeval, uncert, minval, maxval, aoff)
    pushQuery('drop table if exists %s'%(tname))
    tableinitq = "create table %s ("%(tname)
    for an in list(attrs.split(",")) :
        tableinitq += "%s numeric,"%(an)
    tableinitq = tableinitq[:-1]+");"
    print(tableinitq)
    pushQuery(tableinitq)
    tablecopy = "copy %s from '%s/micro.csv' DELIMITER ',' CSV HEADER;"%(tname, dir)
    print(tablecopy)
    pushQuery(tablecopy)
    return attrs
    
def importmicrotable_window(rolnum, rangeval1, uncert1, rangeval2, uncert2, minval, maxval, tn="range_r", aoff=1):
    tname = tn
    tmpname = tn + "_tmp"
    attrs = mcg.tablegen_window(rolnum, rangeval1, uncert1, rangeval2, uncert2, minval, maxval, aoff)
    pushQuery('drop table if exists %s'%(tname))
    pushQuery('drop table if exists %s'%(tmpname))
    tableinitq = "create table %s ("%(tmpname)
    for an in list(attrs.split(",")) :
        tableinitq += "%s numeric,"%(an)
    tableinitq = tableinitq[:-1]+");"
    print(tableinitq)
    pushQuery(tableinitq)
    tablecopy = "copy %s from '%s/micro.csv' DELIMITER ',' CSV HEADER;"%(tmpname, dir)
    print(tablecopy)
    pushQuery(tablecopy)
    idtable = "create table " + tname + " as select CAST(row_number() over() as numeric) as id,* from " + tmpname + ";"
    print(idtable)
    pushQuery(idtable)
    return "id"+attrs
    
def importmicrotablefromtidb(colnum, rolnum, rangeval, uncert, minval, maxval, tsize, tn="range_r", aoff=0):
    tname = tn
    tiname = tn+"_tidb"
    attrs = mcg.tablegentidb(colnum, rolnum, rangeval, uncert, minval, maxval,tsize,aoff)
    pushQuery('drop table if exists %s'%(tname))
    pushQuery('drop table if exists %s'%(tiname))
    titableinitq = "create table %s ("%(tiname)
    for an in list(attrs.split(",")):
        titableinitq += "%s numeric,"%(an)
    titableinitq = titableinitq[:-1]+");"
    print(titableinitq)
    pushQuery(titableinitq)
    tablecopy = "copy %s from '%s/microtidb.csv' DELIMITER ',' CSV HEADER;"%(tiname, dir)
    print(tablecopy)
    pushQuery(tablecopy)
    autable = "create table " + tname + " as select"
    bdlist = ""
    print(attrs)
    for an in list(attrs.split(",")):
        if an=="id":
            autable += " id,"
        else:
            autable += " min(" + an + ") as " +an+","
            bdlist += " max(" + an + ") as ub_" +an+", min(" + an + ") as lb_" +an+","
    autable += (bdlist+"1 as cet_r, 1 as bst_r, 1 as pos_r from " + tiname + " group by id;")
    pushQuery(autable)
    return attrs
    
    
def test_window_varWindowRange():

    print("[altering Window range]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000

    minval = 1
    maxval = 30000

    aggrange = 2000
    
    minrange = 1000
    maxrange = 20000
    rangeintval = 2000 #uncertain attribute range
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.05

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
    
    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
    
    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    mcdb10 = []
    mcdb20 = []
    onlyr = []
    ourr = []
    rojr = []
    rojir = []
    
    last_jr = 1
    
    timeout = 1200
    
    maxx = 0
    maxy = 0
    
    for i in range(minrange, maxrange, rangeintval):
        varrec.append(str(i))
        maxx = i
        attrs = importmicrotable_window(rolnum, i, uncert, aggrange, uncert, minval, maxval)
        print("testing det query")
        resm = timeoutQuery(detquery, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        detr.append(resm)
        print("testing ranks only")
        resm = timeoutQuery(only_ranks, timeout)
        onlyr.append(resm)
        if float(resm) > maxy:
            maxy = float(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        if float(resm)*22 > maxy:
            maxy = float(resm)*22
        print("testing extension")
        resm = timeoutQuery(our_imp, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
#        if float(resm) > maxy:
#            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
    
    print("[Window: altering Window Range]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\n"
#        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\t" + str(rojr[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_changeWindowRange.csv"
    
    writetofile(fname, retstr)
    
    rn = plotwindow(fname, maxx*1.2, maxy*1.5, "Range")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")


def test_window_varWindowUncert():

    print("[altering Window Uncertainty]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000

    minval = 1
    maxval = 30000

    aggrange = 3000
    
    minrange = 10
    maxrange = 300
    rangeintval = 50 #uncertain attribute range
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.05

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
    
    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
    
    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    mcdb10 = []
    mcdb20 = []
    onlyr = []
    ourr = []
    rojr = []
    rojir = []
    
    last_jr = 1
    
    timeout = 1200
    
    maxx = 0
    maxy = 0
    
    for ix in range(minrange, maxrange, rangeintval):
        i = float(ix)/100.0
        varrec.append(str(i))
        maxx = i
        attrs = importmicrotable_window(rolnum, aggrange, i, aggrange, uncert, minval, maxval)
        print("testing det query")
        resm = timeoutQuery(detquery, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        detr.append(resm)
        print("testing ranks only")
        resm = timeoutQuery(only_ranks, timeout)
        onlyr.append(resm)
        if float(resm) > maxy:
            maxy = float(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        if float(resm)*22 > maxy:
            maxy = float(resm)*22
        print("testing extension")
        resm = timeoutQuery(our_imp, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
#        if float(resm) > maxy:
#            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
    
    print("[Window: altering Window Uncert]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\n"
#        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\t" + str(rojr[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_changeWindowUncert.csv"
    
    writetofile(fname, retstr)
    
    rn = plotwindow(fname, maxx*1.2, maxy*1.5, "Uncert")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")

def test_window_varAggRange():

    print("[altering Aggregation range]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000

    minval = 1
    maxval = 30000

    aggrange = 2000
    
    minrange = 1000
    maxrange = 20000
    rangeintval = 2000 #uncertain attribute range
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.05

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
    
    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
    
    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    mcdb10 = []
    mcdb20 = []
    onlyr = []
    ourr = []
    rojr = []
    rojir = []
    
    last_jr = 1
    
    timeout = 1200
    
    maxx = 0
    maxy = 0
    
    for i in range(minrange, maxrange, rangeintval):
        varrec.append(str(i))
        maxx = i
        attrs = importmicrotable_window(rolnum, aggrange, uncert, i, uncert, minval, maxval)
        print("testing det query")
        resm = timeoutQuery(detquery, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        detr.append(resm)
        print("testing ranks only")
        resm = timeoutQuery(only_ranks, timeout)
        onlyr.append(resm)
        if float(resm) > maxy:
            maxy = float(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        if float(resm)*22 > maxy:
            maxy = float(resm)*22
        print("testing extension")
        resm = timeoutQuery(our_imp, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
#        if float(resm) > maxy:
#            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
    
    print("[Window: altering Window Range]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\n"
#        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\t" + str(rojr[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_changeAggRange.csv"
    
    writetofile(fname, retstr)
    
    rn = plotwindow(fname, maxx*1.2, maxy*1.5, "Range")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")


def test_window_varAggUncert():

    print("[altering Aggregation Uncertainty]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000

    minval = 1
    maxval = 30000

    aggrange = 3000
    
    minrange = 10
    maxrange = 300
    rangeintval = 50 #uncertain attribute range
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.2

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
    
    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
    
    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    mcdb10 = []
    onlyr = []
    ourr = []
    rojr = []
    rojir = []
    mcdb20 = []
    
    last_jr = 1
    
    timeout = 1200
    
    maxx = 0
    maxy = 0
    
    for ix in range(minrange, maxrange, rangeintval):
        i = float(ix)/100.0
        varrec.append(str(i))
        maxx = i
        attrs = importmicrotable_window(rolnum, aggrange, uncert, aggrange, i, minval, maxval)
        print("testing det query")
        resm = timeoutQuery(detquery, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        detr.append(resm)
        print("testing ranks only")
        resm = timeoutQuery(only_ranks, timeout)
        onlyr.append(resm)
        if float(resm) > maxy:
            maxy = float(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        if float(resm)*22 > maxy:
            maxy = float(resm)*22
        print("testing extension")
        resm = timeoutQuery(our_imp, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
#        if float(resm) > maxy:
#            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
    
    print("[Window: altering Agg Uncert]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\n"
#        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\t" + str(rojr[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_changeAggUncert.csv"
    
    writetofile(fname, retstr)
    
    rn = plotwindow(fname, maxx*1.2, maxy*1.5, "Uncert")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")
    
def test_window_varScale():

    print("[altering scale]")
    
    resname = "range_r"

    colnum = 3
    rolcur = 100
    rolnumax = 150000

    minval = 1
    maxval = 5000

    rangeval = 100
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.01

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
    
    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
    
    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    ourr = []
    rojr = []
    rojir = []
    mcdb10 = []
    mcdb20 = []
    rojindex = []
    
    last_jr = 1
    
    timeout = 6000
    
    maxx = 0
    maxy = 0
    
    while rolcur < rolnumax:
        varrec.append(str(rolcur))
        maxx = rolcur
        attrs = importmicrotablefromtidb(colnum, rolcur, rangeval, uncert, minval, rolcur, 10)
        print("testing det query only")
        resm = timeoutQuery(detquery, timeout)
        detr.append(resm)
#        print("testing ranks only")
#        resm = timeoutQuery(only_ranks, timeout)
#        onlyr.append(resm)
        print("testing extension")
        resm = timeoutQuery(our_imp, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        print("testing range overlap join")
        if float(last_jr) > 0:
            resm = timeoutQuery(range_overlap_join, timeout)
            if float(resm) > maxy:
                maxy = float(resm)
            last_jr = resm
            rojr.append(resm)
        else:
            rojr.append(last_jr)
        print("creating index")
        materializequery(create_range_table, 'ranged_index')
        pushQuery(index_range)
#        resm = timeoutQuery(index_range, timeout)
#        rojindex.append(resm)
        print("testing index join")
        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        rojir.append(resm)
        if float(resm) < 0:
            break
        
        rolcur = rolcur*2
    
    print("[Window: altering Data Scale]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(rojr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_changeScale.csv"
    
    writetofile(fname, retstr)
    
    print(maxx*1.1)
    print(maxy*1.5)
    rn = plotwindowScale(fname, maxx*1.1, maxy*1.5)
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
        
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(detr))
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")
    
    
#    attrs = importmicrotablefromtidb(colnum, rolnum, rangeval, uncert, minval, maxval, 19)
#    ret = runQuery(qtidb)
#    tn = float(ret[-1][0])
#    ret = runQuery(qaudb)
#    an = float(ret[-1][0])
#    pct = (an-tn)/(tn)*100

def test_window_varScaleLess():

    print("[altering scale]")
    
    resname = "range_r"

    colnum = 3
    rolcur = 100
    rolnumax = 10000000

    minval = 1
    maxval = 5000

    rangeval = 100
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.01

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
    
    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
    
    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    ourr = []
    rojr = []
    rojir = []
    mcdb10 = []
    mcdb20 = []
    
    last_jr = 1
    
    timeout = 600
    
    maxx = 0
    maxy = 0
    
    while rolcur < rolnumax:
        varrec.append(str(rolcur))
        maxx = rolcur
        attrs = importmicrotablefromtidb(colnum, rolcur, rangeval, uncert, minval, rolcur, 19)
        print("testing det query only")
        resm = timeoutQuery(detquery, timeout)
        detr.append(resm)
#        print("testing ranks only")
#        resm = timeoutQuery(only_ranks, timeout)
#        onlyr.append(resm)
        print("testing extension")
        resm = timeoutQuery(our_imp, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        if float(resm)*22 > maxy:
            maxy = float(resm)*22
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
        
        rolcur = rolcur*2
    
    print("[Window: altering Data Scale Less]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\t\n"
    print(retstr)
    
    fname = "window_changeScale.csv"
    
    writetofile(fname, retstr)
    
    print(maxx*1.1)
    print(maxy*1.5)
    rn = plotwindowScaleLess(fname, maxx*1.1, maxy*1.5)
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
        
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(detr))
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")
    
    
#    attrs = importmicrotablefromtidb(colnum, rolnum, rangeval, uncert, minval, maxval, 19)
#    ret = runQuery(qtidb)
#    tn = float(ret[-1][0])
#    ret = runQuery(qaudb)
#    an = float(ret[-1][0])
#    pct = (an-tn)/(tn)*100

def test_window_varWindowsize():

    print("[altering Window Size]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000

    minval = 1
    maxval = 30000

    aggrange = 1000
    
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.01

    minsize_w = 1
    maxsize_w = 10
    sizeinc_w = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id
    
    varrec = []
    detr = []
    mcdb = []
    onlyr = []
    ourr = []
    rojr = []
    rojir = []
    
    last_jr = 1
    
    timeout = 1200
    
    maxx = 0
    maxy = 0
    
    for i in range(minsize_w, maxsize_w, sizeinc_w):
    
        detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND %i FOLLOWING) AS f FROM range_r;"%(i)
    
        only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank limit %i;"%(i)
    
        our_imp = "select * from range_window('sum','a2','a1','range_r', %i) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"%(i)
    
        range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
        pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
        create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
    
        index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
    
        pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
        varrec.append(str(i))
        maxx = i
        attrs = importmicrotable_window(rolnum, aggrange, uncert, aggrange, uncert, minval, maxval)
        print("testing det query")
        resm = timeoutQuery(detquery, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        detr.append(resm)
        print("testing ranks only")
        resm = timeoutQuery(only_ranks, timeout)
        onlyr.append(resm)
        if float(resm) > maxy:
            maxy = float(resm)
        print("testing mcdb")
        resm = timeoutQuery(detquery, timeout)
        mcdb.append(str(float(resm)*mcdbrep))
        if float(resm)*mcdbrep > maxy:
            maxy = float(resm)*mcdbrep
        print("testing extension")
        resm = timeoutQuery(our_imp, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
#        if float(resm) > maxy:
#            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
    
    print("[Window: altering Window size]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\n"
#        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\t" + str(rojr[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_changeWindowSize.csv"
    
    writetofile(fname, retstr)
    
    rn = plotwindow(fname, maxx*1.2, maxy*1.3, "Size")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")

def test_window_full_varyingRange():
    print("[Full window (partition) altering Range]")
    
    resname = "range_r"

    colnum = 4
    rolnum = 5000

    minval = 1
    maxval = 5000

    aggrange = 20
    
    minrange = 20
    maxrange = 400
    rangeintval = 40 #uncertain attribute range
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.01

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, a2, sum(a3) over (PARTITION BY a1 ORDER BY a2 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
#    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
#    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    our_rewrite = "With joinq as (select r1.id as id, r1.a1 as a1, r1.ub_a1 as ub_a1, r1.lb_a1 as lb_a1, r1.a2 as a2, r1.ub_a2 as ub_a2, r1.lb_a2 as lb_a2, r2.id as rid, r2.a1 as ra1, r2.ub_a1 as ub_ra1, r2.lb_a1 as lb_ra1, r2.a2 as ra2, r2.ub_a2 as ub_ra2, r2.lb_a2 as lb_ra2 from range_r r1 join range_r r2 on r1.lb_a1<=r2.lb_a1 and r1.ub_a1>=r2.lb_a1), endpoints as (select *, lb_ra2 as pt, 0::numeric AS isend from joinq UNION ALL select *, ub_ra2 as pt, 1::numeric AS isend from joinq), bounds as (select *, ( CASE WHEN isend=0 THEN COALESCE(SUM( isend ) OVER ( PARTITION BY id ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )) OVER (PARTITION BY id ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranked as (SELECT id,min(a1) as a1,min(ub_a1) as ub_a1,min(lb_a1) as lb_a1,min(a2) as a2,min(ub_a2) as ub_a2,min(lb_a2) as lb_a2,rid,min(ra1) as ra1,min(ub_ra1) as ub_ra1,min(lb_ra1) as lb_ra1,min(ra2) as ra2, min(ub_ra2) as ub_ra2,min(lb_ra2) as lb_ra2, max( UB_rank ) AS ub_rank, max( LB_rank )+1 AS lb_rank FROM bounds GROUP BY id,rid), rankproj as (select id,a1,ub_a1,lb_a1,a2,ub_a2,lb_a2,rid,ra1,ub_ra1,lb_ra1,ra2,ub_ra2,lb_ra2,ub_rank,lb_rank,ub_lrank,lb_lrank from ranked lr join (select id as xid, ub_rank as ub_lrank, lb_rank as lb_lrank from ranked where id=rid) rr on lr.id=rr.xid), inwindow as (select * from rankproj where lb_lrank <= ub_rank and ub_lrank+3>=lb_rank), iscert as (select *,case when ub_a1=lb_a1 and ub_ra1=lb_ra1 and ub_a1=ub_ra1 and lb_lrank <= lb_rank and ub_lrank+3 >= ub_rank then 1::numeric else 0::numeric end as iscert from inwindow) select id,ub_a1,lb_a1,ub_ra1,lb_ra1,ub_lrank,lb_lrank,rid,ub_rank,lb_rank,iscert from iscert order by (id,iscert,ra2);"
    
#    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
#
#    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
#
#    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
#
#    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    mcdb10 = []
    onlyr = []
    ourr = []
    rojr = []
    rojir = []
    mcdb20 = []
    
    last_jr = 1
    
    timeout = 1200
    
    maxx = 0
    maxy = 0
    
    for ix in range(minrange, maxrange, rangeintval):
        i = ix
        varrec.append(str(i))
        maxx = i
#        attrs = importmicrotable_window(rolnum, aggrange, uncert, aggrange, i, minval, maxval)
        attrs = importmicrotablefromtidb(colnum, rolnum, i, uncert, 1, rolnum, 10)
        print("testing det query")
        resm = timeoutQuery(detquery, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        detr.append(resm)
#        print("testing joins only")
#        resm = timeoutQuery(range_overlap_join, timeout)
#        onlyr.append(resm)
#        if float(resm) > maxy:
#            maxy = float(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        if float(resm)*22 > maxy:
            maxy = float(resm)*22
        print("testing audb rewrite")
        resm = timeoutQuery(our_rewrite, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
#        if float(resm) > maxy:
#            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
    
    print("[Full Window: altering Agg Range]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\n"
#        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\t" + str(rojr[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_full_changeAggRange.csv"
    
    writetofile(fname, retstr)
    
    rn = plotwindowfull(fname, maxx*1.1, 1000000, "Range")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")

def test_window_full_varyingUncert():
    print("[Full window (partition) altering Uncertainty]")
    
    resname = "range_r"

    colnum = 4
    rolnum = 5000

    minval = 1
    maxval = 5000

    aggrange = 20
    
    minrange = 10
    maxrange = 100
    rangeintval = 20 #uncertain attribute range
#    uncert = 0.03 #uncertainty percentage

    uncert = 0.2

    cf = 1

#    gp = "select a1 as gp, max(ub_a1) as ub_gp, min(lb_a1) as lb_gp from micro group by a1;"

#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), pos_win AS (SELECT l.id, l.a1, l.ub_a1, l.lb_a1, l.a2, l.ub_a2, l.lb_a2, l.pos_r, r.a2 as a22, r.ub_a2 as ub_a22, r.lb_a2 as lb_a22 FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank + 2 >= l.lb_rank) SELECT id, a1, ub_a1, lb_a1, a2, ub_a2, lb_a2, sum(pos_r) OVER (PARTITION BY id ORDER BY lb_a22) AS ROW_CONTRIB, sum(pos_r*lb_a22) OVER (PARTITION BY id ORDER BY lb_a22) as SUM_CONTRIB FROM pos_win;"
    
#    pg_range_join = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
#    qtidb = "EXPLAIN ANALYSE WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    #
    
#    createranged = "create table ranged as (WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"

#    qaudb = "select count(t2.id) from (select a1 as a1, max(ub_a1) as ub_a1, min(lb_a1) as lb_a1 from micro group by a1) t1, micro t2 where t1.ub_a1 >= t2.lb_a1 and t2.ub_a1 >= t1.lb_a1;" #and t1.id != t2.id

    detquery = "select a1, a2, sum(a3) over (PARTITION BY a1 ORDER BY a2 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
#    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
#    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    our_rewrite = "With joinq as (select r1.id as id, r1.a1 as a1, r1.ub_a1 as ub_a1, r1.lb_a1 as lb_a1, r1.a2 as a2, r1.ub_a2 as ub_a2, r1.lb_a2 as lb_a2, r2.id as rid, r2.a1 as ra1, r2.ub_a1 as ub_ra1, r2.lb_a1 as lb_ra1, r2.a2 as ra2, r2.ub_a2 as ub_ra2, r2.lb_a2 as lb_ra2 from range_r r1 join range_r r2 on r1.lb_a1<=r2.lb_a1 and r1.ub_a1>=r2.lb_a1), endpoints as (select *, lb_ra2 as pt, 0::numeric AS isend from joinq UNION ALL select *, ub_ra2 as pt, 1::numeric AS isend from joinq), bounds as (select *, ( CASE WHEN isend=0 THEN COALESCE(SUM( isend ) OVER ( PARTITION BY id ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )) OVER (PARTITION BY id ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranked as (SELECT id,min(a1) as a1,min(ub_a1) as ub_a1,min(lb_a1) as lb_a1,min(a2) as a2,min(ub_a2) as ub_a2,min(lb_a2) as lb_a2,rid,min(ra1) as ra1,min(ub_ra1) as ub_ra1,min(lb_ra1) as lb_ra1,min(ra2) as ra2, min(ub_ra2) as ub_ra2,min(lb_ra2) as lb_ra2, max( UB_rank ) AS ub_rank, max( LB_rank )+1 AS lb_rank FROM bounds GROUP BY id,rid), rankproj as (select id,a1,ub_a1,lb_a1,a2,ub_a2,lb_a2,rid,ra1,ub_ra1,lb_ra1,ra2,ub_ra2,lb_ra2,ub_rank,lb_rank,ub_lrank,lb_lrank from ranked lr join (select id as xid, ub_rank as ub_lrank, lb_rank as lb_lrank from ranked where id=rid) rr on lr.id=rr.xid), inwindow as (select * from rankproj where lb_lrank <= ub_rank and ub_lrank+3>=lb_rank), iscert as (select *,case when ub_a1=lb_a1 and ub_ra1=lb_ra1 and ub_a1=ub_ra1 and lb_lrank <= lb_rank and ub_lrank+3 >= ub_rank then 1::numeric else 0::numeric end as iscert from inwindow) select id,ub_a1,lb_a1,ub_ra1,lb_ra1,ub_lrank,lb_lrank,rid,ub_rank,lb_rank,iscert from iscert order by (id,iscert,ra2);"
    
#    pg_range_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
#
#    create_range_table = "(WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks);"
#
#    index_range = "CREATE INDEX ranged_index_idx ON ranged_index USING GIST (rank);"
#
#    pg_indexed_rangejoin = "SELECT l.id, r.id, l.rank, r.rank FROM ranged_index l, ranged_index r WHERE r.rank && l.rank;"
    
    varrec = []
    detr = []
    mcdb10 = []
    onlyr = []
    ourr = []
    rojr = []
    rojir = []
    mcdb20 = []
    
    last_jr = 1
    
    timeout = 1200
    
    maxx = 0
    maxy = 0
    
    for ix in range(minrange, maxrange, rangeintval):
        i = float(ix)/100.0
        varrec.append(str(i))
        maxx = i
#        attrs = importmicrotable_window(rolnum, aggrange, uncert, aggrange, i, minval, maxval)
        attrs = importmicrotablefromtidb(colnum, rolnum, aggrange, i, 1, rolnum, 10)
        print("testing det query")
        resm = timeoutQuery(detquery, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        detr.append(resm)
#        print("testing joins only")
#        resm = timeoutQuery(range_overlap_join, timeout)
#        onlyr.append(resm)
#        if float(resm) > maxy:
#            maxy = float(resm)
        print("testing mcdb10")
        resm = timeoutQuery(detquery, timeout)
        mcdb10.append(str(float(resm)*11))
        print("testing mcdb20")
        resm = timeoutQuery(detquery, timeout)
        mcdb20.append(str(float(resm)*22))
        if float(resm)*22 > maxy:
            maxy = float(resm)*22
        print("testing audb rewrite")
        resm = timeoutQuery(our_rewrite, timeout)
        if float(resm) > maxy:
            maxy = float(resm)
        ourr.append(resm)
#        print("testing range overlap join")
#        if float(last_jr) > 0:
#            resm = timeoutQuery(range_overlap_join, timeout)
#            if float(resm) > maxy:
#                maxy = float(resm)
#            last_jr = resm
#            rojr.append(resm)
#        else:
#            rojr.append(last_jr)
#        print("creating index")
#        materializequery(create_range_table, 'ranged_index')
#        pushQuery(index_range)
#        print("testing index join")
#        resm = timeoutQuery(pg_indexed_rangejoin, timeout)
#        if float(resm) > maxy:
#            maxy = float(resm)
#        rojir.append(resm)
#        if float(resm) < 0:
#            break
    
    print("[Full Window: altering Agg Uncert]")
    retstr = ""
    for i in range(len(varrec)):
        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb10[i]) + "\t" + str(mcdb20[i]) + "\n"
#        retstr = retstr + str(varrec[i]) + "\t" + str(detr[i]) + "\t" + str(onlyr[i]) + "\t" + str(ourr[i]) + "\t" + str(mcdb[i]) + "\t" + str(rojr[i]) + "\t" + str(rojir[i]) + "\n"
    print(retstr)
    
    fname = "window_full_changeAggUncert.csv"
    
    writetofile(fname, retstr)
    
    rn = plotwindowfull(fname, maxx*1.1, 1000000, "Uncertainty")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
#    print("*******************************")
#    print(", ".join(varrec))
#    print("*******************************")
#    print(", ".join(onlyr))
#    print(", ".join(ourr))
#    print(", ".join(rojr))
#    print(", ".join(rojir))
#    print("*******************************")

def plottopk(fn, maxx, maxy, xlab, miny = 0):
    with open("%s.gp"%fn, "w+") as file:
        file.write("\n".join([
            "set size ratio 0.6",
            "set terminal postscript color enhanced",
            "set output '%s.ps'"%fn,
            "unset title",
            "set tmargin 0",
            "set bmargin 1",
            "set rmargin 0",
            "set lmargin -9",
            "set border 3 front linetype -1 linewidth 1.500",
            "set style fill solid 0.65 border -1",
            'set xlabel font "Arial,35" offset 0,-1',
            'set xlabel "%s"'%(xlab),
            'set xtics font "Arial,32" offset 0,-0.5',
            "set for [i=1:4] linetype i dt i",
            'set style line 1 lt 1 lc rgb "#FFDD11"  lw 9',
            'set style line 2 lt 1 lc rgb "black" lw 9',
            'set style line 3 lt 1 lc rgb "green" lw 9',
            'set style line 4 lt 1 lc rgb "blue" lw 9',
            'set style line 5 lt 1 lc rgb "red" lw 9',
            'set style line 6 lt 1 lc rgb "#110099" lw 9',
            'set ylabel "Time (ms)" font "Arial,32"',
            'set ylabel offset character -2, 0, 0',
            'set ytics font "Arial,32"',
            'set key inside left top vertical Left noreverse noenhanced autotitle nobox maxrows 2',
            'set key font "Arial,30"',
            'set key spacing 1',
            'set key samplen 3',
            'set key width 0',
#            'set format x "%g%%"',
            'set format x "%.0s%c"',
            'set ytics (200,400,600,800,1000,1200)',
            'set grid nopolar',
            'set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics',
            'set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 3.000',
            "set xrange [ 0 : %s ] noreverse nowriteback"%(str(maxx)),
            "set yrange [ %s : %s ] noreverse nowriteback"%(str(miny),str(maxy)),
#            'plot "%s" using ($1*100):2 title "Det" with lines linestyle 1, "%s" using ($1*100):3 title "Imp" with lines linestyle 5, "%s" using ($1*100):4 title "Rewr" with lines linestyle 3, "%s" using ($1*100):5 title "MCDB10" with lines linestyle 2, "%s" using ($1*100):6 title "MCDB20" with lines linestyle 4'%(fn,fn,fn,fn,fn)
            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Imp" with lines linestyle 5, "%s" using 1:4 title "Rewr" with lines linestyle 3, "%s" using 1:5 title "MCDB10" with lines linestyle 2, "%s" using 1:6 title "MCDB20" with lines linestyle 4'%(fn,fn,fn,fn,fn)
        ]))
        file.close()
        subprocess.call(["gnuplot", "%s.gp"%fn])
        subprocess.call(["ps2pdf", "%s.ps"%fn, "%s.pdf"%fn])
        subprocess.call(["rm", "%s.gp"%fn])
        subprocess.call(["rm", "%s.ps"%fn])
        return "%s.pdf"%fn
    
def plottopkScale(fn, maxx, maxy, minx = 512.00):
    with open("%s.gp"%fn, "w+") as file:
        file.write("\n".join([
            "set size ratio 0.6",
            "set terminal postscript color enhanced",
            "set output '%s.ps'"%fn,
            "unset title",
            "set tmargin 0",
            "set bmargin 1",
            "set rmargin 0",
            "set lmargin -9",
            "set border 3 front linetype -1 linewidth 1.500",
            "set style fill solid 0.65 border -1",
            'set xlabel font "Arial,35" offset 0,-1',
            'set xlabel "Data size"',
            'set xtics font "Arial,28"',
            'set xtics offset 0, -0.5',
            "set for [i=1:7] linetype i dt i",
            'set style line 1 lt 1 lc rgb "#FFDD11"  lw 9',
            'set style line 2 lt 1 lc rgb "black" lw 9',
            'set style line 3 lt 1 lc rgb "green" lw 9',
            'set style line 4 lt 1 lc rgb "blue" lw 9',
            'set style line 5 lt 1 lc rgb "red" lw 9',
            'set style line 6 lt 1 lc rgb "purple" lw 9',
            'set style line 7 lt 1 lc rgb "brown" lw 9',
            'set ylabel "Time (ms)" font "Arial,32"',
            'set ylabel offset character -3, 0, 0',
            'set ytics font "Arial,32"',
            "set logscale y 10",
            "set logscale x 2",
            "set format x '2^{%L}'",
            "set format y '10^{%T}'",
            'set key inside left top vertical Left noreverse noenhanced autotitle nobox',
            'set key font "Arial,25"',
            'set key spacing 1',
            'set key samplen 2.5',
            'set key width 0',
            'set grid nopolar',
            'set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics',
            'set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 3.000',
            "set xrange [ %s : %s ] noreverse nowriteback"%(str(minx),str(maxx)),
            "set yrange [ 0.1 : %s ] noreverse nowriteback"%(str(maxy)),
            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Imp" with lines linestyle 5, "%s" using 1:4 title "Rewr" with lines linestyle 3, "%s" using 1:5 title "MCDB10" with lines linestyle 2, "%s" using 1:6 title "MCDB20" with lines linestyle 4, "%s" using 1:7 title "Symb" with lines linestyle 6,"%s" using 1:8 title "PT-k" with lines linestyle 7'%(fn,fn,fn,fn,fn,fn,fn)
        ]))
        file.close()
        subprocess.call(["gnuplot", "%s.gp"%fn])
        subprocess.call(["ps2pdf", "%s.ps"%fn, "%s.pdf"%fn])
        subprocess.call(["rm", "%s.gp"%fn])
        subprocess.call(["rm", "%s.ps"%fn])
        return "%s.pdf"%fn
        
def plotRankAccuracy(fn, maxx, maxy, xlab, miny = 0):
    with open("%s.gp"%fn, "w+") as file:
        file.write("\n".join([
            "set size ratio 0.4",
            "set terminal postscript color enhanced",
            "set output '%s.ps'"%fn,
            "unset title",
            "set tmargin 0",
            "set bmargin 1",
            "set rmargin 0",
            "set lmargin -9",
            "set border 3 front linetype -1 linewidth 1.500",
            "set style fill solid 0.65 border -1",
            'set xlabel font "Arial,35" offset 0,-1',
            'set xlabel "%s"'%(xlab),
            'set xtics font "Arial,32" offset 0,-0.3',
            "set for [i=1:6] linetype i dt i",
            'set style line 1 lt 1 lc rgb "#FFDD11"  lw 9',
            'set style line 2 lt 1 lc rgb "black" lw 9',
            'set style line 3 lt 1 lc rgb "green" lw 9',
            'set style line 4 lt 1 lc rgb "blue" lw 9',
            'set style line 5 lt 1 lc rgb "red" lw 9',
            'set style line 6 lt 1 lc rgb "#110099" lw 9',
            'set ylabel "Estimated value range" font "Arial,32"',
            'set ylabel offset character -1.5, 0, 0',
            'set ytics font "Arial,32"',
            'set key inside left top vertical Left noreverse noenhanced autotitle nobox',
            'set key font "Arial,30"',
            'set key spacing 1',
            'set key samplen 3',
            'set key width 1',
            'set grid nopolar',
            'set format x "%.0s%c"',
#            'set format x "%g%%"',
            'set arrow from 0.6, 1 to %s, 1 nohead lw 5'%(str(maxx)),
            'set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics',
            'set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 3.000',
            "set xrange [ 0.6 : %s ] noreverse nowriteback"%(str(maxx)),
            "set yrange [ %s : %s ] noreverse nowriteback"%(str(miny),str(maxy)),
#            'plot "%s" using ($1*100):2 title "MCDB10" with lines linestyle 2, "%s" using ($1*100):3 title "MCDB20" with lines linestyle 4, "%s" using ($1*100):(2.0-$4) title "Imp/Rewr" with lines linestyle 5'%(fn,fn,fn)
            'plot "%s" using ($1*10):2 title "MCDB10" with lines linestyle 2, "%s" using ($1*10):3 title "MCDB20" with lines linestyle 4, "%s" using ($1*10):(2.0-$4) title "Imp/Rewr" with lines linestyle 5'%(fn,fn,fn)
        ]))
        file.close()
        subprocess.call(["gnuplot", "%s.gp"%fn])
        subprocess.call(["ps2pdf", "%s.ps"%fn, "%s.pdf"%fn])
        subprocess.call(["rm", "%s.gp"%fn])
        subprocess.call(["rm", "%s.ps"%fn])
        return "%s.pdf"%fn
        
def plotwindow(fn, maxx, maxy, xlab, miny = 1):
    with open("%s.gp"%fn, "w+") as file:
        file.write("\n".join([
            "set size ratio 0.6",
            "set terminal postscript color enhanced",
            "set output '%s.ps'"%fn,
            "unset title",
            "set tmargin 0",
            "set bmargin 1",
            "set rmargin 0",
            "set lmargin -9",
            "set border 3 front linetype -1 linewidth 1.500",
            "set style fill solid 0.65 border -1",
            'set xlabel font "Arial,35" offset 0,-1',
            'set xlabel "%s"'%(xlab),
            'set xtics font "Arial,32"',
            "set for [i=1:4] linetype i dt i",
            'set style line 1 lt 1 lc rgb "#FFDD11"  lw 9',
            'set style line 2 lt 1 lc rgb "black" lw 9',
            'set style line 3 lt 1 lc rgb "green" lw 9',
            'set style line 4 lt 1 lc rgb "blue" lw 9',
            'set style line 5 lt 1 lc rgb "red" lw 9',
            'set style line 6 lt 1 lc rgb "#110099" lw 9',
            'set style line 7 lt 1 lc rgb "yellow" lw 9',
            'set ylabel "Time (ms)" font "Arial,32"',
            'set ylabel offset character -4, 0, 0',
            'set ytics font "Arial,32"',
            'set key inside left top vertical Left noreverse noenhanced autotitle nobox',
            'set key font "Arial,30"',
            'set key spacing 1',
            'set key samplen 2',
            'set key width 0',
            'set grid nopolar',
            'set ytics (500,1000,1500,2000,2500,3000)',
            'set format x "%.0s%c"',
#            'set format x "%g%%"',
            'set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics',
            'set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 3.000',
            "set xrange [ 0.5 : %s ] noreverse nowriteback"%(str(maxx)),
            "set yrange [ %s : %s ] noreverse nowriteback"%(str(miny),str(maxy)),
#            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Rank" with lines linestyle 2, "%s" using 1:4 title "Imp" with lines linestyle 3, "%s" using 1:4 title "MCDB" with lines linestyle 4, "%s" using 1:5 title "Join" with lines linestyle 5, "%s" using 1:6 title "Index" with lines linestyle 6'%(fn,fn,fn,fn,fn,fn)
#            'plot "%s" using ($1*10):2 title "Det" with lines linestyle 1, "%s" using ($1*10):3 title "Imp" with lines linestyle 5, "%s" using ($1*10):4 title "MCDB10" with lines linestyle 2, "%s" using ($1*10):5 title "MCDB20" with lines linestyle 4'%(fn,fn,fn,fn)
            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Imp" with lines linestyle 5, "%s" using 1:4 title "MCDB10" with lines linestyle 2, "%s" using 1:5 title "MCDB20" with lines linestyle 4'%(fn,fn,fn,fn)
        ]))
        file.close()
        subprocess.call(["gnuplot", "%s.gp"%fn])
        subprocess.call(["ps2pdf", "%s.ps"%fn, "%s.pdf"%fn])
        subprocess.call(["rm", "%s.gp"%fn])
        subprocess.call(["rm", "%s.ps"%fn])
        return "%s.pdf"%fn
        
def plotwindowfull(fn, maxx, maxy, xlab):
    with open("%s.gp"%fn, "w+") as file:
        file.write("\n".join([
            "set size ratio 0.6",
            "set terminal postscript color enhanced",
            "set output '%s.ps'"%fn,
            "unset title",
            "set tmargin 0",
            "set bmargin 1",
            "set rmargin 0",
            "set lmargin -9",
            "set border 3 front linetype -1 linewidth 1.500",
            "set style fill solid 0.65 border -1",
            'set xlabel font "Arial,35" offset 0,-1',
            'set xlabel "%s"'%(xlab),
            'set xtics font "Arial,32"',
            "set for [i=1:4] linetype i dt i",
            'set style line 1 lt 1 lc rgb "#FFDD11"  lw 9',
            'set style line 2 lt 1 lc rgb "black" lw 9',
            'set style line 3 lt 1 lc rgb "green" lw 9',
            'set style line 4 lt 1 lc rgb "blue" lw 9',
            'set style line 5 lt 1 lc rgb "red" lw 9',
            'set style line 6 lt 1 lc rgb "#110099" lw 9',
            'set style line 7 lt 1 lc rgb "yellow" lw 9',
            'set ylabel "Time (ms)" font "Arial,32"',
            'set ylabel offset character -4, 0, 0',
            'set ytics font "Arial,32"',
            "set logscale y 10",
            'set key inside left top vertical Left noreverse noenhanced autotitle nobox',
            'set key font "Arial,30"',
            'set key spacing 1',
            'set key samplen 2',
            'set key width 0',
            'set grid nopolar',
#            'set format x "%.0s%c"',
#            'set format x "%g%%"',
            "set format y '10^{%T}'",
            'set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics',
            'set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 3.000',
            "set xrange [ 0 : %s ] noreverse nowriteback"%(str(maxx)),
            "set yrange [ 5 : %s ] noreverse nowriteback"%(str(maxy)),
#            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Rank" with lines linestyle 2, "%s" using 1:4 title "Imp" with lines linestyle 3, "%s" using 1:4 title "MCDB" with lines linestyle 4, "%s" using 1:5 title "Join" with lines linestyle 5, "%s" using 1:6 title "Index" with lines linestyle 6'%(fn,fn,fn,fn,fn,fn)
#            'plot "%s" using ($1*10):2 title "Det" with lines linestyle 1, "%s" using ($1*10):3 title "Rewr" with lines linestyle 3, "%s" using ($1*10):4 title "MCDB10" with lines linestyle 2, "%s" using ($1*10):5 title "MCDB20" with lines linestyle 4'%(fn,fn,fn,fn)
            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Rewr" with lines linestyle 3, "%s" using 1:4 title "MCDB10" with lines linestyle 2, "%s" using 1:5 title "MCDB20" with lines linestyle 4'%(fn,fn,fn,fn)
        ]))
        file.close()
        subprocess.call(["gnuplot", "%s.gp"%fn])
        subprocess.call(["ps2pdf", "%s.ps"%fn, "%s.pdf"%fn])
        subprocess.call(["rm", "%s.gp"%fn])
        subprocess.call(["rm", "%s.ps"%fn])
        return "%s.pdf"%fn
        
def plotwindowScale(fn, maxx, maxy, minx = 80):
    with open("%s.gp"%fn, "w+") as file:
        file.write("\n".join([
            "set size ratio 0.6",
            "set terminal postscript color enhanced",
            "set output '%s.ps'"%fn,
            "unset title",
            "set tmargin 0",
            "set bmargin 1",
            "set rmargin 0",
            "set lmargin -9",
            "set border 3 front linetype -1 linewidth 1.500",
            "set style fill solid 0.65 border -1",
            'set xlabel font "Arial,35" offset 0,-1',
            'set xlabel "Data size"',
            'set xtics font "Arial,28"',
            'set xtics offset 0, -0.5',
            "set for [i=1:7] linetype i dt i",
            'set style line 1 lt 1 lc rgb "#FFDD11"  lw 9',
            'set style line 2 lt 1 lc rgb "black" lw 9',
            'set style line 3 lt 1 lc rgb "green" lw 9',
            'set style line 4 lt 1 lc rgb "blue" lw 9',
            'set style line 5 lt 1 lc rgb "red" lw 9',
            'set style line 6 lt 1 lc rgb "purple" lw 9',
            'set style line 7 lt 1 lc rgb "orange" lw 9',
            'set style line 8 lt 1 lc rgb "#C4A484" lw 9',
            'set ylabel "Time (ms)" font "Arial,32"',
            'set ylabel offset character -3, 0, 0',
            'set ytics font "Arial,32"',
            "set logscale y 10",
            "set logscale x 2",
            "set format x '2^{%L}'",
            "set format y '10^{%T}'",
            'set key inside left top vertical Left noreverse noenhanced autotitle nobox',
            'set key font "Arial,25"',
            'set key spacing 1',
            'set key samplen 2.5',
            'set key width 0',
            'set grid nopolar',
            'set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics',
            'set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 3.000',
            "set xrange [ %s : %s ] noreverse nowriteback"%(str(minx),str(maxx)),
            "set yrange [ 0.1 : %s ] noreverse nowriteback"%(str(maxy)),
            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Imp" with lines linestyle 5, "%s" using 1:4 title "Rewr" with lines linestyle 3, "%s" using 1:5 title "MCDB10" with lines linestyle 2, "%s" using 1:6 title "MCDB20" with lines linestyle 4, "%s" using 1:7 title "Rewr(index)" with lines linestyle 7,"%s" using 1:8 title "Index_create" with lines linestyle 8'%(fn,fn,fn,fn,fn,fn,fn)
        ]))
        file.close()
        subprocess.call(["gnuplot", "%s.gp"%fn])
        subprocess.call(["ps2pdf", "%s.ps"%fn, "%s.pdf"%fn])
        subprocess.call(["rm", "%s.gp"%fn])
        subprocess.call(["rm", "%s.ps"%fn])
        return "%s.pdf"%fn
        
def plotwindowScaleLess(fn, maxx, maxy, minx = 80):
    with open("%s.gp"%fn, "w+") as file:
        file.write("\n".join([
            "set size ratio 0.6",
            "set terminal postscript color enhanced",
            "set output '%s.ps'"%fn,
            "unset title",
            "set tmargin 0",
            "set bmargin 1",
            "set rmargin 0",
            "set lmargin -9",
            "set border 3 front linetype -1 linewidth 1.500",
            "set style fill solid 0.65 border -1",
            'set xlabel font "Arial,35" offset 0,-1',
            'set xlabel "Data size"',
            'set xtics font "Arial,28"',
            'set xtics offset 0, -0.5',
            "set for [i=1:7] linetype i dt i",
            'set style line 1 lt 1 lc rgb "#FFDD11"  lw 9',
            'set style line 2 lt 1 lc rgb "black" lw 9',
            'set style line 3 lt 1 lc rgb "green" lw 9',
            'set style line 4 lt 1 lc rgb "blue" lw 9',
            'set style line 5 lt 1 lc rgb "red" lw 9',
            'set style line 6 lt 1 lc rgb "purple" lw 9',
            'set style line 7 lt 1 lc rgb "orange" lw 9',
            'set ylabel "Time (ms)" font "Arial,32"',
            'set ylabel offset character -3, 0, 0',
            'set ytics font "Arial,32"',
            "set logscale y 10",
            "set logscale x 2",
            "set format x '2^{%L}'",
            "set format y '10^{%T}'",
            'set key inside left top vertical Left noreverse noenhanced autotitle nobox',
            'set key font "Arial,25"',
            'set key spacing 1',
            'set key samplen 2.5',
            'set key width 0',
            'set grid nopolar',
            'set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics',
            'set grid layerdefault   linetype 0 linewidth 1.000,  linetype 0 linewidth 3.000',
            "set xrange [ %s : %s ] noreverse nowriteback"%(str(minx),str(maxx)),
            "set yrange [ 0.1 : %s ] noreverse nowriteback"%(str(maxy)),
            'plot "%s" using 1:2 title "Det" with lines linestyle 1, "%s" using 1:3 title "Imp" with lines linestyle 5, "%s" using 1:4 title "MCDB10" with lines linestyle 2, "%s" using 1:5 title "MCDB20" with lines linestyle 4'%(fn,fn,fn,fn)
        ]))
        file.close()
        subprocess.call(["gnuplot", "%s.gp"%fn])
        subprocess.call(["ps2pdf", "%s.ps"%fn, "%s.pdf"%fn])
        subprocess.call(["rm", "%s.gp"%fn])
        subprocess.call(["rm", "%s.ps"%fn])
        return "%s.pdf"%fn

def test_topk_vark(k=12):
    
    print("[TOP-K: altering k]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000
    rangeval = 500
    
#    uncert = 0.03 #uncertainty percentage
    uncert = 0.05
    
    minval = 1
    maxval = 30000
    
    mink = 2
    maxk = k
    
    
#    rewriting = "Explain analyse select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk where lb_rank <= 5;"
#    implement = "Explain analyse select * from range_topk('range_r', 'a1', 5) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"
#    det = "select * from range_r order by a1 limit 5;"
    
    attrs = importmicrotablefromtidb(colnum, rolnum, rangeval, uncert, minval, maxval, 19)
    
    timeout = 600
    
    ires = []
    impres =[]
    rewriteres = []
    detres = []
    mcdb10 = []
    mcdb20 = []
    
    maxx = 0
    maxy = 0
    
    resstr = ""
    
    for i in range(mink, maxk, 1):
        rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk where lb_rank <= %i;"%(i)
        implement = "select * from range_topk('range_r', 'a1', %i) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"%(i)
        det = "select * from range_r order by a1 limit %i;"%(i)
        print(implement)
#        ires.append(str(i))
        resstr += str(i) + "\t"
        resm = timeoutQuery(det, timeout)
#        impres.append(resm)
        resstr += str(resm) + "\t"
        resm = timeoutQuery(implement, timeout)
#        rewriteres.append(resm)
        resstr += str(resm) + "\t"
        resm = timeoutQuery(rewriting, timeout)
        resstr += str(resm) + "\t"
        maxx = i
        maxy = float(resm)
        resm = timeoutQuery(det, timeout)
        mcdb10.append(str(float(resm)*11))
        resstr += str(float(resm)*11) + "\t"
        resm = timeoutQuery(det, timeout)
        mcdb20.append(str(float(resm)*22))
        resstr += str(float(resm)*22) + "\t\n"

#    rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk;"
#    implement = "select * from range_topk('range_r', 'a1', 0) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"
#    det = "select * from range_r order by a1;"
#    resm = timeoutQuery(implement, timeout)
#    impres.append(resm)
#    resm = timeoutQuery(rewriting, timeout)
#    rewriteres.append(resm)
#    resm = timeoutQuery(det, timeout)
#    detres.append(resm)
#    resm = timeoutQuery(det, timeout)
#    mcdb10.append(str(float(resm)*11))
#    resm = timeoutQuery(det, timeout)
#    mcdb20.append(str(float(resm)*22))
    
#    resstr = ", ".join(ires) + "\n" + ", ".join(detres) + "\n" + ", ".join(impres) + "\n" + ", ".join(rewriteres) + "\n" + ", ".join(mcdb10) + "\n" + ", ".join(mcdb20)
    
#    print("[TOP-K: altering k]")
#    print("*******************************")
#    print(", ".join(ires))
#    print(", ".join(detres))
#    print(", ".join(impres))
#    print(", ".join(rewriteres))
#    print("*******************************")

    print(resstr)
    
    fname = "topk_changek.csv"
    
    writetofile(fname, resstr)
    
    rn = plottopk(fname, maxx*1.1, maxy*1.5, "K")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
    
    
    
def test_topk_varUncert():
    
    print("[TOP-K: altering uncertainty]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000
    
    rangeval = 200
#    uncert = 0.03 #uncertainty percentage
    uncertmin = 0.05
    uncertmax = 0.3
    uncertinc = 0.025
    
    minval = 1
    maxval = 30000
    
    maxx = 0
    maxy = 0
    
    resstr = ""
    
    timeout = 600
    
    uctres = []
    impres =[]
    rewriteres = []
    detres = []
    mcdb10 = []
    mcdb20 = []
    
    for i in np.arange(uncertmin, uncertmax, uncertinc):
        attrs = importmicrotablefromtidb(colnum, rolnum, rangeval, i, minval, maxval, 19)
        rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk;"
        implement = "select * from range_topk('range_r', 'a1', 0) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"
        det = "select * from range_r order by a1;"
        print(implement)
        resstr += str(i) + "\t"
        resm = timeoutQuery(det, timeout)
#        impres.append(resm)
        resstr += str(resm) + "\t"
        resm = timeoutQuery(implement, timeout)
#        rewriteres.append(resm)
        resstr += str(resm) + "\t"
        resm = timeoutQuery(rewriting, timeout)
        resstr += str(resm) + "\t"
        maxx = i
        maxy = float(resm)
        resm = timeoutQuery(det, timeout)
        mcdb10.append(str(float(resm)*11))
        resstr += str(float(resm)*11) + "\t"
        resm = timeoutQuery(det, timeout)
        mcdb20.append(str(float(resm)*22))
        resstr += str(float(resm)*22) + "\t\n"
    
#    print("[TOP-K: altering uncertainty]")
#    print("*******************************")
#    print(", ".join(uctres))
#    print(", ".join(detres))
#    print(", ".join(impres))
#    print(", ".join(rewriteres))
#    print("*******************************")
    print(resstr)
    
    fname = "topk_changeUncert.csv"
    
    writetofile(fname, resstr)
    
    rn = plottopk(fname, maxx*1.1, maxy*1.5, "Uncertainty")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
    
def test_topk_varRange():
    
    print("[TOP-K: altering range]")
    
    resname = "range_r"

    colnum = 3
    rolnum = 30000
    
    rangemin = 1000
    rangemax = 20000
    rangeinc = 2000
#    uncert = 0.03 #uncertainty percentage
    uncert = 0.03
    
    minval = 1
    maxval = 30000
    
    timeout = 600
    
    resstr = ""
    maxx = 0
    maxy = 0
    
    rangeres = []
    impres =[]
    rewriteres = []
    detres = []
    mcdb10 = []
    mcdb20 = []
    
    for i in range(rangemin, rangemax, rangeinc):
        attrs = importmicrotablefromtidb(colnum, rolnum, i, uncert, minval, maxval, 19)
        rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk;"
        implement = "select * from range_topk('range_r', 'a1', 0) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"
        det = "select * from range_r order by a1;"
        print(implement)
        resstr += str(i) + "\t"
        resm = timeoutQuery(det, timeout)
#        impres.append(resm)
        resstr += str(resm) + "\t"
        resm = timeoutQuery(implement, timeout)
#        rewriteres.append(resm)
        resstr += str(resm) + "\t"
        resm = timeoutQuery(rewriting, timeout)
        resstr += str(resm) + "\t"
        maxx = i
        maxy = float(resm)
        resm = timeoutQuery(det, timeout)
        mcdb10.append(str(float(resm)*11))
        resstr += str(float(resm)*11) + "\t"
        resm = timeoutQuery(det, timeout)
        mcdb20.append(str(float(resm)*22))
        resstr += str(float(resm)*22) + "\t\n"
    
#    print("[TOP-K: altering range]")
#    print("*******************************")
#    print(", ".join(rangeres))
#    print(", ".join(detres))
#    print(", ".join(impres))
#    print(", ".join(rewriteres))
#    print("*******************************")

    print(resstr)
    
    fname = "topk_changeRange.csv"
    
    writetofile(fname, resstr)
    
    rn = plottopk(fname, maxx*1.1, maxy*1.5, "Range")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
    
def test_topk_varSize():
    
    print("[TOP-K: altering Data Size (absolute range)]")
    
    resname = "range_r"

    colnum = 3
    rolcur = 1000
    rolnumax = 1000000000
    rolinc = 2
    
    range = 500

#    uncert = 0.03 #uncertainty percentage
    uncert = 0.05
    
    minval = 1
    
    timeout = 600
    
    rangeres = []
    impres =[]
    rewriteres = []
    detres = []
    
    resstr = ""
    maxx = 0
    maxy = 0
    
    while rolcur < rolnumax:
        attrs = importmicrotablefromtidb(colnum, rolcur, range, uncert, minval, rolcur, 19)
        rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk;"
        implement = "select * from range_topk('range_r', 'a1', 0) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"
        det = "select * from range_r order by a1;"
        print(implement)
        rangeres.append(str(rolcur))
        resm = timeoutQuery(implement, timeout)
        resline = ""
        resline += str(rolcur) + "\t"
        resm = timeoutQuery(det, timeout)
#        impres.append(resm)
        resline += str(resm) + "\t"
        resm = timeoutQuery(implement, timeout)
#        rewriteres.append(resm)
        resline += str(resm) + "\t"
        resm = timeoutQuery(rewriting, timeout)
        if float(resm) < 0:
            break
        resline += str(resm) + "\t"
        resstr += resline
        maxx = rolcur
        maxy = float(resm)
        resm = timeoutQuery(det, timeout)
        mcdb10.append(str(float(resm)*11))
        resstr += str(float(resm)*11) + "\t"
        resm = timeoutQuery(det, timeout)
        mcdb20.append(str(float(resm)*22))
        resstr += str(float(resm)*22) + "\t\n"
        rolcur = rolcur*2
    
    print(resstr)
    
    fname = "topk_changeSize.csv"
    
    writetofile(fname, resstr)
    
    rn = plottopk(fname, maxx*1.1, maxy*1.5, "Rows")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
    

def test_topk_varSizeSmall():
    
    print("[TOP-K: altering Data Size (small scale with symb)]")
    
    resname = "range_r"

    colnum = 3
    rolcur = 200
    rolnumax = 30000
    rolinc = 2
    
    range = 100

#    uncert = 0.03 #uncertainty percentage
    uncert = 0.01
    
    minval = 1
    
    timeout = 600
    
    rangeres = []
    impres =[]
    rewriteres = []
    detres = []
    symb = []
    mcdb10 = []
    mcdb20 = []
    
    resstr = ""
    maxx = 0
    maxy = 0
    
    fname = "topk_changeSize_more.csv"
    
    while rolcur < rolnumax:
        attrs = importmicrotablefromtidb(colnum, rolcur, range, uncert, minval, rolcur, 10)
        rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk;"
        implement = "select * from range_topk('range_r', 'a1', 0) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"
        det = "select * from range_r order by a1;"
        print(implement)
        rangeres.append(str(rolcur))
        resm = timeoutQuery(implement, timeout)
        resline = ""
        resline += str(rolcur) + "\t"
        resm = timeoutQuery(det, timeout)
#        impres.append(resm)
        resline += str(resm) + "\t"
        resm = timeoutQuery(implement, timeout)
#        rewriteres.append(resm)
        resline += str(resm) + "\t"
        resm = timeoutQuery(rewriting, timeout)
        if float(resm) < 0:
            break
        resline += str(resm) + "\t"
        resstr += resline
        maxx = rolcur
        maxy = float(resm)
        resm = timeoutQuery(det, timeout)
        mcdb10.append(str(float(resm)*11))
        resstr += str(float(resm)*11) + "\t"
        resm = timeoutQuery(det, timeout)
        mcdb20.append(str(float(resm)*22))
        resstr += str(float(resm)*22) + "\t"
        resm = 1000.0*csolv.rangeSort("microctable.csv", 1, printres=False)
        resstr += str(resm) + "\t\n"
        if resm > maxy:
            maxy = resm
        rolcur = rolcur*2
        
        writetofile(fname, resstr)
    
    print(resstr)
    
    writetofile(fname, resstr)
    
    rn = plottopk(fname, maxx*1.1, maxy*1.5, "Rows")
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])


        
    
#def test_topk_varSizeRelative():
#
#    print("[TOP-K: altering Data Size (relative range)]")
#
#    resname = "range_r"
#
#    colnum = 3
#    rolcur = 1000
#    rolnumax = 100000000
#    rolinc = 2
#
##    uncert = 0.03 #uncertainty percentage
#    uncert = 0.05
#
#    minval = 1
#
#    timeout = 600
#
#    rangeres = []
#    impres =[]
#    rewriteres = []
#    detres = []
#
#    while rolcur < rolnumax:
#        attrs = importmicrotablefromtidb(colnum, rolcur, int(rolcur*0.05), uncert, minval, rolcur, 19)
#        rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk;"
#        implement = "select * from range_topk('range_r', 'a1', 0) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"
#        det = "select * from range_r order by a1;"
#        print(implement)
#        rangeres.append(str(rolcur))
#        if float(resm) < 0:
#            break
#        resm = timeoutQuery(implement, timeout)
#        impres.append(resm)
#        resm = timeoutQuery(rewriting, timeout)
#        rewriteres.append(resm)
#        resm = timeoutQuery(det, timeout)
#        detres.append(resm)
#        rolcur = rolcur*2
#
#    print("[TOP-K: altering data size (relative range)]")
#    print("*******************************")
#    print(", ".join(rangeres))
#    print(", ".join(detres))
#    print(", ".join(impres))
#    print(", ".join(rewriteres))
#    print("*******************************")
    
    
def test_rank_single(row, range, uncert, k):
    attrs = importmicrotablefromtidb(3, row, range, uncert, 1, row, 19)
    rewriting = "select * from ( WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank) as rk where lb_rank <= %i;"%(k)
    implement = "select * from range_topk('range_r', 'a1', %i) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, cet_r integer, bst_r integer, pos_r integer, lb_rank numeric,ub_rank numeric) order by lb_rank;"%(k)
    det = "select * from range_r order by a1 limit %i;"%(k)
    resm = timeoutQuery(implement, 600)
    print("imp: "+resm)
    resm = timeoutQuery(rewriting, 600)
    print("rewr: "+resm)
    resm = timeoutQuery(det, 600)
    print("det: "+resm)
    
def test_window_single(row, range, uncert, w=3):
    attrs = importmicrotablefromtidb(3, row, range, uncert, 1, row, 19)
    
    detquery = "select a1, sum(a2) over (ORDER BY a1 ROWS BETWEEN 0 PRECEDING AND 3 FOLLOWING) AS f FROM range_r;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    
    resm = timeoutQuery(detquery, 600)
    print("DET: "+resm)
    resm = timeoutQuery(our_imp, 600)
    print("IMP: "+resm)
    resm = timeoutQuery(range_overlap_join, 600)
    print("REWR: "+resm)

def test_timeout():
    only_ranks = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints)SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum FROM bounds GROUP BY id Order by lb_rank;"
    
    our_imp = "select * from range_window('sum','a2','a1','range_r', 3) AS t1(id numeric, a1 numeric, ub_a1 numeric, lb_a1 numeric, a2 numeric , ub_a2 numeric, lb_a2 numeric, rank numeric,ub_rank numeric,lb_rank numeric, lb_sum numeric, ub_sum numeric);"
    
    range_overlap_join = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank) SELECT l.id, r.id, l.rank, r.rank FROM ranks l, ranks r WHERE r.lb_rank <= l.ub_rank AND r.ub_rank >= l.lb_rank;"
    print(timeoutQuery(only_ranks, 5))
    print(timeoutQuery(range_overlap_join, 5))

def getschema(tbn):
    query = "select column_name from INFORMATION_SCHEMA.COLUMNS where table_name ='%s'"%(tbn)
    ret = runQuery(query);
    return [i[0] for i in ret]
    
def rangAccuracy(rep=10, time=4000, printinter = False):
#select * from( select id, CAST(row_number() over() as numeric) as lb_rank, CAST(row_number() over() as numeric) as ub_rank from (WITH random_input as (select *, row_number() over(partition by id order by random()) as random_sort from range_r_tidb) SELECT id,a1 from random_input where random_sort = 1 order by a1) x) rk where id = 18;
#    rep = 10
    pushQuery("drop table if exists radb_rank;")
    pushQuery("create table radb_rank as select * from ( WITH endpoints AS (SELECT id, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints) SELECT id, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank from bounds GROUP BY id) as rk;")
    
    pushQuery("create table mcdb_0 (id numeric, ub_rank numeric, lb_rank numeric);")
    for i in range(1,rep+1):
        tn = "mcdb_%i"%(i)
        pushQuery("drop table if exists %s"%(tn))
        randomworld = "WITH random_input as (select *, row_number() over(partition by id order by random()) as random_sort from range_r_tidb) SELECT id,a1 from random_input where random_sort = 1 order by a1"
        single = "select id, CAST(row_number() over() as numeric) as lb_rank, CAST(row_number() over() as numeric) as ub_rank from (%s) x"%(randomworld)
        rankres = "create table %s as select id, max(ub_rank) as ub_rank, min(lb_rank) as lb_rank from (select id, ub_rank, lb_rank from (%s) y union select id, ub_rank, lb_rank from mcdb_%i) x group by id;"%(tn, single, i-1)
#        print(rankres)
        pushQuery(rankres)
    for i in range(rep+1, rep+time):
        tn = "mcdb_%i"%(i)
        pushQuery("drop table if exists %s"%(tn))
        rankres = "create table %s as select id, max(ub_rank) as ub_rank, min(lb_rank) as lb_rank from (select id, ub_rank, lb_rank from (%s) y union select id, ub_rank, lb_rank from mcdb_%i) x group by id;"%(tn, single, i-1)
        pushQuery(rankres)
#        diffquery = "select avg(perc) from (select * from (select l.id, (b.ub_rank-b.lb_rank+1)/(l.ub_rank-l.lb_rank+1) as perc from %s l join mcdb_9 b on l.id=b.id) x where perc < 1) y;"%(tn)
        diffquery = "select avg(perc) from (select * from (select l.id, (b.ub_rank-b.lb_rank+1)/(l.ub_rank-l.lb_rank+1) as perc from %s l join mcdb_%i b on l.id=b.id) x) y;"%(tn, rep-1)
        diffradb = "select avg(perc) from (select * from (select l.id, (b.ub_rank-b.lb_rank+1)/(l.ub_rank-l.lb_rank+1) as perc from radb_rank l join %s b on l.id=b.id) x) y;"%(tn)
        
        lastmcdb = runQuery(diffquery)
        lastaudb = runQuery(diffradb)
        if printinter:
            print(i)
            print("mcdb:")
            print(float(lastmcdb[0][0]))
            print("audb:")
            print(float(lastaudb[0][0]))
        else:
            print(i,end="\r")
        pushQuery("drop table if exists mcdb_%i;"%(i-1))
    print(str(rep+time)+" reps finished. Comparing with mcdb(%i)."%(rep))
    print("mcdb:")
    print(float(lastmcdb[0][0]))
    print("audb:")
    print(float(lastaudb[0][0]))
    return(float(lastmcdb[0][0]), float(lastaudb[0][0]))
        
def windowAccuracy():
    window = 4
    audb = "WITH endpoints AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, lb_a1 as pt, 0 AS isend , cet_r , bst_r , pos_r FROM range_r UNION ALL SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, ub_a1 as pt, 1 AS isend , cet_r , bst_r , pos_r FROM range_r), bounds AS (SELECT id, a2, ub_a2, lb_a2, a1, ub_a1, lb_a1, cet_r , bst_r , pos_r,( CASE WHEN isend=1 THEN SUM( isend *bst_r ) OVER ( ORDER BY a1 ASC ) ELSE 0 END) AS rank ,( CASE WHEN isend=0 THEN COALESCE(SUM( isend *cet_r ) OVER ( ORDER BY pt ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) ELSE 0 END) AS lb_rank ,( CASE WHEN isend=1 THEN SUM((1 - isend )* pos_r ) OVER ( ORDER BY pt ASC ) ELSE 0 END) AS ub_rank FROM endpoints), ranks AS (SELECT id, min(a2) AS a2, min(ub_a2) AS ub_a2, min(lb_a2) AS lb_a2, min(a1) AS a1, min(ub_a1) AS ub_a1, min(lb_a1) AS lb_a1, SUM( rank ) AS rank, SUM( UB_rank ) AS ub_rank, SUM( LB_rank )+1 AS lb_rank, 0::numeric AS lb_sum, 0::numeric AS ub_sum, min(pos_r) as pos_r FROM bounds GROUP BY id Order by lb_rank), ranged AS (SELECT id,('['||lb_rank::text||','||ub_rank::text||']')::numrange as rank from ranks) SELECT l.id, r.id, l.rank, r.rank FROM ranged l, ranged r WHERE r.rank && l.rank;"
    
def test_rank_acc_var_uncert():
    minuncert = 1
    maxuincert = 10
    inc = 1
    resstr = ""
    maxx = 0
    testrep = 5000
    for i in range(minuncert,maxuincert,inc):
        resline = ""
        maxx = float(i)/100.0
        importmicrotablefromtidb(2, 5000, 200, float(i)/100.0, 1, 5000, 10)
        resline += str(float(i)/100.0) + "\t"
        resmcdb10, resaudb1 = rangAccuracy(11,testrep)
        resline += str(resmcdb10) + "\t"
        resmcdb20, resaudb2 =rangAccuracy(21,testrep)
        resline += str(resmcdb20) + "\t"
        resline += str((resaudb1+resaudb2)/2) + "\t\n"
        resstr += resline
    print(resstr)
    
    fname = "rank_acc_changeuncert.csv"
    
    writetofile(fname, resstr)
    
    rn = plotRankAccuracy(fname, maxx*1.1, 1.2, "Uncertainty", 0.5)
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
    
def test_rank_acc_var_range():
    minrange = 10
    maxrange = 500
    inc = 50
    resstr = ""
    maxx = 0
    testrep = 5000
    for i in range(minrange,maxrange,inc):
        resline = ""
        maxx = i
        importmicrotablefromtidb(2, 5000, i, 0.05, 1, 5000, 10)
        resline += str(i) + "\t"
        resmcdb10, resaudb1 = rangAccuracy(11,testrep)
        resline += str(resmcdb10) + "\t"
        resmcdb20, resaudb2 =rangAccuracy(21,testrep)
        resline += str(resmcdb20) + "\t"
        resline += str((resaudb1+resaudb2)/2) + "\t\n"
        resstr += resline
    print(resstr)
    
    fname = "rank_acc_changerange.csv"
    
    writetofile(fname, resstr)
    
    rn = plotRankAccuracy(fname, maxx*1.1, 1.2, "Range", 0.5)
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
    
def windowAccuracy(rep=10, time=4000):
    r, audb = rangAccuracy(rep, time)
    print("window: (%f, %f)"%(r**2,audb**2))
    return (r**2, audb**2)
    
def test_window_acc_var_range():
    minrange = 10
    maxrange = 500
    inc = 50
    resstr = ""
    maxx = 0
    testrep = 5000
    for i in range(minrange,maxrange,inc):
        resline = ""
        maxx = i
        importmicrotablefromtidb(2, 5000, i, 0.05, 1, 5000, 10)
        resline += str(i) + "\t"
        resmcdb10, resaudb1 = windowAccuracy(11,testrep)
        resline += str(resmcdb10) + "\t"
        resmcdb20, resaudb2 = windowAccuracy(21,testrep)
        resline += str(resmcdb20) + "\t"
        resline += str((resaudb1+resaudb2)/2) + "\t\n"
        resstr += resline
    print(resstr)
    
    fname = "window_acc_changerange.csv"
    
    writetofile(fname, resstr)
    
    rn = plotRankAccuracy(fname, maxx*1.1, 1.2, "Range", 0.0)
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])
    
def test_window_acc_var_uncert():
    minuncert = 1
    maxuincert = 10
    inc = 1
    resstr = ""
    maxx = 0
    testrep = 5000
    for i in range(minuncert,maxuincert,inc):
        resline = ""
        maxx = float(i)/100.0
        importmicrotablefromtidb(2, 5000, 200, float(i)/100.0, 1, 5000, 10)
        resline += str(float(i)/100.0) + "\t"
        resmcdb10, resaudb1 = windowAccuracy(11,testrep)
        resline += str(resmcdb10) + "\t"
        resmcdb20, resaudb2 = windowAccuracy(21,testrep)
        resline += str(resmcdb20) + "\t"
        resline += str((resaudb1+resaudb2)/2) + "\t\n"
        resstr += resline
    print(resstr)
    
    fname = "window_acc_changeuncert.csv"
    
    writetofile(fname, resstr)
    
    rn = plotRankAccuracy(fname, maxx*1.1, 1.2, "Uncertainty", 0.0)
    
    subprocess.call(["mv", fname,"results/%s"%fname])
    subprocess.call(["mv", rn,"results/%s"%rn])

if __name__ == '__main__':
    dir = os.path.dirname(os.path.abspath(__file__))
    subprocess.call(["mkdir", "results"])
    print(dir)
    
    sys.setrecursionlimit(10000)
    
#    test_timeout()

#    test_topk_varSizeRelative()
#    test_topk_varSize()
#    test_topk_varUncert()
#    test_topk_vark()
#    test_topk_varRange()

#    test_window_varWindowRange()
#    test_window_varWindowUncert()
#    test_window_varAggRange()
#    test_window_varAggUncert()
#    test_window_varScale()
#    plotwindowScale("window_changeScale_full.csv",112400,1800000)
    tbsize = 50000
    importmicrotablefromtidb(3, tbsize, 30000, 0.01, 1, tbsize, 10)
#    test_window_varScaleLess()
#    test_window_varWindowsize()

#    test_window_full_varyingUncert()
#    test_window_full_varyingRange()
    
#    plotwindowScaleLess("window_changeScale.csv", 661658.25, 7208960)

#    plotwindow("window_changeAggRange.csv", 19500, 3000, "Aggregation Range")
#    plotwindow("window_changeWindowRange.csv", 19500, 3000, "Range")
#    plotwindow("window_changeWindowUncert.csv", 28, 3000, "Uncertainty")
#    plotwindow("window_changeWindowSize.csv", 9.5, 3000, "Window Size")

#    plottopk("topk_changeRange.csv", 19800, 1100, "Attribute Range")
    

#    test_rank_single(582146, 20, 0.001, 5)
#    test_rank_single(4757, 50, 0.01, 5)
#    test_rank_single(3295, 30, 0.011, 5)
#    test_rank_single(33, 3, 0.5, 3)
#    test_rank_single(16, 2, 0.6, 3)

#    test_window_single(4757, 10, 0.01)
#    test_window_single(33, 3, 0.5)
#    test_window_single(16, 2, 0.6)
    
#    Window tests:
#    Altering scale
#    Altering window uncertainty
#    Altering window range
#    Alterng aggregation uncertainty
#    Altering aggregation range

#    Alterting window size

#    importmicrotablefromtidb(3, 5000, 20, 0.01, 1, 5000, 10)

#    test_rank_acc_var_uncert()
#    test_rank_acc_var_range()
#    test_window_acc_var_uncert()
#    test_window_acc_var_range()

#    plottopkScale("topk_changeSize_all.csv", 18000000, 1000000)

#    print(csolv.rangeSort("microctable.csv", 1, printres=False))
#    test_topk_varSizeSmall()
#    plottopkScale("topk_changeSize_more.csv", 30000, 100000000, 170)

#    plotRankAccuracy("rank_acc_changerange.csv", 5000, 1.8, "Attribute Range", 0.2)
#    plotRankAccuracy("window_acc_changerange.csv", 5000, 1.8, "Attribute Range", 0.2)
#    plotRankAccuracy("rank_acc_changeuncert.csv", 9.5, 1.8, "Attribute Uncertainty", 0.2)
#    plotRankAccuracy("window_acc_changeuncert.csv", 9.5, 1.8, "Attribute Uncertainty", 0.2)

#    importmicrotablefromtidb(2, 4757, 30, 0.01, 1, 4757, 6)
#    print(rangAccuracy(20,5000, True))
    
    exittest()
    
