#!/usr/bin/python
import os
import math
import subprocess
import time

import numpy as np
import random
import sys
import copy
import statistics
import time
from z3 import *

def iftl(ifin,thenin,elsein):
    return "If("+ifin+ ", "+thenin+", "+ elsein+")"
    
def notVar(input):
    return input.isnumeric()

def rangeSort(fname, index, delm=",", schema=True, printres=True):
    vars = set()
    allval = []
    start_time = time.time()
    with open(fname) as f:
        if schema:
            f.readline()
        while True:
            tuple = f.readline()[:-1]
            if printres:
                print(tuple)
            if not tuple:
                break
            sp = tuple.split(delm)
            if len(sp) < index:
                print("Number of attributes is less than index.")
                break
            allval.append(sp[index])
    ranks = []
    for i in range(0,len(allval)):
        rk = ""
        rkint = 0
        for j in range(0,len(allval)):
            if i!=j:
                if notVar(allval[j]) and notVar(allval[i]):
                    if float(allval[j])<float(allval[i]):
                        rkint+=1
                else:
                    if not notVar(allval[j]):
                        vars.add(allval[j])
                    if not notVar(allval[i]):
                        vars.add(allval[i])
                    smaller = allval[j]+"<"+allval[i]
                    rk += iftl(smaller,"1","0")+"+"
        rk += str(rkint)
        if printres:
            print(rk)
        else:
            print(i,end="\r")
        ub = apply_solver_rank_ub(rk, vars)
        lb = apply_solver_rank_lb(rk, vars)
        if printres:
            print("(lb, ub): ("+str(lb)+", "+str(ub)+")")
        ranks.append(rk)
    timetaken = time.time() - start_time
    print("time taken: "+ str(timetaken))
    return timetaken
    
def topK(fname, index, k, delm=",", schema=True):
    vars = set()
    allval = []
    start_time = time.time()
    with open(fname) as f:
        if schema:
            f.readline()
        while True:
            tuple = f.readline()
            print(tuple)
            if not tuple:
                break
            sp = tuple.split(delm)
            if len(sp) < index:
                print("Number of attributes is less than index.")
                break
            allval.append(sp[index])
    ranks = []
    for i in range(0,len(allval)):
        rk = ""
        rkint = 0
        for j in range(0,len(allval)):
            if i!=j:
                if notVar(allval[j]) and notVar(allval[i]):
                    if float(allval[j])<float(allval[i]):
                        rkint+=1
                else:
                    if not notVar(allval[j]):
                        vars.add(allval[j])
                    if not notVar(allval[i]):
                        vars.add(allval[i])
                    smaller = allval[j]+"<"+allval[i]
                    rk += iftl(smaller,"1","0")+"+"
        rk += str(rkint)
        if apply_solver_top_k_membership(rk, vars, k):
            print(rk)
            ranks.append(rk)
    print("time taken: "+ str(time.time() - start_time))
    return vars
    
#def apply_solver_rank_ub(condstring, vars):
#    varstr = ""
#    for var in vars:
#        varstr += var+","
##        print("%s = Int('%s')" % (var,var))
#        exec("%s = Int('%s')" % (var,var))
#    varstr = varstr[:-1]
#    rk = Int('rk')
#    rankcond = "And(Exists(["+varstr+"] ,"+condstring+"==rk), Not(Exists(["+varstr+"] ,"+condstring+"==rk+1)))"
##    start_time = time.time()
#    s = Solver()
#    s.add(eval(rankcond))
#    r = s.check()
##    print(r)
#    m = s.model()
#    ub = m[rk]
#
#    return ub
#    print("solver time taken: "+ str((time.time() - start_time)))

def apply_solver_rank_ub(condstring, vars):
    varstr = ""
    for var in vars:
        varstr += var+","
#        print("%s = Int('%s')" % (var,var))
        exec("%s = Int('%s')" % (var,var))
    rk = Int('rk')
    varstr = varstr[:-1]
    opt = Optimize()
    opt.add(eval("rk == " + condstring))
    opt.maximize(rk)
    r = opt.check()
    m = opt.model()
#    print(r)
    ub = m[rk]
    return ub

#def apply_solver_rank_lb(condstring, vars):
#    varstr = ""
#    for var in vars:
#        varstr += var+","
##        print("%s = Int('%s')" % (var,var))
#        exec("%s = Int('%s')" % (var,var))
#    varstr = varstr[:-1]
#    rk = Int('rk')
#    rankcond = "And(Exists(["+varstr+"] ,"+condstring+"==rk),  Not(Exists(["+varstr+"] ,"+condstring+"==rk-1)))"
##    start_time = time.time()
#    s = Solver()
#    s.add(eval(rankcond))
#    r = s.check()
##    print(r)
#    m = s.model()
#    lb = m[rk]
#
#    return lb

def apply_solver_rank_lb(condstring, vars):
    varstr = ""
    for var in vars:
        varstr += var+","
#        print("%s = Int('%s')" % (var,var))
        exec("%s = Int('%s')" % (var,var))
    rk = Int('rk')
    varstr = varstr[:-1]
    opt = Optimize()
    opt.add(eval("rk == " + condstring))
    opt.minimize(rk)
    r = opt.check()
    m = opt.model()
#    print(r)
    lb = m[rk]
    return lb
    
def apply_solver_top_k_membership(condstring, vars, k):
    for var in vars:
        exec("%s = Int('%s')" % (var,var))
    cond = condstring + "<=" + str(k)
#    print(cond)
    s = Solver()
    s.add(eval(cond))
    r = s.check()
#    print(r==sat)
    return r==sat

    
def windowFunc():
    return
    
if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Mode: rank/topk/window")
        sys.exit()
    if sys.argv[1] == "rank":
        if len(sys.argv) != 4:
            print("Usage: rank fname [index]")
            sys.exit()
        if not sys.argv[3].isdigit():
            print("Usage: rank fname [index]")
            sys.exit()
        print("ranking "+sys.argv[2]+" on "+sys.argv[3])
        
        vars = rangeSort(sys.argv[2], int(sys.argv[3]))
        
        sys.exit()
        
    if sys.argv[1] == "topk":
        if len(sys.argv) != 5:
            print("Usage: topk fname [index] [k]")
            sys.exit()
        if not (sys.argv[3].isdigit() and sys.argv[4].isdigit()):
            print("Usage: topk fname [index] [k]")
            sys.exit()
            
        print("top "+sys.argv[4]+" "+sys.argv[2]+" on "+sys.argv[3])
        
        vars = topK(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
        
        sys.exit()
