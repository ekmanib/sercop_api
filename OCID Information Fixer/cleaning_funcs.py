import pandas as pd
import os 
import time

def read(file):
    with open(file,encoding = "utf-8") as f:
        content = f.readlines()
    return content
def identify(content):
    cno = []
    content_2 = []
#     for i in content:
#         cno.append(i.count(","))
#     ind = [cno.index(i) for i in cno if i != 10]
    faulty = []
    good = []
    for i in content:
        if i.count(",") != 10:
            faulty.append(i)
        else:
            good.append(i)
    return good,faulty
def listify(info):
    flist = info.split(",")
    return flist
def fixer(flist):
    error = flist[5:-5]
    des = "".join(error)
    del flist[6:-5]
    flist[5] = des
    return flist
#Using all the functions at the same time as a single function
def attack(year):
    #Read 
    start = time.time()
    poyo = read(f"Compras_Publicas_{year}\\Year_{year}.txt")
    #Identify
    c,f = identify(poyo)
    #listify (Makes a list out of the data)
    flist = [listify(i) for i in f]
    #Fixer (Removes non column commas)
    flist = [fixer(i) for i in flist]
    #Probe items that have more commas than columns
    for i in flist:
        if len(i) > 11:
            print(i)
    fixlist = [",".join(i) for i in flist]
    c.extend(fixlist)
    end = time.time()
    print(f"Total Ieration Time = {end-start}")
    return c