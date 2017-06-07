#!/usr/bin/env python
# pyspark --driver-memory 8G --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run20170525.log &
# spark-submit --master yarn-cluster --num-executors 10 --executor-cores 3 --executor-memory 16g --driver-memory 8g  --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > rec_u2i_20170605-2.log &
# nohup spark-submit --driver-memory 8G --executor-memory 16G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run0606-1805.log &

from pyspark import SparkContext, SparkConf
sc =SparkContext(appName="PythonALSitemSimilarity")

import pandas as pd
import numpy as np
#import scipy.stats as st
#from scipy import stats

import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from time import time
t0 = time()

model_save_path = 'Model0606'
print("begin load model from %s" % (model_save_path))
model = MatrixFactorizationModel.load(sc, model_save_path)

TopKItems = 10
print("recommendProductsForUsers %s" % (TopKItems))

#Collect product feature matrix
bc_productFeatures = sc.broadcast(model.productFeatures().collect())

import operator 
import numpy as np

def cosineSImilarity(x,y):
    return np.dot(x,y)/(np.linalg.norm(x)*np.linalg.norm(y))

def func3(itemId, itemFactor):
    simsdict={}
    for (pid, pfactor) in bc_productFeatures.value:
        simsdict[pid] = cosineSImilarity(np.array(pfactor), np.array(itemFactor))
    sorted_x = sorted(simsdict.items(), key=operator.itemgetter(1), reverse=True)
    itemSimilarityArray = []
    for (pid, score) in sorted_x[1:11]: # first one similarity always equal same item
        itemSimilarityArray.append('%s,%s,%s' % (itemId,pid,score))
    return itemSimilarityArray

recommendations=model.productFeatures().map(lambda (uid,factor): (func3(uid, factor)))
# recommendations.take(30)
recommendations.saveAsTextFile('itemSimilarity.csv')


