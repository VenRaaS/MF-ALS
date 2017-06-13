#!/usr/bin/env python
# pyspark --driver-memory 8G --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run20170525.log &
# spark-submit --master yarn-cluster --num-executors 10 --executor-cores 3 --executor-memory 16g --driver-memory 8g  --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > rec_u2i_20170605-2.log &
# nohup spark-submit --driver-memory 8G --executor-memory 16G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run0606-1805.log &

from pyspark import SparkContext, SparkConf
sc =SparkContext(appName="pyALSitemSimilarity")

import pandas as pd
import numpy as np
import operator 
from sklearn.metrics.pairwise import cosine_similarity

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
productFeatures = model.productFeatures().collect() 
productArray=[]
productFeaturesArray=[]
for (pid, pfactor) in productFeatures:
  productArray.append(pid)
  productFeaturesArray.append(pfactor)   

bc_productArray=sc.broadcast(productArray)
bc_productFeaturesArray=sc.broadcast(productFeaturesArray)


def cosineSImilarity(x,y):
    return np.dot(x,y)/(np.linalg.norm(x)*np.linalg.norm(y))

# sc.parallelize , plan to change to pandas
def vectorSimilarity(itemId, itemFactors):
  sim = cosine_similarity(bc_productFeaturesArray.value, np.array(itemFactors))
  df = pd.DataFrame( { 'sim': sim.T[0], 
                     'iid':bc_productArray.value})
  df_topK = df.nlargest(10, 'sim')
  df_topK.insert(0, 'itemId', np.full((10), itemId, np.int))
  return df_topK[1:].to_records(index=False)

recommendations=model.productFeatures().flatMap(lambda (uid,factor): (vectorSimilarity(uid, factor)))
# recommendations.take(30)
recommendations.saveAsTextFile('itemSimilarity0613.csv')


