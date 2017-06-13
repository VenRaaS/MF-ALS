#!/usr/bin/env python
# pyspark --driver-memory 8G --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run20170525.log &
# spark-submit --master yarn-cluster --num-executors 10 --executor-cores 3 --executor-memory 16g --driver-memory 8g  --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > rec_u2i_20170605-2.log &
# nohup spark-submit --driver-memory 8G --executor-memory 16G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run0606-1101.log &

from pyspark import SparkContext, SparkConf
sc =SparkContext(appName="pyALSrecommendItemsForUsers")

import pandas as pd
import numpy as np
import operator 
#import scipy.stats as st
#from scipy import stats

import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from time import time
t0 = time()

model_save_path = 'Model0607'
print("begin load model from %s" % (model_save_path))
model = MatrixFactorizationModel.load(sc, model_save_path)

TopKItems = 10
print("recommendProductsForUsers %s" % (TopKItems))
# Generate top 10 movie recommendations for each user
#userRecs = model.recommendProductsForUsers(TopKItems)
#userRecs.take(10)

#Collect product feature matrix
productFeatures = model.productFeatures().collect() 
productArray=[]
productFeaturesArray=[]
for x in productFeatures:
  productArray.append(x[0])
  productFeaturesArray.append(x[1])  

bc_productArray=sc.broadcast(productArray)
bc_productFeaturesMatrixT=sc.broadcast(productFeaturesMatrix.T)

def recomdItems4User(uid, ufactor):
  predictMatrix = ufactor * bc_productFeaturesMatrixT.value
  df2 = pd.DataFrame( {'itemId': bc_productArray.value,
                       'rating': predictMatrix.A1})
  df_topK = df2.nlargest(10, 'rating')
  df_topK.insert(0, 'userId', np.full((10), uid, np.int))
  return df_topK.to_records(index=False)

recommendations=model.userFeatures().flatMap(lambda (uid,factor): (recomdItems4User(uid, factor)))
# recommendations.take(30)
recommendations.saveAsTextFile('recommendItems4Users0609.csv')

