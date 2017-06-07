#!/usr/bin/env python
# pyspark --driver-memory 8G --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run20170525.log &
# spark-submit --master yarn-cluster --num-executors 10 --executor-cores 3 --executor-memory 16g --driver-memory 8g  --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > rec_u2i_20170605-2.log &
# nohup spark-submit --driver-memory 8G --executor-memory 16G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run0606-1101.log &

from pyspark import SparkContext, SparkConf
sc =SparkContext(appName="PythonALSrecommendProductsForUsers")

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

matrix=np.matrix(productFeaturesArray)
bc_productArray=sc.broadcast(productArray)
bc_productFeaturesMatrixT=sc.broadcast(matrix.T)

def func2(uid, ufactor):
  # userFeatureMatrix = np.matrix(ufactor)
  userRecommendationMatrix = ufactor*(bc_productFeaturesMatrixT.value)
  df = pd.DataFrame(data=userRecommendationMatrix.T, columns=["rating"], index=np.matrix(bc_productArray.value).T)
  df.sort_values(by='rating', inplace=True, ascending=False)
  mappedUserRecommendationArray = []
  for itemId, row in df[:10].iterrows(): # Top K Item
    mappedUserRecommendationArray.append( '%s,%s,%s' % (uid,itemId,row[0]))  # mappedUserRecommendationArray.append((uid,itemId,row[0])) 
  return mappedUserRecommendationArray

recommendations=model.userFeatures().flatMap(lambda (uid,factor): (func2(uid, factor)))
# recommendations.take(30)
recommendations.saveAsTextFile('recommendProducts4User.csv')

