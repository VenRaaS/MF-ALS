#!/usr/bin/env python
# pyspark --driver-memory 8G --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run20170525.log &
# spark-submit --master yarn-cluster --num-executors 10 --executor-cores 3 --executor-memory 16g --driver-memory 8g  --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > rec_u2i_20170605.log &
# nohup spark-submit --driver-memory 16G --executor-memory 16G ml-als-model-predict.py > run20170525-2.log &

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

model_save_path = 'Model0605'
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
productArrayBroadCast=sc.broadcast(productArray)
productFeaturesArrayBroadcast=sc.broadcast(matrix.T)

def func(iterator):
    userFeaturesArray = []
    userArray = []
    for x in iterator:
        userArray.append(x[0])
        userFeaturesArray.append(x[1])
        userFeatureMatrix = np.matrix(userFeaturesArray)
        userRecommendationArray = userFeatureMatrix*(productFeaturesArrayBroadcast.value)
        mappedUserRecommendationArray = []
        #Extract ratings from the matrix
        i=0
        for i in range(0,len(userArray)):
            ratingdict={}
            j=0
            for j in range(0,len(productArrayBroadcast.value)):
                  ratingdict[str(productArrayBroadcast.value[j])]=userRecommendationArray.item((i,j))
                  j=j+1
            #Take the top 8 recommendations for the user
            sort_apps=sorted(ratingdict.keys(), key=lambda x: x[1])[:8]
            sort_apps='|'.join(sort_apps)
            mappedUserRecommendationArray.append((userArray[i],sort_apps))
            i=i+1
    return [x for x in mappedUserRecommendationArray]


recommendations=model.userFeatures().repartition(2000).mapPartitions(func)
recommendations.take(3)


