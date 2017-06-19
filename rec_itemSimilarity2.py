#!/usr/bin/env python
# pyspark --driver-memory 8G --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run20170525.log &
# spark-submit --master yarn-cluster --num-executors 10 --executor-cores 3 --executor-memory 16g --driver-memory 8g  --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > rec_u2i_20170605-2.log &
# nohup spark-submit --driver-memory 8G --executor-memory 16G --conf spark.yarn.executor.memoryOverhead=4096 rec_ItemForUsers.py > run0606-1805.log &

from pyspark import SparkContext, SparkConf
import pandas as pd
import numpy as np
import operator
from sklearn.metrics.pairwise import cosine_similarity
import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from time import time

def cosineSImilarity(x,y):
    return np.dot(x,y)/(np.linalg.norm(x)*np.linalg.norm(y))

# sc.parallelize , plan to change to pandas
def func(iterator):
  mappedi2iArray = []
  for (iid, ifactor) in iterator:
    sim = cosine_similarity(bc_productFeaturesArray.value, np.array(ifactor))
    df = pd.DataFrame( { 'sim': sim.T[0],
                       'jid':bc_productArray.value})
    df_topK = df.nlargest(10, 'sim')
    df_topK.insert(0, 'iid', np.full((10), iid, np.int))
    #mappedi2iArray.append(df_topK[1:].to_records(index=False))
    mappedi2iArray.append(df_topK[1:].to_csv(index=False, header=False))

  return mappedi2iArray

def process(df):
    i2iArray = []
    for index, row in df.iterrows():
        (iid, ifactor) = (row[0], row[1])  
        sim = cosine_similarity(np.matrix(productFeaturesArray), [np.array(ifactor)])
        df = pd.DataFrame( { 'sim': sim.T[0], 'jid':productArray})
        df_topK = df.nlargest(10, 'sim')
        df_topK.insert(0, 'iid', np.full((10), iid, np.int))
        i2iArray.append(df_topK[1:].to_csv(index=False, header=False)) 
    return i2iArray

if __name__ == '__main__':
    sc =SparkContext(appName="pyALSitemSimilarity")
       
    #model_save_path = 'hdfs://itrihd34:8020/user/ua40168/Model0606'
    model_save_path = 'Model0606'
    print("begin load model from %s" % (model_save_path))
    model = MatrixFactorizationModel.load(sc, model_save_path)

    TopKItems = 10
    print("recommendProductsForUsers %s" % (TopKItems))

    #Collect product feature matrix
    pFeatures = model.productFeatures().toDF()
    pdfItemFeatures = pFeatures.toPandas()
    
    p = mp.Pool(processes=8)
    split_dfs = np.array_split(pdfItemFeatures, 2000)
    pool_results = p.map(process, split_dfs)
    p.close()
    p.join()

    # merging parts processed by different processes
    #parts = pd.concat(pool_results, axis=0)

    # merging newly calculated parts to big_df
    #big_df = pd.concat([big_df, parts], axis=1)

    # checking if the dfs were merged correctly
    #pdt.assert_series_equal(parts['id'], big_df['id'])


