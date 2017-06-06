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

model_save_path = 'Model0606'
print("begin load model from %s" % (model_save_path))
model = MatrixFactorizationModel.load(sc, model_save_path)

TopKItems = 10
print("recommendProductsForUsers %s" % (TopKItems))
# Generate top 10 movie recommendations for each user
userRecs = model.recommendProductsForUsers(TopKItems)
userRecs.take(3)


