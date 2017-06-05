#!/usr/bin/env python
# spark-submit --driver-memory 16G --executor-memory 16G ml-als-model-predict.py > run20170525.log &
# nohup spark-submit --driver-memory 16G --executor-memory 16G ml-als-model-predict.py > run20170525-2.log &

from pyspark import SparkContext, SparkConf
sc =SparkContext(appName="PythonALS")

import pandas as pd
import numpy as np
#import scipy.stats as st
#from scipy import stats


import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from time import time
t0 = time()

model = MatrixFactorizationModel.load(sc, 'Model0605')

# Generate top 10 movie recommendations for each user
userRecs = model.recommendProductsForUsers(10)
userRecs.take(10)




