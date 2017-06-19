#!/usr/bin/env python
# spark-submit --driver-memory 16G --executor-memory 16G ml-als-model-predict.py > run20170525.log &
# nohup spark-submit --driver-memory 16G --executor-memory 16G ml-als-model-predict.py > run20170525-2.log &

from pyspark import SparkContext, SparkConf
sc =SparkContext()

import pandas as pd
import numpy as np
#import scipy.stats as st
#from scipy import stats


import os
import math

#[START read rating file to RDD by format (int userId,int itemId,float rating)]
def read_rating_raw_file(filePath):
  ratings_raw_data = sc.textFile(filePath)
  ratings_raw_data_header = ratings_raw_data.take(1)[0]
  ratings_data = ratings_raw_data.filter(lambda line: line!=ratings_raw_data_header)\
      .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()
  return ratings_data

# ratings_file = os.path.join('.', 'als_userItemRating4_R.csv')
rddTraining = read_rating_raw_file("gohappy_tmp.als_userItemRating_R_201705.csv")
rddTraining.take(3)

import itertools
from math import sqrt
from operator import add
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from time import time
t0 = time()

finalRank  = 20
finalRegul = 0.01
finalIter  = 25

print("Training count: %d" % (rddTraining.count()))
model = ALS.train(rddTraining, finalRank, finalIter, finalRegul)

model.save(sc, 'gohappy_userItemRating_201705')
