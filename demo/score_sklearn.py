## Setup:
# !conda install pyspark openjdk scikit-learn pandas

import sys
import os

script_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(f'{script_path}/..')
os.environ['PYTHONPATH'] = f'{script_path}/..'

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
ss = SparkSession.builder.getOrCreate()

ds = pd.read_csv(f'{script_path}/titanic.csv')
features = ds.drop(['survived', 'alive'], axis=1)
features = features.replace(r'\\s+', np.nan, regex=True)
features = pd.get_dummies(features)
features = features.fillna(features.mean())
target = ds.survived

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

f_t, f_v, t_t, t_v = train_test_split(features, target, test_size=0.5)

m = RandomForestClassifier(n_estimators=10).fit(f_t, t_t)

from pyspark.sql import SparkSession
ss = SparkSession.builder.getOrCreate()

df = ss.createDataFrame(f_v)

from sparktools.core import score

score_df = score(sc=ss.sparkContext, sdf=df, model=m).cache()

scores = score_df.toPandas()

print(scores.head())
