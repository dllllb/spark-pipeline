import argparse
import importlib
import os
import sys
import time
import joblib

from pyhocon import ConfigFactory

import sparktools.core as spark_utils

start = time.time()

print('{tm} ------------------- {nm} started'.format(
    tm=time.strftime("%Y-%m-%d %H:%M:%S"),
    nm=os.path.basename(__file__)
))

parser = argparse.ArgumentParser()
parser.add_argument('--conf', required=True)
args, overrides = parser.parse_known_args()

file_conf = ConfigFactory.parse_file(args.conf, resolve=False)
overrides = ','.join(overrides)
over_conf = ConfigFactory.parse_string(overrides)
conf = over_conf.with_fallback(file_conf)

model_definition = conf['model-definition']
model_path = conf['model-path']

pipeline_full_path = os.path.join(
    os.path.dirname(os.path.realpath(args.conf)),
    model_definition['pipeline-file']
)
sys.path.append(os.path.dirname(pipeline_full_path))

module_name = os.path.splitext(os.path.basename(model_definition['pipeline-file']))[0]

pipeline = importlib.import_module(module_name)

sqc = spark_utils.init_session(conf['spark'], app=os.path.basename(args.conf))

print('{time} loading data...'.format(time=time.strftime("%Y-%m-%d %H:%M:%S")))

sdf = spark_utils.define_data_frame(model_definition['dataset'], sqc)
sdf = spark_utils.pandify(sdf)
df = sdf.toPandas()

df = df.dropna(axis=1, how='all')

target_column = model_definition['dataset.target-column']
features = df.drop([target_column], axis=1)
target = df[target_column]

print('data set shape: {sz}'.format(sz=features.shape))

pl = pipeline.new_pipeline()

pl.fit(features, target)

if not os.path.exists(os.path.dirname(model_path)):
    os.makedirs(os.path.dirname(model_path))

joblib.dump(pl, model_path, compress=3)

print('model is saved to {path}'.format(path=model_path))

print('execution time: {} sec'.format(time.time() - start))
