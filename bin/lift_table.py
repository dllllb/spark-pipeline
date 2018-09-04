import argparse
import os
import sys
import time
from os.path import dirname, join as path_join

from pyhocon import ConfigFactory

start = time.time()

print('{tm} ------------------- {nm} started'.format(
    tm=time.strftime("%Y-%m-%d %H:%M:%S"),
    nm=os.path.basename(__file__)
))

module_path = os.path.realpath(__file__)
root_dir = dirname(dirname(module_path))
sys.path.append(path_join(root_dir, 'dstools'))

import sparktools.core as spark_utils

from sparktools.metrics import lift

parser = argparse.ArgumentParser()
parser.add_argument('--conf', required=True)
args, overrides = parser.parse_known_args()

file_conf = ConfigFactory.parse_file(args.conf, resolve=False)
overrides = ','.join(overrides)
over_conf = ConfigFactory.parse_string(overrides)
conf = over_conf.with_fallback(file_conf)

sqc = spark_utils.init_session(conf['spark'], app=os.path.basename(args.conf))

sdf = spark_utils.define_data_frame(conf['source'], sqc)

df = sdf.select('true_target', 'target_proba').toPandas()

lift_cov = lift(df.true_target.astype(int), df.target_proba, n_buckets=100)

lift_cov.to_csv(conf['report-path'], index_label='top', sep='\t')

print('execution time: {} sec'.format(time.time() - start))
