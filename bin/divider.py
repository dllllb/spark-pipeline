import argparse
import os
import sys
import time
from os.path import dirname, join as path_join

import pandas as pd
import numpy as np

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

parser = argparse.ArgumentParser()
parser.add_argument('--conf', required=True)
args, overrides = parser.parse_known_args()

file_conf = ConfigFactory.parse_file(args.conf, resolve=False)
overrides = ','.join(overrides)
over_conf = ConfigFactory.parse_string(overrides)
conf = over_conf.with_fallback(file_conf)

sqc = spark_utils.init_session(conf['spark'], app=os.path.basename(args.conf))

sdf = spark_utils.define_data_frame(conf['source'], sqc)

sdf = sdf.filter('uid is not null')

row_count = sdf.count()

top_percent = float(conf.get('top-size', '.1'))
top_size = int(row_count * top_percent)

sdf = sdf.orderBy(sdf.target_proba.desc())

df = spark_utils.limit(sdf, top_size).toPandas()

df['current_dt'] = time.strftime('%Y-%m-%dT%H:%M:%S%z')

df['holdout'] = 0

whitelist_file = conf.get('whitelist-file', None)

if whitelist_file is not None:
    if isinstance(whitelist_file, str):
        whitelist_file = [whitelist_file]

    for f in whitelist_file:
        whitelist_path = os.path.join(os.path.dirname(os.path.realpath(args.conf)), os.path.expanduser(f))
        whitelist = pd.read_csv(whitelist_path, squeeze=True)
        whitelist = whitelist.astype(str)

        df.ix[df.uid.isin(whitelist), 'holdout'] = 1

holdout = float(conf.get('holdout', '0.'))

holdout_dyn = df.sample(frac=holdout, random_state=200)
df.ix[holdout_dyn.index, 'holdout'] = 1

result = df[df.holdout == 0]

stats = pd.Series({
    'total records count': row_count,
    'top size': len(result.index),
    'holdout size': (df.holdout == 1).sum(),
    'top unique UIDs': result.uid.nunique(),
    'top 95th percentile': '{:,.2f}'.format(np.percentile(result.target_proba, q=95)),
    'top median': '{:,.2f}'.format(np.percentile(result.target_proba, q=50)),
    'top 5th percentile': '{:,.2f}'.format(np.percentile(result.target_proba, q=5)),
    'source query': conf['source.query'],
    'top %': top_percent,
    'dynamic holdout %': holdout,
})

print(stats)

report_file = conf.get('report-file', None)
if report_file is not None:
    stats.to_csv(report_file, sep='\t')

id_list_file = conf.get('id-list-file', None)
if id_list_file is not None:
    result.uid.to_csv(id_list_file, index=False, sep='\t')

sdf_top = sqc.createDataFrame(df)
spark_utils.write(conf['target'], sdf_top)

print('execution time: {} sec'.format(time.time() - start))
