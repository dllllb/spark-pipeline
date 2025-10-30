import argparse
import os
import sys
import time

from pyhocon import ConfigFactory

import sparktools.core as spark_utils


def main():
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

    sqc = spark_utils.init_session(conf['spark'], app=os.path.basename(args.conf))

    print('{tm} moving data...'.format(tm=time.strftime("%Y-%m-%d %H:%M:%S")))

    sdf = spark_utils.define_data_frame(conf['source'], sqc)
    spark_utils.write(conf['target'], sdf)

    print('data set size: {sz}'.format(sz=sdf.count()))
    print('{tm} download is finished'.format(tm=time.strftime("%Y-%m-%d %H:%M:%S")))

    print('execution time: {} sec'.format(time.time() - start))


main()
