import argparse
import os
import sys
import time
import joblib

from pyhocon import ConfigFactory

import sparktools.core as spark_utils


def run_scorer(sc, sqc, conf):
    start = time.time()

    print('{tm} ------------------- {nm} started'.format(
        tm=time.strftime("%Y-%m-%d %H:%M:%S"),
        nm=os.path.basename(__file__)
    ))

    pipeline_file = conf.get('pipeline-file', None)

    if pipeline_file is not None:
        pipeline_full_path = os.path.join(
            os.path.dirname(os.path.realpath(args.conf)), pipeline_file)
        sc.addPyFile(pipeline_full_path)

    print('{} loading data...'.format(time.strftime("%Y-%m-%d %H:%M:%S")))

    import sparktools.core as spark_utils
    sdf = spark_utils.define_data_frame(conf['source'], sqc)
    sdf = sdf.filter('uid is not null')
    sdf = sdf.withColumn('uid', sdf.uid.astype('string'))
    sdf = spark_utils.pandify(sdf)

    cols_to_save = conf.get('cols-to-save', ['uid', 'true_target', 'business_dt'])
    target_class_names = conf.get('target-class-names', None)
    code_in_pickle = conf.get('code-in-pickle', False)

    model = joblib.load(os.path.expanduser(conf['model-path']))

    score_df = spark_utils.score(
        sc=sc,
        sdf=sdf,
        model=model,
        cols_to_save=cols_to_save,
        target_class_names=target_class_names,
        code_in_pickle=code_in_pickle
    ).cache()

    model_name, _ = os.path.splitext(os.path.basename(conf['model-path']))
    current_dt = time.strftime("%Y-%m-%dT%H-%M")

    score_df = score_df.selectExpr(
        "'{}' as model_name".format(model_name),
        "'{}' as current_dt".format(current_dt),
        '*'
    )

    print('scores generated: {}'.format(score_df.count()))

    print('{} saving scores ...'.format(time.strftime("%Y-%m-%d %H:%M:%S")))

    spark_utils.write(conf['target'], score_df)

    print('execution time: {} sec'.format(time.time() - start))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conf', required=True)
    args, overrides = parser.parse_known_args()

    file_conf = ConfigFactory.parse_file(args.conf, resolve=False)
    overrides = ','.join(overrides)
    over_conf = ConfigFactory.parse_string(overrides)
    conf = over_conf.with_fallback(file_conf)

    sc, sqc = spark_utils.init_session(conf['spark'], app=os.path.basename(args.conf), return_context=True)

    run_scorer(sc, sqc, conf)


if __name__ == "__main__":
    main()
