def pandify(sdf):
    res = sdf

    final_cols = []

    for f in res.schema.fields:
        if '.' in f.name:
            new_name = f.name.replace('.', '__')
            field_name = '`{nm}`'.format(nm=f.name)
            res = res.withColumn(new_name, res[field_name])

        final_cols.append(f.name.replace('.', '__'))

    res = res.select(*final_cols)

    for f in res.schema.fields:
        if f.dataType.typeName() == 'decimal':
            res = res.withColumn(f.name, res[f.name].astype('float'))

    return res


def limit(sdf, n_records):
    res = sdf.rdd.zipWithIndex() \
        .filter(lambda e: e[1] < n_records) \
        .map(lambda e: e[0]).toDF()
    return res


def score_udf(sdf, model, target_class_names=None, cols_to_save=None):
    schema = ', '.join([f'{col} {dtype}' for col, dtype in sdf.selectExpr(cols_to_save).dtypes])
    schema += ', '
    if target_class_names is None or len(target_class_names) < 3:
        schema += 'target_proba float'
    else:
        schema += ', '.join([f'{label} float' for label in target_class_names])

    def _score_udf(it):
        for features_df in it:
            pred = predict(features_df, model, target_class_names, cols_to_save)
            yield pred
    
    scores = sdf.mapInPandas(_score_udf, schema)
    return scores


def predict(features_df, mdl, target_class_names=None, cols_to_save=None):
    from sklearn.base import is_classifier, is_regressor
    import pandas as pd

    if cols_to_save is not None:
        existing_cols_to_save = list(set(cols_to_save).intersection(features_df.columns))
        res_df = features_df[existing_cols_to_save].copy()
    else:
        res_df = pd.DataFrame()

    if is_classifier(mdl):
        pred = mdl.predict_proba(features_df)

        if pred.shape[1] == 2:
            res_df['target_proba'] = pred[:, 1]
        else:
            if target_class_names is None:
                target_class_names =[f'class{i}' for i in range(pred.shape[1])]

            for i, label in enumerate(target_class_names):
                 res_df[label] = pred[:, i]
    elif is_regressor(mdl):
        res_df['pred'] = mdl.predict(features_df)
    else:
        raise AttributeError('unknown model type')

    return res_df


def block_iterator(iterator, size):
    bucket = list()
    for e in iterator:
        bucket.append(e)
        if len(bucket) >= size:
            yield bucket
            bucket = list()
    if bucket:
        yield bucket


def score(sc, sdf, model, cols_to_save=None, target_class_names=None, code_in_pickle=False):
    import json
    import pandas as pd

    if code_in_pickle:
        import dill
        model_bc = sc.broadcast(dill.dumps(model))
    else:
        model_bc = sc.broadcast(model)

    col_bc = sc.broadcast(sdf.columns)

    def block_classify(iterator):
        from sklearn.base import is_classifier, is_regressor

        if code_in_pickle:
            mdl = dill.loads(model_bc.value)
        else:
            mdl = model_bc.value

        for features in block_iterator(iterator, 10000):
            features_df = pd.DataFrame(list(features), columns=col_bc.value)

            res_df = predict(features_df, mdl, target_class_names, cols_to_save)

            for e in json.loads(res_df.to_json(orient='records')):
                yield e

    scores = sdf.rdd.mapPartitions(block_classify)
    score_df = scores.toDF()

    return score_df


def define_data_frame(conf, sqc):
    import pandas as pd

    storage = conf['storage']

    if storage == 'jdbc':
        sdf = jdbc_load(
            sqc=sqc,
            query=conf['query'],
            conn_params=conf['conn'],
            partition_column=conf.get('partition-column', None),
            num_partitions=conf.get('num-partitions', None),
            lower_bound=conf.get('lower-bound', None),
            upper_bound = conf.get('upper-bound', None))
    elif storage == 'local':
        dataset_format = conf.get('dataset-store-format', 'parquet')
        data_path = conf['query']
        sdf = sqc.read.format(dataset_format).load(data_path, header=True)
    elif storage == 'hdfs':
        dataset_format = conf.get('dataset-store-format', 'parquet')
        data_path = conf['query']
        sdf = sqc.read.format(dataset_format).load(data_path, header=True)
    elif storage == 'single-csv':
        data_path = conf['query']
        header = conf.get('header', 'infer')
        sep = conf.get('sep', '\t')
        decimal = conf.get('decimal', '.')
        pdf = pd.read_csv(data_path, sep=sep, header=header, decimal=decimal, encoding='utf8')
        sdf = sqc.createDataFrame(pdf)
    elif storage == 'hive':
        sdf = sqc.sql(conf['query'])
    else:
        raise ValueError('unknown storage type: {st}'.format(st=storage))

    if 'distribute-by' in conf:
        sdf = sdf.repartition(conf['distribute-by.n-partitions'], conf['distribute-by.key'])

    if 'transform-sql' in conf:
        sdf.registerDataFrameAsTable(sdf, 'dataset_temp')
        sdf = sqc.sql(conf['transform-sql'])

    if 'sample' in conf:
        sdf = sdf.sample(False, fraction=conf.get_float('sample'), seed=4233)

    if 'limit' in conf:
        sdf = sdf.limit(conf.get_int('limit'))

    return sdf


def write(conf, sdf):
    if conf.get_bool('disabled', False):
        return

    storage = conf['storage']

    if 'distribute-by' in conf:
        sdf = sdf.repartition(conf['distribute-by.n-partitions'], conf['distribute-by.key'])

    if 'n-partitions' in conf:
        sdf = sdf.repartition(conf['n-partitions'])

    if storage == 'local':
        target_dir = conf['query']
        import os
        if not os.path.exists(os.path.dirname(target_dir)):
            os.makedirs(os.path.dirname(target_dir))

        write_format = conf.get('dataset-store-format', 'orc')
        write_mode = conf.get('write-mode', 'overwrite')
        sdf.write.mode(write_mode).format(write_format).save(target_dir)
    elif storage == 'hdfs':
        target_dir = conf['query']
        write_format = conf.get('dataset-store-format', 'orc')
        write_mode = conf.get('write-mode', 'overwrite')

        w = sdf.write.mode(write_mode).format(write_format)

        partition_by = conf.get('partition-by', None)
        w.save(target_dir, partitionBy=partition_by)
    elif storage == 'jdbc':
        result_table = conf['query']

        write_mode = conf.get('write-mode', 'append')

        sdf.repartition(1).write.mode(write_mode).jdbc(
            url=conf['conn']['url'],
            properties=conf['conn'],
            table=result_table)
    elif storage == 'hive':
        write_mode = conf.get('write-mode', 'append')
        table = conf['query']
        write_format = conf.get('dataset-store-format', 'orc')
        partition_by = conf.get('partition-by', None)

        save_to_hive(sdf, table, write_mode, partition_by, write_format)
    elif storage == 'single-csv':
        data_path = conf['query']
        header = conf.get_bool('header', True)
        sep = str(conf.get('sep', '\t'))
        decimal = str(conf.get('decimal', '.'))
        pdf = sdf.toPandas()
        pdf.to_csv(data_path, sep=sep, header=header, decimal=decimal, encoding='utf8', index=False)
    elif storage == 'csv':
        data_path = conf['query']
        header = conf.get_bool('header', True)
        sep = str(conf.get('sep', '\t'))

        save_to_csv(sdf, data_path, header, sep)
    else:
        raise ValueError('unknown storage type: {st}'.format(st=storage))


def save_to_hive(sdf, table, write_mode='append', partition_by=None, write_format=None):
    db, tname = table.split('.')
    if tname in sdf.sql_ctx.tableNames(db):
        cols = sdf.sql_ctx.sql('show columns in {}'.format(table)).collect()
        column_order = [c[0].strip() for c in cols]
    else:
        column_order = sdf.columns

    w = sdf.select(*column_order).write.mode(write_mode)

    if write_format is not None:
        w = w.format(write_format)

    if tname in sdf.sql_ctx.tableNames(db):
        w.partitionBy(partition_by).insertInto(table)
    else:
        w.saveAsTable(table, partitionBy=partition_by)


def save_to_csv(sdf, data_path, header=True, sep='\t'):
    from csv import DictWriter

    with open(data_path, 'wb') as f:
        dw = DictWriter(f, [c.encode('utf8') for c in sdf.columns], delimiter=sep)
        if header:
            dw.writeheader()

        for row in sdf.toLocalIterator():
            r = dict([(k.encode('utf8'), w.encode('utf8')) for k, w in row.items()])
            dw.writerow(r)


def prop_list(tree, prefix=list()):
    res = dict()
    for k, v in tree.items():
        path = prefix + [k]
        if isinstance(v, dict):
            res.update(prop_list(v, path))
        else:
            res['.'.join(path)] = v
    return res


def init_spark(config, app=None, use_session=False):
    import os
    import sys
    from glob import glob

    if 'spark-home' in config:
        os.environ['SPARK_HOME'] = config['spark-home']

    if 'spark-conf-dir' in config:
        os.environ['SPARK_CONF_DIR'] = config['spark-conf-dir']

    if 'pyspark-python' in config:
        # Set python interpreter on both driver and workers
        os.environ['PYSPARK_PYTHON'] = config['pyspark-python']

    if 'yarn-conf-dir' in config:
        # Hadoop YARN configuration
        os.environ['YARN_CONF_DIR'] = config['yarn-conf-dir']

    if 'spark-classpath' in config:
        # can be used to set external folder with Hive configuration
        # e. g. spark-classpath='/etc/hive/conf.cloudera.hive1'
        os.environ['SPARK_CLASSPATH'] = config['spark-classpath']

    submit_args = []

    driver_mem = config.get('spark-prop.spark.driver.memory', None)
    if driver_mem is not None:
        submit_args.extend(["--driver-memory", driver_mem])

    driver_cp = config.get('spark-prop.spark.driver.extraClassPath', None)
    if driver_cp is not None:
        submit_args.extend(["--driver-class-path", driver_cp])

    driver_java_opt = config.get('spark-prop.spark.driver.extraJavaOptions', None)
    if driver_java_opt is not None:
        submit_args.extend(["--driver-java-options", driver_java_opt])

    jars = config.get('jars', None)
    if jars is not None:
        if isinstance(jars, str):
            jars = [jars]
        submit_args.extend(["--jars", ','.join(jars)])

    mode_yarn = config['spark-prop.spark.master'].startswith('yarn')

    if mode_yarn:
        # pyspark .zip distribution flag is set only if spark-submit have master=yarn in command-line arguments
        # see spark.yarn.isPython conf property setting code
        # in org.apache.spark.deploy.SparkSubmit#prepareSubmitEnvironment
        submit_args.extend(['--master', 'yarn'])

    # pyspark .zip distribution flag is set only if spark-submit have pyspark-shell or .py as positional argument
    # see spark.yarn.isPython conf property setting code
    # in org.apache.spark.deploy.SparkSubmit#prepareSubmitEnvironment
    submit_args.append('pyspark-shell')

    os.environ['PYSPARK_SUBMIT_ARGS'] = ' '.join(submit_args)

    spark_home = os.environ['SPARK_HOME']
    spark_python = os.path.join(spark_home, 'python')
    pyspark_libs = glob(os.path.join(spark_python, 'lib', '*.zip'))
    sys.path.extend(pyspark_libs)

    virtualenv_reqs = config['spark-prop'].get('spark.pyspark.virtualenv.requirements', None)
    if use_session:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder.appName(app or config['app'])

        if mode_yarn:
            builder = builder.enableHiveSupport()

        for k, v in prop_list(config['spark-prop']).items():
            builder = builder.config(k, v)

        ss = builder.getOrCreate()
        if virtualenv_reqs is not None:
            ss.addFile(virtualenv_reqs)
        return ss
    else:
        from pyspark import SparkConf, SparkContext
        conf = SparkConf()
        conf.setAppName(app or config['app'])
        props = [(k, str(v)) for k, v in prop_list(config['spark-prop']).items()]
        conf.setAll(props)
        sc = SparkContext(conf=conf)
        if virtualenv_reqs is not None:
            sc.addFile(virtualenv_reqs)
        return sc


def init_session(config, app=None, return_context=False, overrides=None, use_session=False):
    import os
    from pyhocon import ConfigFactory, ConfigParser

    if isinstance(config, str):
        if os.path.exists(config):
            base_conf = ConfigFactory.parse_file(config, resolve=False)
        else:
            base_conf = ConfigFactory.parse_string(config, resolve=False)
    elif isinstance(config, dict):
        base_conf = ConfigFactory.from_dict(config)
    else:
        base_conf = config

    if overrides is not None:
        over_conf = ConfigFactory.parse_string(overrides)
        conf = over_conf.with_fallback(base_conf)
    else:
        conf = base_conf
        ConfigParser.resolve_substitutions(conf)

    res = init_spark(conf, app, use_session)

    if use_session:
        return res
    else:
        mode_yarn = conf['spark-prop.spark.master'].startswith('yarn')

        if mode_yarn:
            from pyspark.sql import HiveContext
            sqc = HiveContext(res)

            if 'hive-prop' in conf:
                for k, v in prop_list(conf['hive-prop']).items():
                    sqc.setConf(k, str(v))
        else:
            from pyspark.sql import SQLContext
            sqc = SQLContext(res)

        if return_context:
            return res, sqc
        else:
            return sqc


def jdbc_load(
    sqc,
    query,
    conn_params,
    partition_column=None,
    num_partitions=10,
    lower_bound=None,
    upper_bound=None,fetch_size=10000000
):
    import re
    if re.match(r'\s*\(.+\)\s+as\s+\w+\s*', query):
        _query = query
    else:
        _query = '({}) as a'.format(query)

    conn_params_base = dict(conn_params)
    if partition_column and num_partitions and num_partitions > 1:
        if lower_bound is None or upper_bound is None:
            min_max_query = '''
              (select max({part_col}) as max_part, min({part_col}) as min_part
                 from {query}) as g'''.format(part_col=partition_column, query=_query)
            max_min_df = sqc.read.load(dbtable=min_max_query, **conn_params_base)
            tuples = max_min_df.rdd.collect()
            lower_bound = str(tuples[0].max_part)
            upper_bound = str(tuples[0].min_part)
        conn_params_base['fetchSize'] = str(fetch_size)
        conn_params_base['partitionColumn'] = partition_column
        conn_params_base['lowerBound'] = lower_bound
        conn_params_base['upperBound'] = upper_bound
        conn_params_base['numPartitions'] = str(num_partitions)
    sdf = sqc.read.load(dbtable=_query, **conn_params_base)
    return sdf


def hive_to_pandas(query, tmpdir='.', verbose=False):
    '''Load huge tables from Hive slightly faster than over toPandas in Spark
    Parameters
    ----------
    query : sql query or tablename
    tmpdir : dir to store temporary csv files while execution
 
    Returns
    ----------
    df : pandas Dataframe with query results
 
    Examples
    ----------
    # tablename as input
    df = hive_to_pandas('SANDBOX.S3_MSK_10K_SAMPLE_DATA_TEST', verbose=True)
    # query as input
    q = "SELECT uid, actv_strt_tm, tot_data_vol_cnt FROM SANDBOX.S3_MSK_10K_SAMPLE_DATA2 WHERE table_business_date >= '2017-10-01'"
    df = hive_to_pandas(q, verbose=True)
    '''
    import subprocess
    import tempfile
    import pandas as pd
    hive_options = 'set hive.cli.print.header=true; set hive.resultset.use.unique.column.names=false;'
    if not query.lower().startswith('select'):
        query = 'SELECT * FROM {}'.format(query)
    with tempfile.NamedTemporaryFile(dir=tmpdir) as td:
        if verbose:
            print('created temporary file: %s' % td.name)
        cmd = "hive -e '{} {}' > {}".format(hive_options, query.replace("'", "'\\''"), td.name)
        if verbose:
            print('cmd: {}'.format(cmd))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        process.communicate()
        df = pd.read_csv(td.name, sep='\t')
        if verbose:
            print('df shape: {}'.format(df.shape))
        return df


def toPandas(sparkdf, sqlContext, tmpdir='.', verbose=False):
    from random import choice
    import string
    # saving temp table
    postfix = ''.join(choice(string.ascii_uppercase + string.digits) for _ in range(6))
    table_name = 'tmp_table_' + postfix
    if verbose:
        print('created temporary table: %s' % table_name)
    sparkdf.registerTempTable(table_name)
    sqlContext.sql('create table {t} as select * from {t}'.format(t=table_name))
    df = hive_to_pandas(table_name, tmpdir='.', verbose=verbose)
    sqlContext.sql('drop table {t}'.format(t=table_name))
    if verbose:
        print('dropped temporary table: %s' % table_name)
    return df


def partition_iterator(sdf):
    import pyspark.sql.functions as F
    sdf_part = sdf.withColumn('partition', F.spark_partition_id())
    sdf_part.cache()
    for part in range(sdf.rdd.getNumPartitions()):
        yield sdf_part.where(F.col('partition') == part).drop('partition').rdd.toLocalIterator()


def toPandasIterative(sparkdf, batch=10000):
    import pandas as pd
    if batch == 'partition':
        part_iter = partition_iterator(sparkdf)
    else:
        part_iter = block_iterator(sparkdf.rdd.toLocalIterator(), batch)

    df_list = map(lambda rows: pd.DataFrame.from_records(list(rows),
                                                         columns=sparkdf.columns),
                  part_iter)
    return pd.concat(df_list)


def proportion_samples(sdf, proportions_sdf, count_column='rows_count'):
    '''Load huge tables from Hive slightly faster than over toPandas in Spark
    Parameters
    ----------
    sdf : spark Dataframe to sample from
    proportions_sdf : spark Dataframe with counts to sample from sdf
    count_column: column name with counts, other columns used as statifiers
 
    Returns
    ----------
    sampled : spark Dataframe with number of rows lesser or equal proportions_sdf for each strata
    '''
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window
    groupers = [c for c in proportions_sdf.columns if c != count_column]

    sampled = sdf.join(proportions_sdf, groupers, how='inner').withColumn(
        'rownum',
        F.rowNumber().over(Window.partitionBy(groupers))
    ).filter(
        F.col('rownum') <= F.col(count_column)
    ).drop(count_column).drop('rownum')
    return sampled
