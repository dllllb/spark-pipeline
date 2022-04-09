Data Science oriented tools, mostly for Apache Spark

- The pipepline for using Python ML models together with Apache Spark
- Command-line tools
- *demo*: usage demos in form of Jupyter notebooks
  - model inference on cluster: [demo/score-sklearn.ipynb](demo/score-sklearn.ipynb)
  - quick dataset distribution change detection: [demo/datadiff.ipynb](demo/datadiff.ipynb)

# Command-line tools

## python -m sparktools.scorer

Use saved model to calculate prediction scores for a large dataset. Calculation is done in parallel on Spark cluster.

Required config entries:
- Spark configuration (spark)
- data set source (source)
- model definition (model-definition)
- saved model file path (model-path)
- write target (target)

Input data set should contain the following fields:
- uid: record ID
- business_dt: feature calculation date
- true_target: true target value for validation (optional)

Result fields:
- uid; from the input data set
- business_dt; from the input data set
- true_target; from the input data set
- target_proba; calculated probability score for a record
- author: current system user
- model_name; model file name
- current_dt; scoring date + time

## python -m sparktools.trainer

Train an ML model and store it locally in binary form

Required config entries:
- Spark configuration
- model definition
- model write location

## python -m sparktools.mover

Move data between source and target storages

Required config entries:
- Spark configuration
- data source
- write target

Source/target storage options:
- local (default dataset-store-format: parquet)
- single-csv (tab as separator, dot as decimal, with header, utf-8 encoding)
- hdfs (default dataset-store-format: parquet)
- jdbc (database connection options to be included)
- hive (default dataset-store-format from spark)

### Teradata to Hive example:
```
source: {
  storage: jdbc
  query: "(select * from db.target_table where dt = '2016-01-01') a"
  conn: {
    url: jdbc:teradata://USER:PASSWORD@HOST:PORT/DATABASE
  }
  partition-column: hash_id
  num-partitions: 50
}

target {
  storage: hive
  query: db.target_table
  write-mode: overwrite
}

spark: {
  include "conf/spark-yarn.conf"
  spark-prop.spark.driver.extraClassPath: "terajdbc4.jar:tdgssconfig.jar"
  jars: [terajdbc4.jar, tdgssconfig.jar]
}
```

### CSV to Teradata example:
```
source: {
  storage: single-csv
  query: 'data/table.csv'
  header: infer
  sep: '\t'
  decimal: '.'
}

target {
  storage: jdbc
  query: db.target_table
  write-mode: overwrite
  conn: {
    url: jdbc:teradata://USER:PASSWORD@HOST:PORT/DATABASE
  }
}

spark: {
  include "conf/spark-yarn.conf"
  spark-prop.spark.driver.extraClassPath: "terajdbc4.jar:tdgssconfig.jar"
  jars: [terajdbc4.jar, tdgssconfig.jar]
}
```

### Greenplum to Hive example:
```
source: {
  storage: jdbc
  query: "jdbc_schema.jdbc_table"
  conn: {
    url: jdbc:postgres://USER:PASSWORD@HOST:PORT/DATABASE 
  }
  partition-column: gp_segment_id  # Greenplum segment ID
  num-partitions: 50
  lower-bound: 0
  upper-bound: 95
}

target {
  storage: hive
  query: db.target_table
  write-mode: overwrite
}

spark: {
  include "conf/spark-yarn.conf"
  spark-prop.spark.driver.extraClassPath: postgresql-42.2.6.jar
  jars: postgresql-42.2.6.jar
}
```

## Common configuration structure

[HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) format is used for configuration files

## Configuration overrides in console

Any configuration param can be overridden from console by adding param value in form full.name=value 

Example:
```bash
scorer.py --conf sparks.conf spark.spark-home=/home/spark spark.pyspark-python=/opt/python3/python
```

## Data source definition

Supported data storages:
- local: local directory with files in Spark-compatible format 
- jdbc: JDBC query
- hive: Hive query
- hdfs: HDFS directory with files in Spark-compatible format
- single-csv: simple CSV file
    - distribute-by
    - transform-sql
    - sample
    - limit

### Example JDBC data source:
```
source {
  storage: jdbc
  query: somedb.some_table
  conn: {
    url: jdbc:teradata://USER:PASSWORD@HOST:PORT/DATABASE
  }
  partition-column: hash_id
  num-partitions: 50
}
```

Example Hive data source:
```
source: {
  storage: hive
  query: '''select * 
              from db.table 
             where business_dt = ${business_dt}'''
}
```

Example local folder target:
```
target: {
  storage: local
  query: /some/path/some-directory
  write-mode: overwrite
}
```

Example simple CSV target:
```
target: {
  storage: single-csv
  query: /some/path/some-file.csv 
}
```

## Model definition

Pipeline file location is considered to be relative to the main configuration file directory

### Example:
```json
{
  "train-dataset": {
    "storage": "jdbc",
    "query": "somedb.some_table",
    "target-column": "y"
  },
  "pipeline-file": "pipeline.py"
}
```

### Load model definition dataset from JDBC data source:
```hocon
model-definfition: {
  include model-2015-11.json
}

source : ${model-definfition.train-dataset} {
  conn: {
    url: jdbc:teradata://USER:PASSWORD@HOST:PORT/DATABASE
  }
  partition-column: hash_id
  num-partitions: 50
}
```
