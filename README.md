Data Science oriented tools, mostly for Apache Spark

- The pipepline for using Python ML models together with Apache Spark
- Command-line tools (see [readme](bin/README.md))
- *demo*: usage demos in form of Jupyter notebooks

# Spark job execution

### Usage from ipython

```ipython
%run core.py

config = '''
spark-home: /opt/spark/
pyspark-python: /opt/anaconda2/bin/python
spark-prop: {
  spark.driver.memory: 10g,
  spark.master: local[4]
  spark.driver.maxResultSize: 5g
  spark.memory.offHeap.enabled: true
  spark.memory.offHeap.size: 10g
}
'''

ss = init_session(config=config, app='My App', use_session=True)
```

### Usage from ipython (file config with overrides):


```ipython
%run core.py

overrides = '''
spark-prop: {
  spark.executor.instances: 20
  spark.executor.memory: 30g
}
'''

ss = init_session(config='spark.conf', app='My App', use_session=True, overrides=overrides)
```