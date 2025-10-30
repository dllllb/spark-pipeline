import pandas as pd
import numpy as np

from pyspark.sql import SparkSession

from sparktools.core import score
from sparktools.simple_model import simple_model


def test_score():
    ss = SparkSession.builder.getOrCreate()

    pdf = pd.DataFrame(
        {'id': range(1000), 'f1': np.random.rand(1000), 'f2': np.random.rand(1000)}
    )
    df = ss.createDataFrame(pdf)

    score_df = score(sc=ss.sparkContext, sdf=df, model=simple_model, cols_to_save=['id']).cache()

    scores = score_df.toPandas()

    print(scores.head())
