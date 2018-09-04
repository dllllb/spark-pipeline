import pyspark.sql.functions as f


def hash_histogram(df, column_name, max_bins):
    hashed = df.select((f.crc32(f.col(column_name).cast("string")) % max_bins).alias(column_name))
    hist = hashed.groupBy(column_name).count()
    hist = hist.rdd.collectAsMap()

    if None in hist:
        hist["None"] = hist[None]
        del hist[None]

    values_total = float(sum(hist.values()))
    hist = dict((k, v / values_total) for k, v in hist.items())
    return hist


def histogram_to_map(histogram):
    bounds, counts = histogram
    hist = dict()
    for ind in range(len(bounds) - 1):
        key = "{lower}_{upper}".format(lower=bounds[ind], upper=bounds[ind + 1])
        hist[key] = counts[ind]
    return hist


def continuous_comparator(df1, col1, df2, col2, max_bins=1000):
    rdd1 = df1.select(col1).rdd
    source1_f = rdd1.flatMap(lambda x: x).map(lambda x: float(x) if x is not None else None)
    rdd2 = df2.select(col2).rdd
    source2_f = rdd2.flatMap(lambda x: x).map(lambda x: float(x) if x is not None else None)
    hist1 = source1_f.histogram(max_bins)
    hist2 = source2_f.histogram(hist1[0])
    hist1m = histogram_to_map(hist1)
    hist2m = histogram_to_map(hist2)

    values1_total = float(sum(hist1m.values()))
    values2_total = float(sum(hist2m.values()))
    res = 0
    for key in set(hist1m.keys()).union(hist2m.keys()):
        val1 = hist1m[key] / values1_total if key in hist1m else 0
        val2 = hist2m[key] / values2_total if key in hist2m else 0
        res += abs(val1 - val2)
    return res / 2


def discrete_comparator(df1, col1, df2, col2, max_bins=1000):
    hist1 = hash_histogram(df1, col1, max_bins)
    hist2 = hash_histogram(df2, col2, max_bins)

    res = 0
    for key in set(hist1.keys()).union(hist2.keys()):
        val1 = hist1[key] if key in hist1 else 0
        val2 = hist2[key] if key in hist2 else 0
        res += abs(val1 - val2)
    return res / 2
