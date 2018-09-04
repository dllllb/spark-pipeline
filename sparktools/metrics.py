def lift(target, proba, n_buckets=10):
    import pandas as pd
    import numpy as np

    n_records = len(target)
    bucket_sz = int(n_records / n_buckets)

    counts = np.ones(n_buckets, int)*bucket_sz
    counts[:n_records%n_buckets] += 1
    tops = [np.full(c, n, int) for c, n in zip(counts, range(1, n_buckets+1))]
    tops = np.concatenate(tops)

    df = pd.DataFrame({'target': target, 'proba': proba})
    df = df.sort_values('proba', ascending=False)
    df['top'] = tops
    target_sum = df.groupby('top').target.sum()
    ff = pd.DataFrame({'target_cnt': target_sum, 'cnt': counts})
    ff['target_cnt_cum'] = ff.target_cnt.cumsum()
    ff['cnt_cum'] = ff.cnt.cumsum()
    ff['target_share'] = ff.target_cnt/ff.cnt
    ff['target_share_cum'] = ff.target_cnt_cum/ff.cnt_cum
    target_cnt = ff.target_cnt.sum()
    target_share = float(target_cnt)/ff.cnt.sum()
    ff['lift'] = ff.target_share/target_share
    ff['cum_lift'] = ff.target_share_cum/target_share
    ff['coverage'] = ff.target_cnt_cum/target_cnt
    return ff