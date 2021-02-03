from typing import Dict, List

import simplejson as json

import pandas as pd

FILES = [
    'training_set.json',
    'validation_set.json',
    'test_set.json'
]

OUTFILE = 'combined'

FEAT_NAMES = [
    'WS50M_RANGE',
    'WS50M_MIN',
    'WS50M_MAX',
    'WS50M',
    'WS10M_RANGE',
    'WS10M_MIN',
    'WS10M_MAX',
    'WS10M',
    'PRECTOT',
    'PS',
    'QV2M',
    'T2M',
    'T2MDEW',
    'T2MWET',
    'T2M_MAX',
    'T2M_MIN',
    'T2M_RANGE',
    'TS'
]

def extract_features(row: Dict) -> pd.DataFrame:
    try:
        meta = row['meta']
        ds = meta['date']
        fips = meta['fips']

        feats_dict = row['values']

        df = pd.DataFrame(feats_dict)
        df['date'] = ds
        year = ds.split('-')[0]
        df['year'] = year
        df['fips'] = fips

        return df
    except Exception as e:
        print(e)
        print(row)
        return 

def main():
    set_dfs = []
    for fp in FILES:
        print(fp)
        with open(fp) as f:
            data = json.load(f)
        r = data['root']
        row_dfs = []
        for hash_code, row in r.items():
            row_df = extract_features(row)
            if row_df is not None:
                row_dfs.append(row_df)
        set_df = pd.concat(row_dfs, axis=0)
        set_df['source_set'] = fp.split('_')[0]
        set_dfs.append(set_df)
    df = pd.concat(set_dfs, axis=0).reset_index(drop=True)
    print(df)
    df.to_parquet(OUTFILE, partition_cols=['year'])

if __name__ == '__main__':
    main()
