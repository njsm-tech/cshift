import pandas as pd

FILES = [
    '50Hertz.csv',
    'Amprion.csv',
    'TenneTTSO.csv',
    'TransnetBW.csv'
]

OUTFILE = 'wind_power.parquet'

YMD_FMT = '%Y-%m-%d'

def main():
    res_df = None
    for fp in FILES:
        source_name = fp.split('.')[0]
        df = pd.read_csv(fp)
        df['Date'] = pd.to_datetime(df.Date)
        df['Date'] = df['Date'].dt.strftime(YMD_FMT)
        df = df.set_index(['Date']).stack().to_frame(name=source_name)
        if res_df is None:
            res_df = df
        else:
            res_df = res_df.join(df)
    print(res_df)
    res_df.to_parquet(OUTFILE)

if __name__ == '__main__':
    main()
