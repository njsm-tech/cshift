import pandas as pd

FILE = 'country_vaccinations.csv'
OUTFILE = 'country_vaccinations.parquet'

def main():
    df = pd.read_csv(FILE)
    df.to_parquet(OUTFILE)

if __name__ == '__main__':
    main()
