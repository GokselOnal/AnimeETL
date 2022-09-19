import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

if __name__ == '__main__':
    print(pd.read_csv("data/anime_details.csv"))