'''
function to summarize results
'''

import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv("fulltestresultsrecheck.csv")
    summary = df.groupby(by=["function", "batch_size"])["time"].describe()
    summary = summary.round(2)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    print(summary)
