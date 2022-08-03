import sys

import pandas as pd
from parsing import gateway

data = sys.argv[1]
out = sys.argv[2]

df = pd.read_csv(data)
df = gateway.get_unique_cids(df)

df.to_csv(out, index=False, header=False)