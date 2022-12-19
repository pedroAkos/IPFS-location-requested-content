import sys

import pandas as pd
from parsing import gateway

# Extracts the unique cids from the logs of the gateway requests.

data = sys.argv[1]
out = sys.argv[2]

df = pd.read_csv(data)
df = gateway.get_unique_cids(df)

df.to_csv(out, index=False, header=False)