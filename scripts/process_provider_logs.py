import sys

import pandas as pd
from parsing import providers, location

log_file = sys.argv[1]
out_file = sys.argv[2]

cid_providers = []
with open(log_file, 'r') as f:
    for line in f:
        if 'Found' in line:
            _, cid, provs, _ = providers.parse_entry(line)
            for p in provs:
                cid_providers.append({'cid': cid, 'ip': p})

print("Processed file", log_file)
df = pd.DataFrame(cid_providers).dropna()
print("Created Dataframe")
locs = df.apply(lambda r: pd.Series(location.lookup_ip(r['ip']), index=['continent', 'country', 'regions', 'lat', 'long']), axis=1)
print("Got locations")
df = df.join(locs)
print("Joined data")
df = df.dropna()

df.to_csv(out_file)
print("Outputted to file", out_file)
