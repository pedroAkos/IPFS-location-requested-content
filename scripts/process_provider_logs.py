import sys

import pandas as pd
from parsing import providers, location

log_file = sys.argv[1]
out_file = sys.argv[2]

cid_providers = []
with open(log_file, 'r') as f:
    for line in f:
        _, cid, provs, _ = providers.parse_entry(line)
        for p in provs:
            cid_providers.append({'cid': cid, 'ip': p})


df = pd.DataFrame(providers)
locs = df.apply(lambda r: pd.Series(location.lookup_ip(r['ip']), index=['continent', 'country', 'regions']), axis=1)
df = df.join(locs)

df.to_csv(out_file)
