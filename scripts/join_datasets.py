import sys

import pandas as pd


requests_file = sys.argv[1]
providers_file = sys.argv[2]
out = sys.argv[3]


requests = pd.read_csv(requests_file, keep_default_na=False)
providers = pd.read_csv(providers_file, keep_default_na=False)
print("Loaded datasets")

requests = requests[['cid', 'continent', 'country', 'regions']]
providers = providers[['cid', 'continent', 'country', 'regions']]

request_providers = requests.merge(providers, on=['cid'], suffixes=('_request', '_provider'))
print("Merged datasets")

request_providers.to_csv(out)
print("Outputted to file", out)
