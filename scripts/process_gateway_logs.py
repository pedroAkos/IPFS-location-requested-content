import sys

from parsing import location, gateway
import pandas as pd

log_file = sys.argv[1]
out_file = sys.argv[2]

log_data = []
with open(log_file, 'r') as f:
    for line in f:
        try:
            log_data.append(gateway.parse_log_entry(line))
        except Exception as e:
            # line in wrong format
            print("Error parsing line", line, "error:", e)

# Note: for larger datasets consider first writing to csv
print("Processed file", log_file)
df = pd.DataFrame(log_data)
print("Created Dataframe")
df = gateway.filter_data(df)
print("Filtered data")
cids = df.apply(lambda r: pd.Series(gateway.extract_cid(r['http_host'], r['target']), index=['cid']), axis=1)
print("Got cids")
locs = df.apply(lambda r: pd.Series(location.lookup_ip(r['ip']), index=['continent', 'country', 'regions', 'lat', 'long']), axis=1)
print("Got locations")
df = df.join(cids)
df = df.join(locs)
print("Joined data")
df = df.dropna()

df.to_csv(out_file)
print("Outputted to file", out_file)