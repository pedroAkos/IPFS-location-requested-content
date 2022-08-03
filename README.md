# Location of IPFS end-users and requested content


You can find more about this project in this [Notion](https://www.notion.so/pl-strflt/Location-of-IPFS-end-users-and-requested-content-7668e98a725d4eea9f36fcafaabe7120) page.


## How to run:

We provide sample data in ``scripts/data/sample`` folder.
This data is a 80M cut from IPFS gateways from April of 2022.
In this folder the data is already processed, but you can run yourself as well.

```
$> cd scripts
scripts$> python3 -m process_gateway_logs data/sample/sample_data.log data/sample/sample_data.csv
scripts$> python3 -m get_unique_cids data/sample/sample_data.csv data/sample/unique_cids
scripts$> cd ../find_providers
find_providers$> go run find_providers.go --f ../scripts/data/sample/unique_cids --out ../scripts/data/sample/providers.log --progress
find_providers$> cd ../scripts
scripts$> python3 -m process_provider_logs data/sample/providers.log data/sample/providers.csv
scripts$> python3 -m join_datasets data/sample/sample_data.csv data/sample/providers.csv data/sample/merged.csv
scripts$> python3 -m plot_data -d data/sample/merged.csv -o data/sample/by_continent.png -t continent
scripts$> python3 -m plot_data -d data/sample/merged.csv -o data/sample/by_country.png -t country
scripts$> python3 -m plot_data -d data/sample/merged.csv -o data/sample/by_continent_by_requests.png -t continent_by_requests
scripts$> python3 -m plot_data -d data/sample/merged.csv -o data/sample/by_country_by_requests.png -t country_by_requests
```


