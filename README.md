# Location of IPFS end-users and requested content

This project contains a measurement architecture to analyse the locality of interest of requested content in the IPFS network.
Here locality of interest means that content that is requested in a given region is also provided by some peer in that region.



You can find more about this project in this [Notion](https://www.notion.so/pl-strflt/Location-of-IPFS-end-users-and-requested-content-7668e98a725d4eea9f36fcafaabe7120) page.

The project contains two modes of operating:
- As a daemon that consumes the logs of an IPFS gateway and produces a measurement of the locality of interest of the requested content.
- As scripts that process the logs of an IPFS gateway and produce a measurement of the locality of interest of the requested content.

## How to run the daemon:
For the daemon version, we provide a docker-compose file that contains the following services:
- The controller service that is responsible for the orchestration of the other services.
- A parser service that parses the logs of the IPFS gateway into structured data.
- A find providers service that finds the providers of a given CID.
- A database service that stores the parsed data.
- A grafana dashboard service that visualizes the measurement data.
- A nginx service to serve as a reverse proxy for the grafana dashboard.
- A RabbitMQ service for publishing and consuming the IPFS gateway log.
- A helper service that can populate the database with find providers data, in case you don't want to run the find providers service as continuous monitoring due to network resource restrictions.

First build all the services through the following command:
``` 
        export MAXMIND_LICENSE_KEY=<your license key>
        docker-compose build        
```

Then you can run the services through the following command:
```
        docker swarm init
        docker stack deploy -c docker-compose.yaml ipfs-loc
```


## How to run the scripts:

We provide sample data in ``scripts/data/sample`` folder.
This data is a 80M cut from IPFS gateways from March 2022.
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


