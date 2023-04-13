import os

from plotting import requests, providers, requests_x_providers
import db.io as db

db.open_db('localhost', 5433)
os.makedirs('data', exist_ok=True)

requests.plot_requests_over_time('hour', 'data/requests_by_hour.csv', 'data/requests_by_hour.csv', 'res/requests_by_hour.png')
requests.plot_requests_over_time_by_continent('hour', 'data/requests_by_hour_by_continent.csv', 'data/requests_by_hour_by_continent.csv' , 'res/requests_by_hour_by_continent.png')
requests.plot_requests_over_time_by_continent('hour', 'data/requests_by_hour_by_continent.csv', 'data/requests_by_hour_by_continent.csv' , 'res/requests_day_by_hour_by_continent.png', days=3, start='7')

requests.plot_cid_popularity('data/requests_cid_popularity.csv', 'data/requests_cid_popularity.csv', 'res/requests_cid_popularity.png')
requests.plot_cid_popularity_by_continent('data/requests_cid_popularity_by_continent.csv', 'data/requests_cid_popularity_by_continent.csv', 'res/requests_cid_popularity_by_continent.png',)
requests.plot_cid_popularity_ecdf('data/requests_cid_popularity_dist.csv', 'data/requests_cid_popularity_dist.csv', 'res/requests_cid_popularity_ecdf.png')
requests.plot_cid_popularity_by_continent_ecdf('data/requests_cid_popularity_by_continent_dist.csv', 'data/requests_cid_popularity_by_continent_dist.csv', 'res/requests_cid_popularity_by_continent_ecdf.png')

providers.plot_cid_replicas('data/cid_replicas.csv', 'data/cid_replicas.csv', 'res/cid_replicas.png')
providers.plot_cid_replicas_per_continent('data/cid_replicas_per_continent.csv', 'data/cid_replicas_per_continent.csv', 'res/cid_replicas_per_continent.png')

providers.plot_cids_per_provider('data/cid_per_provider.csv', 'data/cid_per_provider.csv', 'res/cid_per_provider.png')
providers.plot_cids_per_provider_per_continent('data/cid_per_provider_per_continent.csv', 'data/cid_per_provider_per_continent.csv', 'res/cid_per_provider_per_continent.png')

requests_x_providers.plot_locality_heatmap('data/heatmap.csv', 'data/heatmap.csv', 'res/heatmap.png')