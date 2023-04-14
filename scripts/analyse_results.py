import os
import argparse

from scripts.plotting import requests, providers, requests_x_providers
import scripts.db.io as db

parser = argparse.ArgumentParser(description='Analyse results.')
parser.add_argument('--db-host', type=str, default='localhost', help='Database host')
parser.add_argument('--db-port', type=int, default=5433, help='Database port')
parser.add_argument('--data-dir', type=str, default='data', help='Data directory')
parser.add_argument('--res-dir', type=str, default='res', help='Results directory')
parser.add_argument('--time-unit', type=str, default='hour', help='Time unit')
args = parser.parse_args()


db.open_db(args.db_host, args.db_port)
os.makedirs(args.data_dir, exist_ok=True)
os.makedirs(args.res_dir, exist_ok=True)

requests.plot_requests_over_time(args.time_unit, f'data/requests_by_{args.time_unit}.csv', f'data/requests_by_{args.time_unit}.csv', f'res/requests_by_{args.time_unit}.png')
requests.plot_requests_over_time_by_continent(args.time_unit, f'data/requests_{args.time_unit}_by_continent.csv', f'data/requests_{args.time_unit}_by_continent.csv' , f'res/requests_{args.time_unit}_by_continent.png')
if args.time_unit == 'hour':
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