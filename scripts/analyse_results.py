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

requests.plot_requests_over_time(args.time_unit, f'{args.data_dir}/requests_by_{args.time_unit}.csv', f'{args.data_dir}/requests_by_{args.time_unit}.csv', f'{args.res_dir}/requests_by_{args.time_unit}.png')
requests.plot_requests_over_time_by_continent(args.time_unit, f'{args.data_dir}/requests_{args.time_unit}_by_continent.csv', f'{args.data_dir}/requests_{args.time_unit}_by_continent.csv' , f'{args.res_dir}/requests_{args.time_unit}_by_continent.png')
if args.time_unit == 'hour':
    requests.plot_requests_over_time_by_continent('hour', f'{args.data_dir}/requests_by_hour_by_continent.csv', f'{args.data_dir}/requests_by_hour_by_continent.csv' , f'{args.res_dir}/requests_day_by_hour_by_continent.png', days=3, start='7')


requests.plot_cid_popularity(f'{args.data_dir}/requests_cid_popularity.csv', f'{args.data_dir}/requests_cid_popularity.csv', f'{args.res_dir}/requests_cid_popularity.png')
requests.plot_cid_popularity_by_continent(f'{args.data_dir}/requests_cid_popularity_by_continent.csv', f'{args.data_dir}/requests_cid_popularity_by_continent.csv', f'{args.res_dir}/requests_cid_popularity_by_continent.png',)
requests.plot_cid_popularity_ecdf(f'{args.data_dir}/requests_cid_popularity_dist.csv', f'{args.data_dir}/requests_cid_popularity_dist.csv', f'{args.res_dir}/requests_cid_popularity_ecdf.png')
requests.plot_cid_popularity_by_continent_ecdf(f'{args.data_dir}/requests_cid_popularity_by_continent_dist.csv', f'{args.data_dir}/requests_cid_popularity_by_continent_dist.csv', f'{args.res_dir}/requests_cid_popularity_by_continent_ecdf.png')

providers.plot_cid_replicas(f'{args.data_dir}/cid_replicas.csv', f'{args.data_dir}/cid_replicas.csv', f'{args.res_dir}/cid_replicas.png')
providers.plot_cid_replicas_per_continent(f'{args.data_dir}/cid_replicas_per_continent.csv', f'{args.data_dir}/cid_replicas_per_continent.csv', f'{args.res_dir}/cid_replicas_per_continent.png')

providers.plot_cids_per_provider(f'{args.data_dir}/cid_per_provider.csv', f'{args.data_dir}/cid_per_provider.csv', f'{args.res_dir}/cid_per_provider.png')
providers.plot_cids_per_provider_per_continent(f'{args.data_dir}/cid_per_provider_per_continent.csv', f'{args.data_dir}/cid_per_provider_per_continent.csv', f'{args.res_dir}/cid_per_provider_per_continent.png')

requests_x_providers.plot_locality_heatmap(f'{args.data_dir}/heatmap.csv', f'{args.data_dir}/heatmap.csv', f'{args.res_dir}/heatmap.png')