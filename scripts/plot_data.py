import sys

from plotting import plot_heatmap

import pandas as pd
import argparse

parser = argparse.ArgumentParser()

#-d data -o output -t type
parser.add_argument('-d', '--dataset', help="Dataset file")
parser.add_argument('-o', '--output', help="Output file to plot")
parser.add_argument('-t', '--type', help="Heatmap type", choices=['country', 'continent', 'country_by_requests', 'continent_by_requests'])

args = parser.parse_args()

df = pd.read_csv(args.dataset, keep_default_na=False)

if args.type == 'country':
    plot_heatmap.plot_by_country(df, args.output)

if args.type == 'continent':
    plot_heatmap.plot_by_continent(df, args.output)

if args.type == 'country_by_requests':
    plot_heatmap.plot_by_country_by_requests(df, args.output)

if args.type == 'continent_by_requests':
    plot_heatmap.plot_by_continent_by_requests(df, args.output)
