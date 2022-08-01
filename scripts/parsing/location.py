from geoip import geolite2
import pandas as pd


def lookup_ip(ip):
    match = geolite2.lookup(ip)

    continent, country, regions = pd.NA, pd.NA, pd.NA

    if match is not None:
        continent = match.continent  # return the continent
        country = match.country  # returns the country
        regions = match.subdivisions  # this will return a list of the regions

    return continent, country, regions
