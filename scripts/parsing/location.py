from geoip import geolite2
import pandas as pd


def lookup_ip(ip):
    match = geolite2.lookup(ip)
    print(ip, match)

    continent, country, regions = pd.NA, pd.NA, pd.NA
    location = None

    if match is not None:
        continent = match.continent  # return the continent
        country = match.country  # returns the country
        regions = match.subdivisions  # this will return a list of the regions
        location = match.location #a tuple of (lat, long)

    if location is None:
        location = ('', '')

    return continent, country, regions, str(location[0]), str(location[1])
