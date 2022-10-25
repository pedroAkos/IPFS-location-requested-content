import geoip2.database
import geoip2.errors
from geoip import geolite2
import pandas as pd


def lookup_ip(ip):
    match = geolite2.lookup(ip)
    #print(ip, match)

    continent, country, regions = pd.NA, pd.NA, pd.NA
    location = None
    asn, aso = pd.NA, pd.NA

    if match is not None:
        continent = match.continent  # return the continent
        country = match.country  # returns the country
        regions = match.subdivisions  # this will return a list of the regions
        location = match.location #a tuple of (lat, long)

    if location is None:
        location = ('', '')

    return continent, country, regions, str(location[0]), str(location[1]), asn, aso




def lookup_geoip2(ip):
    continent, country, region = None, None, None
    latitude, longitude = '', ''
    asn, aso = None, None
    with geoip2.database.Reader('maxmind/GeoLite2-City.mmdb') as reader:
        try:
            response = reader.city(ip)
            latitude = response.location.latitude
            longitude = response.location.longitude

            continent = response.continent.code
            country = response.country.iso_code
            region = response.subdivisions.most_specific.iso_code

        except geoip2.errors.AddressNotFoundError as e:
            pass

    with geoip2.database.Reader('maxmind/GeoLite2-ASN.mmdb') as reader:
        try:
            response = reader.asn(ip)
            asn = response.autonomous_system_number
            aso = response.autonomous_system_organization
        except geoip2.errors.AddressNotFoundError as e:
            pass

    #print(ip, continent, country, region, latitude, longitude, asn, aso)
    return continent, country, region, latitude, longitude, asn, aso
