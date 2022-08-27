import os

import pandas as pd
from flask import Flask, request, jsonify

from parsing import gateway, providers, location

app = Flask(__name__)


def convert_location(continent, country, regions, lat, long, asn, aso):
    if pd.isna(continent):
        continent = ""
    if pd.isna(country):
        country = ""
    if pd.isna(regions) or len(regions) == 0:
        regions = ""
    elif type(regions) is frozenset:
        regions = regions[0]
    if pd.isna(asn):
        asn = ""
    if pd.isna(aso):
        aso = ""
    return continent, country, regions, str(lat), str(long), str(asn), aso

@app.post("/parse")
def parse():
    logEntry = request.data.decode('UTF-8')
    print("Parsing", logEntry)
    try:
        entry = gateway.parse_log_entry(logEntry)
        if entry['op'] == 'GET' and \
                200 <= int(entry['status']) < 400 and \
                entry['ip'] != '127.0.0.1' and \
                entry['ip'] != '::1':
            cid = gateway.extract_cid(entry['http_host'], entry['target'])
            if cid is not pd.NA:
                continent, country, regions, lat, long, asn, aso = location.lookup_geoip2(entry['ip'])
                entry['time'] = entry['time'].strip('][')
                entry['cid'] = cid
                continent, country, regions, lat, long, asn, aso = convert_location(continent, country, regions, lat, long, asn, aso)
                entry['continent'] = continent
                entry['country'] = country
                entry['region'] = regions
                entry['lat'] = lat
                entry['long'] = long
                entry['asn'] = asn
                entry['aso'] = aso

                return jsonify(entry), 200
            return {"error": "Log entry has no cid"}, 400
        return {"error": "Log entry is not a valid GET"}, 400
    except Exception as e:
        raise e
        return {"error": f"Cannot parse log entry, error: {e}"}, 400


@app.post("/locate_providers")
def locate_providers():
    if request.is_json:
        provs = request.get_json()
        for i in range(0, len(provs)):
            provs[i]['locations'] = []
            for maddr in provs[i]['maddrs']:
                ip = providers.extract_ips_from_maddr((provs[i]['peerId'], maddr))
                if ip != '127.0.0.1' and ip != '::1':
                    continent, country, regions, lat, long, asn, aso = location.lookup_geoip2(ip)
                    continent, country, regions, lat, long, asn, aso = convert_location(continent, country, regions,
                                                                                        lat, long, asn, aso)
                    provs[i]['locations'].append({'continent': continent, 'country': country, 'region': regions, 'lat': lat, 'long': long, "asn": asn, "aso": aso})
        print(provs)
        return provs, 200
    return {"error": "Request must be a JSON"}, 400


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 9000))
    app.run(host='0.0.0.0', port=port)
