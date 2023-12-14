YOUR_LICENSE_KEY=$1

if [ -z $YOUR_LICENSE_KEY ]; then
    echo "LICENSE_KEY is required."
    exit 1
fi

curl -o geolite2-asn.tar.gz https://download.maxmind.com/app/geoip_download\?edition_id=GeoLite2-ASN\&license_key=${YOUR_LICENSE_KEY}\&suffix=tar.gz
curl -o geolite2-city.tar.gz https://download.maxmind.com/app/geoip_download\?edition_id=GeoLite2-City\&license_key=${YOUR_LICENSE_KEY}\&suffix=tar.gz
#curl -o geolite2-country.tar.gz https://download.maxmind.com/app/geoip_download\?edition_id=GeoLite2-Country\&license_key=${YOUR_LICENSE_KEY}\&suffix=tar.gz

tar -xf geolite2-asn.tar.gz
tar -xf geolite2-city.tar.gz
#tar -xf geolite2-country.tar.gz

mv GeoLite2-ASN_*/GeoLite2-ASN.mmdb .
mv GeoLite2-City_*/GeoLite2-City.mmdb .
#mv GeoLite2-Country_*/GeoLite2-Country.mmdb .

rm -rf GeoLite2-ASN_*
rm -rf GeoLite2-City_*
#rm -rf GeoLite2-Country_*

rm -rf geolite2-asn.tar.gz
rm -rf geolite2-city.tar.gz
#rm -rf geolite2-country.tar.gz
