import zlib
import json
import requests
from base64 import b64encode, b64decode
import os
import argparse
import redis

parser = argparse.ArgumentParser(description="parser for args")

parser.add_argument("--site", required=True, help="The site key")

parser.add_argument("--region", required=True, help="The region")

parser.add_argument("--redis_host", required=True, help="Redis Host")

parser.add_argument("--redis_db", required=True, help="Redis DB number")

args = parser.parse_args()

site_key = args.site
region = args.region
redis_host = args.redis_host
redis_db = args.redis_db

redis_client = redis.StrictRedis(host=redis_host, port=6379, db=redis_db)

def _decompress_encoded_text(enc_text):
    return zlib.decompress(b64decode(enc_text))

def _compress_n_encode_text(text):
    return b64encode(zlib.compress(text.encode('utf-8'))).decode('ascii')

def update_mimir_config(url, data):
    headers = { "Content-Type": "application/json"}
    response = requests.put(url, json=data, headers=headers)
    print("Updating mimir config response code",response.status_code)

def get_cache_key(site_key):
    return "facet_affinity."+site_key+".default"

def add_to_cache(json_data):
    key = get_cache_key(site_key)
    for item in json_data.get("data"):
        for filter_field, filter_values in item.items():
            compressed_value = _compress_n_encode_text(json.dumps(filter_values))
            redis_client.hset(key, filter_field, compressed_value)
    print("Added to cache")


json_data = {
  "data": [
    {
      "mfName_ntk_cs_uFilter": [
        {
          "value": "Jump",
          "score": 10
        },
        {
          "value": "DKNY",
          "score": 8
        }
      ]
    },
    {
      "v_ads_f10514_uFilter": [
        {
          "value": "Midi",
          "score": 2
        }
      ]
    },
    {
      "dil_shipinternational": [
        {
          "value": "Y",
          "score": 4
        }
      ]
    }
  ],
  "msTaken": 0
}

add_to_cache(json_data)

url = "http://configstore.prod.{}.infra/sites/{}/config/facet.personalization.fields?service=mimir&handler=default".format(region, site_key)
print(url)

personalization_fields={
    "value":[]
}
for item in json_data.get("data"):
    for filter_field, filter_values in item.items():
        temp_field = {
            "fieldName": filter_field,
            "disableGlobalAffinity": False
        }
        personalization_fields.get("value").append(temp_field)

print(personalization_fields)
update_mimir_config(url,personalization_fields)
