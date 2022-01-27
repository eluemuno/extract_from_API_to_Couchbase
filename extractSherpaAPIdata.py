import datetime
import json
import logging
import os
import uuid

import requests
import yaml
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator

logging.basicConfig(level=logging.INFO, filename=os.path.dirname(os.path.abspath('logs.txt')) + '/logs.txt',
                    format='%(asctime)s :: %(levelname)s :: %(message)s')


# open up the config file for use
def fetch_params():
    configfile = os.path.dirname(os.path.abspath('parameters.yaml')) + '/parameters.yaml'
    with open(configfile) as file:
        params = yaml.load(file, Loader=yaml.FullLoader)
    return params


def get_sherpa_procedures():
    while True:
        # fetch user input for the country code to extract from the sherpa API
        country = str(
            input("Please enter the corresponding 2 or 3 letter ISO code for the country you wish to search: "))
        if len(country) not in [2, 3]:
            print("Please enter the corresponding 2 or 3 letter ISO code for the country you wish to search: ")
            logging.info('user entered wrong ISO code')
            continue
        else:
            break
    url = fetch_params()['sherpa']['baseUrl']
    key = fetch_params()['sherpa']['key']
    # build the full url
    full_url = url + "/v2/procedures?key=" + key + "&filter[country]=" + country

    response = requests.get(full_url)

    return response.json()


# extract the needed aspects of the api response for load into the couchbase db
def prep_data_for_upload():
    data_for_to_couchbase = []
    logging.info('Sherpa API data preparation started...')
    for items in get_sherpa_procedures()['data']:
        # data_prep = {'Category': items['attributes']['category']}
        data_prep = {'id': '1c3-' + str(uuid.uuid4()), 'Category': items['attributes']['category'],
                     'Date of Extraction': datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'),
                     'Country': items['attributes']['country'],
                     'Description': items['attributes']['description'],
                     'SubCategory': items['attributes']['subCategory'],
                     'Source': items['attributes']['source'],
                     'Title': items['attributes']['title']}
        if 'documentType' in items['attributes']:
            data_prep['DocumentType'] = items['attributes']['documentType']
        else:
            data_prep['DocumentType'] = 'No Data for DocType'
        data_for_to_couchbase.append(data_prep)
    logging.info('Sherpa API data preparation ended...')
    return json.dumps(data_for_to_couchbase, sort_keys=True, indent=4)


# open up connection to the couchbase and upload the data extracted from sherpa API
def upload_to_couchbase():
    user = fetch_params()['couchbase']['cb_user']
    password = fetch_params()['couchbase']['cb_pass']
    cbucket = fetch_params()['couchbase']['cb_bucket']
    cluster = Cluster('couchbase://127.0.0.1', ClusterOptions(PasswordAuthenticator(user, password)))

    bucket = cluster.bucket(cbucket)
    coll = bucket.default_collection()
    # attach a timestamp as the document id (this is a required param for couchbase uploads - it can be anything really)
    logging.info('loading data into couchbase started...')
    bucket.insert(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), json.loads(prep_data_for_upload()))
    logging.info('loading data into couchbase ended...')

logging.info(upload_to_couchbase())