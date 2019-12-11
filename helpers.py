
import datetime
import sys
import logging
import json
import requests

# -- logging

FORMAT = '%(asctime)s %(levelname)s: %(module)s: %(funcName)s(): %(message)s'
logging.basicConfig(level=logging.DEBUG, format = FORMAT, filename = "smhi.log", filemode = "w")
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# -- functions

def write_json(json_obj, file_name = 'file.json'):
    """ write a json file to wd/file.json"""
    with open(file_name, 'w') as outfile:
        json.dump(json_obj, outfile)

def api_return_data(adr):
    """ initate API call and return the JSON data """
    # initiate the call
    req_obj = requests.get(adr)
    # try to get the json data (exceptions will be catched later)
    json_data = req_obj.json()
    return json_data
    