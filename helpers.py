
import datetime
import sys
import logging
import json
import requests

# -- logging

FORMAT = '%(asctime)s %(levelname)s: %(module)s: %(funcName)s(): %(message)s'
logging.basicConfig(level=logging.DEBUG,format = FORMAT,filename = "smhi.log", filemode = "w")

# -- functions

def test_api_connection(req_obj):
    """ tests the API connection, exits if not 200 """
    if req_obj.status_code != 200:
        logging.error("Server error! Exiting with status code" + str(req_obj.status_code))
        sys.exit()

def parse_date(datestring):
    """ parses a date string of type 2000-01-01 to a python date """
    # create integers 
    year = int(datestring[0:4])
    month = int(datestring[5:7])
    day = int(datestring[8:10])
    # construct datetime object and makes it into a only date format
    date = datetime.datetime(year, month, day).date()
    return (date)

def write_json(json_obj, file_name = 'file.json'):
    """ write a json file to wd/file.json"""
    with open(file_name, 'w') as outfile:
        json.dump(json_obj, outfile)

def api_return_data(adr):
    """ initate API call and return the JSON data """
    # initiate the call
    ret_obj = requests.get(adr)
    logging.debug(f"Making API call to {adr}")
    # test the connection
    test_api_connection(ret_obj)
    # get the json data
    json_data = ret_obj.json()
    return json_data
    