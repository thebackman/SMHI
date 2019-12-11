
import api_endpoints
import helpers
import requests
import pandas as pd
import json
import logging

# -- functions

def list_params():
    """ Lists avaliable parameters """

    # -- API call

    data_json = helpers.api_return_data(api_endpoints.ADR_VERSION)

    # -- print collected data

    # subset and loop over all avaliable parameters
    resource = data_json["resource"]
    # loop over the json entries and print each parameter that is avaliable

    # store all param keys in a dict for later use (maybe)
    params = []
    for param in resource:
        print(param["title"] + " | " +  param["summary"] + " | " + param["key"] )
        params.append(param["key"])
    # return params


def list_stations(param):
    """ list stations that have a certain wheather parameter """

    # -- API call
    
    # create the API adress
    adr = api_endpoints.ADR_PARAMETER
    adr_full = adr.format(parameter = param)

    # send request and get data
    data1 = helpers.api_return_data(adr_full)
    print("Parameter choosen: " + data1["title"])

    # -- gather and wrangle the data about avaliable stations
    
    # take out an array with all the stations
    stations = data1["station"]
    
    # convert the JSON data to a pandas DF
    df_raw = pd.DataFrame(stations)
    
    # limit the data frame
    df_clean = df_raw[["name", "id", "height", "latitude", "longitude", "active", "from", "to", "updated", "title", "key"]]
    
    # rename columns to abide to python reserved keywords
    df_clean = df_clean.rename(columns={"from": "starting", "to": "ending"})
    
    # fix the date and time variables into something readable
    tmp1 = pd.to_datetime(df_clean["starting"], unit = "ms")
    tmp2 = pd.to_datetime(df_clean["ending"], unit = "ms")
    tmp3 = pd.to_datetime(df_clean["updated"], unit = "ms")
    df_clean = df_clean.assign(starting = tmp1)
    df_clean = df_clean.assign(ending = tmp2)
    df_clean = df_clean.assign(updated = tmp3)
    
    return(df_clean)


def get_latest_months(param, station):
    """
    get data for latest months via JSON download """

    # -- API call
    
    # create the API adress
    adr = api_endpoints.ADR_LATEST_MONTHS
    adr_full = adr.format(parameter = param, station = station)
    
    # send request and get data
    data1 = helpers.api_return_data(adr_full)
    
    # create a data frame from the JSON data
    df = pd.DataFrame(data1["value"])

    # fix the timestamps
    df = df.rename(columns={"from": "starting", "to": "ending"})
    tmp1 = pd.to_datetime(df["starting"], unit = "ms")
    tmp2 = pd.to_datetime(df["ending"], unit = "ms")
    df = df.assign(starting = tmp1)
    df = df.assign(ending = tmp2)

    # convert value to float64
    df["value"] = df.value.astype(float)

    # add the station id
    df["station_id"] = station
    
    return df


def get_corrected(param, station):
    """ get corrected archive via CSV download """
    
    # -- API call
    
    # create the API adress
    adr = api_endpoints.ADR_CORRECTED
    adr_full = adr.format(parameter = param, station = station)
    
    # download the csv data
    df = pd.read_csv(filepath_or_buffer= adr_full, skiprows= 9, delimiter=";")
       
    # remove columns not needed and reorder to match latest months data
    df_lim = df.iloc[:,[0, 4, 2, 1, 3]] 
        
    # rename the columns
    df_lim.columns = ["starting", "quality", "ref", "ending", "value"]
        
    # fix datetime columns
    tmp1 = pd.to_datetime(df_lim["starting"])
    tmp2 = pd.to_datetime(df_lim["ending"])
    df_lim = df_lim.assign(starting = tmp1)
    df_lim = df_lim.assign(ending = tmp2)

    
    # add the station id
    df_lim["station_id"] = station

    return df_lim


def get_stations(param, station_keys):
    """
    gets both latest months and corrected archive for
    a set of stations. Contains the try catch logic needed
    if any of the calls fail
    """

    # -- create the iterable

    if isinstance(station_keys, tuple):
        iterable = station_keys
    elif isinstance(station_keys, pd.DataFrame):
        iterable = station_keys["key"]
    
    # -- Construct some holder structures for data frames

    df_new = dict()
    df_old = dict()
    
    # -- loop through set of stations
    
    # start loop over each station id and collect the data if avaliable
    print(">>> Start downloading each station")
    for station_id in iterable:
        print(f">>> Downloading {station_id}")
        logging.info(f"# -- Downloading station {station_id}")
        # get the latest months
        logging.info(f"Downloading latest months for {station_id}")
        try:
            df_new[station_id] = get_latest_months(param = param, station = station_id )
            logging.debug(f"downloading latest months for {station_id} successful")
        except json.decoder.JSONDecodeError:
            logging.error(f"not possible to download latest months for {station_id}")
        # get the corrected archive
        logging.info(f"downloading corrected archive for {station_id}")
        try:
            df_old[station_id] = get_corrected(param = param, station = station_id)
            logging.debug(f"downloaded corrected archive for {station_id} successful")
        except Exception as error:
            logging.error(f"not possible to download corrected archive for {station_id}")
    
    # -- gather the data

    # get the number of data frames in each dict
    len_new = len(df_new)
    len_old = len(df_old)

    # Stack the latest months into one data frame for each station
    if len_new > 0:
        df_latest = pd.concat(df_new.values(), ignore_index=True)
    else:
        df_latest = None

    # Stack the corrected archive into one data frame for each station
    if len_old > 0:
        df_corrected = pd.concat(df_old.values(), ignore_index=True)
    else:
        df_corrected = None

    # return all data
    print("Check smhi.log for data download details!")
    if df_latest is not None and df_corrected is not None :
        logging.debug("both df_latest and df_corrected contains data")
        dictus = {"df_latest": df_latest, "df_corrected": df_corrected}
    elif df_latest is not None:
        logging.info("only df_latest contain data")
        dictus = {"df_latest": df_latest}
    elif df_corrected is not None:
        logging.info("only df_corrected contain data")
        dictus = {"df_corrected": df_corrected}
    else:
        logging.info("no data frame contains data")
        dictus = None
    
    # -- shutdown logging

    logging.shutdown()
    
    return(dictus)
    