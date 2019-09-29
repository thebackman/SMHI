
import api_endpoints
import helpers
import requests
import pandas as pd

# FIXME:

# remove unneccesary comments
# change variable names within functions
# write docstrings
# check so that functions work with all parameters depending on data structure


# -- functions

def list_params():
    """ Lists avaliable parameters """

    # -- API call

    data_json = helpers.api_return_data(api_endpoints.ADR_VERSION)

    # -- print collected data

    # subset and loop over all avaliable parameters
    resource = data_json["resource"]
    # loop over the json entries and print each parameter that is avaliable

    # stora all param keys in a dict for later use
    params = []
    for param in resource:
        print(param["title"] + " | " +  param["summary"] + " | " + param["key"] )
        params.append(param["key"])
    return params




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
    """ get data for latest months """

    # -- API call
    
    # create the API adress
    adr = api_endpoints.ADR_LATEST_MONTHS
    adr_full = adr.format(parameter = param, station = station)
    
    # send request and get data
    data1 = helpers.api_return_data(adr_full)
    
    # create a data frame from the JSON data
    df = pd.DataFrame(data1["value"])

    # fix the time stamps
    df = df.rename(columns={"from": "starting", "to": "ending"})
    tmp1 = pd.to_datetime(df["starting"], unit = "ms")
    tmp2 = pd.to_datetime(df["ending"], unit = "ms")
    df = df.assign(starting = tmp1)
    df = df.assign(ending = tmp2)

    # convert value to float64
    df["value"] = df.value.astype(float)
    
    return df

def get_corrected(param, station):
    """ get corrected archive """
    
    # -- API call
    
    # create the API adress
    adr = api_endpoints.ADR_CORRECTED
    adr_full = adr.format(parameter = param, station = station)
    
    # download the csv data
    try:
        df = pd.read_csv(filepath_or_buffer= adr_full, skiprows= 9, delimiter=";")
        
        # remove columns not needed and reorder to match latest
        df_lim = df.iloc[:,[0, 4, 2, 1, 3]] 
        
        # rename the columns
        df_lim.columns = ["starting", "quality", "ref", "ending", "value"]
        
        # fix datetime columns
        tmp1 = pd.to_datetime(df_lim["starting"])
        tmp2 = pd.to_datetime(df_lim["ending"])
        df_lim = df_lim.assign(starting = tmp1)
        df_lim = df_lim.assign(ending = tmp2)

    except:
        print(f"The csv data for param {param} and station {station} could not be downloaded")

    return df_lim


def get_stations(param, station_keys):
    """ gets both latest months and corrected archive """

    # -- create the iterable

    if isinstance(station_keys, tuple):
        iterable = station_keys
    elif isinstance(station_keys, pd.DataFrame):
        iterable = station_keys["key"]
    else:
        print(f"No method avaliable for {type(station_keys)}")
        return
    
    # -- loop through set of stations

    for station_id in iterable:
        try:
            df_months = get_latest_months(param = param, station= station_id)
            df_corrected = get_corrected(param = param, station= station_id)
            print(f"# -- processing station {station_id}")
            print(df_months.head())
            print(df_corrected.head())
            # TODO: stack the data 
        except:
            print(f"Failed to download the data from station {station_id}")
            continue
            