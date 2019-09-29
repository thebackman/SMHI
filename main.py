
# -- import the libs and some reload 

import pandas as pd
import smhi as smhi
import helpers as helpers  # remove later on
import importlib
importlib.reload(smhi)
importlib.reload(helpers)

# -- listing the parameters that are avaliable

smhi.list_params()

# -- for one parameter, see what stations have it and in what timeframe, lon lat area etc.

# all stations 
df_stations = smhi.list_stations(param = 5)

# one station
df_stations.loc[df_stations["key"] == "159880"]

# limit the stations further to those that have been online in recent years
df_stations = df_stations.loc[(df_stations["starting"] >= '2000-01-01')]


# FIXME: 
# limit the stations to within a geographical area
# limit to a geographical area

# -- get the actual data

# use get_stations functions to get all data from one or a set of stations
# accepts a tuple of stations IDs or a dataframe with columns named key from
# smhi.list_stations() above

smhi.get_stations(param = 5, station_keys= df_stations)