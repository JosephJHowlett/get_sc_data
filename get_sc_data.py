# mostly copied from historian example by Joey

import urllib
import urllib.request
import json
import ssl
import getpass
import credentials
from sys import exit
from datetime import datetime
import pandas as pd
 
 
urlLogin = 'https://172.16.2.105:5544/Login'
urlData = 'https://172.16.2.105:5544/GetSCData?name=XE1T.CRY_PT103_PCHAMBER_AI.PI&QueryType=lab&StartDateUnix=1483056000&EndDateUnix=1483056900&Interval=1'

def string_to_lngs_epoch(string, datetime_format='%Y-%m-%d %H:%M:%S'):
    # Just putting things in LNGS time and making a unified date format
    # to make things simple.
    epoch = datetime(1970, 1, 1)
    return (datetime.strptime(string, datetime_format) - epoch).total_seconds()

def flatten(data):
    # I prefer two arrays to a list of ordered pairs
    times = [line['timestampseconds'] for line in data]
    values = [line['value'] for line in data]
    return times, values

def get_df(names, start_time, stop_time, time_interval=60, query_type='lab'):
    # Wraps the other functions in a stupid way, just making a query
    # for each variable and assuming the time arrays are the same
    # (I think this is safe but lazy). Sorry SC server...
    cols = {}
    for name in names:
        two_col_list = get_data(
            make_query(
                name,
                start_time,
                stop_time,
                time_interval,
                query_type
            )
        )
        times, data = flatten(two_col_list)
        cols['time'] = times
        cols[name] = data
    try:
        output = pd.DataFrame(cols)
    except ValueError as err:
        for key, val in cols.items():
            print(key, len(val))
        raise ValueError(err)
    return pd.DataFrame(cols)

def make_query(name, start_time, stop_time, time_interval=60, query_type='lab'):
    # Just formatting some args into a simple query to the server (one var only)
    query = 'https://172.16.2.105:5544/GetSCData?name={name}&QueryType={query_type}&StartDateUnix={start_epoch}&EndDateUnix={stop_epoch}&Interval={time_interval}'
    query_formatted = query.format(
        name=name,
        query_type=query_type,
        start_epoch=int(string_to_lngs_epoch(start_time)),
        stop_epoch=int(string_to_lngs_epoch(stop_time)),
        time_interval=time_interval,
    )
    return query_formatted
 
def get_data(urlData, prompt=False, verbose=False):
    # Just turned the code here:
    # https://xe1t-wiki.lngs.infn.it/doku.php?id=xenon:xenon1t:slowcontrol:webserviceNew
    # into a function and translated to python3
    if prompt:
        username = raw_input("Username: ")
        password = getpass.getpass()
    else:
        username = credentials.username
        password = credentials.password
     
    valuesLogin = {'username': username, 'password': password}
    context = ssl._create_unverified_context()
     
    try:
        data = urllib.parse.urlencode(valuesLogin)
        req = urllib.request.Request(urlLogin, data.encode('utf-8'))
        req.get_method = lambda: 'POST'
        response = urllib.request.urlopen(req, context=context)
        responseJSON = json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(e)
        print('\nError. Status code ' + str(e.code) + ".")
        exit()
     
    if response.code != 200:
        print(responseJSON['status'] + ": " + responseJSON['message'])
        exit()
     
    headers = {'Authorization': responseJSON['token']}
     
    if verbose:
        print('\nAuthentication OK')
        print('Token ' + responseJSON['token'])
     
    try:
        req = urllib.request.Request(urlData, None, headers)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req, context=context)
        responseJSON = json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print('\nError. Status code ' + str(e.code) + ".")
        exit()
     
     
    if verbose:
        print('\nSCData Results:')
        if response.code == 200:
            for item in responseJSON:
                print(str(item['timestampseconds']) + ' : ' + str(item['value']))
        else:
            print(responseJSON['status'] + ": " + responseJSON['message'])
    return responseJSON
