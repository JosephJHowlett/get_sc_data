# mostly copied from historian example by Joey

import urllib
import json
import ssl
import getpass
import credentials
from sys import exit
from datetime import datetime
import pandas as pd
 
 
urlLogin = 'https://172.16.2.105:5544/Login'
urlData = 'https://172.16.2.105:5544/GetSCData?name=XE1T.CRY_PT103_PCHAMBER_AI.PI&QueryType=lab&StartDateUnix=1483056000&EndDateUnix=1483056900&Interval=1'

def string_to_lngs_epoch(string):
    datetime_format = '%Y-%m-%d %H:%M:%S'
    epoch = datetime(1970, 1, 1)
    return (datetime.strptime(string, datetime_format) - epoch).total_seconds() - (2*3600)

def flatten(data):
    times = [line['timestampseconds'] for line in data]
    values = [line['value'] for line in data]
    return times, values

def get_df(names, start_time, stop_time, time_interval=60, query_type='lab'):
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
    return pd.DataFrame(cols)

def make_query(name, start_time, stop_time, time_interval=60, query_type='lab'):
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
