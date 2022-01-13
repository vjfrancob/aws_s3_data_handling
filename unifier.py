
'''
Global Libraries
'''

import pandas as pd
import glob
import json

'''
Local libraries import
'''

import s3_access as aws
import secrets

'''
Downloading data from s3
'''

#aws.download_s3_folder(secrets.aws_bucket, secrets.file_path,'./files')


'''
Funtion for reading and organizing json files
'''

def json_read (path):
    dfs = []
    for file in path:
        with open(file) as f:
            #json_load = pd.read_json (f, typ ='series')
            json_data = pd.json_normalize(json.loads(f.read()))
        dfs.append(json_data)
    
    return pd.concat(dfs,sort = False)

'''
Function calling
'''
path = glob.iglob('./files/FarmWeather*/*.json')
we_df = json_read(path)
del(path)
we_df.shape

path = glob.iglob('./files/FarmGateway*/*.json')
s1_df = json_read(path)
del(path)
s1_df.shape

'''
Dealing with Timestamp
'''
#* Weather Station

we_df['dateTime'] = pd.to_datetime(we_df['dateTime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/Bogota').dt.tz_localize(None)
we_df['date'] = pd.to_datetime(we_df['dateTime']).dt.date
we_df['time'] = pd.to_datetime(we_df['dateTime']).dt.time
we_df['year'] = pd.to_datetime(we_df['dateTime']).dt.year
we_df['month'] = pd.to_datetime(we_df['dateTime']).dt.month
we_df['month_name'] = pd.to_datetime(we_df['dateTime']).dt.month_name()
we_df['day'] = pd.to_datetime(we_df['dateTime']).dt.day
we_df['hour'] = pd.to_datetime(we_df['dateTime']).dt.hour

#* Minew Local Sensors

s1_df['timestamp'] = pd.to_datetime(
    s1_df['timestamp']).dt.floor('S').dt.tz_convert('America/Bogota').dt.tz_localize(None)
s1_df['date'] = pd.to_datetime(s1_df['timestamp']).dt.date
s1_df['time'] = pd.to_datetime(s1_df['timestamp']).dt.time
s1_df['year'] = pd.to_datetime(s1_df['timestamp']).dt.year
s1_df['month'] = pd.to_datetime(s1_df['timestamp']).dt.month
s1_df['month_name'] = pd.to_datetime(s1_df['timestamp']).dt.month_name()
s1_df['day'] = pd.to_datetime(s1_df['timestamp']).dt.day
s1_df['hour'] = pd.to_datetime(s1_df['timestamp']).dt.hour

'''
Sensor Location Organization
'''

#* Weather Station

we_df['farm_name'] = we_df['sensor_id'].map({'FarmWeatherGateway0009':'Buenavista',
                                             'FarmWeatherGateway0010':'Lirio',
                                             'FarmWeatherGateway0011':'Luxemburgo'})

#* Minew Local Sensors

def sensor_location(mac):
    
    if (mac == 'AC233FA9CE6A'):
        return 'Luxemburgo,Almacenamiento'
    elif (mac == 'AC233FA9CE68'):
        return 'Luxemburgo,Secado'
    elif (mac == 'AC233FA9CE69'):
        return 'Luxemburgo,Beneficio'    
    elif (mac == 'AC233FA9F064'):
        return 'Luxemburgo,Fermentacion'   
        
    elif (mac == 'AC233FA9CE62'):
        return 'Buenavista,Almacenamiento'
    elif (mac == 'AC233FA9CE65'):
        return 'Buenavista,Secado'
    elif (mac == 'AC233FA9CE63'):
        return 'Buenavista,Beneficio'    
    elif (mac == 'AC233FA9F065'):
        return 'Buenavista,Fermentacion'  
    
    elif (mac == 'AC233FA9CE64'):
        return 'Lirio,Almacenamiento'
    elif (mac == 'AC233FA9CF0D'):
        return 'Lirio,Secado'
    elif (mac == 'AC233FA9CE61'):
        return 'Lirio,Beneficio'    
    elif (mac == 'AC233FA9F096'):
        return 'Lirio,Fermentacion'  
    
    elif mac == ('AC233FC0AB6C'):
        return 'Luxemburgo,Gateway'
    elif mac == ('AC233FC0AB71'): 
        return 'Lirio,Gateway'
    elif mac == ('AC233FC0AB70'): 
        return 'Buenavista,Gateway'
    
s1_df = s1_df[s1_df['type']!='Gateway']

s1_df[['farm_name','sensor_location']] = s1_df['mac'].apply(lambda x: sensor_location(x)).str.split(',',expand=True)

'''
Minew Local Sensors Summarizing
'''

s1_grouped = s1_df.groupby(['farm_name','sensor_location','date','hour'], as_index=False)[['temperature','humidity']].mean()

s1_grouped.to_csv('./minew_sensors.csv',index=False)
we_df.to_csv('./we.csv',index=False)

#print(s1_grouped.head(5))