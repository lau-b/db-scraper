import requests
import json
import pandas as pd
# import db_utils
import sqlalchemy
import pytz
from datetime import datetime
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(f'{Path(__file__).parent}/.env')


def create_db_engine():
    host = os.getenv('host')
    user = os.getenv('user')
    port = os.getenv('port')
    db = os.getenv('db')
    password = os.getenv('password')

    connection_string = f'postgres://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(connection_string)
    return engine

def call_api(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:  # TODO: learn about exceptions
        print('Whoops')
        return None


def create_dataframe_from_db_response(reponse, tz):
    dbikes_list = []
    now = datetime.now(tz)
    for item in reponse['items']:
        coords_geoj = item['position']
        lng = item['position']['coordinates'][0]
        lat = item['position']['coordinates'][1]
        object_id = item['rentalObject']['href']
        dbikes_list.append([now, object_id, lng, lat, coords_geoj])
    df = pd.DataFrame(
        data=dbikes_list,
        columns=['point_in_time','bike_name', 'longitude', 'latitude', 'geojson_point']
    )
    return df

if __name__ == '__main__':
    tz = pytz.timezone('Europe/Berlin')
    key = os.getenv('db_access_token')
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {key}'
    }
    url = 'https://api.deutschebahn.com/flinkster-api-ng/v1/bookingproposals?lat=50.9382412&lon=6.9481769&radius=2000&limit=100&&providernetwork=2'
    print(datetime.now(tz), ': start scraping db api')
    body = call_api(url, headers)
    frame = create_dataframe_from_db_response(body, tz)

    localhost_engine = create_db_engine()
    frame.to_sql(
        name='db_import',
        con=localhost_engine,
        if_exists='append',
        index=False,
        dtype={
            'geojson_point': sqlalchemy.types.JSON
        }
    )
    print(datetime.now(tz), ': data successfully written to database')
    print('------------------------------------------------------------------------')
