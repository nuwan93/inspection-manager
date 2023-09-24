import json
import requests
import logging
import traceback
import sys
import sqlite3
import yaml
import os
import time



base_url = 'https://app.prod.assetowl.com'

base_path = os.path.dirname(os.path.realpath(__file__))

config_file = os.path.join(base_path, "config.yml")

with open(config_file, "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
org_name = config["org_name"]
pirsee_un = config["pirsee_un"]
pirsee_pw = config["pirsee_pw"]
org_id = config["org_id"]

db_path = os.path.join(base_path, f'OUTPUT/{org_name}/db/{org_name}.db')
    
connection = sqlite3.connect(db_path)
c = connection.cursor()
    
session = requests.Session()
# session.verify = False  # when using local environment without valid SSL certificate


def login(username, password):
    login_json = {
        'username': username,
        'password': password,
        'deviceId': 'SCRIPT'
    }
    login_response = session.post(f'{base_url}/api/auth/login', data=json.dumps(login_json))
    login_response.raise_for_status()
    return login_response.json()

def get_Auth():
    access_token = login(pirsee_un, pirsee_pw)['accessToken']

    session.headers = {
        'Authorization': f'Bearer {access_token}'
    }


def get_inspections():
    try:
        c.execute('''
            SELECT property.address, property.subburb, inspection.inspection_id, inspection.type, property.pirsee_prop_id
            FROM property, inspection 
            WHERE inspection.status = 'Closed' AND 
                inspection.is_converted = 1 AND
                inspection.is_uploaded = 0 AND
                property.property_id = inspection.property_id
            GROUP BY inspection.inspection_id, inspection.type

        ''' )
        result = c.fetchall()

        connection.commit()

    except sqlite3.Error as er:
        logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
        logging.ERROR("Exception class is: ", er.__class__)
        logging.ERROR('SQLite traceback: ')
        exc_type, exc_value, exc_tb = sys.exc_info()
        logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

    
    return result

def mark_as_uploaded(inspection_id):
    try:
        c.execute(f'''
            UPDATE inspection
            SET is_uploaded = 1
            WHERE inspection_id = '{inspection_id}'
        ''' )

        connection.commit()

    except sqlite3.Error as er:
        logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
        logging.ERROR("Exception class is: ", er.__class__)
        logging.ERROR('SQLite traceback: ')
        exc_type, exc_value, exc_tb = sys.exc_info()
        logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

get_Auth()
for inspection in get_inspections():
    address = inspection[0] + ", " + inspection[1]
    address = address.replace("/","_")
    inspection_id = inspection[2]
    pirsee_property_id = inspection[4]
    type = inspection[3]

    if type == 'Ingoing':
        json_path = os.path.join(base_path, f'OUTPUT/{org_name}/properties/{address}/entry/{inspection_id}.json')
    else:
        json_path = os.path.join(base_path, f'OUTPUT/{org_name}/properties/{address}/routine/{inspection_id}.json')

    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            template_json = file.read()
    except Exception as e:
        print(e)
        continue              

    try:
        print(f"-------------------------- Strating to Import {type} for {address} -----------------------------")
        session.post(f'{base_url}/api/import/orgs/{org_id}/properties/{pirsee_property_id}/inspections', data=template_json.encode('utf-8'), headers={'Content-Type': 'application/json'}).raise_for_status()
        mark_as_uploaded(inspection_id)
        print(f"Imported {type} for {address} successfully")
        time.sleep(2)
    except requests.HTTPError as e:
        print(e)
        # get_Auth()
        continue

connection.close()