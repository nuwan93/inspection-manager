import json
import requests
import logging
import traceback
import sys
import sqlite3
import yaml
import threading
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

db_path = os.path.join(base_path, f'OUTPUT/{org_name}/db/{org_name}.db')
    


session = requests.Session()
# session.verify = False  # when using local environment without valid SSL certificate


maxthreads = 3
sema = threading.Semaphore(value=maxthreads)
threads = list()

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


def upload_file(path, content_type):
    with open(path, 'rb') as file:
        response = session.post(f'{base_url}/fs/files/uploads', data=file, headers={'Content-Type': content_type})
        response.raise_for_status()
        return response.json()
        


def fetch_all_images():
    connection = sqlite3.connect(db_path)
    c = connection.cursor()  
    try:
        c.execute('''
            SELECT image.image_id, image.URL, image.path, property.address, property.subburb
            FROM  image, inspection, property
            WHERE image.is_downloaded = 1 AND
            image.is_uploaded = 0 AND
            image.inspection_id = inspection.inspection_id AND
            inspection.property_id = property.property_id
        ''' )
        result = c.fetchall()

        connection.commit()
        connection.close()
    except sqlite3.Error as er:
        logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
        logging.ERROR("Exception class is: ", er.__class__)
        logging.ERROR('SQLite traceback: ')
        exc_type, exc_value, exc_tb = sys.exc_info()
        logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

    
    return result

def img_upload(*image):
    sema.acquire()
    connection = sqlite3.connect(db_path)
    c = connection.cursor()
    address = image[3] + ", " + image[4]
    address = address.replace("/","_")
    image_id = image[0]
    link = image[1]
    image_main_dir = os.path.join(base_path, f'OUTPUT/{org_name}/properties/{address}/entry/images')
    image_sub_dir = os.path.join(image_main_dir, image[2])
    image_path = os.path.join(image_sub_dir, f'{str(image[0])}.jpeg')

    try:
        response = upload_file(image_path, 'image/jpeg')
        pirsee_img_id = response.get('fileId')
        query = '''
                UPDATE image
                SET is_uploaded = 1,
                    pirsee_img_id = ?
                WHERE image_id = ?
            '''
        c.execute(query, (pirsee_img_id, image_id) )
        connection.commit()
        print(f'{image_path} uploaded!!')
        connection.close()
        sema.release()
    except Exception as e:
        print(e)
        get_Auth()
        sema.release()
        


    
images = fetch_all_images()
get_Auth()
for image in images:
    if threading.active_count() > 100:
        time.sleep(5)
    
    thread = threading.Thread(target=img_upload,args=(image))
    threads.append(thread)
    thread.start()
       



    

