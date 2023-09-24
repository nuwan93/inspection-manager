from PIL import Image
import io
import requests
import logging
import traceback
import sys
import sqlite3
import yaml
from pathlib import Path
import os

base_path = os.path.dirname(os.path.realpath(__file__))

config_file = os.path.join(base_path, "config.yml")

with open(config_file, "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
org_name = config["org_name"]

db_path = os.path.join(base_path, f'OUTPUT/{org_name}/db/{org_name}.db')
    
connection = sqlite3.connect(db_path)
c = connection.cursor()

def fetch_all_images():  
    try:
        c.execute('''
            SELECT image.image_id, image.URL, image.path, property.address, property.subburb
            FROM  image, inspection, property
            WHERE image.is_downloaded = 0 AND
            image.inspection_id = inspection.inspection_id AND
            inspection.property_id = property.property_id
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


    
images = fetch_all_images()
for image in images:
    address = image[3] + ", " + image[4]
    address = address.replace("/","_")
    image_id = image[0]
    link = image[1]
    image_main_dir = os.path.join(base_path, f'OUTPUT/{org_name}/properties/{address}/entry/images')
    image_sub_dir = os.path.join(image_main_dir, image[2])
    image_path = os.path.join(image_sub_dir, f'{str(image[0])}.jpeg')

    response  = requests.get(link).content 
    image_file = io.BytesIO(response)
    image  = Image.open(image_file)
    with open(image_path , "wb") as f:
        image.save(f , "JPEG")
        print(f'{image_path} downloaded!!')

        try:
            c.execute(f'''
                UPDATE image
                SET is_downloaded = 1
                WHERE image_id = '{image_id}'
            ''', )
            connection.commit()
        except sqlite3.Error as er:
            logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
            logging.ERROR("Exception class is: ", er.__class__)
            logging.ERROR('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

connection.close()