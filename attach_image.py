import json
import logging
import traceback
import sys
import sqlite3
import yaml
import os

base_path = os.path.dirname(os.path.realpath(__file__))

config_file = os.path.join(base_path, "config.yml")

with open(config_file, "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
org_name = config["org_name"]

db_path = os.path.join(base_path, f'OUTPUT/{org_name}/db/{org_name}.db')
    
connection = sqlite3.connect(db_path)
c = connection.cursor()

def get_inspections():
    try:
        c.execute('''
           SELECT property.address, property.subburb, inspection.inspection_id
            FROM property, inspection, image 
            WHERE inspection.status = 'Closed' AND 
                inspection.type = 'Ingoing' AND
                inspection.is_converted = 1 AND
                image.is_photo_attached = 0 AND
                property.property_id = inspection.property_id AND
                inspection.inspection_id = image.inspection_id
            GROUP BY inspection.inspection_id
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

def get_images(inspection_id, path):
    query = '''
            SELECT image.pirsee_img_id, image.image_id
            FROM inspection, image 
            WHERE image.inspection_id = ? AND
                image.path = ? AND
                image.is_uploaded = 1 AND
                image.is_photo_attached = 0 AND
                inspection.inspection_id = image.inspection_id
        '''
    try:
        c.execute(query, (inspection_id, path) )
        result = c.fetchall()
        connection.commit()

    except sqlite3.Error as er:
        logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
        logging.ERROR("Exception class is: ", er.__class__)
        logging.ERROR('SQLite traceback: ')
        exc_type, exc_value, exc_tb = sys.exc_info()
        logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))


    return result

def create_attachement(image_list):
    attachemnet = []

    for image in image_list:

        files_list = []
        files_list.append({
            'type': 'ORIGINAL',
            'fileId': image[0]
        })

        attachemnet.append({
            'type': 'PHOTO', 
            'filename': str(image[1]),
            'files' : files_list
        })
        
    return attachemnet

for inspection in get_inspections():
    address = inspection[0] + ", " + inspection[1]
    address = address.replace("/","_")
    inspection_id = inspection[2]
    json_path = os.path.join(base_path, f'OUTPUT/{org_name}/properties/{address}/entry/{inspection_id}.json')
    print(f'attaching images for {address}' )

    with open(json_path, 'r+', encoding='utf-8') as f:
        data = json.load(f)

        room_number = 0
        for room in data["rooms"]:
            room_number += 1
            room_title = room["title"].replace("/","_")

            item_number = 0
            for item in room["items"]:
                item_number += 1
                item_title = item["title"].replace("/","_")
                path_to_pic = f'{room_number}-{room_title}/{item_number}-{item_title}'

                images = get_images(inspection_id, path_to_pic)

                for attachemnet in create_attachement(images):
                    try:
                        item['attachments'].append(attachemnet)
                    except Exception as e:
                        item['attachments'] = []
                        item['attachments'].append(attachemnet)
                    
                    try:
                        c.execute(f'''
                            UPDATE image
                            SET is_photo_attached = 1
                            WHERE image_id = '{attachemnet.get('filename')}'
                        ''', )
                        connection.commit()
                    except sqlite3.Error as er:
                        logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
                        logging.ERROR("Exception class is: ", er.__class__)
                        logging.ERROR('SQLite traceback: ')
                        exc_type, exc_value, exc_tb = sys.exc_info()
                        logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(data, f, indent=4)                
        f.truncate()

connection.close()