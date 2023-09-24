# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from datetime import datetime
import json
import math
import time
import os
import sqlite3
import yaml
from pathlib import Path
import traceback
import sys
import logging



class EntryPipeline:
    def open_spider(self, spider):
        with open("config.yml", "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)

        self.org_name = config["org_name"]
        
        self.base_path = Path(__file__).parents[2]
        self.db_path = os.path.join(self.base_path, f'OUTPUT/{self.org_name}/db/{self.org_name}.db')
        
        self.connection = sqlite3.connect(self.db_path)
        self.c = self.connection.cursor()
    
    def create_folder(self, path):
        try:
            os.makedirs(path)
        except Exception as e:
            logging.error(e)

    def set_date(self,inspection_date, format):
        d = datetime.strptime(inspection_date, format)
        unix_time = time.mktime(d.timetuple()) * 1000
        return math.trunc(unix_time)

    #cretin a JSON file and writing 
    def create_json(self,file_name):
        with open(rf'{file_name}.json', 'w', encoding='utf-8') as outfile:
            json.dump(self.data, outfile, ensure_ascii=False, indent=4)

   

    def process_item(self, item, spider):
        if spider.name == 'entry':

            self.data = {
                'importMode': 'CREATE_ONLY',
                'type': 'ENTRY',
            }

            self.data['inspectedAt'] = self.set_date(item['date'], '%d/%m/%y %I:%M %p')
            self.data.update(item['data'])
            inspection_id = item.get('inspection_id')

            address = item.get('address')

            image_main_dir = os.path.join(self.base_path, f'OUTPUT/{self.org_name}/properties/{address}/entry/images')
            
            room_number = 0
            for room in self.data.get('rooms'):
                room_number += 1
                room_title = room.get('title').replace("/","_")

                item_number = 0
                for room_item in room.get('items'):
                    item_number += 1
                    item_title = room_item.get('title').replace("/","_")
                    
                    path_to_pic = f'{room_number}-{room_title}/{item_number}-{item_title}'
                    item_path = os.path.join(image_main_dir, path_to_pic)
                    self.create_folder(item_path)

                    photos = room_item.pop('photos', None)
                    
                    for photo in photos:
                        try:
                            self.c.execute('''
                                INSERT INTO image(image_id, URL, path, is_downloaded, is_uploaded, is_photo_attached, inspection_id) VALUES(?,?,?,?,?,?,?)
                            ''', (
                                    photo.get('photoId'),
                                    photo.get('url'),
                                    path_to_pic,
                                    0,
                                    0,
                                    0,
                                    inspection_id,

                                ))
                            self.connection.commit()
                        except sqlite3.IntegrityError as e:
                            pass
                        except sqlite3.Error as er:
                            logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
                            logging.ERROR("Exception class is: ", er.__class__)
                            logging.ERROR('SQLite traceback: ')
                            exc_type, exc_value, exc_tb = sys.exc_info()
                            logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

                
            
            
            json_path = os.path.join(item.get('property_path'), str(inspection_id))
            self.create_json(json_path)

            try:
                self.c.execute(f'''
                    UPDATE inspection
                    SET is_converted = 1
                    WHERE inspection.inspection_id = '{inspection_id}'
                ''', )
                self.connection.commit()
            except sqlite3.Error as er:
                logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
                logging.ERROR("Exception class is: ", er.__class__)
                logging.ERROR('SQLite traceback: ')
                exc_type, exc_value, exc_tb = sys.exc_info()
                logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

        return item

    
    def close_spider(self, spider):
            self.connection.close()

