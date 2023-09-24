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



class RoutinePipeline:
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
        if spider.name == 'routine':

            self.data = {
                'importMode': 'CREATE_ONLY',
                'type': 'ROUTINE',
            }

            self.data['inspectedAt'] = self.set_date(item['date'], '%d/%m/%y %I:%M %p')
            self.data.update(item['data'])
            inspection_id = item.get('inspection_id')
            
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

