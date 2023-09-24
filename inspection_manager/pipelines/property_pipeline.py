# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import traceback
import sys
import os
import sqlite3
import yaml
from pathlib import Path
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

class PropertyPipeline:
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
            os.mkdir(path)
        except Exception as e:
            logging.ERROR("Error creating ", path)
            logging.ERROR(e)

    def process_item(self, item, spider):
        if spider.name == 'properties_list':
            #inserting property into db
            try:
                self.c.execute('''
                    INSERT INTO property(property_id, address, subburb, manager, owner, tentant) VALUES(?,?,?,?,?,?)
                ''', (
                        item.get('property_id'),
                        item.get('address'),
                        item.get('subburb'),
                        item.get('manager'),
                        item.get('owner'),
                        item.get('tentant'),
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
            
            #Creating folder for each property
            address = item.get('address') + ", " + item.get('subburb')
            address = address.replace("/","_")

            property_path = os.path.join(self.base_path, f'OUTPUT/{self.org_name}/properties/{address}')
            self.create_folder(property_path)

            entry_path = os.path.join(property_path, f'entry')
            self.create_folder(entry_path)
            images_path = os.path.join(entry_path, f'images')
            self.create_folder(images_path)

            routine_path = os.path.join(property_path, f'routine')
            self.create_folder(routine_path)

        return item

    def close_spider(self, spider):
        self.connection.close()
