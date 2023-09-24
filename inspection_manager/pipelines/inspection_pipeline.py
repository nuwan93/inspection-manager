# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import os
import sqlite3
import yaml
from pathlib import Path
# useful for handling different item types with a single interface
import traceback
import sys



class InspectionPipeline:
    def open_spider(self, spider):
        
        with open("config.yml", "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)

        self.org_name = config["org_name"]
        
        self.base_path = Path(__file__).parents[2]
        self.db_path = os.path.join(self.base_path, f'OUTPUT/{self.org_name}/db/{self.org_name}.db')
        
        self.connection = sqlite3.connect(self.db_path)
        self.c = self.connection.cursor()
    
    def process_item(self, item, spider):
        if spider.name == 'inspections_list':
            try:
                self.c.execute('''
                    INSERT INTO inspection(inspection_id, inspector, type, date, time, status, report, is_converted, is_uploaded, property_id) VALUES(?,?,?,?,?,?,?,?,?,?)
                ''', (
                        item.get('inspection_id'),
                        item.get('inspector'),
                        item.get('type'),
                        item.get('date'),
                        item.get('time'),
                        item.get('status'),
                        item.get('report'),
                        0,
                        0,
                        item.get('property_id'),
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

        return item

    def close_spider(self, spider):
        self.connection.close()
