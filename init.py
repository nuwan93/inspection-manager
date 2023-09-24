import os
import yaml
import sqlite3

#This script creates the folder structure to download the files and the database


def create_folder(path):
    try:
        os.mkdir(path)
    except Exception as e:
        pass

base_path = os.path.dirname(os.path.realpath(__file__)) # directory path of the app

config_file = os.path.join(base_path, "config.yml")

with open(config_file, "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
org_name = config["org_name"]


base_path = os.path.dirname(os.path.realpath(__file__)) # directory path of the app

######### Creating folder structure #########

#create output folder if not exist
output_folder = os.path.join(base_path, 'OUTPUT')
create_folder(output_folder)

#create org folder if not exist
org_folder = os.path.join(output_folder, org_name)
create_folder(org_folder)

#create properties folder if not exist
properties_folder = os.path.join(org_folder, 'properties')
create_folder(properties_folder)

#create db folder if not exist
db_folder = os.path.join(org_folder, 'db')
create_folder(db_folder)

######### Creating db and the tables #########

connection = sqlite3.connect(os.path.join(db_folder, f'{org_name}.db'))
c = connection.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS property(
        property_id INTEGER NOT NULL PRIMARY KEY,
        address TEXT,
        subburb TEXT,
        manager TEXT,
        owner TEXT,
        tentant TEXT,
        pirsee_prop_id TEXT
    )
''')

c.execute('''
    CREATE TABLE IF NOT EXISTS inspection(
        inspection_id INTEGER NOT NULL PRIMARY KEY,
        inspector TEXT,
        type TEXT,
        date TEXT,
        time TEXT,
        status TEXT,
        report TEXT,
        is_converted INTEGER,
        is_uploaded INTEGER,
        pirsee_id TEXT,
        property_id INTEGER NOT NULL,
        FOREIGN KEY (property_id)
            REFERENCES property (property_id)
    )
''')

c.execute('''
    CREATE TABLE IF NOT EXISTS image(
        image_id INTEGER NOT NULL PRIMARY KEY,
        URL TEXT,
        path TEXT,
        is_downloaded INTEGER,
        is_uploaded INTEGER,
        is_photo_attached INTEGER,
        pirsee_img_id TEXT,       
        inspection_id INTEGER NOT NULL,
        FOREIGN KEY (inspection_id)
            REFERENCES inspection (inspection_id)
    )
''')

connection.commit()

connection.close()