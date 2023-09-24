import logging
import traceback
import sys
import scrapy
from scrapy.http import HtmlResponse
import sqlite3
import yaml
from pathlib import Path
import os

class PropertiesInspectionsListSpider(scrapy.Spider):
    name = 'entry'
    
    def start_requests(self):    
        url = 'https://www.google.com/'
        yield scrapy.Request(url, meta=dict(
            playwright = True,
            playwright_include_page = True,
            errback=self.errback,
        ))
    
    async def fetch_all_entries(self):
        with open("config.yml", "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)

        self.org_name = config["org_name"]
        
        self.base_path = Path(__file__).parents[2]
        self.db_path = os.path.join(self.base_path, f'OUTPUT/{self.org_name}/db/{self.org_name}.db')
        
        self.connection = sqlite3.connect(self.db_path)
        self.c = self.connection.cursor()

        try:
            self.c.execute('''
                SELECT property.address, property.subburb, MAX(inspection.inspection_id) as inspection_id, inspection.property_id, inspection.report, inspection.date, inspection.time, inspection.is_converted 
                FROM property NATURAL JOIN inspection 
                WHERE inspection.status = 'Closed' and inspection.type = 'Ingoing'
                GROUP BY inspection.property_id
            ''' )
            records = self.c.fetchall()

            result = []
            for record in records:
                if record[7] == 0:
                    result.append(record)

            self.connection.commit()
        except sqlite3.Error as er:
            logging.ERROR('SQLite error: %s' % (' '.join(er.args)))
            logging.ERROR("Exception class is: ", er.__class__)
            logging.ERROR('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            logging.ERROR(traceback.format_exception(exc_type, exc_value, exc_tb))

        self.connection.close()
        return result

    async def check_conditions(self, condition_marker):
        if condition_marker == 'icon-yes fs-20 color-green':
            return 'YES'
        elif condition_marker == 'icon-no fs-20 color-red':
            return 'NO'
        else:
            return 'NA'
    
    #creating a new item
    async def add_item(self, title, clean, undamaged, working,  comment, photos):
        condition  = {
            'isClean': await self.check_conditions(clean),
            'isUndamaged': await self.check_conditions(undamaged),
            'isWorking': await self.check_conditions(working),
        }
        
        self.items.append({
            'title' : title,
            "type": "FIXTURE",
            'comment': str(comment) if comment != None else "",
            'condition': condition,
            'photos': photos           
            })
        return 1

    #check if room name is reapiting
    async def is_same_room(self, room_name):
        if 'title' in self.room and room_name is not None:
            current_room_name = self.room['title']
            current_room_name = current_room_name.replace(" ", "").replace('\n', '')
            room_name = room_name.replace(" ", "").replace('\n', '')
            if current_room_name == room_name:
                return True
            return False
        return False

    async def parse(self, response):
        page = response.meta["playwright_page"]
        page.set_default_timeout(300000.0)

        for report in await self.fetch_all_entries():
            address = report[0] + ", " + report[1]
            address = address.replace("/","_")
            self.property_path = os.path.join(self.base_path, f'OUTPUT/{self.org_name}/properties/{address}/entry')

            self.data = {}
            self.data['rooms'] = []
            self.room = {}
            self.items = [] 

            await page.goto(report[4])

            
            await page.wait_for_selector("i.icon-download")

            content = await page.content()
            response = HtmlResponse(url="next page", body=content, encoding='utf-8')
            
            for tabe_row in response.xpath("//div[contains(@class, 'itemRow')]"):
                if "heading-row" in tabe_row.xpath("@class").get():
                    room_name = tabe_row.xpath(".//div[@class= 'fs-12 cs-b Items']/text()").get()
                    if await self.is_same_room(room_name):
                        pass
                    elif "title" in self.room :
                        self.room['items'] = self.items
                        self.data['rooms'].append(self.room)
                        self.items = []
                        self.room = {}
                        self.room['title'] = room_name
                    else:
                        self.room['title'] = room_name
                else:
                    item_name = tabe_row.xpath("./div[1]/div/text()").get()
                    comment = tabe_row.xpath("./div[5]/div/section/span/text()").get()
                    clean = tabe_row.xpath("./div[2]/i/@class").get()
                    undamaged = tabe_row.xpath("./div[3]/i/@class").get()
                    working = tabe_row.xpath("./div[4]/i/@class").get()

                    # If comment span section contained more divs
                    for additional_comment in tabe_row.xpath("./div[5]/div/section/span/div"):
                        comment = f'{comment} {additional_comment.xpath("text()").get()}'
                    
                    if comment:
                        comment = comment.replace(' ', '\n')
                    
                    photos = []
                    for image in tabe_row.xpath(".//div[@class='areabutton d-i-b']"):
                        photoId = image.xpath("./span/@data-photo-id").get()
                        url = response.xpath(f"//span[@id='photo-{photoId}']/following-sibling::div/div/span/img/@src").get()
                        photo = {
                        'photoId': photoId,
                        'url' : url
                        }
                        photos.append(photo)

                    await self.add_item(item_name, clean, undamaged, working, comment, photos)
            
            self.room['items'] = self.items
            self.data['rooms'].append(self.room)

            self.pdf_path = os.path.join(self.property_path, f'{address}.pdf')
            await page.emulate_media(media="screen")
            await page.pdf(path=self.pdf_path)

            
            yield{
                'data': self.data,
                'date': f'{report[5]} {report[6]}',
                'address': address,
                'inspection_id': report[2],
                'property_path': self.property_path
            }
        
        await page.close()   
                


    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()
