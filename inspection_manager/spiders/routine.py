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
    name = 'routine'
    
    def start_requests(self):    
        url = 'https://www.google.com/'
        yield scrapy.Request(url, meta=dict(
            playwright = True,
            playwright_include_page = True,
            errback=self.errback,
        ))
    
    async def fetch_all_routines(self):
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
                WHERE inspection.status = 'Closed' AND
                      inspection.type = 'Routine' 
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
    async def add_room(self, title, comment, clean, undamaged, working):

        is_clean = self.check_conditions(clean)
        is_undamaged = self.check_conditions(undamaged)
        is_working = self.check_conditions(working)

        condition_markers = (f'Condition Satisfactory - {is_clean}, '
                   f'Action required by tenant - {is_undamaged},  '
                   f'Action required by landlord - {is_working}\n\n'
                  )
        comment = condition_markers + comment
        
        self.data['rooms'].append({
            'title' : title,
            'comment': str(comment) if comment != None else "",        
            })
        return 1



    async def parse(self, response):
        page = response.meta["playwright_page"]
        page.set_default_timeout(300000.0)

        for report in await self.fetch_all_routines():
            address = report[0] + ", " + report[1]
            address = address.replace("/","_")
            self.property_path = os.path.join(self.base_path, f'OUTPUT/{self.org_name}/properties/{address}/routine')

            self.data = {}
            self.data['rooms'] = []
            self.room = {}

            await page.goto(report[4])

            
            await page.wait_for_selector("i.icon-download")

            content = await page.content()
            response = HtmlResponse(url="next page", body=content, encoding='utf-8')
            
            for tabe_row in response.xpath("//div[contains(@class, 'itemRow')]"):
                if "heading-row" in tabe_row.xpath("@class").get():
                   pass
                else:
                    room_name = tabe_row.xpath("./div[1]/div/text()").get()
                    comments = tabe_row.xpath("./div[5]/div/section/span//text()").getall()

                    clean = tabe_row.xpath("./div[2]/i/@class").get()
                    undamaged = tabe_row.xpath("./div[3]/i/@class").get()
                    working = tabe_row.xpath("./div[4]/i/@class").get()
                    
                    for text in comments:
                        comment += ' ' + text
                    
                    comment = comment.replace(' ', '\n')
                    comment = comment.lstrip()
                  
                    await self.add_room(room_name, comment, clean, undamaged, working)
                    

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
