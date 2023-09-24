import logging
import scrapy
from scrapy_playwright.page import PageMethod
from scrapy.http import HtmlResponse
import yaml

class PropertiesInspectionsListSpider(scrapy.Spider):
    name = 'inspections_list'
    
    def start_requests(self):
        with open("config.yml", "r") as ymlfile:
            config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
        self.email = config["email"]
        self.password = config["password"]
        self.last_record = config["last_record"]
        
        url = 'https://cms.inspectionmanager.com.au/User/LogOn'
        yield scrapy.Request(url, meta=dict(
            playwright = True,
            playwright_include_page = True,
            errback=self.errback,
            playwright_page_methods = [
                #Login to the Inspection manager
                PageMethod('wait_for_selector', 'a#btnLogin1'),
                PageMethod("fill", "#Username", self.email),
                PageMethod("fill", "#Password", self.password),
                PageMethod("click", selector="#btnLogin1"),
                PageMethod('wait_for_selector', 'a#propertyTabMenu'),

                #Clicking on property tab
                PageMethod("click", selector="#propertyTabMenu"),
                PageMethod('wait_for_selector', 'a#propertyTabMenu'),         
                ]
        ))
    
    

    async def parse(self, response):
        page = response.meta["playwright_page"]
        page.set_default_timeout(200000.0)
        
        await page.select_option('select#jq_rmPageSize', value='25000')
        await page.wait_for_selector(f"//td[contains(@title, '{self.last_record}')]")

        content = await page.content()
        response = HtmlResponse(url="next page", body=content, encoding='utf-8')

        inspection_row_number = 1
        for inspections in response.xpath("//td/a[contains(@class, 'jq_ViewInspections')]"):        
            await page.locator(f":nth-match(a.jq_ViewInspections, {inspection_row_number})").click()
            logging.debug(f'Clicked property {inspection_row_number}  to view all inspections')
    
            await page.locator("text=Hide Inspection(s)").click()
            logging.debug(f'Property {inspection_row_number} hided')

            inspection_row_number+=1

        content = await page.content()
        response = HtmlResponse(url="next page", body=content, encoding='utf-8')
            
        i = 1
        for inspection in response.xpath("//table[contains(@class, 'jq_InspectionTable')]/tbody/tr[contains(@class,'jq_InspectionRow ')]"):
            yield {
                'inspection_id': inspection.xpath("@data-inspectionid").get(),
                'property_id': inspection.xpath("@data-propertyid").get(),
                'inspector': inspection.xpath(".//td[2]/text()").get(),
                'type': inspection.xpath(".//td[3]/text()").get().strip(),
                'date': inspection.xpath(".//td[4]/text()").get(),
                'time': inspection.xpath(".//td[5]/text()").get(),
                'status': inspection.xpath(".//td[6]/label/i[contains(@class, 'fs-10')]/text()").get(),
                'report': inspection.xpath(".//td[10]//a[contains(@class, 'previewInHtmlLink')]/@href").get(),    
            }
            
            
        await page.close()   
            
    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()
