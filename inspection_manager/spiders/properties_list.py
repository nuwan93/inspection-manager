import logging
import scrapy
from scrapy_playwright.page import PageMethod
from scrapy.http import HtmlResponse
import yaml


class PropertiesInspectionsListSpider(scrapy.Spider):
    name = 'properties_list'
    
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

        for property in response.xpath("//table[@class='full main-table main-table_custom table-eclipse234']/tbody/tr[contains(@class,'propertyCheckList jq_PropertyRow ')]"):
            yield {
                'property_id': property.xpath("@data-propertyid").get(),
                'address': property.xpath(".//td[3]/text()").get(),
                'subburb': property.xpath(".//td[4]/text()").get(),
                'manager': property.xpath(".//td[5]/text()").get(),
                'owner':  property.xpath(".//td[6]/text()").get(),
                'tentant': property.xpath(".//td[7]/text()").get(),
            }
               
        await page.close()   
            


    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()
