# -*- coding: utf-8 -*-

import re
import scrapy
from gas_price_extracter.items import GasPriceExtractItem
from gas_price_extracter.settings import DATA_CATEGORIES

class GasPriceExtractSpider(scrapy.Spider):
    name = "price_extract_spider"
    allowed_domains = ["www.eia.gov"]
    start_urls = ['https://www.eia.gov/dnav/ng/hist/%s.htm' % category for category in DATA_CATEGORIES]
    # start_urls = ['https://www.eia.gov/dnav/ng/hist/rngwhhdM.htm']

    def parse(self, response):

        category = next(category for category in DATA_CATEGORIES if category in response.url)
        print 'category ', category
        if category == "rngwhhdD":
            table_data = response.xpath('//table[@summary="Henry Hub Natural Gas Spot Price (Dollars per Million Btu)"]/tr')
            for each_item in  table_data[1:]:
                #Proceed for daily price extraction\#Iterate through each table row:
                if each_item.xpath('td[@class="B6"]'):

                    #Extract each column item from the row
                    column_data = [row_data.xpath('text()').extract_first() for row_data in each_item.xpath('td')]
                    
                    week_of = column_data[0].strip()
                    matched = re.search(r'(\S+\s+\S+)-\s*(\S+)\s*to', week_of)
                    
                    date_initials = matched.group(1) 
                    starting_day = int(matched.group(2))
                    
                    for each_day_value in column_data[1:]:
                        item = GasPriceExtractItem()
                        item['date'] = "%s %s" % (date_initials, starting_day)
                        item['price'] = each_day_value
                        item['category'] = category
                        starting_day += 1
                        
                        yield item

        else:
            table_data = response.xpath('//table[@width="675" and @cellpadding="2"]/tr')
            for each_item in  table_data[1:]:
                #Proceed for monthly price extraction                
                if each_item.xpath('td[@class="B4"]'):
                    month_map = {
                            1: "Jan",
                            2: "Feb",
                            3: "Mar",
                            4: "Apr",
                            5: "May",
                            6: "Jun",
                            7: "Jul",
                            8: "Aug", 
                            9: "Sep",
                            10: "Oct",
                            11: "Nov",
                            12: "Dec"
                    }
                    #Extract each column item from the row
                    column_data = [row_data.xpath('text()').extract_first() for row_data in each_item.xpath('td')]
                    
                    year = column_data[0].strip()
                    
                    for month_index, monthly_price in enumerate(column_data[1:]):
                        item = GasPriceExtractItem()
                        item['date'] = "%s %s 1" % (year, month_map[month_index+1])
                        item['price'] = monthly_price
                        item['category'] = category
                        yield item

            
    
    # def parse_data(self, column_data):
        
    #     week_of = column_data[0].strip()
    #     matched = re.search(r'(\S+\s+\S+)-\s*(\S+)\s*to', week_of)
    #     date_initials = matched.group(1) 
    #     starting_day = int(matched.group(2))
    #     for each_day_value in column_data[1:]:
    #         item = GasPriceExtractItem()
    #         item['date'] = "%s %s" % (date_initials, starting_day)
    #         item['price'] = each_day_value
    #         starting_day += 1
            
    #         return item
    # 