# -*- coding: utf-8 -*-
import os
import csv
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from settings import DATA_CATEGORIES
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class GasPriceExtractPipeline(object):
    def __init__(self):
        self.files = {}
        self.file_name = {
                            DATA_CATEGORIES[0]:"output_data_daily_price.csv",
                            DATA_CATEGORIES[1]:"output_data_monthly_price.csv"
                        }
        self.export_fields = ['date', 'price']

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.exporters = {}
        for category in DATA_CATEGORIES:
            output_file = open(self.file_name[category], 'w+b')
            exporter = CsvItemExporter(output_file, fields_to_export = self.export_fields)
            exporter.start_exporting()
            self.exporters[category] = exporter
            

    def spider_closed(self, spider):
        for exporter in self.exporters.itervalues(): 
            exporter.finish_exporting()
            

    def process_item(self, item, spider):
        self.exporters[item['category']].export_item(item)
        return item
