# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BestbuyDetailsItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()

class BestCatLinksItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()

class SubcatLinksItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()

class Subcat_Of_cat_LinksItem(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()
