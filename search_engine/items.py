from scrapy import Item, Field

class GoogleResultItem(Item):
    query = Field()
    titles = Field()
    snippets = Field()