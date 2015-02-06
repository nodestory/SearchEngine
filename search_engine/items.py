from scrapy import Item, Field

class GoogleResultItem(Item):
    _id = Field()
    query = Field()
    titles = Field()
    snippets = Field()