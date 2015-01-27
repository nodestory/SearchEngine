# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http.request import Request
from search_engine.items import GoogleResultItem

ES_NODES = [{'host': 'ec2-54-168-209-173.ap-northeast-1.compute.amazonaws.com', 'port': 9200}]

class GoogleSearchSpider(CrawlSpider):
    name = "google"

    def __init__(self, *args, **kwargs): 
        super(CrawlSpider, self).__init__(*args, **kwargs)

        # self.query = "好初"
        # self.url = 'http://www.google.com.tw/search?q=%s&oe=utf-8&gws_rd=ssl' % self.query
        # self.start_urls = ['http://www.google.com.tw/search?q=%s&oe=utf-8&gws_rd=ssl' % self.query]

    def start_requests(self):
        es = Elasticsearch(ES_NODES)

        query_dsl = {
          "fields": ["name"], 
          "query": {
            "filtered": {
              "query": {
                "match_all": {}
              },
              "filter": {
                "and": {
                  "filters": [
                    {
                      "geo_distance": {
                      "distance": "10km",
                      "lnglat": [121.522438, 25.026662]
                      }
                    },
                    {
                      "not": {
                        "filter": {
                          "term": {
                            "gid": ""
                          }
                          }
                        }
                    }
                  ]
                }
              }
            }
          }
        }
        resp = es.search(index='tw-textsearch', doc_type='yp', body=query_dsl,
                            search_type='scan', scroll='10m', 
                            fields='name', size=15)
        _scroll_id = resp['_scroll_id']
        resp = es.scroll(scroll_id=resp['_scroll_id'], scroll='1m')
        hits = resp['hits']['hits']
        while hits:
            names = [hit['fields']['name'][0] for hit in hits]
            for name in names:
              if es.get(index='keyword-expansion', doc_type='search-results',id=name)['found']:
                continue
                
              url = 'http://www.google.com.tw/search?q=%s&oe=utf-8&gws_rd=ssl' % name
              yield Request(url, callback=self.parse, meta={"query": name})

            resp = es.scroll(scroll_id=resp['_scroll_id'], scroll='1m')
            hits = resp['hits']['hits']


    def parse(self, response):
        result = GoogleResultItem()
        result['query'] = response.meta['query']

        titles = []
        snippets = []
        for sel in response.xpath('//li[@class="g"]/div'):
            title = ''.join(sel.xpath('h3/a/text()').extract())
            titles.append(title)
            
            snippet = ''.join(sel.xpath('div/div/span[@class="st"]//text()').extract())
            snippet = snippet.replace('\n', '')
            snippets.append(snippet)
        result['titles'] = titles
        result['snippets'] = snippets
        yield result
