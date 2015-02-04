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

        # self.query = "藝之鄉特產"
        # self.url = 'http://www.google.com.tw/search?q=%s&oe=utf-8&gws_rd=ssl' % self.query
        # self.start_urls = ['http://www.google.com.tw/search?q="%s"&oe=utf-8&gws_rd=ssl' % self.query]

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
        resp = es.search(index='tw-textsearch', doc_type='yp',
                            search_type='scan', scroll='10m', 
                            fields='name', size=1)
        _scroll_id = resp['_scroll_id']
        resp = es.scroll(scroll_id=resp['_scroll_id'], scroll='10m')
        hits = resp['hits']['hits']
        while hits:
            names = [hit['fields']['name'][0] for hit in hits]
            for name in names:
              search_dsl = {"query": {"term": {"query": {"value": name}}}}
              resp = es.search(index='keyword-expansion', doc_type='descriptions', body=search_dsl)
              if len(resp['hits']['hits']) > 0:
                continue
              else:
                url = 'http://www.google.com.tw/search?q="%s"&oe=utf-8&gws_rd=ssl' % name
                yield Request(url, callback=self.parse, meta={"query": name})

            resp = es.scroll(scroll_id=resp['_scroll_id'], scroll='10m')
            hits = resp['hits']['hits']

    def parse(self, response):
        result = GoogleResultItem()
        result['query'] = response.meta['query']

        main_title = ''
        main_snippet = ''
        main_result = response.xpath('//*[@id="rso"]/li/div')
        if main_result:
          if main_result.xpath('div/h3/a/text()'):
            main_title = main_result.xpath('div/h3/a/text()').extract()[0]
          else:
            main_title = main_result.xpath('h3/a/text()').extract()[0]
          if main_result.xpath('div/div/div/span/text()'):
            main_snippet = ''.join(main_result.xpath('div/div/div/span/text()').extract())
          else:
            main_snippet = ''.join(main_result.xpath('div/div/span/text()').extract())

        titles = [main_title]
        snippets = [main_snippet]

        for sel in response.xpath('//div[@class="srg"]/li[@class="g"]/div'):
            title = ''.join(sel.xpath('h3/a/text()').extract())
            titles.append(title)
            
            snippet = ''.join(sel.xpath('div/div/span[@class="st"]//text()').extract())
            snippet = snippet.replace('\n', '')
            snippets.append(snippet)
        result['titles'] = titles
        result['snippets'] = snippets
  
        yield result
