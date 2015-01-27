# -*- coding: utf-8 -*-
import re
from elasticsearch import Elasticsearch
from scrapy import log
from datetime import datetime

ES_NODES = [{'host': 'ec2-54-168-209-173.ap-northeast-1.compute.amazonaws.com', 'port': 9200}]

class ESPipeline(object):
    """Elasticsearch Pipeline for scrapy"""

    def __init__(self):
		self.es = Elasticsearch(ES_NODES)

    def process_item(self, item, spider):
        snippets = item['snippets']
        snippets = [re.sub(u'\d{4}年\d{,2}月\d{,2}日| ...', '', s).strip() for s in snippets]
        snippets = [s.lower() for s in snippets]

        doc = {
            "titles": item['titles'],
            "snippets": snippets
        }
        self.es.index(index='keyword-expansion', doc_type='search-results',
                        id=item['query'], body=doc)

        return item
