# -*- coding: utf-8 -*-
import re

import string
from collections import Counter
from elasticsearch import Elasticsearch
from scrapy import log
from datetime import datetime

ES_NODES = [{'host': 'ec2-54-168-209-173.ap-northeast-1.compute.amazonaws.com', 'port': 9200}]


RE_PUNCTUATIONS = u'[\u3000-\u303F]|[\uFF00-\uFFEF]|[\u25A0-\u25FF]|[%s·]'
RE_URL = u'(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w\.-]*)*\/?'

class ESPipeline(object):
    """Elasticsearch Pipeline for scrapy"""

    def __init__(self):
		self.es = Elasticsearch(ES_NODES)

    def process_item(self, item, spider):
        snippets = item['snippets']
        snippets = [re.sub(u'\d{4}年\d{,2}月\d{,2}日| ...', '', s).strip() for s in snippets]
        snippets = [s.lower() for s in snippets]

        keywords = self._get_keywords(item['query'], snippets)
        doc = {
            'oid': item['_id'],
            'query': item['query'],
            'titles': item['titles'],
            'snippets': snippets,
            'keywords': keywords
        }
        self.es.index(index='keyword-expansion', doc_type='results', body=doc)

        return item

    def _get_keywords(self, query, snippets):
        resp = self.es.indices.analyze(index='tw-textsearch-v1-dev', text=query, analyzer="icu_analyzer")
        query_tokens = [t['token'] for t in resp['tokens']]
        query_tokens = [t for t in query_tokens if len(t) > 1]

        description = ''.join(snippets)
        
        seed_players = []
        snippet_tokens = []
        for snippet in snippets:
            if query_tokens:
                snippet = re.sub('[%s]' % '|'.join(query_tokens), '', snippet)
            
            snippet = remove_urls(snippet)
            snippet = remove_phone_numbers('TW', snippet)

            segments = re.split(RE_PUNCTUATIONS % re.escape(string.punctuation), snippet)
            for s in segments:
                s = s.strip()

                if len(s) < 2:
                    continue
                elif 2 <= len(s) < 5:
                    # currently ignore digits and alphabets now
                    if not re.match('[\dA-Za-z]*', s).group(0):
                        seed_players.append(s)
                else:
                    resp = self.es.indices.analyze(text=s, analyzer="standard")
                    # currently ignore digits and alphabets now
                    # alphanum_tokens = [t['token'] for t in resp['tokens'] if t['type'] == '<ALPHANUM>']
                    # alpha_tokens = [t for t in alphanum_tokens if not t.isdigit() and len(t) > 1]
                    # seed_players.extend(alpha_tokens)

                    non_alphanum_tokens = [t['token'] for t in resp['tokens'] if t['type'] != '<ALPHANUM>']
                    token = ''.join(non_alphanum_tokens)
                    snippet_tokens.append(token)
        
        bench_players = get_bench_players(snippet_tokens)

        candidates = list(set(seed_players + bench_players))
        keywords = []
        for c in candidates:
            count = description.count(c)
            if count > 3:
                keyword = {"keyword": c, "count": count}
                keywords.append(keyword)

        return keywords


def remove_urls(string):
    match = re.search(RE_URL, string)
    while match:
        string = string.replace(match.group(0), '')
        match = re.search(RE_URL, string)
    return string

import phonenumbers
def remove_phone_numbers(region, string):
    for match in phonenumbers.PhoneNumberMatcher(string, region):
        string = string.replace(match.raw_string, '')
    return string

def get_bench_players(tokens):
    bench_players = []
    for i in range(len(tokens)):
        for j in range(i + 1, len(tokens)):
            lcs = get_lcs(tokens[i], tokens[j]).strip()
            if len(lcs) > 1 and not lcs.isdigit():
                bench_players.append(lcs)
    return bench_players

def get_lcs(s1, s2):
    m = [[0] * (1 + len(s2)) for i in range(1 + len(s1))]
    longest, x_longest = 0, 0
    for x in range(1, 1 + len(s1)):
        for y in range(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return s1[x_longest - longest: x_longest]
