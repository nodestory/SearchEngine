# -*- coding: utf-8 -*-
import base64
import os
import random
import pymongo
from scrapy import log

class ProxyMiddleware(object):

    def __init__(self, settings):
        conn = pymongo.MongoClient('mongodb://whoscallapi:Jp87B2ir4dJD@db-staging.whoscall.com:27017')
        proxyip_collection = conn['whoscallproxyip']['Proxyip']
        self.proxies = [x['proxyip'] for x in proxyip_collection.find()]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        proxy_tokens = random.choice(self.proxies).split("@")
        proxy_user_pass = proxy_tokens[0]
        proxy_ip = proxy_tokens[1]

        request.meta['proxy'] = "http://" + proxy_ip
        request.headers['Proxy-Authorization'] = 'Basic ' + base64.encodestring(proxy_user_pass)

    def process_response(self, request, response, spider):
        if response.status == 407:
            log.msg('proxy: ' + str(response.status) + ", " + request.meta['proxy'], level=log.WARNING)
            return request
        elif response.status != 200 and response.status != 502:
            log.msg('response code = '+ str(response.status) , level=log.ERROR)
            return request
        return response
