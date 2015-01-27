import random
from scrapy import log
 
class RandomUserAgentMiddleware(object):
 
    def __init__(self, settings):
        self.user_agent_list = list(settings.get('USER_AGENT_LIST'))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        ua = random.choice(self.user_agent_list)
        if ua:
            request.headers.setdefault('User-Agent', ua)
