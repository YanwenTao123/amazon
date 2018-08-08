# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json

import pymongo
import re
from redis import StrictRedis, ConnectionPool


class RedisPipeline(object):
    def __init__(self,redisport,redishost,redispassword,redisdb):
        self.redisport = redisport
        self.redishost = redishost
        self.redisdb = redisdb
        self.redispassword = redispassword

    @classmethod
    def from_crawler(cls, crawler):
        s = cls(
            redisport = crawler.settings.get("REDISPORT"),
            redishost = crawler.settings.get("REDISHOST"),
            redisdb = crawler.settings.get("REDISDB"),
            redispassword = crawler.settings.get("REDISPASSWORD")
        )
        return s

    def open_spider(self,spider):
        self.pool = ConnectionPool(host=self.redishost,port=self.redisport,password=self.redispassword)
        self.redis = StrictRedis(connection_pool=self.pool)
        # self.redis = StrictRedis(host=self.host,port=self.port,password=self.password)

    def process_item(self, item, spider):
        self.redis.rpush('list',item)
        return item

class MongoPipeline(object):
    def __init__(self,port,host,password,db,collection,redisport,redishost,redispassword,redisdb):
        self.port = port
        self.host = host
        self.db = db
        self.password = password
        self.collection = collection
        self.redisport = redisport
        self.redishost = redishost
        self.redisdb = redisdb
        self.redispassword = redispassword

    @classmethod
    def from_crawler(cls, crawler):
        s = cls(
            port = crawler.settings.get("MONGOPORT"),
            host = crawler.settings.get("MONGOHOST"),
            db = crawler.settings.get("MONGODB"),
            password = crawler.settings.get("MONGOPASSWORD"),
            collection = crawler.settings.get("COLLECTION"),
            redisport=crawler.settings.get("REDISPORT"),
            redishost=crawler.settings.get("REDISHOST"),
            redisdb=crawler.settings.get("REDISDB"),
            redispassword=crawler.settings.get("REDISPASSWORD")
        )
        return s

    def open_spider(self,spider):
        self.conn = pymongo.MongoClient(host=self.host,port=self.port)
        self.db = self.conn[self.db]
        self.coll = self.db[self.collection]
        self.pool = ConnectionPool(host=self.redishost, port=self.redisport, password=self.redispassword)
        self.redis = StrictRedis(connection_pool=self.pool)


    def process_item(self, item, spider):
        count = self.redis.llen('list')
        for i in range(count):
            s = self.redis.rpop('list').decode()
            data = re.sub("\'\s+?\'", " ", s)
            data = re.sub('\'', '\"', data)
            data = json.loads(data)
            self.coll.insert(data)
        return item



