# -*- coding: utf-8 -*-
import re
from urllib.parse import urljoin
from amazon.items import AmazonItem
import scrapy
from scrapy_splash import SplashRequest


class AmazonprojectSpider(scrapy.Spider):
    name = 'amazonproject'
    allowed_domains = ['www.amazon.cn']
    lua = """
        function main(splash,args)
            splash:go(args.url)
            splash:wait(2)
            return splash:html()
        end
    """
    # start_urls = ['http://www.amazon.cn/']
    def start_requests(self):
        for page in range(1,2):
            url = 'https://www.amazon.cn/s/ref=lp_117198071_pg_2?rh=n%3A2016116051%2Cn%3A%212016117051%2Cn%3A755653051%2Cn%3A755654051%2Cn%3A117198071&page='+str(page)+'&ie=UTF8&qid=1533612200'
            yield scrapy.Request(url=url,callback=self.url_parse)

    def url_parse(self, response):
        li = response.xpath("//ul[contains(@class,'s-result-list')]/li")
        for i in li:
            url = i.xpath(".//div[contains(@class,'a-spacing-mini')][1]//a/@href").extract()[0]
            yield SplashRequest(url=url,callback=self.detail_parse,args={
                "lua_source":self.lua,
            },endpoint="execute",splash_url='http://192.168.99.100:8050')
            # yield scrapy.Request(url=url,callback=self.detail_parse)

    def detail_parse(self,response):
        item = AmazonItem()
        result = response.xpath('//div[@id="centerCol"]')
        brand = result.xpath('.//a[@id="bylineInfo"]/text()').extract()[0]
        price = result.xpath('.//span[@id="priceblock_ourprice"]/text()').extract()[0]
        desc = result.xpath('.//span[@id="productTitle"]/text()').extract()[0].strip()
        shop = result.xpath('.//span[@id="ddmMerchantMessage"]/a/text()').extract()[0]
        shop_url = urljoin("https://www.amazon.cn",result.xpath('.//span[@id="ddmMerchantMessage"]/a/@href').extract()[0])
        storage_lst = ["brand","price","desc","shop","shop_url"]
        data_lst = [brand,price,desc,shop,shop_url]
        for i in range(len(data_lst)):
            item[storage_lst[i]] = data_lst[i]
        return item


