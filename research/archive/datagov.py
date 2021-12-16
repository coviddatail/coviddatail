
import scrapy
class DatwagovSpider(scrapy.Spider):
    name = 'blogspider'
    start_urls = ['https://data.gov.il/dataset/covid-19']

    def parse(self, response):
        for title in response.css('.oxy-post-title'):
            yield {'title': title.css('::text').get()}

        for next_page in response.css('a.next'):
            yield response.follow(next_page, self.parse)
