import csv

import scrapy
from scrapy.crawler import CrawlerProcess


URL = "https://ทองคําราคา.com/"


class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)

        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)

        for row in rows:
            print(row.css("td::text").extract())
            # print(row.xpath("td//text()").extract())

        # Write to CSV
        # YOUR CODE HERE
        with open("gold_07-05-2023.csv", "w") as f:
            writer = csv.writer(f)
            header = rows[0].css("td::text").extract()
            writer.writerow(header)

            for row in rows[1:]:
                row = row.css("td::text").extract()
                writer.writerow(row)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
