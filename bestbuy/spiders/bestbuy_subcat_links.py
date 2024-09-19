import pymysql
import scrapy
from scrapy.cmdline import execute

from bestbuy.items import SubcatLinksItem


class BestbuyCityLinksSpider(scrapy.Spider):
    name = "bestbuy_subcat_links"
    # allowed_domains = ["google.com"]
    # start_urls = ["https://google.com"]

    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='bestbuy_db',
            # cursorclass=pymysql.cursor.Dictcurosr
        )
        self.cursor = self.conn.cursor()
    def start_requests(self):
        query = f"SELECT * FROM cat_link  where status='pending' "
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            cat_link=row[1]

            cookies = {
                'CTT': 'ed49dc5bc4389de13ba84445999059ab',
                'bby_rdp': 'l',
                'SID': '0b426f2e-1108-48d6-a432-17cbede7a4de',
                '_abck': '9A87CE0F9E8CB7E1942809E75A5272B4~-1~YAAQFmfRF5W+yMSRAQAAZLn7BAz5AsL6mJFBj2x9CVk+VXNBZSHP8AGUGjqFqmxR+jCRI4sKYY3AQktQlQo28zEt5ZqRaU5lsFQtbv76hcQizFyiZHX6owCHkujOqixMMl1IurKkDVaoBBrCwii2cfO5UNm4hdd7z8wyoURF2gc777A1ABkF1lepfSzDSfm692czWpXYZtfji0n1Ex+R0kAC6GsZYVFWN+DBCPrVeOM/CIFiqZDBb2gZdCHwpzGLyTww7rtTwAYG1iWgKANaSvAVssGVZNPmdKH0xp/CFDCM2CIR10J1eL5LxSdw/eWlau+dX7e07WR2tBqroafnsct+r99u0zLWgmVR/4rqDIqEOH5x650E77emuogBX7RnXL0sCbnnFAMM2uWWE87ihbSlDtw5ucYvk3Xh8Z8MrmdgSossEif1yMRSIufVBSeCfhRILA==~-1~-1~-1',
                'bm_sz': '9D311B72444F27A61B4CB741CB093A55~YAAQFmfRF5a+yMSRAQAAZLn7BBlJk7b0m+hDcGYW21xyWnT4i4gnNOuq3UlWumElnajoYjH08XGUMhK4WyirdGqfTNqgnYewZbOhlTcoeBwoHBrrJboQ9RMARy0T37nAwCEL9pJhGDZpn+e/GhgP6sFNWJlAC6Xrid2QkfNcu9wVGqpW41lwfJPxx5iqvt4+8eBHCJVjlWu1pVxIbZCW3Dt3E5z8epPLCFsNu6bH30fbYGLTa3/eOHbP2frd+cisIHMXqxTkpnQCi/XUODxrLpqGWJL3HEy44PN+sH1HrjQJNlm+dTktQEkRTD+WwDmFYEWs8m3XDPNqi7l/ej1rT2SU5lGwmbCCVju5BQoGhwMQIlGuAH57J0ITcq5wDGiRzTLsHF+QEVfxEWUZnembyo0TH2YLoQ11+E8tVQQZ~3749176~4539959',
                '__cf_bm': 'WlBfjoPG649KjInIVegn8LQS9z9Qws4BvcbH4Xst4DA-1726661612-1.0.1.1-28FHDS_.438hL2B5decCdMX1l5AlMBCLmT3TeEsLqvZbWLW_xkyhjjA_9Ipm.G4YyNp6NFPBFUhY1lnb2nwBew',
            }

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                # 'cookie': 'CTT=ed49dc5bc4389de13ba84445999059ab; bby_rdp=l; SID=0b426f2e-1108-48d6-a432-17cbede7a4de; _abck=9A87CE0F9E8CB7E1942809E75A5272B4~-1~YAAQFmfRF5W+yMSRAQAAZLn7BAz5AsL6mJFBj2x9CVk+VXNBZSHP8AGUGjqFqmxR+jCRI4sKYY3AQktQlQo28zEt5ZqRaU5lsFQtbv76hcQizFyiZHX6owCHkujOqixMMl1IurKkDVaoBBrCwii2cfO5UNm4hdd7z8wyoURF2gc777A1ABkF1lepfSzDSfm692czWpXYZtfji0n1Ex+R0kAC6GsZYVFWN+DBCPrVeOM/CIFiqZDBb2gZdCHwpzGLyTww7rtTwAYG1iWgKANaSvAVssGVZNPmdKH0xp/CFDCM2CIR10J1eL5LxSdw/eWlau+dX7e07WR2tBqroafnsct+r99u0zLWgmVR/4rqDIqEOH5x650E77emuogBX7RnXL0sCbnnFAMM2uWWE87ihbSlDtw5ucYvk3Xh8Z8MrmdgSossEif1yMRSIufVBSeCfhRILA==~-1~-1~-1; bm_sz=9D311B72444F27A61B4CB741CB093A55~YAAQFmfRF5a+yMSRAQAAZLn7BBlJk7b0m+hDcGYW21xyWnT4i4gnNOuq3UlWumElnajoYjH08XGUMhK4WyirdGqfTNqgnYewZbOhlTcoeBwoHBrrJboQ9RMARy0T37nAwCEL9pJhGDZpn+e/GhgP6sFNWJlAC6Xrid2QkfNcu9wVGqpW41lwfJPxx5iqvt4+8eBHCJVjlWu1pVxIbZCW3Dt3E5z8epPLCFsNu6bH30fbYGLTa3/eOHbP2frd+cisIHMXqxTkpnQCi/XUODxrLpqGWJL3HEy44PN+sH1HrjQJNlm+dTktQEkRTD+WwDmFYEWs8m3XDPNqi7l/ej1rT2SU5lGwmbCCVju5BQoGhwMQIlGuAH57J0ITcq5wDGiRzTLsHF+QEVfxEWUZnembyo0TH2YLoQ11+E8tVQQZ~3749176~4539959; __cf_bm=WlBfjoPG649KjInIVegn8LQS9z9Qws4BvcbH4Xst4DA-1726661612-1.0.1.1-28FHDS_.438hL2B5decCdMX1l5AlMBCLmT3TeEsLqvZbWLW_xkyhjjA_9Ipm.G4YyNp6NFPBFUhY1lnb2nwBew',
                'pragma': 'no-cache',
                'priority': 'u=0, i',
                'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            }

            yield scrapy.Request(url=cat_link, cookies=cookies, headers=headers,callback=self.parse,meta={"cat_link":cat_link})


    def parse(self, response):
        print()
        cat_link= response.request.meta['cat_link']
        item=SubcatLinksItem()

        links=response.xpath('//div[@class="container my-8"]/ul/li')
        if links:
            for each_link in links:
                link=each_link.xpath('./a/@href').extract_first()
                if link:
                    link='https://stores.bestbuy.com/'+link
                    item['link'] = link
                    yield item

                else:
                    link = each_link.xpath('./div/div[@class="flex md:w-1/3"]/h3/a/@href').extract_first()
                    link = link.replace('..', '')
                    link = 'https://stores.bestbuy.com' + link
                    item['link']=link
                    yield item
            cat_link_value = cat_link  # Replace with the actual cat_link value

            query = '''
                    UPDATE cat_link 
                    SET status = %s 
                    WHERE status = %s AND cat_link = %s
                    '''
            # Parameters to update: setting status to 'done', where status is 'pending' and cat_link matches
            params = ('done', 'pending', cat_link_value)

            # Execute the query
            self.cursor.execute(query, params)
            self.conn.commit()
        else:
            query = 'INSERT INTO subcat_link (link) VALUES (%s)'
            self.cursor.execute(query, (cat_link,))
            self.conn.commit()
            cat_link_value = cat_link  # Replace with the actual cat_link value

            query = '''
                                UPDATE cat_link 
                                SET status = %s 
                                WHERE status = %s AND cat_link = %s
                                '''
            # Parameters to update: setting status to 'done', where status is 'pending' and cat_link matches
            params = ('done', 'pending', cat_link_value)

            # Execute the query
            self.cursor.execute(query, params)
            self.conn.commit()




if __name__ == '__main__':
    execute('scrapy crawl bestbuy_subcat_links'.split())

