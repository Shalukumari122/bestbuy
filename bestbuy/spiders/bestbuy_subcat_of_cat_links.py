import pymysql
import scrapy
from scrapy.cmdline import execute

from bestbuy.items import Subcat_Of_cat_LinksItem


class BestbuySubcatOfCatLinksSpider(scrapy.Spider):
    name = "bestbuy_subcat_of_cat_links"
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
        query = f"SELECT * FROM subcat_link where  status='pending' "
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            link = row[1]


            cookies = {
                'CTT': 'ed49dc5bc4389de13ba84445999059ab',
                'SID': '0b426f2e-1108-48d6-a432-17cbede7a4de',
                '_abck': '9A87CE0F9E8CB7E1942809E75A5272B4~-1~YAAQFmfRF3m38sSRAQAAQceNCAx6rlLeoB0etgmgr4rKi/6nxaagvuh328+m5ZKSLJHOsmyUxg2c/WDqLwqRbFGzdH0xpbGgCNq66+iIF0duIr0gp9i7iTlL7rAojv/TGqz+HrQch3TN7rjMZjriouJaA67uUwn3uDZTyr3mu25mdXk9MwEqvwpdAhKCb3hX8Uod6SHBzg8k4aA8Qo1c7ybxed43LYoKAGjuhWPIZH1a+H975h3QA0Acaq2LR4W8Bd0ns1ZNv51P5QjyQdpdCtrHEtjCQ2euo7j/+rqNbM6ZBsEC0iPdof87HDs6KzkpPZyYb0e8k0vFrLBjZOYeQ2Rr7mKqvscS6MBQiOiyRPJxVvllzABvJkrQln1tCRSRoXbA5QcmfxRhvlYStb1qfdjZN9PKjm4F3m8rua0FG6WNXaDnBA99xBeIhMBU/crGLjMcwqJKxiCqWG8oczbGJsohGg==~-1~-1~-1',
                'bm_sz': '1E8802F4E2E74CF211F905A555089874~YAAQFmfRF3q38sSRAQAAQceNCBm4lXLIxyMjdi+/Juj4RQ/QD9JOrjv4gmahcST7M25QD90aZtHxh6iGGr+zmkDJd5taVbY+DlLizembFwMxolHIjUPzjpllb40ZPkvKzL7xLAqnvFA6FbwhwxpYc2tPihnAtuLep4R3hKJ92Gs1D3IMmIvtcf38u17EBSi76hk0XtfhyyZJWm77JqNOqnTf8ZFPzORRA5cqezdzQZtXgHxYJXU3x/9IvdZF5InN2clMmVtBEKZttZzvHUXlVIGcKH02AYX9ENHay7wx7M7Jb4v4LFqXM4bdRi2uDJv78+FGZlz1NymaFJsfGL2z02VvvzL5Qh6uxNd8NwFYULSZeFxp12ax8eTkmUcqj4rblt13Y0GQ~4273209~3159600',
                'bby_rdp': 'l',
                '__cf_bm': 'w7oXUgmkf2bqPh_JoRBoS_XNu1u9qu7WIFXkbbTN9SU-1726724517-1.0.1.1-1AThCjl30J4rASYmz_lKk3HlOyFUNywOrO0ckdVsvmnLA9PE6nztla2Alq35g7p3R5yQSM_4XhkIQgThPcJTaw',
            }

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                # 'cookie': 'CTT=ed49dc5bc4389de13ba84445999059ab; SID=0b426f2e-1108-48d6-a432-17cbede7a4de; _abck=9A87CE0F9E8CB7E1942809E75A5272B4~-1~YAAQFmfRF3m38sSRAQAAQceNCAx6rlLeoB0etgmgr4rKi/6nxaagvuh328+m5ZKSLJHOsmyUxg2c/WDqLwqRbFGzdH0xpbGgCNq66+iIF0duIr0gp9i7iTlL7rAojv/TGqz+HrQch3TN7rjMZjriouJaA67uUwn3uDZTyr3mu25mdXk9MwEqvwpdAhKCb3hX8Uod6SHBzg8k4aA8Qo1c7ybxed43LYoKAGjuhWPIZH1a+H975h3QA0Acaq2LR4W8Bd0ns1ZNv51P5QjyQdpdCtrHEtjCQ2euo7j/+rqNbM6ZBsEC0iPdof87HDs6KzkpPZyYb0e8k0vFrLBjZOYeQ2Rr7mKqvscS6MBQiOiyRPJxVvllzABvJkrQln1tCRSRoXbA5QcmfxRhvlYStb1qfdjZN9PKjm4F3m8rua0FG6WNXaDnBA99xBeIhMBU/crGLjMcwqJKxiCqWG8oczbGJsohGg==~-1~-1~-1; bm_sz=1E8802F4E2E74CF211F905A555089874~YAAQFmfRF3q38sSRAQAAQceNCBm4lXLIxyMjdi+/Juj4RQ/QD9JOrjv4gmahcST7M25QD90aZtHxh6iGGr+zmkDJd5taVbY+DlLizembFwMxolHIjUPzjpllb40ZPkvKzL7xLAqnvFA6FbwhwxpYc2tPihnAtuLep4R3hKJ92Gs1D3IMmIvtcf38u17EBSi76hk0XtfhyyZJWm77JqNOqnTf8ZFPzORRA5cqezdzQZtXgHxYJXU3x/9IvdZF5InN2clMmVtBEKZttZzvHUXlVIGcKH02AYX9ENHay7wx7M7Jb4v4LFqXM4bdRi2uDJv78+FGZlz1NymaFJsfGL2z02VvvzL5Qh6uxNd8NwFYULSZeFxp12ax8eTkmUcqj4rblt13Y0GQ~4273209~3159600; bby_rdp=l; __cf_bm=w7oXUgmkf2bqPh_JoRBoS_XNu1u9qu7WIFXkbbTN9SU-1726724517-1.0.1.1-1AThCjl30J4rASYmz_lKk3HlOyFUNywOrO0ckdVsvmnLA9PE6nztla2Alq35g7p3R5yQSM_4XhkIQgThPcJTaw',
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

            yield scrapy.Request(url=link, cookies=cookies, headers=headers,
                                 callback=self.parse,meta={"cat_link":link})


    def parse(self, response):

        item=Subcat_Of_cat_LinksItem()
        cat_link=response.request.meta['cat_link']
        links = response.xpath('//div[@class="container my-8"]/ul/li')
        if links:
            for each_link in links:
                link = each_link.xpath('./div/div[@class="flex md:w-1/3"]/h3/a/@href').extract_first()
                link=link.replace('..','')
                link = 'https://stores.bestbuy.com' + link
                item['link'] = link
                yield item
            cat_link_value = cat_link  # Replace with the actual cat_link value

            query = '''
                                UPDATE subcat_link 
                                SET status = %s 
                                WHERE status = %s AND link = %s
                                '''
            # Parameters to update: setting status to 'done', where status is 'pending' and cat_link matches
            params = ('done', 'pending', cat_link_value)

            # Execute the query
            self.cursor.execute(query, params)
            self.conn.commit()
        else:
            query = 'INSERT INTO subcat_of_cat_link (link) VALUES (%s)'
            self.cursor.execute(query, (cat_link,))
            self.conn.commit()
            cat_link_value = cat_link  # Replace with the actual cat_link value

            query = '''
                                       UPDATE subcat_link 
                                       SET status = %s 
                                       WHERE status = %s AND link = %s
                                       '''
            # Parameters to update: setting status to 'done', where status is 'pending' and cat_link matches
            params = ('done', 'pending', cat_link_value)

            # Execute the query
            self.cursor.execute(query, params)
            self.conn.commit()

        # print(response.text)


if __name__ == '__main__':
    execute('scrapy crawl bestbuy_subcat_of_cat_links'.split())

