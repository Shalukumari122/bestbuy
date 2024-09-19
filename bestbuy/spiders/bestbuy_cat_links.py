import scrapy
import pandas as pd
from scrapy.cmdline import execute

from bestbuy.items import  BestCatLinksItem


class BestbuyCatLinksSpider(scrapy.Spider):
    name = "bestbuy_cats_links"
    # allowed_domains = ["google.com"]
    # start_urls = ["https://google.com"]

    def __init__(self, *args, **kwargs):
        super(BestbuyCatLinksSpider, self).__init__(*args, **kwargs)
        # Load the states data once during initialization
        self.states_df = pd.read_excel('C:\\Shalu\\LiveProjects\\bestbuy\\input\\states.xlsx')

    def start_requests(self) :


        cookies = {
            'CTT': 'ed49dc5bc4389de13ba84445999059ab',
            'bby_rdp': 'l',
            '_abck': '9A87CE0F9E8CB7E1942809E75A5272B4~-1~YAAQFmfRF/SSnMSRAQAAMjNoAwyc4zI3CnSwi1LkuwOCYqQslJWJQ1v5uWzRfQr38cMxW8NrHKJgwvgrBfooD2sMjiYKM/eBxPJL7Z0WLyvl0M1lIPXkIZEgUhfmO2mduF/0/UbZ3yUV46dtY/wdjy6qjS8H3vkha+due0GSRiIojD5XKwqE8skFzd9M3uFU1Cm6KCrHsu902fYZv4Q3bGIn6oxBRqA+Euf3aBISzsGs1FzQbofvqk405UVWgGXpeULdh5RmCE1wu24PUjY70hbNWNM/nB5qI047z7lfqN6kllKRqGb3aS5UCiXfWv/AuNBotiYgga+Vvry2hv/D8VJJ0T5aMqJtTID0nq/B8DcxjvzzZuy+nWFqaLmXlkxavqFPsbmQkjOAMyREYFledp03giefs4FPdQX8Otbjz4x86T5q0G9hR2LjqyZ+YTVADtof/jvLg941u8kjEAgPbeE+QA==~-1~-1~-1',
            'SID': '0b426f2e-1108-48d6-a432-17cbede7a4de',
            'bm_sz': '0BF901363C236ED4032915130F3849DD~YAAQFmfRF64CsMSRAQAAD48uBBmg8fznA9JJXeophnle6DgUuq+sja5l3aZtyHOz5oWjINMADj26nxF7o/nVAewItZ545MijQkQjh97hAfO9+AqswVDhc/huKr6zYM6me1twKohHR0dnaAQaAZPawFrYO9cdwb1IdjavhHLrMKDBuqBXh2s+YGN+4Sr3ciwQCBMQLv00gAvbdgpIGJi2R94DdTJqC9YsHC35/bM54qBWFn0+gwuCF7NKoX3YO2DOSDtzXZNVifPlwMqIxlppD/Ah7VK0yeogyeXr6arlmxSfvlEF8r8laFdeo7d2VlkAyXdoBW5U/Q1NkhUSR1xkKo3UpDpu9jGEqBO6YR9bJyOL2F0sej1drutHockvdvHrpmJElrrZlwuwXf9KQmYfVITnhQmTInqjvLfzP6Rn~3621170~3422274',
            '__cf_bm': 'saZwDNTBNoMCwUZfzKoMJG_Mjduq4cYJcYGvidXr530-1726655361-1.0.1.1-SN6UiOBpS_EhCjLSoLXrRjbXn.b3ixvXgbEe6zFWKLLmKdUTNllZ9k6IVHylS.R3GeK7J3bkE8UAx9GqwXXJVQ',
        }

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            # 'cookie': 'CTT=ed49dc5bc4389de13ba84445999059ab; bby_rdp=l; _abck=9A87CE0F9E8CB7E1942809E75A5272B4~-1~YAAQFmfRF/SSnMSRAQAAMjNoAwyc4zI3CnSwi1LkuwOCYqQslJWJQ1v5uWzRfQr38cMxW8NrHKJgwvgrBfooD2sMjiYKM/eBxPJL7Z0WLyvl0M1lIPXkIZEgUhfmO2mduF/0/UbZ3yUV46dtY/wdjy6qjS8H3vkha+due0GSRiIojD5XKwqE8skFzd9M3uFU1Cm6KCrHsu902fYZv4Q3bGIn6oxBRqA+Euf3aBISzsGs1FzQbofvqk405UVWgGXpeULdh5RmCE1wu24PUjY70hbNWNM/nB5qI047z7lfqN6kllKRqGb3aS5UCiXfWv/AuNBotiYgga+Vvry2hv/D8VJJ0T5aMqJtTID0nq/B8DcxjvzzZuy+nWFqaLmXlkxavqFPsbmQkjOAMyREYFledp03giefs4FPdQX8Otbjz4x86T5q0G9hR2LjqyZ+YTVADtof/jvLg941u8kjEAgPbeE+QA==~-1~-1~-1; SID=0b426f2e-1108-48d6-a432-17cbede7a4de; bm_sz=0BF901363C236ED4032915130F3849DD~YAAQFmfRF64CsMSRAQAAD48uBBmg8fznA9JJXeophnle6DgUuq+sja5l3aZtyHOz5oWjINMADj26nxF7o/nVAewItZ545MijQkQjh97hAfO9+AqswVDhc/huKr6zYM6me1twKohHR0dnaAQaAZPawFrYO9cdwb1IdjavhHLrMKDBuqBXh2s+YGN+4Sr3ciwQCBMQLv00gAvbdgpIGJi2R94DdTJqC9YsHC35/bM54qBWFn0+gwuCF7NKoX3YO2DOSDtzXZNVifPlwMqIxlppD/Ah7VK0yeogyeXr6arlmxSfvlEF8r8laFdeo7d2VlkAyXdoBW5U/Q1NkhUSR1xkKo3UpDpu9jGEqBO6YR9bJyOL2F0sej1drutHockvdvHrpmJElrrZlwuwXf9KQmYfVITnhQmTInqjvLfzP6Rn~3621170~3422274; __cf_bm=saZwDNTBNoMCwUZfzKoMJG_Mjduq4cYJcYGvidXr530-1726655361-1.0.1.1-SN6UiOBpS_EhCjLSoLXrRjbXn.b3ixvXgbEe6zFWKLLmKdUTNllZ9k6IVHylS.R3GeK7J3bkE8UAx9GqwXXJVQ',
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

        yield scrapy.Request(url='https://stores.bestbuy.com/index.html', cookies=cookies, headers=headers,
                             callback=self.parse)

    def parse(self, response):

        item = BestCatLinksItem()
        for index, row in self.states_df.iterrows():

            state_name = row['state']
            print(state_name)

            link = response.xpath(f"//li/a[span[contains(text(), '{state_name}')]]/@href").extract_first()

            if link:
                cat_link = f'https://stores.bestbuy.com/{link}'
                print(f"Link for {state_name}: {cat_link}")
                item['cat_link'] = cat_link
                yield item

            else:
                print(f"No link found for {state_name}")


if __name__ == '__main__':
    execute('scrapy crawl bestbuy_cats_links'.split())
