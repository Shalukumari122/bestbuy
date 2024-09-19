# Import necessary modules
import gzip  # Module for handling Gzip file compression
import io  # Module for handling I/O operations
import json  # Module for handling JSON data
import os  # Module for interacting with the operating system
import zipfile  # Module for handling ZIP file compression

from datetime import date, datetime  # Module for date and time operations

import pymysql  # Module for interacting with MySQL databases
import scrapy  # Scrapy framework for web scraping
from scrapy.cmdline import execute  # Function to run Scrapy spiders from the command line

from bestbuy.items import BestbuyDetailsItem  # Custom item class for storing scraped data

# Function to dynamically adjust the file path for saving pages
def dynamic_drive(page_save_path):
    return page_save_path

# Functions to extract specific details from the JSON-LD data and the Scrapy response
def get_Banner(data):
    """
    Extracts the banner name from the JSON-LD data.
    """
    banner = data['@graph'][0]['name']
    return banner if banner else ''

def get_name(response):
    """
    Extracts the name from the Scrapy response using XPath.
    """
    name = response.xpath('//h1/text()').extract_first()
    return name if name else ''

def get_street(data):
    """
    Extracts the street address from the JSON-LD data.
    """
    street = data['@graph'][0]['address']['streetAddress']
    return street if street else ''

def get_city(data):
    """
    Extracts the city from the JSON-LD data.
    """
    city = data['@graph'][0]['address']['addressLocality']
    return city if city else ''

def get_country(data):
    """
    Extracts the country from the JSON-LD data.
    """
    country = data['@graph'][0]['address']['addressCountry']
    return country if country else ''

def get_postalCode(data):
    """
    Extracts the postal code from the JSON-LD data.
    """
    postalCode = data['@graph'][0]['address']['postalCode']
    return postalCode if postalCode else ''

def get_Lat(data):
    """
    Extracts the latitude from the JSON-LD data.
    """
    lat = data['@graph'][0]['geo']['latitude']
    return lat if lat else ''

def get_Long(data):
    """
    Extracts the longitude from the JSON-LD data.
    """
    long = data['@graph'][0]['geo']['longitude']
    return long if long else ''

def get_phone(data):
    """
    Extracts the phone number from the JSON-LD data if it exists.
    """
    if 'telephone' in data['@graph'][0].keys():
        phone = data['@graph'][0]['telephone']
        return phone if phone else ''

def get_direction(response):
    """
    Extracts the directions URL from the Scrapy response using XPath.
    """
    direction = response.xpath(
        '//div[@class="flex flex-wrap pt-2.5"]/a[contains(text(), "Directions")]/@href'
    ).extract_first()
    return direction if direction else ''

def get_open_hours(data):
    """
    Extracts and formats the opening hours from the JSON-LD data.
    """
    opening_hours = data['@graph'][0]['openingHours']

    # Helper function to format opening hours into a readable format
    def format_opening_hours(opening_hours):
        # Mapping of day abbreviations to full day names
        day_map = {
            'Mo': 'monday',
            'Tu': 'tuesday',
            'We': 'wednesday',
            'Th': 'thursday',
            'Fr': 'friday',
            'Sa': 'saturday',
            'Su': 'sunday'
        }

        result = []

        for entry in opening_hours:
            days, hours = entry.split()
            start_time, end_time = hours.split('-')

            # Convert day abbreviations into full day names
            for day in days.split(','):
                day_name = day_map.get(day, day)
                formatted = f"{day_name}:-{start_time} - {end_time}"
                result.append(formatted)

        return " | ".join(result)

    return format_opening_hours(opening_hours)

def get_Url(data):
    """
    Extracts the URL from the JSON-LD data.
    """
    url = data['@graph'][0]['hasMap']['url']
    return url.split('#')[0]

# Spider class for scraping Best Buy details page
class BestbuyDetailsPageSpider(scrapy.Spider):
    name = "bestbuy_details_page"
    # The allowed domains and start URLs can be defined here if needed
    def __init__(self):
        """
        Initialize the spider with MySQL database connection and create necessary directories.
        """
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='bestbuy_db',
        )
        self.cursor = self.conn.cursor()

    def start_requests(self):
        """
        Generate initial requests from the URLs in the database with a 'pending' status.
        """
        query = f"SELECT * FROM subcat_of_cat_link where status='pending'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            link = row[1]

            # Define cookies and headers for the request
            cookies = {
                'CTT': 'ed49dc5bc4389de13ba84445999059ab',
                'bby_rdp': 'l',
                '__cf_bm': 'ZZmTOPKNraJQeog7Xzy7f.U9O1Ay1cfski3I9H5lalo-1726633999-1.0.1.1-NIg4VYhr6bnLxr_vwbDMfi_SQSXFDeZ5h.CjZXOGHgV_f.HEn72ib6nRfnxyyi5ONPEhOujOLZdPAgl7jHcaJw',
                '_abck': '9A87CE0F9E8CB7E1942809E75A5272B4~-1~YAAQFmfRF/SSnMSRAQAAMjNoAwyc4zI3CnSwi1LkuwOCYqQslJWJQ1v5uWzRfQr38cMxW8NrHKJgwvgrBfooD2sMjiYKM/eBxPJL7Z0WLyvl0M1lIPXkIZEgUhfmO2mduF/0/UbZ3yUV46dtY/wdjy6qjS8H3vkha+due0GSRiIojD5XKwqE8skFzd9M3uFU1Cm6KCrHsu902fYZv4Q3bGIn6oxBRqA+Euf3aBISzsGs1FzQbofvqk405UVWgGXpeULdh5RmCE1wu24PUjY70hbNWNM/nB5qI047z7lfqN6kllKRqGb3aS5UCiXfWv/AuNBotiYgga+Vvry2hv/D8VJJ0T5aMqJtTID0nq/B8DcxjvzzZuy+nWFqaLmXlkxavqFPsbmQkjOAMyREYFledp03giefs4FPdQX8Otbjz4x86T5q0G9hR2LjqyZ+YTVADtof/jvLg941u8kjEAgPbeE+QA==~-1~-1~-1',
                'bm_sz': '5277DC8494ADE945532F34C683F560E6~YAAQFmfRF/WSnMSRAQAAMjNoAxm740s3gcAQ8YTBM+Y70NzMhbXYRG+W6EBd34gVYvOhbqqun9bxFk8YrnOVIqyqF61rvY+jo+VDBrEI0DbheGHXul7e9uEhOP1s0j6M3feEupa53f2mMo6cUEe08SOzY3QUcEzpdCLt3jyrUgFsbyDQmzm3QAfeodeHubQ1AM0xUQBRxiHCClIFXyhyYcGrAhwq2eib5a7ZpYkCWyPAoNtzFCW6C/2w9KKUwxXa4r5YFFaDEWrAyj6TTU+H8dYDVgHu46OBU4kYpUXSiNue7S90vottR63E0f61ckmLseSdkKzIrS7MZ+/cMRAyFqmNCNtCm+80H+Y22gqyVXIbf1GfhK0wPdrRZ8/y4AvER9hyFgUx~4474423~4273217',
                'SID': '0b426f2e-1108-48d6-a432-17cbede7a4de',
            }

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
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

            # Start the request to the target URL
            yield scrapy.Request(
                url=link,
                cookies=cookies,
                headers=headers,
                callback=self.parse
            )

    def parse(self, response):
        """
        Parse the response and extract relevant information.
        """

        # Extract and process JSON-LD data
        data = json.loads(response.xpath("//script[@type='application/ld+json']/text()").extract_first().strip())

        # Extract data using helper functions
        name = get_name(response)  # Extract name from the page
        latitude = get_Lat(data)  # Extract latitude from JSON-LD
        longitude = get_Long(data)  # Extract longitude from JSON-LD
        street = get_street(data)  # Extract street address from JSON-LD
        city = get_city(data)  # Extract city from JSON-LD
        country = get_country(data)  # Extract country from JSON-LD
        zip_Code = get_postalCode(data)  # Extract postal code from JSON-LD
        address = f"{street}, {city}, {country}, {zip_Code}"  # Format the full address
        phone = get_phone(data)  # Extract phone number from JSON-LD
        open_Hours = get_open_hours(data)  # Extract and format opening hours from JSON-LD
        url = get_Url(data)  # Extract URL from JSON-LD
        banner = get_Banner(data)  # Extract banner from JSON-LD
        provider = f"{banner} Wholesale"  # Create provider name
        today_date = datetime.today().strftime('%d-%m-%Y')  # Get current date in dd-mm-yyyy format
        updated_Date = today_date  # Set updated date
        direction_URL = get_direction(response)  # Extract directions URL from the page

        # Create and yield the item with extracted data
        item = BestbuyDetailsItem()
        item['Name'] = name
        item['Latitude'] = latitude
        item['Longitude'] = longitude
        item['Street'] = street
        item['City'] = city
        item['Country'] = country
        item['Zip_Code'] = zip_Code
        item['Address'] = address
        item['Phone'] = phone
        item['Open Hours'] = open_Hours
        item['URL'] = url
        item['Email'] = ''  # Placeholder for email if needed
        item['Provider'] = provider
        item['Banner'] = banner
        item['Updated Date'] = updated_Date

        # Check phone availability to determine store status
        if item['Phone']:
            item['Status'] = 'Open'
        else:
            item['Status'] = 'Close'

        item['Direction URL'] = direction_URL
        url = url.split('/')[-1].replace('.html', '')  # Extract and format the URL for the file name
        page_save_path = r'C:\paga_save\live_project\pages'  # Define the path to save HTML pages
        file_name = f"{url}.html.gz"
        full_file_path = os.path.join(page_save_path, file_name)

        # Save the HTML response body as a Gzip-compressed file
        with gzip.open(full_file_path, "wb") as f:
            f.write(response.body)

        yield item

# Entry point to run the spider from the command line
if __name__ == '__main__':
    execute('scrapy crawl bestbuy_details_page'.split())
