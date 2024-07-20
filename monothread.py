import asyncio
import aiohttp
import os
import sqlite3
import logging
import random
import json
from argparse import ArgumentParser
from tqdm import tqdm
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class WebScraper:
    def __init__(self, urls, cycles, db_path, data_dir):
        self.urls = urls
        self.cycles = cycles
        self.db_path = db_path
        self.data_dir = data_dir
        self.init_db()
        self.ensure_data_dir_exists()

    def ensure_data_dir_exists(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logging.info(f"Data directory {self.data_dir} created.")

    def init_db(self):
        self.conn = sqlite3.connect(self.db_path)
        self.c = self.conn.cursor()
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                title TEXT,
                date TEXT,
                content TEXT,
                images TEXT,
                author TEXT,
                description TEXT,
                url TEXT
            )
        ''')
        self.conn.commit()

    async def perform_scraping(self):
        async with aiohttp.ClientSession() as session:
            for cycle in range(self.cycles):
                for url in tqdm(self.urls, desc=f'Cycle {cycle+1}/{self.cycles}'):
                    await asyncio.sleep(random.uniform(1, 3))  # Mimic human behavior
                    await self.scrape_url(session, url)

    async def scrape_url(self, session, url):
        try:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
                if response.status == 200:
                    text = await response.text()
                    data = await self.process_data(text, session)
                    await self.save_data(data, url)
                    logging.info(f'Scraped {url} successfully.')
                else:
                    logging.error(f'HTTP error {response.status} for URL {url}')
        except aiohttp.ClientError as e:
            logging.error(f'Failed to scrape {url}: {str(e)}')

    async def process_data(self, html, session):
        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.find_all('article')
        result = []
        for article in articles:
            title = article.find('h1').text if article.find('h1') else 'No title'
            date = article.find('time').text if article.find('time') else 'No date'
            content = ' '.join([p.text for p in article.find_all('p')])
            images = [await self.download_image(img['src'], session) for img in article.find_all('img') if img.get('src')]
            author = article.find('meta', attrs={'name': 'author'})['content'] if article.find('meta', attrs={'name': 'author'}) else 'Unknown'
            description = article.find('meta', attrs={'name': 'description'})['content'] if article.find('meta', attrs={'name': 'description'}) else 'No description'
            result.append((title, date, content, ','.join(images), author, description))
        return result

    async def download_image(self, img_url, session):
        if not img_url.startswith(('http:', 'https:')):
            img_url = f"https:{img_url}"
        filename = os.path.basename(img_url)
        path = os.path.join(self.data_dir, filename)
        async with session.get(img_url) as response:
            if response.status == 200:
                with open(path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
        return path

    async def save_data(self, data, url):
        for article in data:
            self.c.execute('INSERT INTO articles (title, date, content, images, author, description, url) VALUES (?, ?, ?, ?, ?, ?, ?)', article + (url,))
            self.conn.commit()

            # Save article data to JSON file
            file_path = os.path.join(self.data_dir, f"{article[0].replace(' ', '').replace('/', '')}.json")
            with open(file_path, 'w') as file:
                json.dump({
                    'title': article[0],
                    'date': article[1],
                    'content': article[2],
                    'images': article[3],
                    'author': article[4],
                    'description': article[5],
                    'url': url
                }, file, ensure_ascii=False, indent=4)

    def __del__(self):
        self.conn.close()

def parse_arguments():
    parser = ArgumentParser(description='Asynchronous Web Scraper')
    parser.add_argument('--urls', type=str, required=True, help='Comma-separated list of URLs to scrape')
    parser.add_argument('--cycles', type=int, default=2, help='Number of cycles to run the scraper')
    parser.add_argument('--db', type=str, default='articles.db', help='Database file path')
    parser.add_argument('--data-dir', type=str, default='data', help='Directory to save scraped data')
    args = parser.parse_args()
    return args

def main():
    args = parse_arguments()
    urls = args.urls.split(',')
    scraper = WebScraper(urls, args.cycles, args.db, args.data_dir)
    asyncio.run(scraper.perform_scraping())

if __name__ == "__main__":
    main()
