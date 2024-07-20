import asyncio
import aiohttp
import os
import sqlite3
import threading
import time
from bs4 import BeautifulSoup
import logging
import random
import json
from argparse import ArgumentParser
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.patches as mpatches
import io
from contextlib import redirect_stdout
from asyncio import Semaphore

# Configure logging
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class AsyncPhilosopher(threading.Thread):
    def __init__(self, name, left_fork, right_fork, url, cycles, db_path, data_dir, progress_bar, progress_list, state_list, index, semaphore):
        threading.Thread.__init__(self, name=name)
        self.left_fork = left_fork
        self.right_fork = right_fork
        self.url = url
        self.cycles = cycles
        self.db_path = db_path
        self.data_dir = data_dir
        self.progress_bar = progress_bar
        self.progress_list = progress_list
        self.state_list = state_list
        self.index = index
        self.semaphore = semaphore
        self.init_db()
        self.ensure_data_dir_exists()

    def ensure_data_dir_exists(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logging.info(f"Data directory {self.data_dir} created.")

    def init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
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

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.perform_scraping())
        loop.close()

    async def perform_scraping(self):
        async with aiohttp.ClientSession() as session:
            for _ in range(self.cycles):
                await asyncio.sleep(random.uniform(1, 3))  # Mimic human behavior
                self.progress_bar.set_description(f'{self.name} is hungry.')
                self.state_list[self.index] = 'hungry'
                await self.dine(session)

    async def dine(self, session):
        fork1, fork2 = self.left_fork, self.right_fork
        with fork1, fork2:
            self.progress_bar.set_description(f'{self.name} starts scraping at {self.url}')
            self.state_list[self.index] = 'eating'
            await self.web_scrape(session)
            self.progress_bar.set_description(f'{self.name} finishes scraping and leaves to think.')
            self.state_list[self.index] = 'thinking'
            self.progress_bar.update(1)
            self.progress_list[self.index] += 1

    async def web_scrape(self, session):
        async with self.semaphore:
            try:
                async with session.get(self.url, headers={"User-Agent": "Mozilla/5.0"}) as response:
                    if response.status == 200:
                        text = await response.text()
                        data = await self.process_data(text, session)
                        await self.save_data(data)
                        logging.info(f'{self.name} scraped {self.url} successfully.')
                    else:
                        logging.error(f'{self.name} HTTP error: {response.status}')
            except aiohttp.ClientError as e:
                logging.error(f'Failed to scrape {self.url}: {str(e)}')

    async def process_data(self, html, session):
        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.find_all('article')
        if not articles:
            logging.info("No articles found on the page.")
        result = []
        for article in articles:
            title = article.find('h1').text if article.find('h1') else 'No title'
            date = article.find('time').text if article.find('time') else 'No date'
            content = ' '.join([p.text for p in article.find_all('p')])
            images = [await self.download_image(img['src'], session) for img in article.find_all('img') if img.get('src')]
            author = article.find('meta', attrs={'name': 'author'})['content'] if article.find('meta', attrs={'name': 'author'}) else 'Unknown'
            description = article.find('meta', attrs={'name': 'description'})['content'] if article.find('meta', attrs={'name': 'description'}) else 'No description'
            result.append((title, date, content, ','.join(images), author, description, self.url))
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

    async def save_data(self, data):
        for article in data:
            self.c.execute('INSERT INTO articles (title, date, content, images, author, description, url) VALUES (?, ?, ?, ?, ?, ?, ?)', article)
            self.conn.commit()

            # Save article data to JSON file
            file_path = os.path.join(self.data_dir, f"{article[0].replace(' ', '_').replace('/', '_')}.json")
            with open(file_path, 'w') as file:
                json.dump({
                    'title': article[0],
                    'date': article[1],
                    'content': article[2],
                    'images': article[3],
                    'author': article[4],
                    'description': article[5],
                    'url': article[6]
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
    semaphore = Semaphore(5)  # Limiting to 5 simultaneous scrapers
    args = parse_arguments()
    urls = args.urls.split(',')  # Split the comma-separated URLs
    db_path = args.db
    data_dir = args.data_dir
    forks = [threading.Lock() for _ in range(len(urls))]
    cycles = args.cycles
    philosophers = []
    progress_list = [0] * len(urls)
    state_list = ['thinking'] * len(urls)

    with tqdm(total=cycles, desc='Overall Progress', position=0) as overall_progress:
        for i, url in enumerate(urls):
            progress_bar = tqdm(total=cycles, desc=f'Philosopher {i}', position=i+1)
            philosopher = AsyncPhilosopher(f'Philosopher {i}', forks[i % len(forks)], forks[(i + 1) % len(forks)], url.strip(), cycles, db_path, data_dir, progress_bar, progress_list, state_list, i, semaphore)
            philosophers.append(philosopher)
            philosopher.start()

        fig, ax = plt.subplots()
        ax.set_xlim(0, cycles)
        ax.set_ylim(-0.5, len(urls) - 0.5)
        bars = ax.barh(range(len(urls)), progress_list, color='blue')
        state_colors = {'thinking': 'blue', 'hungry': 'orange', 'eating': 'green'}

        def update(*args):
            for bar, val, state in zip(bars, progress_list, state_list):
                bar.set_width(val)
                bar.set_color(state_colors[state])
            return bars

        # Suppress the FuncAnimation message
        f = io.StringIO()
        with redirect_stdout(f):
            ani = animation.FuncAnimation(fig, update, interval=100, cache_frame_data=False)

        plt.xlabel('Progress')
        plt.ylabel('Philosophers')
        plt.title('Dining Philosophers Web Scraping Progress')

        # Create legend
        thinking_patch = mpatches.Patch(color='blue', label='Thinking')
        hungry_patch = mpatches.Patch(color='orange', label='Hungry')
        eating_patch = mpatches.Patch(color='green', label='Eating')
        plt.legend(handles=[thinking_patch, hungry_patch, eating_patch])

        plt.show()

        for philosopher in philosophers:
            philosopher.join()
            overall_progress.update(1)

if __name__ == "__main__":
    main()
