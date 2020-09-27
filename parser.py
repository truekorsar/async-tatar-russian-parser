"""
Scraping words from 'http://tatar_russian.academic.ru/' and writing them in csv and json files
"""
import asyncio
import aiohttp
import time
import json
import csv
import bs4
from copy import copy
from typing import Dict

words: Dict[str, str] = {}  # Dict to place words in. Key is actual tatar word, value - translation
JSON_FILENAME = "tatar-russian.json"
CSV_FILENAME = "tatar-russian.csv"
MAX_CONCURRENT = 3000  # Maximum number of tasks working concurrently
MAIN_PAGE = 'https://tatar_russian.academic.ru/'
PARSER = 'lxml'


def write_csv(filename: str) -> None:
    """Write in csv file"""
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(words.items())


def write_json(filename: str) -> None:
    """Write in json file"""
    with open(filename, "w") as file:
        json.dump(words, file)


def write_both(json_filename: str, csv_filename: str) -> None:
    """Write in both"""
    write_json(json_filename)
    write_csv(csv_filename)


async def get_section_links(session: aiohttp.ClientSession) -> bs4.element.ResultSet:
    """Fetching links to sections"""
    async with session.request('get', MAIN_PAGE, verify_ssl=False) as resp:
        main_html = await resp.text()
    sections = bs4.BeautifulSoup(main_html, PARSER).find('div', class_='contents-wrap').find_all('a')
    return sections


async def process_section(semaphore: asyncio.Semaphore,
                          session: aiohttp.ClientSession,
                          section: bs4.element.Tag) -> None:
    """Fetching links to pagination pages from the section page and launch 'process_pagination_page' tasks"""
    async with semaphore, session.request('get', MAIN_PAGE + section.attrs['href'], verify_ssl=False) as resp:
        section_html = await resp.text()
    pagination_pages = (bs4.BeautifulSoup(section_html, PARSER).
                        find('div', class_='page-nav').
                        find_all('ul')[-1].find_all('a'))
    # Building link to first pagination page
    last_page = pagination_pages[-1]
    first_page = copy(last_page)
    last_page_href = last_page.attrs['href']
    first_page.attrs['href'] = last_page_href[:last_page_href.rfind('&')]  # Actual link to first pagination page
    pagination_pages.append(first_page)
    tasks = [process_pagination_page(semaphore, session, pagination_page) for pagination_page in pagination_pages]
    await asyncio.gather(*tasks)


async def process_pagination_page(semaphore: asyncio.Semaphore,
                                  session: aiohttp.ClientSession,
                                  pagination_page: bs4.element.Tag) -> None:
    """Fetch links to words from the pagination page and launch 'process_word_link' tasks"""
    async with semaphore, session.request('get', MAIN_PAGE + pagination_page.attrs['href'], verify_ssl=False) as resp:
        pagination_page_html = await resp.text()
    word_links = bs4.BeautifulSoup(pagination_page_html, PARSER).find('div', 'terms-wrap').find_all('a')
    tasks = [process_word_link(semaphore, session, word_link) for word_link in word_links]
    await asyncio.gather(*tasks)


async def process_word_link(semaphore: asyncio.Semaphore,
                            session: aiohttp.ClientSession,
                            word: bs4.element.Tag) -> None:
    """Fetch tatar word and it's translation from the link to word and write this info to dict 'words'"""
    async with semaphore, session.request('get', word.attrs['href'], verify_ssl=False) as resp:
        word_html = await resp.text()
    tatar_word = bs4.BeautifulSoup(word_html, PARSER).find('div', id='TerminTitle').find('h1').text

    #  Building valid word translation
    lines = bs4.BeautifulSoup(word_html, PARSER).find('dd', class_='descript').find_all('div')
    translation = ""
    for line in lines:
        if line.text.endswith('.'):
            continue
        else:
            translation += line.text + '\n'
    if len(translation) > 0:
        print(tatar_word, translation)
        words[tatar_word] = translation


async def main():
    try:
        # Semaphore control needed, as otherwise server may close the connection
        semaphore = asyncio.Semaphore(value=MAX_CONCURRENT)
        async with aiohttp.ClientSession() as session:
            sections = await get_section_links(session)
            tasks = [process_section(semaphore, session, section) for section in sections]
            await asyncio.gather(*tasks)

    except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError):
        print('Error occurred, all progress will be saved. Try to reduce MAX_CONCURRENT')
    except KeyboardInterrupt:
        print('Keyboard interrupt, all progress will be saved')
    finally:
        write_both(JSON_FILENAME, CSV_FILENAME)


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print(f"Program worked {time.time() - start_time} seconds")
