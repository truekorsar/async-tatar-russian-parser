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

# Type annotations
LinkToSectionAsTag = bs4.element.Tag
LinksToSections = bs4.element.ResultSet
LinkToPaginationPageAsTag = bs4.element.Tag
LinksToPaginationPages = bs4.element.ResultSet
LinkToWordPageAsTag = bs4.element.Tag
LinksToWordPages = bs4.element.ResultSet

words: Dict[str, str] = {}  # Dict to place words in. Key is actual tatar word, value - translation
JSON_FILENAME = "tatar-russian.json"
CSV_FILENAME = "tatar-russian.csv"
MAX_CONCURRENT = 4500  # Maximum number of tasks 'get_word_info' working concurrently
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


async def get_section_links(session: aiohttp.ClientSession) -> LinksToSections:
    """Fetching links to sections"""
    async with session.request('get', MAIN_PAGE, verify_ssl=False) as resp:
        main_html = await resp.text()
    sections = bs4.BeautifulSoup(main_html, PARSER).find('div', class_='contents-wrap').find_all('a')
    return sections


async def get_pagination_pages_links(session: aiohttp.ClientSession,
                                     section: LinkToSectionAsTag) -> LinksToPaginationPages:
    """Fetching links to pagination pages from the section page"""
    async with session.request('get', MAIN_PAGE + section.attrs['href'], verify_ssl=False) as resp:
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
    return pagination_pages


async def get_word_links(session: aiohttp.ClientSession,
                         pagination_page: LinkToSectionAsTag) -> LinksToWordPages:
    """Fetching links to words from the pagination page"""
    async with session.request('get', MAIN_PAGE + pagination_page.attrs['href'], verify_ssl=False) as resp:
        pagination_page_html = await resp.text()
    word_links = bs4.BeautifulSoup(pagination_page_html, PARSER).find('div', 'terms-wrap').find_all('a')
    return word_links


async def get_word_info(session: aiohttp.ClientSession, word: LinkToWordPageAsTag) -> None:
    """Fetching tatar word and it's translation from the link to word and write this info to dict 'words'"""
    async with session.request('get', word.attrs['href'], verify_ssl=False) as resp:
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
        async with aiohttp.ClientSession() as session:
            print("Collecting links to sections...")
            sections = await get_section_links(session)

            print("Collecting links to pagination pages...")
            tasks = [get_pagination_pages_links(session, section) for section in sections]
            all_pagination_pages = await asyncio.gather(*tasks)

            print("Collecting links to words (2-3 min)...")
            tasks = []
            for section in all_pagination_pages:
                tasks.extend([get_word_links(session, pagination_page) for pagination_page in section])
            all_word_links = await asyncio.gather(*tasks)

            print("Collecting words (30 - 40 min)...\n\n")

            #  Launching tasks 'get_word_info' in parts, as otherwise server may close the connection
            tasks = []
            chunk_start_position = 0
            for pagination_page in all_word_links:
                tasks.extend([get_word_info(session, word) for word in pagination_page])
            while chunk_start_position < len(tasks):
                await asyncio.gather(*tasks[chunk_start_position:chunk_start_position+MAX_CONCURRENT])
                chunk_start_position += MAX_CONCURRENT

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
