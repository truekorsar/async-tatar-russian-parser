"""
Парсинг слов с сайта 'http://tatar_russian.academic.ru/' и запись в csv и json файлы
"""
import asyncio
import aiohttp
import time
import json
import csv
from copy import copy
from bs4 import BeautifulSoup as BS

words = {}  # Словарь, куда помещаются слова с переводами
JSON_FILENAME = "tatar-russian.json"
CSV_FILENAME = "tatar-russian.csv"
MAX_CONCURRENT = 4500  # Максимальное число задач по парсингу слов, работающих одновременно (get_word_info)
MAIN_PAGE = 'https://tatar_russian.academic.ru/'  # Главна страница словаря
PARSER = 'lxml'  # Парсер


def write_csv(filename):
    """Запись в csv файл"""
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(words.items())


def write_json(filename):
    """Запись в json файл"""
    with open(filename, "w") as file:
        json.dump(words, file)


def write_both(json_filename, csv_filename):
    """Запись в оба файла"""
    write_json(json_filename)
    write_csv(csv_filename)


async def get_section_links(session):
    """Получение ссылок на разделы"""
    async with session.request('get', MAIN_PAGE, verify_ssl=False) as resp:
        main_html = await resp.text()
    sections = BS(main_html, PARSER).find('div', class_='contents-wrap').find_all('a')
    return sections


async def get_pagination_pages_links(session, section):
    """Получение ссылок на страницы пагинации со страницы раздела"""
    async with session.request('get', MAIN_PAGE + section.attrs['href'], verify_ssl=False) as resp:
        section_html = await resp.text()
    pagination_pages = BS(section_html, PARSER).\
        find('div', class_='page-nav'). \
        find_all('ul')[-1].find_all('a')
    #  Формирование первой страницы пагинации
    last_page = pagination_pages[-1]
    first_page = copy(last_page)
    last_page_href = last_page.attrs['href']
    first_page.attrs['href'] = last_page_href[:last_page_href.rfind('&')]  # Ссылка на первую страницу пагинации
    pagination_pages.append(first_page)
    return pagination_pages


async def get_word_links(session, pagination_page):
    """Получение ссылок на слова со страницы пагинации """
    async with session.request('get', MAIN_PAGE + pagination_page.attrs['href'], verify_ssl=False) as resp:
        pagination_page_html = await resp.text()
    word_links = BS(pagination_page_html, PARSER).find('div', 'terms-wrap').find_all('a')
    return word_links


async def get_word_info(session, word):
    """Получение слова и его перевода используя ссылку на это слово"""
    async with session.request('get', word.attrs['href'], verify_ssl=False) as resp:
        word_html = await resp.text()
    tatar_word = BS(word_html, PARSER).find('div', id='TerminTitle').find('h1').text

    #  Формирование перевода слова
    lines = BS(word_html, PARSER).find('dd', class_='descript').find_all('div')
    translation = ""
    for line in lines:
        if line.text.endswith('.'):
            continue
        else:
            translation += line.text + '\n'
    if len(translation) > 0:
        print(tatar_word, translation)
        words[tatar_word] = translation  # Запись в словарь


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

            #  Запускаем задачи по получению слов по частям, иначе сервер может перестать отвечать
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
