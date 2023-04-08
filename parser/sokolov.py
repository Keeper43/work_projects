import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from time import sleep # парсер должен останавливаться на паузы, чтобы не нагружать систему и не забанили адрес
from random import choice
import numpy as np
import datetime as dt
from tqdm import tqdm # прогрессбар
try: # берём headers и путь для сохранения результатов из общих для всех парсеров скриптов
    from py.parsers.headers_pars import headers
    from py.parsers.path_save_temp import save_path
except:
    from headers_pars import headers
    from path_save_temp import save_path

def full_catalog(url): # функция для прохода по страницам каталога и сбора ссылок. Получает на вход ссылку на каталог.
    resp = requests.get(url, headers=headers(), timeout=5) # получение данных первой страницы каталога
    soup = BeautifulSoup(resp.text, 'html.parser')
    homepage = 'https://sokolov.ru' # для формирования ссылки
    links = [] # список, в который будет помещаться ссылка на товар и иная информация со страницы каталога
    pages = [] # список, в который будут переданы номера страниц, доступных для перехода с первой страницы каталога
    page_not_connect = [] # список, в который будет помещаться ссылка на страницу каталога с неудачной попыткой подключения
    [pages.append(int(re.sub('\D|[.]', '0', p.text))) for p in soup.find('div', {'class':'pagination'})
     .find_all('a')] # добавление в список номеров страниц в пагинаторе первой страницы каталога
    max_page = max(pages) + 1 # сколько страниц доступно в каталоге
    for i in tqdm(range(1, max_page)): # прохожу по всем страницам каталога
        url_page = f'{url}&page={str(i)}'
        try:
            resp = requests.get(url_page, headers=headers(), timeout=10) # попытка получить данные
            soup = BeautifulSoup(resp.text, 'html.parser')
            for j in soup.find('div', {'class':'list'}).find('div', {'class': 'product-list'} # прохожу по каждой карточке со страницы каталога
                              ).find_all('a', {'class': 'sklv-product-link', 'data-product': True}):
                links.append({'link':homepage + j['href'],
                              'num_page':i}) # добавление ссылки на товар и номера страницы каталога, на которой этот товар расположен
        except Exception as e: # добавляю в список страницу, с которой не удалось получить данные
            page_not_connect.append({
                'link': url_page,
                'err': e
            })         
    return links, page_not_connect

def full_catalog_pars(links):
    sleep_time = range(10, 30) # диапазон для выбора времени малой паузы работы парсинга, чтобы не забанили
    grand_sleep = range(600, 900) # диапазон для выбора времени большой паузы работы парсинга, чтобы не забанили
    date = dt.datetime.now().date().strftime('%d.%m.%Y') # дата сбора данных
    data = [] # список для записи данных о товаре
    not_connect = [] # список для записи данных о неудачных попытках подключения
    counter = 0 # счётчик запросов на малую паузу
    grand_counter = 0 # счётчик запросов на большую паузу
    for link in tqdm(links): # прохожу по ссылкам
        try:
            resp = requests.get(link['link'], headers=headers(), timeout=10) # попытка получить данные
            soup = BeautifulSoup(resp.text, 'html.parser')
            group = soup.find_all('span', itemprop='name')[1].text # запись группы товара
            price = float(''.join(soup.find('div', {'class':'sklv-price__top price'}).text.split('р')[0].split())) # запись цены на товар
            try: # определение скидки
                discount = soup.find('div', {'class':'sklv-coupon__desc'}).text.split()[-1]
            except:
                discount = np.nan
            try: # список для создания словаря по характеристикам товара
                name = [] # список для названий характеристик, который будет использован в словаре, как ключ
                val = [] # список для значейни характеристик, который будет использован в словаре, как значение
                for ch in soup.find_all('div', {'class':'characteristics-el'}):
                    title = ch.find('h4', {'class':'characteristics-el-title'}).text
                    if 'характер' in title or 'атериал' in title:
                        for row in ch.find_all('div', {'class':'characteristics-el-row'}):
                            name.append(' '.join(row.find('div', {'class':'name'}).text.split()))
                            val.append(' '.join(row.find('div', {'class':'val'}).text.split()))
                params = dict(zip(name, val))
            except:
                params = np.nan

            try: # все доступные размеры товара
                sizes = [el['data-size'] for el in soup.find('div', {'class':'sklv-sizes__wrapper'}).find_all('button')]
            except:
                sizes = np.nan

            try: # название товара в карточке
                name_prod = soup.find('h1', itemprop='name')['data-detail-name']
            except:
                name_prod = ' '.join(soup.find('h1', {'class':'sklv-product-page-title'}).text.split('Арт')[0].split())
            try: # все вставки, которые есть
                inserts = []
                for ch in soup.find_all('div', {'class':'characteristics-el'}):
                    title = ch.find('h4', {'class':'characteristics-el-title'}).text
                    if 'азмер' not in title and 'характер' not in title and 'атериал' not in title:
                        key_ins = [' '.join(n.text.split()) for n in ch.find_all('div', {'class':'name'})]
                        val_ins = [' '.join(v.text.split()) for v in ch.find_all('div', {'class':'val'})]
                        inserts.append(dict(zip(key_ins, val_ins)))
            except:
                insert = np.nan
                    
            data.append({'date':date,
                         'group':group,
                         'name':name_prod,
                         'price':price,
                         'discount':discount,
                         'sizes':sizes,
                         'params':params,
                         'insert':inserts,
                         'url':link['link'],
                         'num_page':link['num_page']}) # запись данных в список в виде словаря
        except Exception as e: # добавление в список неудачных попыток подключиться
            not_connect.append({
                'link': link['link'],
                'err': e
            })
            continue
        if grand_counter >= 1000: # каждые 1000 запросов - большой перерыв
            sleep(choice(grand_sleep))
            counter = 0 # обнуление счётчиков
            grand_counter = 0
        else:
            grand_counter += 1 # подсчёт запросов в большоё счётчки
            counter += 1 # подсчёт запросов в малый счётчик
        if counter >= 100: # каждые 100 запросов - малый перерыв
            sleep(choice(sleep_time))
            counter = 0 # обнуление малого счётчика
        else:
            counter += 1
    df = pd.DataFrame(data) # создание датасета по собранным данным
    return df, not_connect

def main():
    df_path, links_path = save_path('sokolov') # передаю в скрипт название конкурента для создания пути сохранения
    links_catalog, page_not_connect_catalog = full_catalog('https://sokolov.ru/jewelry-catalog/?stock=Y') # запуск сбора ссылок на товары
    pd.DataFrame(links_catalog).to_csv(links_path, index=False) # сохранение ссылок
    print('Сейчас', dt.datetime.now().strftime("%H:%M %d.%m.%Y")) # время окончания сбора ссылок на товары
    print(f'Количество собранных ссылок: {len(links_catalog)}\nКоличество ошибочных ссылок: {len(page_not_connect_catalog)}') # количество удачных и неудачных попыток
    df_catalog, df_not_connect = full_catalog_pars(links_catalog) # запуск парсера по товарам
    print('Сейчас', dt.datetime.now().strftime("%H:%M %d.%m.%Y")) # время окончания сбора данных
    print(f'Количество спарсенных: {df_catalog.shape[0]}\nКоличество не спарсенных: {len(df_not_connect)}') # количество удачных и неудачных попыток
    df_catalog.to_csv(df_path, index=False) # сохранение данных в csv файл

if __name__ == '__main__':
    main()