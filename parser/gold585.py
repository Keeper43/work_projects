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

def full_catalog(url, prods): # функция для прохода по страницам каталога и сбора данных о товаре. Принимает на вход ссылку на каталог и количество изделий.
    homepage = 'https://zoloto585.ru' # для формирования ссылки
    links = [] # список, в который будет помещаться ссылка на товар и иная информация со страницы каталога
    page_not_connect = [] # список, в который будет помещаться ссылка на страницу каталога с неудачной попыткой подключения
    max_page = prods // 22 + 2 # до какой страницы будем листать каталог
    for page in tqdm(range(1, max_page)): # прохожу по всем страницам каталога
        url_page = f'{url}?page={str(page)}'
        try:
            resp = requests.get(url_page, headers=headers(), timeout=10) # попытка получить данные
            soup = BeautifulSoup(resp.text, "html.parser") 
            for quote in soup.find_all('div', {'data-sizes':True}): # прохожу по всем карточкам товаров на странице и добавляю в словарь нужные данные
                links.append({'name':' '.join(quote.find('a', {'class':'catalog-card__product-name'}).text.split()), # название товара
                              'sizes':re.findall('["][0-9]{,2}["]|["][0-9]{,2}[.][0-9]["]', quote['data-sizes']), # доступные размеры изделия, если есть
                              'link':homepage+quote.find('a')['href'], # ссылка на товар
                              'num_page':page}) # страница каталога, на которой находится товар
        except Exception as e: # добавляю в список страницу, с которой не удалось получить данные
            page_not_connect.append({
                'link': url_page,
                'err': e
            })
    return links, page_not_connect

def full_catalog_pars(links): # функция для прохода по товарам
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
            soup = BeautifulSoup(resp.text, "html.parser")
            try:
                group = [cat.text for cat in soup.find_all('span', itemprop="name")][-2] # группа товара
            except:
                group = np.nan
            
            name_params = [] # список для создания словаря по характеристикам товара. Здесь будут ключи
            for i in soup.find('div', class_="block-content").find_all('span'):
                name_params.append(' '.join(i.text.split()[:2]))
            val_params = [] # список для значений характеристик
            for i in soup.find('div', class_="block-content").find_all('div', class_=False):
                val_params.append(' '.join(i.text.split()))
            params = dict(zip(name_params, val_params)) # словарь с характеристиками товара. Далее, если есть характеристика товара, помещаю её в соответствующую переменную
            try:
                article = params['Артикул']
            except:
                article = np.nan
            try:
                metal = params['Металл']
            except:
                metal = np.nan
            try:
                proba = params['Проба']
            except:
                proba = np.nan
            try:
                weight = params['Средний вес']
            except:
                weight = np.nan
            try:
                insert = params['Вставка']
            except:
                insert = np.nan
            try:
                stones = params['Камни i']
            except:
                stones = np.nan
            try:
                dop_vstavka = params['Дополнительная вставка']
            except:
                dop_vstavka = np.nan
            try:
                design = params['Дизайн']
            except:
                design = np.nan
            try:
                type_lock = params['Замок']
            except:
                type_lock = np.nan
            try:
                color_insert = params['Цвет вставки']
            except:
                color_insert = np.nan
            try:
                cover = params['Покрытие']
            except:
                cover = np.nan
            try:
                type_chain = params['Пустотелость изделия']
            except:
                type_chain = np.nan
            
            try:
                price = float(''.join(soup.find('span', class_='price-default').text.split())) # получаю цену товара
            except:
                price = np.nan
            try:
                name_prod = soup.find('div', {'class':'product-card__title'}).text.replace('\n', '') # получаю название товара, указанное в карточке
            except:
                try:
                    name_prod = link['name'] # если нет названия в карточке, беру название, которое указано в каталоге
                except:
                    name_prod = np.nan
            data.append({'date':date, # добавление словарём всю собранную информацию в список
                         'group':group,
                         'article':article,
                         'name':name_prod,
                         'metal':metal,
                         'proba':proba,
                         'weight':weight,
                         'price':price,
                         'sizes':link['sizes'],
                         'insert':insert,
                         'dop_vstavka':dop_vstavka,
                         'stones':stones,
                         'design':design,
                         'type_lock':type_lock,
                         'cover':cover,
                         'color_insert':color_insert,
                         'type_chain':type_chain,
                         'url':link['link'],
                         'num_page':link['num_page']})
        except:
            not_connect.append(link['link']) # добавление в список неудачных попыток подключиться
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
    df_path, links_path = save_path('gold585') # передаю в скрипт название конкурента для создания пути сохранения
    links, pnc = full_catalog('https://zoloto585.ru/catalog/yuvelirnye_izdeliya/', 30000) # запуск сбора ссылок на товары
    pd.DataFrame(links).to_csv(links_path, index=False) # сохранение ссылок
    print('Сейчас', dt.datetime.now().strftime("%H:%M %d.%m.%Y")) # время окончания сбора ссылок на товары
    print(f'Количество собранных ссылок: {len(links)}\nКоличество ошибочных ссылок: {len(pnc)}') # количество удачных и неудачных попыток
    df, not_connect = full_catalog_pars(links) # запуск парсера по товарам
    print(f'Собрано товаров: {df.shape[0]}\nОшибки: {len(not_connect)}') # количество удачных и неудачных попыток
    df.to_csv(df_path, index=False) # сохранение данных в csv файл

if __name__ == '__main__':
    main()