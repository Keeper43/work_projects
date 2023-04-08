#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from time import sleep
from random import choice
import numpy as np
import datetime as dt
from tqdm import tqdm
try:
    from py.parsers.headers_pars import headers
    from py.parsers.path_save_temp import save_path
except:
    from headers_pars import headers
    from path_save_temp import save_path

def full_catalog(url, prods):
    homepage = 'https://zoloto585.ru'
    pagen = '?page='
    links = []
    page_not_connect = []
    max_page = prods // 22 + 2
    for i in tqdm(range(1, max_page)):
        try:
            resp = requests.get(url+pagen+str(i), headers=headers(), timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            for quote in soup.find_all('div', {'data-sizes':True}):
                links.append({'name':' '.join(quote.find('a', {'class':'catalog-card__product-name'}).text.split()),
                              'sizes':re.findall('["][0-9]{,2}["]|["][0-9]{,2}[.][0-9]["]', quote['data-sizes']),
                              'link':homepage+quote.find('a')['href'],
                              'num_page':i})
        except:
            page_not_connect.append(url+pagen+str(i))
            continue
    return links, page_not_connect

def full_catalog_pars(links):
    sleep_time = range(10, 30)
    grand_sleep = range(600, 900)
    date = dt.datetime.now().date().strftime('%d.%m.%Y')
    data = []
    not_connect = []
    counter = 0
    grand_counter = 0
    for link in tqdm(links):
        try:
            resp = requests.get(link['link'], headers=headers(), timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            try:
                group = [cat.text for cat in soup.find_all('span', itemprop="name")][-2]
            except:
                group = np.nan
            
            name_params = []
            for i in soup.find('div', class_="block-content").find_all('span'):
                name_params.append(' '.join(i.text.split()[:2]))
            val_params = []
            for i in soup.find('div', class_="block-content").find_all('div', class_=False):
                val_params.append(' '.join(i.text.split()))
            params = dict(zip(name_params, val_params))
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
                weight = float(soup.find_all('span', {'class':'weight-span'})[2].find('span').text.split()[0])
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
                price = float(''.join(soup.find('span', {'class':'price-default'}).text.split()[:2]))
            except:
                price = np.nan
            try:
                name_prod = soup.find('div', {'class':'product-card__title'}).text.replace('\n', '')
            except:
                try:
                    name_prod = link['name']
                except:
                    name_prod = np.nan
            data.append({'date':date,
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
                         'url':link['link'],
                         'num_page':link['num_page']})
        except:
            not_connect.append(link['link'])
            continue
        if grand_counter >= 1000:
            sleep(choice(grand_sleep))
            counter = 0
            grand_counter = 0
        else:
            grand_counter += 1
        if counter >= 100:
            sleep(choice(sleep_time))
            counter = 0
        else:
            counter += 1
    df = pd.DataFrame(data)
    return df, not_connect

def main():
    print('Сейчас', dt.datetime.now().strftime("%H:%M %d.%m.%Y"))
    df_path, links_path = save_path('gold_discount')
    links, pnc = full_catalog('https://zoloto585.ru/catalog/diskont-zona/', 10000)
    pd.DataFrame(links).to_csv(links_path, index=False)
    print('Сейчас', dt.datetime.now().strftime("%H:%M %d.%m.%Y"))
    print(f'Количество собранных ссылок: {len(links)}\nКоличество ошибочных ссылок: {len(pnc)}')
    df, not_connect = full_catalog_pars(links)
    print(f'Собрано товаров: {df.shape[0]}\nОшибки: {len(not_connect)}')
    df.to_csv(df_path, index=False)

if __name__ == '__main__':
    main()