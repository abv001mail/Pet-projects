"""Парсинг сайта кинозал, сохранение названий фильмов и сериалов с рейнингом IMDb."""

import logging
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, filename='serial.log', filemode='w')

categores_dict = {
    46: 'Сериал - Буржуйский',
    1002: 'Все фильмы',
    1003: 'Все мульты',
    45: 'Сериал - Русский',
}
error_page = []


def get_html(url: str):
    """Чтение страницы сайта.

    Args:
        url: адрес страницы сайта

    Returns:
        return: Возвращает страницу сайта
    """
    # url_proxy = ''
    # url_proxy = '163.172.31.44' + ':80'
    # url_proxy = '203.150.243.80' + ':3128'
    # url_proxy = 'http://' + '3.129.155.163' + ':80'
    # url_proxy = 'http://' + '20.111.54.16' + ':8123'
    # url_proxy = 'http://' + '24.199.106.239' + ':3128'

    # print(f'Парсим {url}') #Прокси {url_proxy}
    # </td></tr><tr><td colspan="5"><span class="bulet"></span>
    # Найдено 1885  раздач
    # </td></tr></table></div>
    for _ in range(3):  # 3 попытки прочитать страницу
        result = requests.get(url)  # , verify=False)#, proxies={'https' : url_proxy})
        if result.status_code == 200:
            break
        print(f'ОШИБКА чтения страницы - {result.status_code} - {url}')
        error_page.append(url)
        logging.error(url)
        logging.error(result)
    return result


def get_count_pages(soup) -> int:
    """Получение количества страниц в таблице с фильмами.

    Args:
        soup: первая страница таблицы

    Returns:
        return (int): Возвращает число страниц в таблице ответа на запрос
    """
    # Извлекаем число из строчки:
    # </td></tr><tr><td colspan="5"><span class="bulet"></span>
    # Найдено 1885  раздач</td></tr></table></div>
    count_page = 0
    str_count_page = soup.find('td', colspan='5')
    if str_count_page is not None:
        count_page = int(str_count_page.text.split()[1])  # на странице максимум 50 записей
    return count_page


def write_page_pd(soup: str, year: int) -> pd.DataFrame:
    """Возвращает ОДНУ страницу таблицы с исходными данными из прочитанной с сайта страницы.

    3 поля. Строка, ссылка на страницу фильма, год по фильтру поиска.

    Args:
        soup: первая страница таблицы
        year (int): за какой год смотрим фильмы

    Returns:
        return (pd.DataFrame): Возвращает ОДНУ страницу таблицы со списком фильмов в сыром виде.
        Строка таблицы, ссылка на карточку фильма, год.
    """

    write_row_pd = lambda row, year: [row.text, 'https://kinozal.tv' + row.get('href'), year]
    df_work = pd.DataFrame(columns=['Строка', 'Ссылка', 'Год'])
    table = soup.find_all('tr', class_='bg')
    for row in table:
        df_work.loc[len(df_work.index)] = write_row_pd(row.find('a'), year)
    return df_work


def get_soup(url: str) -> str:
    """Возвращает страницу сайта с таблицей или фильмом."""
    return BeautifulSoup(get_html(url).text, 'lxml')


def write_csv_xlsx(df_work, file_name: str):
    """Записывает файл данных csv и файл xls для просмотра."""
    df_work.to_csv(f'{file_name}.csv', sep='^', index=False)
    df_work.to_excel(f'{file_name}.xlsx', index=False)
    return 0


def read_site(category: int, years: int):
    """Парсит таблицу с сылками на все фильмы по фильтру с сайта в DataFrame."""
    df_work = pd.DataFrame(columns=['Строка', 'Ссылка', 'Год'])
    for year in years:
        format_video = [0]  # все форматы

        # Читаем первую страницу
        soup = get_soup(f'https://kinozal.tv/browse.php?s=&g=0&c={category}&v=0&d={year}')

        # Определяем количество страниц
        count_pages = get_count_pages(soup)
        if not count_pages:
            print('Не нашли количество страниц в запросе или нет раздач')
            continue
        count_pages = count_pages // 50  # количество строк на одной странице таблицы
        print(f'{categores_dict[category]}: год {year} - количество страниц = {count_pages}')

        # Если количество страниц больше 100, читаем частями
        if count_pages > 100:
            format_video = range(1, 8)  # 1, 8

        # Читаем все страницы и записываем их в рабочий df. ['Строка', 'Ссылка', 'Год']
        for number_format in format_video:
            print(f'Записываем с сайта {categores_dict[category]} страницы:')
            for page in range(10):  # count_pages + 1
                time.sleep(1)
                soup = get_soup(
                    f'https://kinozal.tv/browse.php?c={category}&v={number_format}&d={year}&page={page}'
                )
                df_temp = write_page_pd(soup, year)
                if not len(df_temp):
                    print(soup)
                    print('****************************')
                    print(df_temp)
                    break
                df_work = pd.concat([df_work, df_temp], ignore_index=True)
                print(f'{page}', end=' ')
                write_csv_xlsx(df_work, f'df_work_{categores_dict[category]}')
            print('')

    return df_work


def fill_df(df_work):
    """Заполняет DataFrame данными о фильмах из исходной таблицы и возвращает предварительную таблицу.

    Args:
        df_work: DataFrame с сырыми данными

    Returns:
        pre_df: DataFrame с предварительными данными
    """
    pre_df = pd.DataFrame(columns=[
        'Название',
        'Оригинальное название',
        'Год',
        'Ссылка',
        'Строка',
        'Год из названия',
    ])
    table = [[], [], []]
    for row in df_work['Строка'].values:
        row_list = row.split(' / ')
        if len(row_list) > 6:
            print(f'АНОМАЛИЯ - {row_list}')
            logging.error(f'АНОМАЛИЯ - {row_list}')
        for i in range(3):
            table[i].append(row_list[i])

    # temp_df = pd.DataFrame({
    #     'Название': table[0],
    #     'Оригинальное название': table[1],
    #     'Год из названия': table[2],
    # })
    pre_df[['Год', 'Ссылка', 'Строка']] = df_work[['Год', 'Ссылка', 'Строка']]
    pre_df[['Название', 'Оригинальное название', 'Год из названия']] = pd.DataFrame({
        'Название': table[0],
        'Оригинальное название': table[1],
        'Год из названия': table[2],
    })
        #temp_df[['Название', 'Оригинальное название', 'Год из названия']]
    pre_df['Фильтр'] = pre_df['Оригинальное название'] + '_' + pre_df['Год'].astype('str')
    return pre_df


def read_page_film(soup):
    """Читает карточку фильма и возвращает значимые поля."""
    df_film = pd.DataFrame(columns=[
        'Страница IMDb',
        'Рейтинг IMDb',
        'Проголосовало',
        'Страна',
        'Жанр',
        'Выпущено',
        'О фильме',
    ])
    film = soup.find('div', class_='content').find_all('li')
    for li_tag in film:
        if 'IMDb' in li_tag.text:
            print(li_tag.text[:4], li_tag.text[4:], li_tag.find('a').get('href'))
            break
    film1 = soup.find_all('div', class_='bx1 justify')  # .find_all("b")
    for _ in film1:
        for i in _.text.split('\n'):
            if 'Жанр' in i:
                genre_full = i[5:].strip()
                genre = genre_full.split(',')[0]
            if 'Выпущено' in i:
                izd = i[9:].strip().split(',')[0]
            if 'О фильме' in i:
                anot = i[9:].strip()
    print(genre, izd, anot, sep='\n')

    # print(soup)
    pass
    # for row in table:
    #     df_work.loc[len(df_work.index)] = write_row_pd(row.find('a'), year)
    # return df_work


def read_films(pre_df):
    """По предварительной таблице формирует окончательную и возвращает ее."""
    df = pre_df.copy()
    for url in df['Ссылка'].values:
        # print(url)
        soup = get_soup(url)
        read_page_film(soup)
    return df


def main():
    """Парсинг сайта кинозал и сохранение данных о фильмах и сериалах
    с рейтингом взятым с сайта IMDb.
    """
    # Сделать update сохраненной базы фильмов/сериалов/мультиков
    categores = [46]  # 1002, 46, 45, 1003] # Номер категории - фильм/сериал/мультик
    CATEGORY = 46
    # categores_dict = {46 : 'Сериал - Буржуйский', 1002 : 'Все фильмы'}
    years = range(2023, 2021, -1)  # За какие года читать данные
    READ_LOCAL = False

    # df = pd.DataFrame(columns=["Название", "Год", "Жанр", "Рейтинг IMDB",
    # "Оригинальное название", "Ссылка", "Строка"])

    """
    https://kinozal.tv/browse.php?s=&g=0&c=1001&v=1&d=2018&w=0&t=0&f=0
    c - категория, v - формат, d - год. | g - где искать,
    Не нужные - s - строка поиска, w,t,f - фильтры поиска
    "0" : Все разделы
    "1001" : Все сериалы
    "1002" : Все фильмы
    "1003" : Все мульты
    "1006" : Шоу, концерты, спорт
    "1004" : Вся музыка
    "45" : Сериал - Русский
    "46" : Сериал - Буржуйский

    v - формат
    "0" : Все форматы
    "3" : |- Рипы HD(1080|720)
    "1" : |- Рипы DVD и BD(HD)
    "4" : |- HD Blu-Ray и Remux
    "2" : |- DVD-5 и DVD-9
    "5" : |- Рипы TV<
    "6" : |- 3D
    "7" : |- 4K
    """

    # Парсинг сайта или чтение данных из локального файла csv
    if READ_LOCAL:
        print('Читаем локальный архив')
        df_work = pd.read_csv(f'df_work_{categores_dict[CATEGORY]}.csv', sep='^')  # _DROP
        pre_df = fill_df(df_work)
    # write_csv_xlsx(pre_df, f'pre_df_fill_{categores_dict[CATEGORY]}')
    else:
        for category in categores:
            df_work = read_site(category, years)
            pre_df = fill_df(df_work)
            write_csv_xlsx(pre_df, f'pre_df_fill_{categores_dict[category]}')

    # Первоначальное удаление дубликатов
    pre_df.drop_duplicates(['Фильтр'], inplace=True)
    # write_csv_xlsx(pre_df, f'pre_df_fill_{categores_dict[CATEGORY]}_DROP')

    # Чтение карточек фильмов
    df = read_films(pre_df)
    write_csv_xlsx(df, f'df_{categores_dict[CATEGORY]}')
    pass
    return 0


if __name__ == '__main__':
    main()
