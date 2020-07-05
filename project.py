from bs4 import BeautifulSoup, element
import requests
import datetime
import time
import csv
import pandas as pd
from os import system, name


class GosParser:
    def __init__(self, period_from, period_to, page=1, money_period=0, logging=True, interval=1, output='data', read_cards_from='cards', only44=False, start_from_card=0):
        self.base_url_start = 'https://zakupki.gov.ru/epz/order/extendedsearch/results.html?morphology=on&search-filter=Дате+размещения&pageNumber='
        self.base_url_end = '&sortDirection=false&recordsPerPage=_50&showLotsInfoHidden=false&sortBy=UPDATE_DATE&fz44=on&fz223=on&pc=on'

        # logging flag
        self.logging = logging

        if logging:
            self._clear()

        self.only44 = only44

        self.start_from_card = start_from_card

        # output filename
        self.output = output

        # date interval
        self.interval = interval

        # request headers
        self.headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
            'Connection': 'keep-alive',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Upgrade-Insecure-Requests': '1',
            'Host': 'zakupki.gov.ru',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Cookie': '_ym_uid=1590413769299894330; _ym_d=1590413769; _ym_isad=2; _ym_visorc_36706425=b'
        }

        # read cards from csv
        self.blocks = pd.read_csv(read_cards_from + '.csv', parse_dates=True)

        # dates
        self.start_period = datetime.date.fromisoformat(period_from)
        self.end_period = datetime.date.fromisoformat(period_to)
        self.current_period = datetime.date.fromisoformat(period_from)

        # money periods
        self.money = [[0, 400000], [400000, 1000000], [1000000, 5000000], [
            5000000, 20000000], [20000000, 100000000], [100000000, 1000000000], [1000000000]]
        self.current_money = money_period

        # currencies
        self.currency = {
            '$': 'usd',
            '€': 'eur',
            '₽': 'rub'
        }

        self.current_page = page

        # current link to parse
        self.current_url = self.construct_url()

    def construct_url(self):
        url_page = self._add_page()
        url_page_day = self._add_day(url_page)
        url_page_day_money = self._add_money(url_page_day)

        return url_page_day_money + '&currencyIdGeneral=-1'

    def parse_cards(self):
        # collect blocks
        while self.current_period <= self.end_period:
            while self.current_money != len(self.money) - 1:
                # make very first iteration
                page_blocks = self._collect_blocks()
                # self.blocks.extend(page_blocks['data'])
                self.current_page += 1
                self.current_url = self.construct_url()

                # read data from blocks while page has blocks
                while ((page_blocks['status'] == 'timeout') or (page_blocks['status'] == 'ok' and len(page_blocks['data']) > 0)) and self.current_page <= 20:
                    page_blocks = self._collect_blocks()
                    self.current_page += 1
                    self.current_url = self.construct_url()

                # go to next money period
                self.current_money += 1

                # reset current page
                self.current_page = 1

                # reconstruct url using new data
                self.current_url = self.construct_url()

            # increment interval days
            self.current_period += datetime.timedelta(days=self.interval)

            # reset current money
            self.current_money = 0

            # reconstruct url using new data
            self.current_url = self.construct_url()

        # read blocks (cards)
        self.blocks = pd.read_csv('cards.csv', parse_dates=True)

    def _collect_blocks(self):
        try:
            if self.logging:
                print('page:', self.current_page, '| money:',
                      self.money[self.current_money], '| date:', self.current_period)

            response = requests.get(
                self.current_url, headers=self.headers, verify=True, timeout=2)
        except:
            if self.logging:
                print('timeout')

            self.write_to_csv([{
                'date': self._create_iso_date_str(self.current_period.day, self.current_period.month, self.current_period.year),
                'page': self.current_page,
                'money': self.current_money
            }], 'gaps.csv')

            return {
                'data': [],
                'status': 'timeout'
            }

        response.encoding = 'utf-8'
        page = response.text
        soup = BeautifulSoup(page, 'html.parser')

        cards = soup.find_all(
            'div', {'class': 'registry-entry__form'})

        cards_data = []

        for card in cards:
            card_data = {}
            price = card.find_all('div', {'class': 'price-block__value'})
            link = card.find_all(
                'div', {'class': 'registry-entry__header-mid__number'})

            # collect link, id and price, also add date, page and money period
            if link and link[0] and price and price[0]:
                a = link[0].find_all('a')
                if a and a[0]:
                    card_data['id'] = a[0].contents[0].strip()[2:]
                    card_data['link'] = a[0]['href']

                price = price[0].contents[0].strip()

                try:
                    card_data['price'] = float(
                        price[:-2].replace(u'\xa0', '').replace(' ', '').replace(',', '.'))
                except:
                    continue

                if price[-1] in self.currency:
                    card_data['currency'] = self.currency[price[-1]]
                else:
                    card_data['currency'] = price[-1]

                # add current date
                card_data['date'] = self._create_iso_date_str(
                    self.current_period.day, self.current_period.month, self.current_period.year)

                # add technical data (page and money period)
                card_data['money'] = self.current_money
                card_data['page'] = self.current_page

                cards_data.append(card_data)

        if len(cards_data) > 0:
            self.write_to_csv(cards_data, 'cards.csv')

        return {
            'data': cards_data,
            'status': 'ok'
        }

    def write_to_csv(self, data, file_name):
        keys = data[0].keys()

        with open(file_name, 'a+') as output_file:
            dw = csv.DictWriter(output_file, keys)
            dw.writerows(data)

    def parse223(self, link):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': '_ym_uid=1590413769299894330; _ym_d=1590413769; _ym_isad=2; _ym_visorc_36706425=b',
            'Host': 'zakupki.gov.ru',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
        }

        lots_link = link.replace('common-info', 'lot-list')

        try:
            main = requests.get(link, headers=headers, verify=True, timeout=2)
            lots = requests.get(lots_link, headers=headers,
                                verify=True, timeout=2)
        except:
            return None

        purchase = {}

        # main page
        main.encoding = 'utf-8'
        soup_main = BeautifulSoup(main.text, 'html.parser')
        table_main = soup_main.find_all('table')
        table_customer = table_main[3]
        rows = table_customer.find_all('tr')

        for row in rows:
            name = row.find_all('td')[0].contents[0]

            if type(name) == element.Tag:
                name = name.contents[0]

            if name == 'Наименование организации':
                purchase['customer'] = row.find_all(
                    'td')[1].find('a').contents[0]

            if name == 'Место нахождения' or name == 'Адрес места нахождения':
                address = row.find_all('td')[1].contents[0]
                split_char = ','

                if len(address.split(split_char)) > 1:
                    pass
                else:
                    if len(address.split(' ')) > 1:
                        split_char = ' '
                    else:
                        return None

                try:
                    zipcode = int(address.split(split_char)[0].strip())
                except:
                    return None

                purchase['zipcode'] = zipcode
                purchase['location'] = address.split(split_char)[1].strip()

        # lots page
        lots.encoding = 'utf-8'
        soup_lots = BeautifulSoup(lots.text, 'html.parser')
        table_lots = soup_lots.find(
            'table', {'id': 'lot'})
        headers = table_lots.find_all('th')
        cells = table_lots.find_all('td')

        purchase['num_of_lots'] = 0

        for i in range(len(headers)):
            if headers[i].contents[0].strip() == 'Классификация по ОКПД2' or headers[i].contents[0].strip() == 'ОКПД2' or headers[i].contents[0].strip() == 'ОКПД 2':
                purchase['lots'] = cells[i].contents[0].strip().replace(
                    u'\xa0', ' ').split(' ')[0]
                purchase['num_of_lots'] += 1

        if purchase['num_of_lots'] == 0:
            return None

        return purchase

    def parse_links(self):
        for i in range(self.start_from_card, len(self.blocks)):
            row = self.blocks.iloc[i]
            root = 'https://zakupki.gov.ru'

            if self.logging:
                self._clear()
                print(row.id)
                print('parsed links:', str(i) + '/' + str(len(self.blocks)))

            # 223-ФЗ
            if row.link.find('https') == 0:
                if self.only44:
                    continue

                link = row.link
                parsed = self.parse223(link)

                row_dict = row.to_dict()
                del row_dict['money']
                del row_dict['page']

                if parsed:
                    res = dict(row_dict, **parsed)
                    self.write_to_csv([res], self.output + '.csv')

            # 44-ФЗ / zk504
            else:
                link = root + row.link
                parsed = self.parse44(link)

                row_dict = row.to_dict()
                del row_dict['money']
                del row_dict['page']
                row_dict['link'] = link

                if parsed:
                    res = dict(row_dict, **parsed)
                    self.write_to_csv([res], self.output + '.csv')

        if self.logging:
            self._clear()
            print('parsed links:', str(len(self.blocks)) +
                  '/' + str(len(self.blocks)))

    def parse44(self, link):
        try:
            main = requests.get(link, headers=self.headers,
                                verify=True, timeout=2)
        except:
            return None

        purchase = {}

        # page
        main.encoding = 'utf-8'
        soup = BeautifulSoup(main.text, 'html.parser')
        h2s = soup.find_all('h2')
        row_blockInfos = soup.find_all('div', {'class': 'row blockInfo'})

        contact_i = -1
        table_i = -1
        for i in range(len(h2s)):
            if h2s[i].contents[0].strip() == 'Контактная информация':
                contact_i = i

            if h2s[i].contents[0].strip() == 'Информация об объекте закупки':
                table_i = i

        titles = None
        infos = None

        if contact_i > -1:
            titles = row_blockInfos[contact_i].find_all(
                'span', {'class': 'section__title'})
            infos = row_blockInfos[contact_i].find_all(
                'span', {'class': 'section__info'})

            for i in range(len(titles)):
                if titles[i].contents[0].strip() == 'Место нахождения':
                    try:
                        zipcode = int(
                            infos[i].contents[0].strip().split(',')[1].strip())
                        location = infos[i].contents[0].strip().split(',')[
                            2].strip()
                    except:
                        return None

                    purchase['zipcode'] = zipcode
                    purchase['location'] = location

                if titles[i].contents[0].strip() == 'Организация, осуществляющая размещение' or titles[i].contents[0].strip() == 'Наименование организации':
                    purchase['customer'] = infos[i].contents[0].strip()

            if table_i > -1:
                table = row_blockInfos[table_i].find('table')

                headers = table.find_all('th')
                cells = table.find('tbody').find_all('td')

                column = -1
                for i in range(len(headers)):
                    if headers[i].contents[0].strip() == 'Код позиции':
                        column = i + 1
                        break

                rows = table.find('tbody').find_all(
                    'tr', {'class': 'tableBlock__row'}, recursive=False)

                lots = []
                for row in rows:
                    cells = row.find_all('td')

                    if len(cells) > 0 and column > -1:
                        lot = cells[column]

                        if lot.find('a'):
                            lot = lot.find('a').contents[0]
                        else:
                            lot = lot.contents[0]

                        lots.append(lot.strip().split('-')[0])

                purchase['lots'] = ', '.join(lots)
                purchase['num_of_lots'] = len(lots)
        else:
            return None

        if purchase['num_of_lots'] == 0:
            return None

        return purchase

    def _create_date_str(self, day, month, year):
        return str(day).zfill(2) + '.' + str(month).zfill(2) + '.' + str(year)

    def _create_iso_date_str(self, day, month, year):
        return str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)

    def _add_day(self, url):
        date = self._create_date_str(
            self.current_period.day, self.current_period.month, self.current_period.year)
        return url + '&publishDateFrom=' + date + '&publishDateTo=' + date

    def _add_money(self, url):
        money_period = self.money[self.current_money]

        if (len(money_period) == 2):
            return url + '&priceFromGeneral=' + str(money_period[0]) + '&priceToGeneral=' + str(money_period[1])
        else:
            return url + '&priceFromGeneral=' + str(money_period[0])

    def _add_page(self):
        return self.base_url_start + str(self.current_page) + self.base_url_end

    def _next_page(self):
        self.current_page += 1
        self.current_url = self.construct_url()

    def _clear(self):
        # windows
        if name == 'nt':
            _ = system('cls')

        # posix
        else:
            _ = system('clear')


parser = GosParser(period_from='2014-01-01',
                   period_to='2014-12-31', output='data', interval=3, only44=True, start_from_card=39669)
# parser = GosParser(period_from='2019-06-01',
#                    period_to='2019-06-07', output='data', interval=3)
# parser.parse_cards()
parser.parse_links()

# print(parser.parse223(
#     'https://zakupki.gov.ru/223/purchase/public/purchase/info/common-info.html?regNumber=31400799230'))
# print(parser.parse44(
#     'https://zakupki.gov.ru/epz/order/notice/zk44/view/common-info.html?regNumber=0303300071914000998'))
