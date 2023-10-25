import csv
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class MOEX_news_scraper:
    """Pageinator, moex edition:)
    Creates a generator which giver element of next page button.
    Easier way to collect data
    """

    def __init__(self, driver: webdriver = webdriver.Firefox, url: str = "") -> None:
        """initialization

        :param driver: driver like Firefox, Chrome, Edge, etc (full list -> https://selenium-python.readthedocs.io/api.html)
        :type driver: webdriver
        :param url: url with news search from moex.com.
        I know it's a little too specified class but I need it for now
        :type url: string
        """
        self.driver = driver()
        self.driver.implicitly_wait(10)
        self.driver.get(url)
        time.sleep(10)
        self.current_page_number = 1
        self.base_url = "https://www.moex.com"
        self.wait = WebDriverWait(self.driver, 10)

    def next_page(self):
        """Clicks and go on to next page

        :return: either 0 or 1 state, if next page is exist or no respectively
        :rtype: _type_
        """

        elems = self.driver.find_elements(By.CLASS_NAME, "searchAdvanced_pagingItem")
        if len(elems) <= 2:
            return 0
        for idx, elem in enumerate(elems):
            if "current" in elem.get_attribute("outerHTML"):
                pbx = idx + 1
                break
        try:
            elems[pbx].click()
            time.sleep(5)
            return 1
        except:
            return 0

        # there is always one template button, and if we have only one page,
        # there is one more for first page
        # for elem in elems:
        #     tmp_elem_attr = elem.get_attribute("outerHTML")
        #     if self.current_page_number == int(
        #         re.findall(r"<span>(\d+)</span>", tmp_elem_attr)[0]
        #     ):
        #         elem.click()
        #         self.wait.until(
        #             EC.presence_of_all_elements_located(
        #                 (By.CLASS_NAME, "searchAdvanced_pagingItem")
        #             )
        #         )
        #     if killer>0:
        #         elem.click()
        #         return 1
        # return 0

    def get_news_list(self):
        """Pull news list from current page"""

        html = self.driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        raw_news_list = soup.find_all("div", "searchAdvanced_row")
        news_list = []
        for raw_news in raw_news_list:
            # Извлекаем значения из тегов <a> и <div> (если они присутствуют)
            link_element = raw_news.find("a")
            div_element = raw_news.find("div", class_="searchAdvanced_rowCategory")

            # Проверяем, что элемент не пустой
            if link_element.has_attr("href"):
                link_url = link_element["href"]
                link_title = link_element.text
                category = div_element.text
                news_list.append(
                    {
                        "title": link_title,
                        "url": self.base_url + link_url,
                        "category": category,
                    }
                )
        return news_list

    def get_all_news_list(self, auto_close=True):
        """
        Traverse through all pages and pull all data together

        :param auto_close: closing connection after success, defaults to True
        :type auto_close: bool, optional
        """
        all_news_list = self.get_news_list()
        while self.next_page():
            all_news_list.extend(self.get_news_list())
        if auto_close:
            self.close_connection()
        return all_news_list

    def close_connection(self):
        self.driver.close()


def extract_link_info(
    url: str,
    headers: dict = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582"
    },
):
    """extract all neccessary info from news

    :param url: url -> news page
    :type url: str
    :param headers: request header, important for moex, defaults to { 'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582" }
    :type headers: _type_, optional
    :return: url, datetime, table info, and body text
    :rtype: dict
    """
    data = {}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    html_table = soup.find("table", class_="table1")
    if html_table:
        # some tables broken.
        # there are several table there column names not th tag but tr
        table_all_rows = html_table.find_all("tr")
        if html_table.find_all("th"):
            table_headers_html = html_table.find_all("th")
        else:
            table_headers_html = table_all_rows[0].find_all("td")
            table_all_rows = table_all_rows[1:]

        table_columns = [header.text for header in table_headers_html]
        table_data = [
            {table_columns[i]: cell.text for i, cell in enumerate(row.find_all("td"))}
            for row in table_all_rows
        ]
    else:
        table_data = None
    data["url"] = url
    data["datetime"] = soup.find("div", class_="news_date").text
    data["table_data"] = table_data
    data["body"] = soup.find("div", class_="news_text").text
    return data


def get_all_and_merge(df: pd.DataFrame):
    """
    Get all base info and connects it with
    info in every link

    :param df: dataframe with links
    :type df: pd.DataFrame
    """
    data = []
    for url in df.url:
        data.append(extract_link_info(url))
    adv_news_info_df = pd.DataFrame(data)
    full_news_info_df = pd.merge(df, adv_news_info_df, on="url")
    full_news_info_df.to_csv("moex_value_deviation_dataset.csv")


def save_as_csv(filename: str, data: list[dict]):
    with open(filename, mode="w", encoding="UTF-8") as csvfile:
        writter = csv.DictWriter(csvfile, data[0].keys())
        writter.writeheader()
        writter.writerows(data)


if __name__ == "__main__":
    test1 = MOEX_news_scraper(
        url="https://www.moex.com/ru/search.aspx?mode=and&sstr=%22%D0%BE%D1%82%D0%BA%D0%BB%D0%BE%D0%BD%D0%B5%D0%BD%D0%B8%D1%8F%20%D1%86%D0%B5%D0%BD%20%D0%B7%D0%B0%D1%8F%D0%B2%D0%BE%D0%BA%22&isnews=1"
    )
    all_news = test1.get_all_news_list(True)
    save_as_csv("first_time_here.csv", all_news)
