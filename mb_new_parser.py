import requests
import re
import time
import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


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
