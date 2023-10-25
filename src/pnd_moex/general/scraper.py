import asyncio
from urllib.parse import urljoin

import arrow
import bs4
import httpx
import re
import pandas as pd
from fuzzywuzzy import fuzz
from isswrapper.loaders.securities import security_description
from isswrapper.util.async_helpers import fetch_all


def get_security_names(token: str) -> tuple:
    """
    Get short and full names of a security from MOEX security description page.

    :param token: The token name.
    :type token: str
    :return: A tuple containing the short and full names of the security.
    :rtype: tuple
    """
    s_df = security_description(q=token)
    s_df.set_index("name", inplace=True)
    return s_df.loc["SHORTNAME", "value"], s_df.loc["NAME", "value"]


def get_smartlab_forum_urls(tokens: list[str], confidence_threshold: int = 90) -> list:
    """
    Get forum links from SmartLab for the given tokens, if available.

    :param tokens: A list of token names to find forum threads.
    :type tokens: list[str]
    :param confidence_threshold: Similarity threshold used in the fuzzywuzzy partial_ratio method, defaults to 90.
    :type confidence_threshold: int, optional
    :return: A list of tuples containing the token and its full forum link (or None if not found).
    :rtype: list
    """
    # Get security names for further search
    sec_names = [get_security_names(token) for token in tokens]
    # Initialize client connection
    base_url = "https://smart-lab.ru/"
    with httpx.Client(base_url=base_url) as client:
        # Soup from sectors page, contains almost all companies
        response = client.get("forum/sectors")
        soup = bs4.BeautifulSoup(response.text, "html.parser")

        forum_token_endpoints = []

        for token, (s_name, l_name) in zip(tokens, sec_names):
            # Extisting endpoints
            response = client.get(f"/forum/{token}")
            if response.status_code == 200:
                forum_token_endpoints.append((token, f"/forum/{token}"))
                continue
            # Slight difference in short names
            tmp = soup.find(
                "a",
                string=lambda x: x
                and len(x) > 3
                and fuzz.partial_ratio(x.lower(), s_name.lower())
                >= confidence_threshold,
            )
            if tmp:
                forum_token_endpoints.append((token, tmp.get("href")))
                continue
            # Difference in full names
            tmp = soup.find(
                "a",
                string=lambda x: x
                and len(x) > 5
                and fuzz.partial_ratio(x.lower(), l_name.lower())
                >= confidence_threshold,
            )
            if tmp:
                forum_token_endpoints.append((token, tmp.get("href")))
                continue
            forum_token_endpoints.append((token, None))
    return forum_token_endpoints


def generate_forum_thread_page_urls(
    base_url: str,
    relative_url: str,
    last_page: dict,
    page_fstr: str = "/page{i}",
    zero_index: bool = False,
) -> list[str]:
    """
    Generate URLs for all pages of a certain forum thread.

    :param relative_url: The relative URL of the forum thread, e.g., /forum/ALBK.
    :type relative_url: str
    :return: A list of URLs to parse.
    :rtype: list[str]
    """
    with httpx.Client(base_url=base_url) as client:
        response = client.get(relative_url)
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    active_page = soup.find(**last_page)
    last_page = int(1 if active_page is None else active_page.text)
    f_index = 0 if zero_index else 1
    pages = [
        urljoin(base_url, relative_url + page_fstr.format(i=i))
        for i in range(f_index, last_page + f_index)
    ]
    return pages


def extract_smartlab_comment_data(comment: bs4.element.Tag) -> dict:
    """
    Extract valuable data from a comment and package it into a dictionary.

    :param comment: The comment element.
    :type comment: bs4.element.Tag
    :return: Extracted data as a dictionary.
    :rtype: dict
    """
    comment_body = comment.find("div", class_="cmt_body")
    return dict(
        comment_text=comment_body.find("div", class_="text").text,
        comment_score=int(
            comment_body.find("a", class_="cm_mrk").text.replace(" ", "")
        ),
        comment_datetime=arrow.get(comment_body.find("time").get("datetime")).datetime,
        user_id=comment_body.find("a", class_="a_name trader_other")
        .get("href")
        .split("/")[2],
        badges=not comment_body.find("a", class_="image_true") is None,
    )


def extract_mfd_comment_data(comment: bs4.element.Tag) -> dict:
    """
    Extract valuable data from a comment and package it into a dictionary.
    :param comment: The comment element.
    :type comment: bs4.element.Tag
    :return: Extracted data as a dictionary.
    :rtype: dict
    """
    comment_misc = comment.find("div", class_="mfd-post-remark")
    user_id = comment.find("a", class_="mfd-poster-link")
    comment_score = comment.find("span", class_="u")
    user_score = comment.find(
        "div", class_="mfd-poster-info-rating mfd-icon-profile-star"
    )
    comment_text = comment.find("div", class_="mfd-post-text")
    return dict(
        comment_text=None if comment_text is None else comment_text.text,
        comment_score=0 if comment_score is None else int(comment_score.text),
        comment_datetime=arrow.get(
            comment.find("a", class_="mfd-post-link").text, "DD.MM.YYYY HH:mm"
        ).datetime,
        comment_misc=None if comment_misc is None else comment_misc.text,
        user_id=None if user_id is None else user_id.get("href"),
        user_score=None
        if user_score is None
        else int(re.search(r"\((\d+)\)", user_score.find("a").get("title")).group(1)),
    )


def get_smartlab_forum_data(
    tokens: list[str],
    chunk_size: int = 200,
    max_connections: int = 8,
    max_keepalive_connections: int = 4,
) -> pd.DataFrame:
    """
    Fetch all comment data for the given list of tokens asynchronously.

    :param tokens: List of token names to fetch forum data for.
    :type tokens: List[str]
    :param chunk_size: Number of URLs to process in each batch, defaults to 200.
    :type chunk_size: int, optional
    :param max_connections: Maximum number of concurrent connections, defaults to 8.
    :type max_connections: int, optional
    :param max_keepalive_connections: Maximum number of keep-alive connections, defaults to 4.
    :type max_keepalive_connections: int, optional
    :return: DataFrame with all forum pages for all tokens.
    :rtype: pd.DataFrame
    """
    raw_url_df_list = []
    base_url = "https://smart-lab.ru/"
    # Token forum pages
    forum_token_endpoints = get_smartlab_forum_urls(tokens)
    for token, url in forum_token_endpoints:
        if url is None:
            continue
        # Get last page number and generates url for each page
        token_thread_page_list = generate_forum_thread_page_urls(
            base_url, url, last_page=dict(name="span", class_="page active")
        )

        url_df = pd.DataFrame(
            {
                "token": [token] * len(token_thread_page_list),
                "url": token_thread_page_list,
            }
        )
        raw_url_df_list.append(url_df)
    # Unite all tokens into one DataFrame
    final_df = pd.concat(raw_url_df_list)
    links = final_df["url"].unique().tolist()
    # Fetch all urls
    responses = asyncio.run(
        fetch_all(
            links,
            chunk_size=chunk_size,
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        )
    )
    tmp_df = pd.DataFrame(responses, columns=["body"])
    tmp_df["url"] = tmp_df["body"].apply(lambda x: str(x.url))
    # merge every page back
    return final_df.merge(tmp_df, on="url", how="inner")


def extract_comments_from_page(
    page: httpx.Response,
    comment_dict: dict = dict(name="li", attrs={"data-type": "comment"}),
    comment_extraction_func: callable = extract_smartlab_comment_data,
) -> pd.DataFrame:
    """
    Extract comments from a web page and store them in a DataFrame.

    :param page: The HTTP response object containing the page content.
    :type page: httpx.Response
    :return: A DataFrame containing the extracted comment data.
    :rtype: pd.DataFrame
    """
    soup = bs4.BeautifulSoup(page.text, "html.parser")
    comment_objs = soup.find_all(**comment_dict)
    comment_list = [comment_extraction_func(obj) for obj in comment_objs]
    return pd.DataFrame(comment_list)


def preprocess_comment_data(
    response_df: pd.DataFrame,
    token_col: str = "token",
    url_col: str = "url",
    response_col: str = "body",
) -> pd.DataFrame:
    """
    Preprocess comment data obtained from smartlab forum responses.

    :param response_df: DataFrame containing responses, which are comment pages from the smartlab forum.
    :type response_df: pd.DataFrame
    :param token_col: Column name for tokens, defaults to "token".
    :type token_col: str, optional
    :param url_col: Column name for URLs, defaults to "url".
    :type url_col: str, optional
    :param response_col: Column name for responses, defaults to "body".
    :type response_col: str, optional
    :return: Processed data from all the provided responses from the forum.
    :rtype: pd.DataFrame
    """
    processed_dfs = []
    for _, row in response_df.iterrows():
        page_df = extract_comments_from_page(row[response_col])
        page_df[token_col] = row[token_col]
        page_df[url_col] = row[url_col]
        processed_dfs.append(page_df)
    return pd.concat(processed_dfs)


if __name__ == "__main__":
    import os
    import time

    # current_path = os.getcwd()
    # datasets_folder_path = os.path.join(current_path, "datasets")
    # pnd_token_date_df = pd.read_parquet(
    #     os.path.join(datasets_folder_path, "pnd_token_date.parquet")
    # )
    # tokens = pnd_token_date_df["token"].sample(5).unique().tolist()

    # print("execution started")
    # start_time = time.time()

    # df = get_smartlab_forum_data(tokens)
    # f_df = preprocess_comment_data(df)
    # f_df.to_parquet(os.path.join(datasets_folder_path, "smartlab_forum_data.parquet"))

    # print(f"time consumed {time.time() - start_time} seconds")
    # print(f_df.head())

    