import asyncio
import httpx
import numpy as np
from isswrapper.util.async_helpers import fetch_data, safe_fetch
from isswrapper.util.helpers import preprocess_site_news_data, request_cursor
import nest_asyncio
import pandas as pd

nest_asyncio.apply()


async def __fetch_all_news(max_connections: int = 10):
    """
    Didn't work properly

    :param max_connections: _description_, defaults to 10
    :type max_connections: int, optional
    :return: _description_
    :rtype: _type_
    """
    base_url = "https://iss.moex.com/"
    endpoints = [
        "iss/sitenews.json?start={0}&lang=ru".format(i) for i in np.arange(0, 10000, 50)
    ]
    responses = []
    async with httpx.AsyncClient(
        base_url=base_url, limits=httpx.Limits(max_connections=max_connections)
    ) as client:
        for endpoint in endpoints:
            response = await client.get(endpoint)
            responses.append(response)
    return responses


async def fetch_multiple_urls(
    base_url, endpoint_format: str, url_variables: list, max_connections: int = 10
):
    endpoints = [endpoint_format.format(var) for var in url_variables]

    async with httpx.AsyncClient(
        base_url=base_url,
        limits=httpx.Limits(max_connections=max_connections),
        timeout=httpx.Timeout(30.0),
    ) as client:
        valid_responses = []
        for endpoint in endpoints:
            try:
                response = await client.get(endpoint)
                if response.status_code == httpx.codes.OK:
                    valid_responses.append(response)
                    # print(f"Fetched {endpoint} successfully")
                else:
                    print(
                        f"Request failed with status code {response.status_code}, skipping..."
                    )
            except Exception as e:
                print(f"Error fetching {endpoint}: {e}")
                await asyncio.sleep(5)
    return valid_responses

    # requests = [client.get(endpoint) for endpoint in endpoints]
    # responses = await asyncio.gather(*requests)
    # valid_responses = []
    # for response in responses:
    #     if response.status_code == httpx.codes.OK:
    #         valid_responses.append(response)
    #     else:
    #         print(
    #             f"Request failed with status code {response.status_code}, skipping..."
    #         )


async def load_sitenews_range(limit: int = -1, max_connections: int = 10):
    """
    Asynchronously load news with a specified limit.

    :param limit: Number of news to load, if negative, load all news.
    :type limit: int, optional
    :param max_connections: Maximum number of simultaneous connections, defaults to 10
    :type max_connections: int, optional
    :return: List of asynchronous responses from servers.
    :rtype: list
    """
    base_url = "https://iss.moex.com/"
    endpoint_format = "iss/sitenews.json?start={0}&lang=ru"
    cursor = request_cursor("https://iss.moex.com/iss/sitenews.json", "sitenews")

    if limit < 0:
        limit = cursor["total"]

    url_vars = np.arange(0, limit, cursor["step"])
    responses = await fetch_multiple_urls(
        base_url, endpoint_format, url_vars, max_connections
    )
    return responses


async def load_sitenews_body(news_ids: list, max_connections: int = 10):
    """
    Asynchronously load the content of news based on their IDs.

    :param news_ids: List of news IDs.
    :type news_ids: list
    :param max_connections: Maximum number of simultaneous connections, defaults to 10
    :type max_connections: int, optional
    :return: List of asynchronous responses from servers.
    :rtype: list
    """
    base_url = "https://iss.moex.com/"
    endpoint_format = "iss/sitenews/{0}.json"
    responses = await fetch_multiple_urls(
        base_url, endpoint_format, news_ids, max_connections
    )
    return responses


async def load_all_sitenews(max_connections: int = 10):
    print("Loading site news...")

    # Load site news metadata
    sitenews_list = await load_sitenews_range(limit=-1, max_connections=max_connections)

    # Preprocess site news data
    sitenews_list_df = pd.DataFrame(
        preprocess_site_news_data([page.json() for page in sitenews_list])
    )

    # Extract news IDs for loading bodies
    news_ids = sitenews_list_df["id"].unique().tolist()

    print("Loaded site news. Now loading news bodies...")

    # Load site news bodies
    sitenews_bodies_list = await load_sitenews_body(news_ids)

    print("Loaded news bodies. Now combining data...")

    # Preprocess site news bodies data
    news_body_df = pd.DataFrame(
        preprocess_site_news_data(
            [body.json() for body in sitenews_bodies_list], name="content"
        )
    )

    # Merge site news metadata and bodies data
    final_df = pd.merge(
        sitenews_list_df, news_body_df[["id", "body"]], on="id", how="left"
    )

    print("Data processing complete.")

    return final_df


def run_async(func, *args, **kwargs):
    return asyncio.run(func(*args, **kwargs))


async def __fetch_sitenews_batched(
    n: int = 1000,
    base_url: str = "https://iss.moex.com/iss",
    endpoint: str = "/sitenews.json",
    batch_size: int = 50,
    n_semaphore: int = 5,
) -> list:
    """
    Didn't work properly
    Fetches site news data asynchronously in batches from a specified API endpoint.


    :param n: The total number of records to fetch. If negative or zero, no data will be fetched.
    :type n: int, optional
    :param base_url: The base URL of the API to connect to.
    :type base_url: str, optional
    :param endpoint: The API endpoint to fetch site news data from, e.g., "/sitenews.json".
    :type endpoint: str, optional
    :param batch_size: The size of each batch for fetching data in parallel. For sitenews defaults to 50, other option don't work for now
    :type batch_size: int, optional
    :param n_semaphore: The limit of concurrency coroutines.
    :type n_semaphore: int, optional
    :return: A list of dictionaries containing the fetched site news data.
    :rtype: list
    """
    if n <= 0:
        return []
    sem = asyncio.Semaphore(n_semaphore)
    tasks = [
        safe_fetch(
            fetch_data,
            sem,
            base_url=base_url,
            endpoint=endpoint,
            params={"start": st, "lang": "ru"},
        )
        for st in np.arange(0, n, batch_size)
    ]
    results = await asyncio.gather(*tasks)

    return results


async def __fetch_sitenews_body(
    news_ids: int,
    n_semaphore: int = 5,
):
    """
    Didn't work properly


    :param news_ids: _description_
    :type news_ids: int
    :param n_semaphore: _description_, defaults to 5
    :type n_semaphore: int, optional
    :return: _description_
    :rtype: _type_
    """
    if not news_ids:
        return []
    sem = asyncio.Semaphore(n_semaphore)
    tasks = [
        safe_fetch(
            fetch_data,
            sem,
            base_url="https://iss.moex.com/iss",
            endpoint="/sitenews/{0}.json".format(id),
        )
        for id in news_ids
    ]
    results = await asyncio.gather(*tasks)
    return results


def __load_all_news_(n_semaphore: int = 5, load_content_df: bool = False) -> list:
    """
    Didn't work properly

    Load all news from https://iss.moex.com/iss/sitenews.json

    :param run: if true creates event loop using asyncio.run(), defaults to False
    :type run: bool, optional
    :return: all pages of news
    :rtype: list
    """
    cursor = request_cursor("https://iss.moex.com/iss/sitenews.json", "sitenews")
    results = run_async(
        fetch_sitenews_batched,
        base_url="https://iss.moex.com/iss/sitenews",
        endpoint=".json",
        n=cursor["total"],
        batch_size=cursor["step"],
        n_semaphore=n_semaphore,
    )
    sitenews_df = pd.DataFrame(preprocess_site_news_data(results))
    if not load_content_df:
        return sitenews_df, None
    sitenews_ids = sitenews_df["id"].unique().tolist()
    body_raw_data = run_async(
        fetch_sitenews_body,
        news_ids=sitenews_ids,
        n_semaphore=n_semaphore,
    )
    body_df = pd.DataFrame(preprocess_site_news_data(body_raw_data, "content"))
    return sitenews_df, body_df


if __name__ == "__main__":
    import time
    from isswrapper.util.helpers import save_dataframe_to_parquet

    st_time = time.time()
    final_df = run_async(load_all_sitenews)
    print(final_df.head())
    save_dataframe_to_parquet(final_df, "example.parquet")
    print("____________________")
    print(("--- %s seconds ---" % (time.time() - st_time)))
