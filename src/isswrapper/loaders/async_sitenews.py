import asyncio
import httpx
import numpy as np
from isswrapper.util.helpers import preprocess_raw_json, request_cursor
from isswrapper.util.async_helpers import fetch_all
import nest_asyncio
import pandas as pd

nest_asyncio.apply()


async def fetch_multiple_urls(
    base_url, endpoint_format: str, url_variables: list, max_connections: int = 10
):
    """
    Old/ Outdated


    :return: _description_
    :rtype: _type_
    """
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
    f_url = "https://iss.moex.com/iss/sitenews.json?start={0}&lang=ru"
    cursor = request_cursor("https://iss.moex.com/iss/sitenews.json", "sitenews")

    if limit < 0:
        limit = cursor["total"]
    urls = [f_url.format(x) for x in np.arange(0, limit, cursor["step"])]
    responses = await fetch_all(urls, max_connections=max_connections)
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
    f_url = "https://iss.moex.com/iss/sitenews/{0}.json"
    urls = [f_url.format(n_id) for n_id in news_ids]
    responses = await fetch_all(urls=urls, max_connections=max_connections)
    return responses


async def load_all_sitenews(max_connections: int = 10):
    """
    Asynchronously load the news and all its content

    :param max_connections: Maximum number of simultaneous connections, defaults to 10
    :type max_connections: int, optional
    :return: DataFrame with all news and its content
    :rtype: pd.DataFrame
    """
    print("Loading site news...")

    # Load site news metadata
    sitenews_list = await load_sitenews_range(
        limit=-1,
        max_connections=max_connections,
    )

    # Preprocess site news data
    sitenews_list_df = pd.DataFrame(
        preprocess_raw_json([page.json() for page in sitenews_list])
    )

    # Extract news IDs for loading bodies
    news_ids = sitenews_list_df["id"].unique().tolist()

    print("Loaded site news. Now loading news bodies...")

    # Load site news bodies
    sitenews_bodies_list = await load_sitenews_body(news_ids)

    print("Loaded news bodies. Now combining data...")

    # Preprocess site news bodies data
    news_body_df = pd.DataFrame(
        preprocess_raw_json(
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


if __name__ == "__main__":
    import time
    from isswrapper.util.helpers import save_dataframe_to_parquet

    st_time = time.time()
    final_df = run_async(load_all_sitenews)
    print(final_df.head())
    save_dataframe_to_parquet(final_df, "example.parquet")
    print("____________________")
    print(("--- %s seconds ---" % (time.time() - st_time)))
